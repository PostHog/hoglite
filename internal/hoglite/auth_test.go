package hoglite

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const authTestBackend = "7c9e2a14-3f51-4b8d-9e60-2d5a1f8c44aa"

type authFixture struct {
	key   *ecdsa.PrivateKey
	auth  *JWTAuth
	now   time.Time
	clock atomic.Int64
	file  string
}

func authJWK(t *testing.T, kid string, key *ecdsa.PrivateKey) map[string]any {
	t.Helper()
	return map[string]any{
		"kid": kid, "kty": "EC", "crv": "P-256", "alg": "ES256", "use": "sig",
		"x": base64.RawURLEncoding.EncodeToString(key.X.FillBytes(make([]byte, 32))),
		"y": base64.RawURLEncoding.EncodeToString(key.Y.FillBytes(make([]byte, 32))),
	}
}

func writeAuthJWKS(t *testing.T, file string, keys ...map[string]any) {
	t.Helper()
	b, err := json.Marshal(map[string]any{"keys": keys})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, b, 0600); err != nil {
		t.Fatal(err)
	}
}

func newAuthFixture(t *testing.T) *authFixture {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "public.json")
	writeAuthJWKS(t, file, authJWK(t, "k1", key))
	a, err := NewJWTAuth(context.Background(), AuthConfig{
		Mode: "enforce", Issuer: "https://issuer.test", BackendID: authTestBackend, JWKSFile: file,
	})
	if err != nil {
		t.Fatal(err)
	}
	f := &authFixture{key: key, auth: a, now: time.Now().Truncate(time.Second), file: file}
	f.clock.Store(f.now.UnixNano())
	a.now = func() time.Time { return time.Unix(0, f.clock.Load()) }
	a.fetched, a.tried = f.now, f.now
	t.Cleanup(func() { awaitAuthRefresh(t, a) })
	return f
}

func (f *authFixture) advance(d time.Duration) {
	f.now = f.now.Add(d)
	f.clock.Store(f.now.UnixNano())
}

func awaitAuthRefresh(t *testing.T, a *JWTAuth) {
	t.Helper()
	a.mu.Lock()
	done := a.refreshing
	a.mu.Unlock()
	if done == nil {
		return
	}
	select {
	case <-done:
	case <-time.After(6 * time.Second):
		t.Fatal("JWKS refresh did not finish")
	}
}

func (f *authFixture) claims() jwt.MapClaims {
	return jwt.MapClaims{
		"iss": "https://issuer.test", "aud": []string{"hoglake", "hoglite"},
		"sub": "test-client", "jti": "test-jti", "ten": "tenant", "org": "org-not-the-tid",
		"cat": map[string][]string{"catalog-not-the-tid": {"write"}},
		"rtg": map[string]any{"b": "hoglite", "be": authTestBackend, "t": "opaque_1"},
		"exp": f.now.Add(15 * time.Minute).Unix(), "iat": f.now.Unix(), "nbf": f.now.Unix(),
	}
}

func signAuth(t *testing.T, key any, method jwt.SigningMethod, kid string, claims jwt.MapClaims) string {
	t.Helper()
	token := jwt.NewWithClaims(method, claims)
	token.Header["kid"] = kid
	raw, err := token.SignedString(key)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func (f *authFixture) token(t *testing.T, mutate func(jwt.MapClaims)) string {
	t.Helper()
	c := f.claims()
	if mutate != nil {
		mutate(c)
	}
	return signAuth(t, f.key, jwt.SigningMethodES256, "k1", c)
}

func TestJWTAuthCryptoAndClaims(t *testing.T) {
	f := newAuthFixture(t)
	if a, err := f.auth.verify(context.Background(), f.token(t, nil)); err != nil || !a.write || a.tenant != "opaque_1" {
		t.Fatalf("valid authority: %+v %v", a, err)
	}
	reversed := f.token(t, func(c jwt.MapClaims) { c["aud"] = []string{"hoglite", "hoglake"} })
	if _, err := f.auth.verify(context.Background(), reversed); err != nil {
		t.Fatal("audience order changed authority")
	}
	tests := map[string]func(jwt.MapClaims){
		"issuer":             func(c jwt.MapClaims) { c["iss"] = "https://other.test" },
		"audience":           func(c jwt.MapClaims) { c["aud"] = "hoglake" },
		"duplicate audience": func(c jwt.MapClaims) { c["aud"] = []string{"hoglite", "hoglite"} },
		"extra audience":     func(c jwt.MapClaims) { c["aud"] = []string{"hoglake", "hoglite", "other"} },
		"vend":               func(c jwt.MapClaims) { c["aud"] = []string{"hoglake", "hoglite", "hogtower-vend"} },
		"missing exp":        func(c jwt.MapClaims) { delete(c, "exp") },
		"expired":            func(c jwt.MapClaims) { c["exp"] = f.now.Add(-61 * time.Second).Unix() },
		"future nbf":         func(c jwt.MapClaims) { c["nbf"] = f.now.Add(61 * time.Second).Unix() },
		"future iat":         func(c jwt.MapClaims) { c["iat"] = f.now.Add(61 * time.Second).Unix() },
		"bad time":           func(c jwt.MapClaims) { c["nbf"] = "0" },
		"float time":         func(c jwt.MapClaims) { c["exp"] = 123.5 },
		"null time":          func(c jwt.MapClaims) { c["iat"] = nil },
		"subject":            func(c jwt.MapClaims) { delete(c, "sub") },
		"jti":                func(c jwt.MapClaims) { delete(c, "jti") },
		"backend":            func(c jwt.MapClaims) { c["rtg"].(map[string]any)["be"] = "other" },
		"type":               func(c jwt.MapClaims) { c["rtg"].(map[string]any)["b"] = "postgres" },
		"no routing":         func(c jwt.MapClaims) { delete(c, "rtg") },
		"routing URL":        func(c jwt.MapClaims) { c["rtg"].(map[string]any)["url"] = "http://attacker.test" },
		"invalid tid":        func(c jwt.MapClaims) { c["rtg"].(map[string]any)["t"] = "../escape" },
		"two cats": func(c jwt.MapClaims) {
			c["cat"] = map[string][]string{"a": {"write"}, "b": {"write"}}
		},
		"no scope":          func(c jwt.MapClaims) { c["cat"] = map[string][]string{"a": {}} },
		"unknown scope":     func(c jwt.MapClaims) { c["cat"] = map[string][]string{"a": {"unknown"}} },
		"predicate":         func(c jwt.MapClaims) { c["prd"] = map[string]any{} },
		"scope claim":       func(c jwt.MapClaims) { c["scope"] = "hoglake:a:read" },
		"scoped":            func(c jwt.MapClaims) { c["ten"] = "scoped" },
		"operator nonadmin": func(c jwt.MapClaims) { c["ten"] = "operator" },
		"usr":               func(c jwt.MapClaims) { c["usr"] = "user" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := f.auth.verify(context.Background(), f.token(t, mutate)); err == nil {
				t.Fatal("accepted invalid claims")
			}
		})
	}
	valid := f.token(t, nil)
	parts := strings.Split(valid, ".")
	payload, _ := base64.RawURLEncoding.DecodeString(parts[1])
	tampered := strings.Replace(string(payload), "opaque_1", "opaque_2", 1)
	duplicate := string(payload[:len(payload)-1]) + `,"iss":"https://issuer.test"}`
	input := parts[0] + "." + base64.RawURLEncoding.EncodeToString([]byte(duplicate))
	sig, err := jwt.SigningMethodES256.Sign(input, f.key)
	if err != nil {
		t.Fatal(err)
	}
	critical := jwt.NewWithClaims(jwt.SigningMethodES256, f.claims())
	critical.Header["kid"], critical.Header["crit"] = "k1", []string{"unimplemented"}
	criticalRaw, err := critical.SignedString(f.key)
	if err != nil {
		t.Fatal(err)
	}
	for name, raw := range map[string]string{
		"tampered":         parts[0] + "." + base64.RawURLEncoding.EncodeToString([]byte(tampered)) + "." + parts[2],
		"unknown kid":      signAuth(t, f.key, jwt.SigningMethodES256, "missing", f.claims()),
		"HMAC":             signAuth(t, []byte("not-a-key"), jwt.SigningMethodHS256, "k1", f.claims()),
		"none":             signAuth(t, jwt.UnsafeAllowNoneSignatureType, jwt.SigningMethodNone, "k1", f.claims()),
		"malformed":        "one.two",
		"padding":          parts[0] + "=." + parts[1] + "." + parts[2],
		"oversize":         strings.Repeat("x", maxTokenSize+1),
		"signed duplicate": input + "." + base64.RawURLEncoding.EncodeToString(sig),
		"critical header":  criticalRaw,
		"duplicate": parts[0] + "." + base64.RawURLEncoding.EncodeToString(
			[]byte(`{"iss":"x","iss":"y"}`)) + "." + parts[2],
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := f.auth.verify(context.Background(), raw); err == nil {
				t.Fatal("accepted invalid token")
			}
		})
	}
}

func TestJWTAuthHoglakeOwnedMetadataVectors(t *testing.T) {
	b, err := os.ReadFile("testdata/hoglake-metadata-vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var file struct {
		Issuer   string          `json:"issuer"`
		VerifyAt int64           `json:"verify_at"`
		JWKS     json.RawMessage `json:"jwks"`
		Vectors  []struct {
			Name  string `json:"name"`
			Token string `json:"token"`
		} `json:"vectors"`
	}
	if err := json.Unmarshal(b, &file); err != nil {
		t.Fatal(err)
	}
	keys, err := parsePublicJWKS(file.JWKS)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(file.VerifyAt, 0)
	a := &JWTAuth{
		issuer: file.Issuer, backend: authTestBackend, keys: keys,
		now: func() time.Time { return now }, fetched: now, tried: now,
	}
	if len(file.Vectors) != 4 {
		t.Fatal("expected all four focused Hoglake-owned metadata vectors")
	}
	for _, v := range file.Vectors {
		t.Run(v.Name, func(t *testing.T) {
			auth, err := a.verify(context.Background(), v.Token)
			if v.Name == "scoped_routed" {
				if err == nil {
					t.Fatal("raw SQL widened a canonical constrained grant")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if v.Name == "operator_provisioner" {
				m := NewManager(NewFakeStore(), "fake", "fleet", "standard", t.TempDir(), testOpts(), 1000, &Metrics{}, ControllerOpts{})
				t.Cleanup(m.ShutdownAll)
				h := NewServer(m, nil).WithAuth(a).Handler()
				for _, route := range []struct {
					method, path string
					status       int
				}{
					{"GET", "/metrics", 200}, {"GET", "/v1/info", 200}, {"POST", "/v1/shutdown", 202},
				} {
					if got := authRequest(t, h, route.method, route.path, "", "").Code; got != 401 {
						t.Fatal("fleet endpoint became public")
					}
					if got := authRequest(t, h, route.method, route.path, v.Token, "").Code; got != route.status {
						t.Fatalf("issuer-compatible fleet operator refused: %s = %d", route.path, got)
					}
				}
				if got := authRequest(t, h, "POST", "/v1/t/opaque_1/query", v.Token, `{}`).Code; got != 403 {
					t.Fatal("fleet-only operator gained tenant data authority")
				}
				return
			}
			if auth.tenant != "t_3fa8c1d9e2_1" || auth.operator != (v.Name == "operator_routed") ||
				auth.write != (v.Name == "operator_routed") {
				t.Fatal("canonical grant changed authority")
			}
		})
	}
}

func TestJWTAuthRSAAndKeyTypes(t *testing.T) {
	f := newAuthFixture(t)
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	jwk := map[string]any{
		"kty": "RSA", "kid": "rsa", "alg": "RS256", "use": "sig",
		"n": base64.RawURLEncoding.EncodeToString(key.N.Bytes()),
		"e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes()),
	}
	writeAuthJWKS(t, f.file, jwk)
	f.advance(time.Minute)
	raw := signAuth(t, key, jwt.SigningMethodRS256, "rsa", f.claims())
	_, _ = f.auth.verify(context.Background(), raw)
	awaitAuthRefresh(t, f.auth)
	if _, err := f.auth.verify(context.Background(), raw); err != nil {
		t.Fatal(err)
	}
	if _, err := f.auth.verify(context.Background(), signAuth(t, f.key, jwt.SigningMethodES256, "rsa", f.claims())); err == nil {
		t.Fatal("accepted mismatched key type")
	}
	jwk["n"] = base64.RawURLEncoding.EncodeToString([]byte{17})
	shortJWKS, _ := json.Marshal(map[string]any{"keys": []any{jwk}})
	if _, err := parsePublicJWKS(shortJWKS); err == nil {
		t.Fatal("accepted short RSA key")
	}
}

func TestJWTAuthConfigAndJWKS(t *testing.T) {
	f := newAuthFixture(t)
	good := AuthConfig{Mode: "enforce", Issuer: "https://issuer.test", BackendID: authTestBackend, JWKSFile: f.file}
	for name, mutate := range map[string]func(*AuthConfig){
		"mode":              func(c *AuthConfig) { c.Mode = "warn" },
		"issuer":            func(c *AuthConfig) { c.Issuer = "" },
		"URL":               func(c *AuthConfig) { c.Issuer = "relative" },
		"backend":           func(c *AuthConfig) { c.BackendID = "" },
		"canonical backend": func(c *AuthConfig) { c.BackendID = strings.ToUpper(authTestBackend) },
		"no keys":           func(c *AuthConfig) { c.JWKSFile = "" },
		"two sources": func(c *AuthConfig) {
			c.JWKSURL = "https://keys.test"
		},
		"unreadable": func(c *AuthConfig) { c.JWKSFile += "-missing" },
		"bad scheme": func(c *AuthConfig) { c.JWKSFile, c.JWKSURL = "", "file:///tmp/key" },
	} {
		t.Run(name, func(t *testing.T) {
			c := good
			mutate(&c)
			if _, err := NewJWTAuth(context.Background(), c); err == nil {
				t.Fatal("accepted invalid enforcement config")
			}
		})
	}
	for _, mode := range []string{"", "off"} {
		if a, err := NewJWTAuth(context.Background(), AuthConfig{Mode: mode}); err != nil || a != nil {
			t.Fatal("off mode requires configuration")
		}
	}
	for name, mutate := range map[string]func(map[string]any){
		"private":   func(j map[string]any) { j["d"] = "private-never-accepted" },
		"use":       func(j map[string]any) { j["use"] = "enc" },
		"key ops":   func(j map[string]any) { j["key_ops"] = []string{"sign"} },
		"alg":       func(j map[string]any) { j["alg"] = "HS256" },
		"alg type":  func(j map[string]any) { j["alg"] = 1 },
		"curve":     func(j map[string]any) { j["crv"] = "P-384" },
		"point":     func(j map[string]any) { j["x"] = strings.Repeat("A", 43) },
		"empty kid": func(j map[string]any) { j["kid"] = "" },
		"symmetric": func(j map[string]any) { j["kty"] = "oct" },
	} {
		t.Run(name, func(t *testing.T) {
			j := authJWK(t, "k1", f.key)
			mutate(j)
			writeAuthJWKS(t, f.file, j)
			if _, err := NewJWTAuth(context.Background(), good); err == nil {
				t.Fatal("accepted unusable or private JWKS")
			}
		})
	}
	for _, keys := range [][]map[string]any{nil, {authJWK(t, "k1", f.key), authJWK(t, "k1", f.key)}} {
		writeAuthJWKS(t, f.file, keys...)
		if _, err := NewJWTAuth(context.Background(), good); err == nil {
			t.Fatal("accepted empty or duplicate JWKS")
		}
	}
}

func TestJWTAuthMixedPublicJWKS(t *testing.T) {
	f := newAuthFixture(t)
	usable := authJWK(t, "k1", f.key)
	writeAuthJWKS(t, f.file, usable,
		map[string]any{"kty": "RSA", "use": "enc"},
		map[string]any{"kty": "EC", "use": "sig", "alg": "ES384"},
	)
	a, err := NewJWTAuth(context.Background(), AuthConfig{
		Mode: "enforce", Issuer: "https://issuer.test", BackendID: authTestBackend, JWKSFile: f.file,
	})
	if err != nil {
		t.Fatal("shared public JWKS rejected unrelated entries", err)
	}
	if _, err := a.verify(context.Background(), f.token(t, nil)); err != nil {
		t.Fatal(err)
	}
	usable["alg"] = "RS256"
	writeAuthJWKS(t, f.file, usable)
	if _, err := NewJWTAuth(context.Background(), AuthConfig{
		Mode: "enforce", Issuer: "https://issuer.test", BackendID: authTestBackend, JWKSFile: f.file,
	}); err == nil {
		t.Fatal("EC key labelled RS256 was accepted")
	}
}

func TestJWTAuthRefreshNeverBlocksCachedVerification(t *testing.T) {
	f := newAuthFixture(t)
	data, _ := os.ReadFile(f.file)
	var calls atomic.Int64
	entered, release := make(chan struct{}), make(chan struct{})
	var unblock sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) > 1 {
			close(entered)
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
		}
		w.Write(data)
	}))
	defer server.Close()
	defer unblock.Do(func() { close(release) })
	a, err := NewJWTAuth(context.Background(), AuthConfig{
		Mode: "enforce", Issuer: "https://issuer.test", BackendID: authTestBackend, JWKSURL: server.URL,
	})
	if err != nil {
		t.Fatal(err)
	}
	a.now = f.auth.now
	a.fetched, a.tried = f.now, f.now
	f.advance(time.Minute)
	known := f.token(t, nil)
	unknown := signAuth(t, f.key, jwt.SigningMethodES256, "unknown", f.claims())
	var wg sync.WaitGroup
	misses := make(chan error, 40)
	for range 40 {
		wg.Go(func() {
			_, err := a.verify(context.Background(), unknown)
			misses <- err
		})
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("background refresh did not start")
	}
	valid := make(chan error, 1)
	go func() { _, err := a.verify(context.Background(), known); valid <- err }()
	select {
	case err := <-valid:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("cached verification blocked behind the JWKS fetch")
	}
	for range 40 {
		select {
		case err := <-misses:
			if err == nil {
				t.Fatal("unknown key accepted")
			}
		case <-time.After(250 * time.Millisecond):
			t.Fatal("unknown-key verification waited for a network response")
		}
	}
	wg.Wait()
	if calls.Load() != 2 {
		t.Fatalf("concurrent misses triggered %d refreshes", calls.Load()-1)
	}
	unblock.Do(func() { close(release) })
	awaitAuthRefresh(t, a)
}

func TestJWTAuthRotationAndBoundedRefresh(t *testing.T) {
	f := newAuthFixture(t)
	var fetches atomic.Int64
	fetch := f.auth.fetch
	f.auth.fetch = func(ctx context.Context) ([]byte, error) { fetches.Add(1); return fetch(ctx) }
	key2, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	next := signAuth(t, key2, jwt.SigningMethodES256, "k2", f.claims())
	old := f.token(t, nil)
	writeAuthJWKS(t, f.file, authJWK(t, "k1", f.key), authJWK(t, "k2", key2))
	if _, err := f.auth.verify(context.Background(), next); err == nil {
		t.Fatal("refresh was not rate limited after boot")
	}
	awaitAuthRefresh(t, f.auth)
	if fetches.Load() != 0 {
		t.Fatal("unknown key bypassed the refresh interval")
	}
	f.advance(jwksMinRetry)
	if _, err := f.auth.verify(context.Background(), next); err == nil {
		t.Fatal("unknown key was not refused during asynchronous rotation")
	}
	awaitAuthRefresh(t, f.auth)
	if _, err := f.auth.verify(context.Background(), next); err != nil {
		t.Fatal(err)
	}
	if _, err := f.auth.verify(context.Background(), old); err != nil {
		t.Fatal("rotation invalidated an unexpired token", err)
	}
	fetches.Store(0)
	f.advance(jwksMinRetry)
	var wg sync.WaitGroup
	for i := range 40 {
		raw := signAuth(t, f.key, jwt.SigningMethodES256, fmt.Sprintf("unknown-%d", i), f.claims())
		wg.Go(func() { _, _ = f.auth.verify(context.Background(), raw) })
	}
	wg.Wait()
	awaitAuthRefresh(t, f.auth)
	if fetches.Load() != 1 {
		t.Fatalf("unknown-kid fetches = %d", fetches.Load())
	}
	// Failed refresh retains old public keys, but never overrides expiry.
	os.WriteFile(f.file, []byte(`{"keys":[]}`), 0600)
	f.advance(time.Minute)
	if _, err := f.auth.verify(context.Background(), old); err != nil {
		t.Fatal("failed refresh lost cached key", err)
	}
	awaitAuthRefresh(t, f.auth)
	f.advance(15 * time.Minute)
	if _, err := f.auth.verify(context.Background(), old); err == nil {
		t.Fatal("cached key bypassed expiry")
	}
}

func TestJWTAuthURLSource(t *testing.T) {
	f := newAuthFixture(t)
	data, _ := os.ReadFile(f.file)
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Write(data)
	}))
	defer server.Close()
	a, err := NewJWTAuth(context.Background(), AuthConfig{
		Mode: "enforce", Issuer: "https://issuer.test", BackendID: authTestBackend, JWKSURL: server.URL,
	})
	if err != nil {
		t.Fatal(err)
	}
	for range 10 {
		if _, err := a.verify(context.Background(), f.token(t, nil)); err != nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 1 {
		t.Fatal("verification used a network RPC")
	}
	untrustedCalls := atomic.Int64{}
	untrusted := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		untrustedCalls.Add(1)
		w.Write(data)
	}))
	defer untrusted.Close()
	token := jwt.NewWithClaims(jwt.SigningMethodES256, f.claims())
	token.Header["kid"], token.Header["jku"], token.Header["x5u"] = "k1", untrusted.URL, untrusted.URL
	raw, err := token.SignedString(f.key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.verify(context.Background(), raw); err != nil {
		t.Fatal(err)
	}
	if untrustedCalls.Load() != 0 {
		t.Fatal("token selected its own key URL")
	}
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) }))
	defer bad.Close()
	if _, err := NewJWTAuth(context.Background(), AuthConfig{
		Mode: "enforce", Issuer: "https://issuer.test", BackendID: authTestBackend, JWKSURL: bad.URL,
	}); err == nil {
		t.Fatal("boot accepted unavailable JWKS")
	}
}

func TestJWTAuthForwardBatchPreservesAuthority(t *testing.T) {
	f := newAuthFixture(t)
	raw := f.token(t, nil)
	var observed atomic.Bool
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		observed.Store(r.Header.Get("Authorization") == "Bearer "+raw && r.Header.Get("x-hw-noproxy") == "1")
		w.Write([]byte(`{"results":[{"status":200,"body":{"cols":["x"],"rows":[[1]],"affected":0}}]}`))
	}))
	defer peer.Close()
	r := httptest.NewRequest("POST", "/v1/query-batch", nil)
	r = r.WithContext(context.WithValue(r.Context(), authorityKey{}, &requestAuthority{token: raw, tenant: "opaque_1"}))
	s := NewServer(&Manager{metrics: &Metrics{}}, nil)
	got, ok := s.forwardBatch(r, peer.URL, []BatchQuery{{Tenant: "opaque_1", SQL: "SELECT 1"}}, []int{0})
	if !ok || len(got) != 1 || got[0].Status != 200 || !observed.Load() {
		t.Fatal("peer sub-batch did not preserve verified authority")
	}
}

func TestJWTAuthOwnerConfirmationPreservesAuthority(t *testing.T) {
	f := newAuthFixture(t)
	raw := f.token(t, func(c jwt.MapClaims) {
		c["ten"], c["cat"] = "operator", map[string][]string{"catalog": {"admin"}}
	})
	var observed atomic.Bool
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		observed.Store(r.Header.Get("Authorization") == "Bearer "+raw && r.Header.Get("x-hw-noproxy") == "1")
		w.WriteHeader(200)
	}))
	defer peer.Close()
	r := httptest.NewRequest("PUT", "/v1/tenants/opaque_1", nil)
	r = r.WithContext(context.WithValue(r.Context(), authorityKey{}, &requestAuthority{token: raw, tenant: "opaque_1"}))
	s := NewServer(nil, map[string]string{"peer": peer.URL})
	if s.confirmOwner(r, "opaque_1", &Manifest{Owner: "peer"}, nil) != ownerConfirmed || !observed.Load() {
		t.Fatal("owner confirmation did not preserve verified authority")
	}
}

func authRequest(t *testing.T, handler http.Handler, method, path, token, body string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	return w
}

func TestJWTAuthRefusesBeforeStorage(t *testing.T) {
	f := newAuthFixture(t)
	// A nil manager deliberately panics if a refused request reaches storage.
	h := NewServer(nil, nil).WithAuth(f.auth).Handler()
	token := f.token(t, nil)
	for _, route := range []struct{ method, path string }{
		{"PUT", "/v1/tenants/opaque_1"}, {"DELETE", "/v1/tenants/opaque_1"},
		{"GET", "/v1/t/opaque_1/status"}, {"POST", "/v1/t/opaque_1/release"},
		{"GET", "/metrics"}, {"GET", "/v1/info"}, {"POST", "/v1/shutdown"}, {"GET", "/v1/shutdown"},
		{"POST", "/v1/t/other/query"}, {"POST", "/v1/t/other/tx"},
	} {
		if got := authRequest(t, h, route.method, route.path, token, `{}`).Code; got != 403 {
			t.Errorf("%s %s = %d", route.method, route.path, got)
		}
		if got := authRequest(t, h, route.method, route.path, "", `{}`).Code; got != 401 {
			t.Errorf("anonymous %s = %d", route.path, got)
		}
	}
	body := `{"queries":[{"tenant":"opaque_1","sql":"SELECT 1"},{"tenant":"other","sql":"SELECT 1"}]}`
	if got := authRequest(t, h, "POST", "/v1/query-batch", token, body).Code; got != 403 {
		t.Fatalf("mixed batch = %d", got)
	}
	read := f.token(t, func(c jwt.MapClaims) { c["cat"] = map[string][]string{"catalog": {"read"}} })
	if got := authRequest(t, h, "POST", "/v1/t/opaque_1/tx", read,
		`{"client":"c","txid":"x","statements":[{"sql":"CREATE TABLE x(id)"}]}`).Code; got != 403 {
		t.Fatalf("read token write = %d", got)
	}
	scoped := f.token(t, func(c jwt.MapClaims) { c["prd"] = map[string]any{"team_id": map[string]string{"42": "read"}} })
	for _, path := range []string{"/v1/t/opaque_1/query", "/v1/t/opaque_1/tx", "/v1/query-batch"} {
		if got := authRequest(t, h, "POST", path, scoped, body).Code; got != 403 {
			t.Fatalf("constrained authority %s = %d", path, got)
		}
	}
	op := f.token(t, func(c jwt.MapClaims) {
		c["ten"], c["cat"] = "operator", map[string][]string{"catalog": {"admin"}}
	})
	if got := authRequest(t, h, "DELETE", "/v1/tenants/other", op, "").Code; got != 403 {
		t.Fatal("operator escaped catalog binding")
	}
	if got := authRequest(t, h, "POST", "/v1/shutdown", op, "").Code; got != 403 {
		t.Fatal("catalog operator gained fleet authority")
	}
}

func TestJWTAuthRoutesReadOnlyAndProxy(t *testing.T) {
	f := newAuthFixture(t)
	fs := NewFakeStore()
	mgrs := map[string]*Manager{}
	peers := map[string]string{}
	servers := map[string]*httptest.Server{}
	handlers := map[string]*Server{}
	for _, id := range []string{"a", "b"} {
		m := NewManager(fs, "fake", id, "standard", t.TempDir(), testOpts(), 1000, &Metrics{}, ControllerOpts{})
		mgrs[id] = m
		handler := NewServer(m, peers).WithAuth(f.auth)
		handlers[id] = handler
		s := httptest.NewServer(handler.Handler())
		servers[id], peers[id] = s, s.URL
		t.Cleanup(m.ShutdownAll)
		t.Cleanup(s.Close)
	}
	do := func(replica, method, path, token, body string) (int, []byte) {
		t.Helper()
		r, _ := http.NewRequest(method, servers[replica].URL+path, strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+token)
		resp, err := http.DefaultClient.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, b
	}
	operator := f.token(t, func(c jwt.MapClaims) {
		c["ten"], c["cat"] = "operator", map[string][]string{"catalog": {"admin"}}
	})
	write := f.token(t, nil)
	read := f.token(t, func(c jwt.MapClaims) { c["cat"] = map[string][]string{"catalog": {"read"}} })
	check := func(replica, method, path, token, body string, want int) []byte {
		t.Helper()
		status, b := do(replica, method, path, token, body)
		if status != want {
			t.Fatalf("%s %s via %s = %d %s, want %d", method, path, replica, status, b, want)
		}
		return b
	}
	check("a", "PUT", "/v1/tenants/opaque_1", operator, `{}`, 200)
	// Idempotent create's owner confirmation must carry the operator token.
	check("b", "PUT", "/v1/tenants/opaque_1", operator, `{}`, 200)
	check("b", "POST", "/v1/t/opaque_1/tx", write,
		`{"client":"c","txid":"seed","statements":[{"sql":"CREATE TABLE kv(id PRIMARY KEY)"},{"sql":"INSERT INTO kv VALUES(1)"}]}`, 200)
	check("b", "POST", "/v1/t/opaque_1/query", read, `{"sql":"SELECT * FROM kv"}`, 200)
	verified, err := f.auth.verify(context.Background(), read)
	if err != nil {
		t.Fatal(err)
	}
	parent := httptest.NewRequest("POST", "/v1/query-batch", nil)
	parent = parent.WithContext(context.WithValue(parent.Context(), authorityKey{}, verified))
	if got := handlers["b"].queryOne(parent, false, BatchQuery{Tenant: "opaque_1", SQL: "SELECT * FROM kv"}); got.Status != 200 {
		t.Fatal("in-process fallback lost authority on its separate owner-proxy hop")
	}
	check("b", "POST", "/v1/t/opaque_1/tx", read,
		`{"client":"c","txid":"read","statements":[{"sql":"SELECT * FROM kv"}]}`, 200)
	// Prove engine-level enforcement independently of the textual SQL boundary.
	// This is the actual reader connection used by authenticated queries and read transactions.
	if _, err := mgrs["a"].LocalTenant("opaque_1").roDB.Exec("INSERT INTO kv VALUES(9)"); err == nil {
		t.Fatal("SQLite reader connection was writable")
	}
	// The classifier accepts this prefix.
	// The statement boundary must reject smuggling.
	attack := "SELECT 1; INSERT INTO kv VALUES(2)"
	if !allReadOnly([]Statement{{SQL: attack}}) {
		t.Fatal("test no longer reaches the SQLite enforcement layer")
	}
	status, _ := do("b", "POST", "/v1/t/opaque_1/tx", read,
		fmt.Sprintf(`{"client":"c","txid":"attack","statements":[{"sql":%q}]}`, attack))
	if status == 200 {
		t.Fatal("query_only accepted a multi-statement write")
	}
	check("b", "POST", "/v1/t/opaque_1/query", write, `{"sql":"INSERT INTO kv VALUES(3)"}`, 409)
	batch := check("b", "POST", "/v1/query-batch", read,
		`{"queries":[{"tenant":"opaque_1","sql":"SELECT COUNT(*) FROM kv"},{"tenant":"opaque_1","sql":"SELECT 1"}]}`, 200)
	if !bytes.Contains(batch, []byte(`"status":200`)) || !bytes.Contains(batch, []byte(`"rows":[[1]]`)) {
		t.Fatalf("batch forwarding lost read authority or data changed: %s", batch)
	}
	if mgrs["b"].metrics.Proxied.Load() < 4 {
		t.Fatal("proxy paths were not exercised")
	}
	check("b", "POST", "/v1/t/opaque_1/release", operator, "", 200)
	check("b", "DELETE", "/v1/tenants/opaque_1", operator, "", 200)
}

func TestJWTAuthClockToleranceMatchesHoglake(t *testing.T) {
	f := newAuthFixture(t)
	for _, claim := range []string{"iat", "nbf", "exp"} {
		for _, seconds := range []int{59, 60, 61} {
			t.Run(fmt.Sprintf("%s-%d", claim, seconds), func(t *testing.T) {
				delta := time.Duration(seconds) * time.Second
				if claim == "exp" {
					delta = -delta
				}
				raw := f.token(t, func(c jwt.MapClaims) { c[claim] = f.now.Add(delta).Unix() })
				_, err := f.auth.verify(context.Background(), raw)
				if (err == nil) != (seconds <= 60) {
					t.Fatalf("clock tolerance mismatch at %s boundary %d", claim, seconds)
				}
			})
		}
	}
}

func TestJWTAuthUnsupportedAuthorityIsForbidden(t *testing.T) {
	f := newAuthFixture(t)
	h := NewServer(nil, nil).WithAuth(f.auth).Handler()
	for name, mutate := range map[string]func(jwt.MapClaims){
		"scoped":       func(c jwt.MapClaims) { c["ten"] = "scoped" },
		"predicate":    func(c jwt.MapClaims) { c["prd"] = map[string]any{} },
		"scope":        func(c jwt.MapClaims) { c["scope"] = "hoglake:a:read" },
		"unknown-only": func(c jwt.MapClaims) { c["cat"] = map[string][]string{"a": {"future"}} },
	} {
		t.Run(name, func(t *testing.T) {
			for _, path := range []string{"/v1/t/opaque_1/query", "/v1/t/opaque_1/tx", "/v1/query-batch"} {
				r := authRequest(t, h, "POST", path, f.token(t, mutate), `{}`)
				if r.Code != 403 || r.Header().Get("WWW-Authenticate") != "" {
					t.Fatal("unsupported authority invited credential refresh")
				}
			}
		})
	}
	if got := authRequest(t, h, "POST", "/v1/t/opaque_1/query", f.token(t, func(c jwt.MapClaims) { c["exp"] = f.now.Add(-61 * time.Second).Unix() }), `{}`).Code; got != 401 {
		t.Fatal("expired credential was not unauthorized")
	}
}

func TestJWTAuthCanonicalScopeVocabulary(t *testing.T) {
	f := newAuthFixture(t)
	for _, grant := range []struct {
		scopes []string
		write  bool
	}{
		{[]string{"read", "future"}, false}, {[]string{"write", "future"}, true},
		{[]string{"maintenance"}, true}, {[]string{"admin"}, true},
	} {
		a, err := f.auth.verify(context.Background(), f.token(t, func(c jwt.MapClaims) { c["cat"] = map[string][]string{"a": grant.scopes} }))
		if err != nil || a.write != grant.write {
			t.Fatal("canonical grant vocabulary changed authority")
		}
	}
}

func TestJWTAuthCustomUpstreamAudience(t *testing.T) {
	f := newAuthFixture(t)
	cfg := AuthConfig{Mode: "enforce", Issuer: "https://issuer.test", BackendID: authTestBackend, JWKSFile: f.file, UpstreamAudience: "custom-metadata"}
	a, err := NewJWTAuth(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	a.now = f.auth.now
	raw := f.token(t, func(c jwt.MapClaims) { c["aud"] = []string{"hoglite", "custom-metadata"} })
	if _, err := a.verify(context.Background(), raw); err != nil {
		t.Fatal("configured upstream audience refused")
	}
	if _, err := f.auth.verify(context.Background(), raw); err == nil {
		t.Fatal("default profile accepted a foreign upstream audience")
	}
	for _, invalid := range []string{"hoglite", "hogtower-vend", " custom"} {
		cfg.UpstreamAudience = invalid
		if _, err := NewJWTAuth(context.Background(), cfg); err == nil {
			t.Fatal("unsafe upstream audience configuration accepted")
		}
	}
}

func TestJWTAuthReusesOnlyInProcessVerifiedAuthority(t *testing.T) {
	f := newAuthFixture(t)
	s := NewServer(nil, nil).WithAuth(f.auth)
	r := httptest.NewRequest("POST", "/v1/t/opaque_1/query", nil)
	r.Pattern = "POST /v1/t/{tid}/query"
	r.SetPathValue("tid", "opaque_1")
	r.Header.Set("Authorization", "Bearer "+f.token(t, nil))
	verified, status := s.authorize(r)
	if status != 0 {
		t.Fatal(status)
	}
	f.advance(17 * time.Minute)
	if _, status := s.authorize(verified); status != 0 {
		t.Fatal("same-request item reverified its signature")
	}
	external := verified.Clone(context.Background())
	if _, status := s.authorize(external); status != 401 {
		t.Fatal("separate HTTP hop reused stale authority")
	}
	verified.SetPathValue("tid", "other")
	if _, status := s.authorize(verified); status != 403 {
		t.Fatal("same-request authority skipped tenant authorization")
	}
}

func TestJWTAuthPeerRedirectCannotForwardBearer(t *testing.T) {
	f := newAuthFixture(t)
	var leaked atomic.Bool
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { leaked.Store(true); w.WriteHeader(200) }))
	defer destination.Close()
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, destination.URL, http.StatusTemporaryRedirect)
	}))
	defer peer.Close()
	s := NewServer(&Manager{metrics: &Metrics{}}, nil).WithAuth(f.auth)
	r := httptest.NewRequest("POST", "/v1/query-batch", nil)
	r = r.WithContext(context.WithValue(r.Context(), authorityKey{}, &requestAuthority{token: f.token(t, nil), tenant: "opaque_1"}))
	s.forwardBatch(r, peer.URL, []BatchQuery{{Tenant: "opaque_1", SQL: "SELECT 1"}}, []int{0})
	if leaked.Load() {
		t.Fatal("peer redirect leaked authenticated request")
	}
}

func TestJWTAuthWarningsAreBoundedAndRedacted(t *testing.T) {
	f := newAuthFixture(t)
	var output bytes.Buffer
	saved := log.Writer()
	log.SetOutput(&output)
	t.Cleanup(func() { log.SetOutput(saved) })
	if a, err := NewJWTAuth(context.Background(), AuthConfig{}); a != nil || err != nil {
		t.Fatal(err)
	}
	log.SetOutput(saved)
	if output.Len() != 0 {
		t.Fatal("empty legacy quick start warned")
	}
	log.SetOutput(&output)
	_, _ = NewJWTAuth(context.Background(), AuthConfig{Mode: "off", Issuer: "https://private-config.invalid"})
	log.SetOutput(saved)
	if !strings.Contains(output.String(), "authentication is off") || strings.Contains(output.String(), "private-config") {
		t.Fatal("partial configuration warning missing or exposed configuration")
	}
	output.Reset()
	log.SetOutput(&output)
	f.auth.fetch = func(context.Context) ([]byte, error) {
		return nil, errors.New("https://private-keys.invalid secret-token key-material")
	}
	f.advance(jwksTTL)
	for range 40 {
		if _, err := f.auth.verify(context.Background(), f.token(t, nil)); err != nil {
			t.Fatal("cached key failed during refresh outage")
		}
	}
	awaitAuthRefresh(t, f.auth)
	log.SetOutput(saved)
	text := output.String()
	if strings.Count(text, "key refresh failed") != 1 || strings.Contains(text, "private-keys") || strings.Contains(text, "secret-token") || strings.Contains(text, "key-material") {
		t.Fatal("refresh warning was unbounded or exposed private details")
	}
}
