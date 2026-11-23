package hoglite

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const (
	readSessionTestProfile = "hogtower.aws-session"
	readSessionTestBucket  = "example-owner"
	readSessionTestPrefix  = "root"
	readSessionTestRegion  = "us-east-1"
	readSessionTestTenant  = "fixture_3"
	readSessionTestCatalog = "example-catalog"
	readSessionTestBackend = "11111111-1111-4111-8111-111111111111"
)

var readSessionTestRemoteTargets = []string{"remote:events", "remote:imports"}

type readSessionAuthFixture struct {
	auth   *JWTAuth
	key    *ecdsa.PrivateKey
	rsaKey *rsa.PrivateKey
	kid    string
	rsaKid string
	file   string
	clock  atomic.Int64
}

func (f *readSessionAuthFixture) setNow(t time.Time) {
	f.clock.Store(t.UnixNano())
}

func (f *readSessionAuthFixture) now() time.Time {
	return time.Unix(0, f.clock.Load())
}

func newReadSessionAuthFixture(t *testing.T) *readSessionAuthFixture {
	t.Helper()
	ecKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	kid := "ec-read-k1"
	rsaKid := "rsa-read-k1"
	ecJWK := authJWK(t, kid, ecKey)
	rsaJWK := map[string]any{
		"kty": "RSA",
		"alg": "RS256",
		"use": "sig",
		"kid": rsaKid,
		"n":   base64.RawURLEncoding.EncodeToString(rsaKey.N.Bytes()),
		"e":   base64.RawURLEncoding.EncodeToString([]byte{1, 0, 1}),
	}
	jwksBytes, err := json.Marshal(map[string]any{"keys": []any{ecJWK, rsaJWK}})
	if err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "jwks.json")
	if err := os.WriteFile(file, jwksBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(2_000_000_001, 0)
	f := &readSessionAuthFixture{
		key: ecKey, rsaKey: rsaKey, kid: kid, rsaKid: rsaKid, file: file,
	}
	f.setNow(now)
	a, err := NewJWTAuth(context.Background(), AuthConfig{
		Mode:                           "enforce",
		Issuer:                         "https://issuer.test",
		BackendID:                      readSessionTestBackend,
		JWKSFile:                       file,
		ScopedNamespaceEnabled:         true,
		ScopedNamespacePredicateFields: []string{"team_id"},
		ReadSession: &ReadSessionConfig{
			Profile:       readSessionTestProfile,
			RemoteTargets: append([]string(nil), readSessionTestRemoteTargets...),
			Bucket:        readSessionTestBucket,
			Prefix:        readSessionTestPrefix,
			Region:        readSessionTestRegion,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	a.now = f.now
	a.fetched, a.tried = now, now
	f.auth = a
	t.Cleanup(func() { awaitAuthRefresh(t, a) })
	return f
}

func (f *readSessionAuthFixture) baseClaims() map[string]any {
	return map[string]any{
		"iss":              "https://issuer.test",
		"aud":              []string{"trino-session", "hoglake-session", "hoglite-session"},
		"sub":              "example.alice",
		"usr":              "example.alice",
		"jti":              "token-a",
		"profile":          readSessionTestProfile,
		"version":          1,
		"purpose":          "user-session",
		"ten":              "scoped",
		"org":              "example-org",
		"primary_catalog":  readSessionTestCatalog,
		"sid":              "session-a",
		"generation":       1,
		"session_deadline": int64(2_000_000_120),
		"iat":              int64(2_000_000_000),
		"nbf":              int64(2_000_000_000),
		"exp":              int64(2_000_000_060),
		"cat":              map[string][]string{readSessionTestCatalog: {"read"}},
		"prd":              map[string]map[string]string{"team_id": {"42": "read"}},
		"rtg": map[string]any{
			"b":  "hoglite",
			"be": readSessionTestBackend,
			"t":  readSessionTestTenant,
		},
		"remote_schemas": map[string][]string{"team": {"read"}},
		"aws": map[string]any{
			"access_key_id":     "ASIAEXAMPLEALICE0001",
			"secret_access_key": "secret-alice-material-only-for-test",
			"session_token":     "session-token-alice-only-for-test",
			"region":            readSessionTestRegion,
			"expires_at":        int64(2_000_000_060),
		},
		"grants": []map[string]any{
			{
				"catalog":    readSessionTestCatalog,
				"target":     readSessionTestCatalog,
				"bucket":     "example-customer",
				"object_key": "tables/data.parquet",
				"region":     readSessionTestRegion,
				"actions":    []string{"s3:GetObject"},
			},
			{
				"catalog":    readSessionTestCatalog,
				"target":     readSessionTestBackend,
				"bucket":     readSessionTestBucket,
				"object_key": manifestKey(readSessionTestPrefix, readSessionTestTenant),
				"region":     readSessionTestRegion,
				"actions":    []string{"s3:GetObject"},
			},
			{
				"catalog": readSessionTestCatalog,
				"target":  "remote:events",
				"bucket":  "example-remote",
				"prefix":  "events/team_id=42/",
				"region":  readSessionTestRegion,
				"actions": []string{"s3:GetObject"},
			},
			{
				"catalog": readSessionTestCatalog,
				"target":  "remote:imports",
				"bucket":  "example-remote",
				"prefix":  "imports/team_id=42/",
				"region":  readSessionTestRegion,
				"actions": []string{"s3:GetObject"},
			},
		},
	}
}

func (f *readSessionAuthFixture) signClaims(t *testing.T, claims map[string]any) string {
	t.Helper()
	hdrBytes, err := json.Marshal(map[string]any{"alg": "ES256", "kid": f.kid, "typ": "JWT"})
	if err != nil {
		t.Fatal(err)
	}
	payloadBytes, err := json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}
	input := base64.RawURLEncoding.EncodeToString(hdrBytes) + "." + base64.RawURLEncoding.EncodeToString(payloadBytes)
	sig, err := jwt.SigningMethodES256.Sign(input, f.key)
	if err != nil {
		t.Fatal(err)
	}
	return input + "." + base64.RawURLEncoding.EncodeToString(sig)
}

func (f *readSessionAuthFixture) token(t *testing.T, mutate func(map[string]any)) string {
	t.Helper()
	claims := f.baseClaims()
	if mutate != nil {
		mutate(claims)
	}
	return f.signClaims(t, claims)
}

func TestReadSessionConfigValidation(t *testing.T) {
	f := newReadSessionAuthFixture(t)
	validRS := func() *ReadSessionConfig {
		return &ReadSessionConfig{
			Profile:       readSessionTestProfile,
			RemoteTargets: []string{"remote:events", "remote:imports"},
			Bucket:        readSessionTestBucket,
			Prefix:        readSessionTestPrefix,
			Region:        readSessionTestRegion,
		}
	}
	base := func() AuthConfig {
		return AuthConfig{
			Mode:                           "enforce",
			Issuer:                         "https://issuer.test",
			BackendID:                      readSessionTestBackend,
			JWKSFile:                       f.file,
			ScopedNamespaceEnabled:         true,
			ScopedNamespacePredicateFields: []string{"team_id"},
			ReadSession:                    validRS(),
		}
	}

	// Default is off when ReadSession is nil.
	offCfg := base()
	offCfg.ReadSession = nil
	a, err := NewJWTAuth(context.Background(), offCfg)
	if err != nil || a.readSessionEnabled() {
		t.Fatal("read session verification does not default to off")
	}
	if _, err := a.verifyReadSession(context.Background(), f.token(t, nil), readSessionTestTenant, readSessionTestCatalog); !errors.Is(err, errUnsupportedAuthority) {
		t.Fatal("disabled read session verifier accepts authority")
	}

	badCases := map[string]func(*AuthConfig){
		"mode off":                func(c *AuthConfig) { c.Mode = "off" },
		"scoped disabled":         func(c *AuthConfig) { c.ScopedNamespaceEnabled = false },
		"jwks URL":                func(c *AuthConfig) { c.JWKSFile, c.JWKSURL = "", "https://keys.test/jwks" },
		"two predicate fields":    func(c *AuthConfig) { c.ScopedNamespacePredicateFields = []string{"team_id", "other"} },
		"wrong predicate field":   func(c *AuthConfig) { c.ScopedNamespacePredicateFields = []string{"account_id"} },
		"empty profile":           func(c *AuthConfig) { c.ReadSession.Profile = "" },
		"blank profile":           func(c *AuthConfig) { c.ReadSession.Profile = " " },
		"experimental profile":    func(c *AuthConfig) { c.ReadSession.Profile = "storage-session" },
		"one remote target":       func(c *AuthConfig) { c.ReadSession.RemoteTargets = []string{"remote:events"} },
		"three remote targets":    func(c *AuthConfig) { c.ReadSession.RemoteTargets = []string{"a", "b", "c"} },
		"duplicate remote target": func(c *AuthConfig) { c.ReadSession.RemoteTargets = []string{"remote:events", "remote:events"} },
		"blank remote target":     func(c *AuthConfig) { c.ReadSession.RemoteTargets = []string{"remote:events", " "} },
		"backend as target":       func(c *AuthConfig) { c.ReadSession.RemoteTargets = []string{"remote:events", readSessionTestBackend} },
		"empty bucket":            func(c *AuthConfig) { c.ReadSession.Bucket = "" },
		"uppercase bucket":        func(c *AuthConfig) { c.ReadSession.Bucket = "Example-Owner" },
		"dotdot bucket":           func(c *AuthConfig) { c.ReadSession.Bucket = "example..owner" },
		"empty prefix":            func(c *AuthConfig) { c.ReadSession.Prefix = "" },
		"slash prefix":            func(c *AuthConfig) { c.ReadSession.Prefix = "/" },
		"wildcard prefix":         func(c *AuthConfig) { c.ReadSession.Prefix = "root/*" },
		"dotdot prefix":           func(c *AuthConfig) { c.ReadSession.Prefix = "root/../other" },
		"double slash prefix":     func(c *AuthConfig) { c.ReadSession.Prefix = "root//sub" },
		"empty region":            func(c *AuthConfig) { c.ReadSession.Region = "" },
		"uppercase region":        func(c *AuthConfig) { c.ReadSession.Region = "US-EAST-1" },
	}
	for name, mutate := range badCases {
		t.Run(name, func(t *testing.T) {
			cfg := base()
			mutate(&cfg)
			if _, err := NewJWTAuth(context.Background(), cfg); err == nil {
				t.Fatalf("invalid read session config %q started", name)
			}
		})
	}

	// Trimmed multi-segment prefix is valid.
	nested := base()
	nested.ReadSession.Prefix = "/env/root/"
	a, err = NewJWTAuth(context.Background(), nested)
	if err != nil || !a.readSessionEnabled() || a.readSession.prefix != "env/root" {
		t.Fatalf("valid nested prefix failed: err=%v", err)
	}

	// S3 endpoint validation checks.
	if err := ValidateReadSessionS3Config(ReadSessionS3Config{}); err != nil {
		t.Fatalf("empty endpoint failed: %v", err)
	}
	if err := ValidateReadSessionS3Config(ReadSessionS3Config{BaseEndpoint: "http://127.0.0.1:9000", UsePathStyle: true}); err != nil {
		t.Fatalf("valid endpoint failed: %v", err)
	}
	for _, bad := range []string{"ftp://127.0.0.1", "http://127.0.0.1:9000?x=1", "/relative", "http://user:pass@127.0.0.1"} {
		if err := ValidateReadSessionS3Config(ReadSessionS3Config{BaseEndpoint: bad}); err == nil {
			t.Fatalf("invalid endpoint %q passed", bad)
		}
	}
}

func TestReadSessionHoglakeConformanceVectors(t *testing.T) {
	raw, err := os.ReadFile("testdata/hoglake-read-session-vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Issuer string          `json:"issuer"`
		JWKS   json.RawMessage `json:"jwks"`
		Config struct {
			Profile        string   `json:"profile"`
			BackendID      string   `json:"backend_id"`
			PredicateField string   `json:"predicate_field"`
			RemoteTargets  []string `json:"remote_targets"`
		} `json:"config"`
		Vectors []struct {
			Name     string `json:"name"`
			VerifyAt int64  `json:"verify_at"`
			Claims   struct {
				Sub string `json:"sub"`
			} `json:"claims"`
			Token  string `json:"token"`
			Expect struct {
				Read     string `json:"read"`
				Ordinary string `json:"ordinary"`
			} `json:"expect"`
		} `json:"vectors"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	if len(doc.Vectors) < 13 {
		t.Fatalf("expected at least 13 vectors, got %d", len(doc.Vectors))
	}
	jwksFile := filepath.Join(t.TempDir(), "jwks.json")
	if err := os.WriteFile(jwksFile, doc.JWKS, 0o600); err != nil {
		t.Fatal(err)
	}

	for _, vec := range doc.Vectors {
		t.Run(vec.Name, func(t *testing.T) {
			now := time.Unix(vec.VerifyAt, 0)
			readAuth, err := NewJWTAuth(context.Background(), AuthConfig{
				Mode:                           "enforce",
				Issuer:                         doc.Issuer,
				BackendID:                      doc.Config.BackendID,
				JWKSFile:                       jwksFile,
				ScopedNamespaceEnabled:         true,
				ScopedNamespacePredicateFields: []string{doc.Config.PredicateField},
				ReadSession: &ReadSessionConfig{
					Profile:       doc.Config.Profile,
					RemoteTargets: doc.Config.RemoteTargets,
					Bucket:        "example-owner",
					Prefix:        "root",
					Region:        "us-east-1",
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			readAuth.now = func() time.Time { return now }
			readAuth.fetched, readAuth.tried = now, now

			claims, err := readAuth.verifyReadSessionToken(context.Background(), vec.Token)
			if vec.Expect.Read == "accept" {
				if err != nil {
					t.Fatalf("expected accept, got %v", err)
				}
				if claims.subject != vec.Claims.Sub {
					t.Fatalf("subject=%q want %q", claims.subject, vec.Claims.Sub)
				}
				// Also verify full route + storage target binding succeeds.
				rsa, err := readAuth.verifyReadSession(context.Background(), vec.Token, "fixture_3", "example-catalog")
				if err != nil {
					t.Fatalf("route binding failed: %v", err)
				}
				if !rsa.authority.permits("team_id", 42) || rsa.authority.permits("team_id", 43) {
					t.Fatal("bound predicate permissions differ")
				}
			} else {
				var rerr *readSessionError
				if !errors.As(err, &rerr) {
					t.Fatalf("expected readSessionError %q, got %v", vec.Expect.Read, err)
				}
				if rerr.reason != vec.Expect.Read {
					t.Fatalf("reason=%q want %q", rerr.reason, vec.Expect.Read)
				}
			}

			// Ordinary SQL verifier must refuse every read-session vector.
			if _, err := readAuth.verify(context.Background(), vec.Token); err == nil {
				t.Fatal("ordinary SQL verifier accepted a read-session vector")
			}
			// Ordinary scoped metadata verifier must refuse every read-session vector.
			ordinaryScoped, err := NewJWTAuth(context.Background(), AuthConfig{
				Mode:                           "enforce",
				Issuer:                         doc.Issuer,
				BackendID:                      doc.Config.BackendID,
				JWKSFile:                       jwksFile,
				ScopedNamespaceEnabled:         true,
				ScopedNamespacePredicateFields: []string{doc.Config.PredicateField},
			})
			if err != nil {
				t.Fatal(err)
			}
			ordinaryScoped.now = func() time.Time { return now }
			ordinaryScoped.fetched, ordinaryScoped.tried = now, now
			if _, err := ordinaryScoped.verifyScopedNamespace(context.Background(), vec.Token, "fixture_3", "example-catalog"); err == nil {
				t.Fatal("ordinary scoped metadata verifier accepted a read-session vector")
			}
		})
	}
}

func TestReadSessionClaimsAndGrantRejection(t *testing.T) {
	f := newReadSessionAuthFixture(t)

	// Valid baseline passes.
	if _, err := f.auth.verifyReadSession(context.Background(), f.token(t, nil), readSessionTestTenant, readSessionTestCatalog); err != nil {
		t.Fatalf("valid read session failed: %v", err)
	}

	// Refusals that must fail token validation (401 class, not errUnsupportedAuthority).
	tokenRejections := map[string]struct {
		mutate func(map[string]any)
		reason string
	}{
		"wrong issuer": {func(c map[string]any) { c["iss"] = "https://other.test" }, readSessionErrInvalidIssuer},
		"mixed legacy audience": {func(c map[string]any) {
			c["aud"] = []string{"trino-session", "hoglake-session", "hoglite-session", "hoglite"}
		}, readSessionErrInvalidAudience},
		"legacy audiences only": {func(c map[string]any) { c["aud"] = []string{"hoglake", "hoglite"} }, readSessionErrInvalidAudience},
		"string audience":       {func(c map[string]any) { c["aud"] = "hoglite-session" }, readSessionErrInvalidAudience},
		"duplicate audience":    {func(c map[string]any) { c["aud"] = []string{"trino-session", "trino-session", "hoglite-session"} }, readSessionErrInvalidAudience},
		"missing profile":       {func(c map[string]any) { delete(c, "profile") }, readSessionErrInvalidToken},
		"experimental profile":  {func(c map[string]any) { c["profile"] = "storage-session" }, readSessionErrInvalidToken},
		"wrong version":         {func(c map[string]any) { c["version"] = 2 }, readSessionErrInvalidToken},
		"string version":        {func(c map[string]any) { c["version"] = "1" }, readSessionErrInvalidToken},
		"float version":         {func(c map[string]any) { c["version"] = 1.5 }, readSessionErrInvalidToken},
		"wrong purpose":         {func(c map[string]any) { c["purpose"] = "maintenance" }, readSessionErrInvalidToken},
		"tenant kind":           {func(c map[string]any) { c["ten"] = "tenant" }, readSessionErrInvalidToken},
		"sub usr mismatch":      {func(c map[string]any) { c["usr"] = "example.bob" }, readSessionErrInvalidToken},
		"long usr":              {func(c map[string]any) { s := strings.Repeat("a", 201); c["sub"], c["usr"] = s, s }, readSessionErrInvalidToken},
		"dlg present":           {func(c map[string]any) { c["dlg"] = false }, readSessionErrInvalidToken},
		"scope present":         {func(c map[string]any) { c["scope"] = "hoglake:example-catalog:read" }, readSessionErrInvalidToken},
		"svc present":           {func(c map[string]any) { c["svc"] = "truffle" }, readSessionErrInvalidToken},
		"generation 2":          {func(c map[string]any) { c["generation"] = 2 }, readSessionErrInvalidToken},
		"write cat scope":       {func(c map[string]any) { c["cat"] = map[string][]string{readSessionTestCatalog: {"write"}} }, readSessionErrInvalidToken},
		"two cats": {func(c map[string]any) {
			c["cat"] = map[string][]string{readSessionTestCatalog: {"read"}, "other": {"read"}}
		}, readSessionErrInvalidToken},
		"write remote schema": {func(c map[string]any) { c["remote_schemas"] = map[string][]string{"team": {"write"}} }, readSessionErrInvalidToken},
		"named remote schema": {func(c map[string]any) { c["remote_schemas"] = map[string][]string{"team_42": {"read"}} }, readSessionErrInvalidToken},
		"wildcard team prd":   {func(c map[string]any) { c["prd"] = map[string]map[string]string{"team_id": {"*": "read"}} }, readSessionErrInvalidToken},
		"write team prd":      {func(c map[string]any) { c["prd"] = map[string]map[string]string{"team_id": {"42": "write"}} }, readSessionErrInvalidToken},
		"two teams prd": {func(c map[string]any) {
			c["prd"] = map[string]map[string]string{"team_id": {"42": "read", "43": "read"}}
		}, readSessionErrInvalidToken},
		"leading zero team prd": {func(c map[string]any) { c["prd"] = map[string]map[string]string{"team_id": {"042": "read"}} }, readSessionErrInvalidToken},
		"nbf after iat":         {func(c map[string]any) { c["nbf"] = int64(2_000_000_001) }, readSessionErrInvalidToken},
		"iat equals exp":        {func(c map[string]any) { c["exp"] = c["iat"] }, readSessionErrInvalidToken},
		"exp after aws expiry":  {func(c map[string]any) { c["aws"].(map[string]any)["expires_at"] = int64(2_000_000_059) }, readSessionErrInvalidToken},
		"aws after deadline":    {func(c map[string]any) { c["session_deadline"] = int64(2_000_000_059) }, readSessionErrInvalidToken},
		"lifetime over 900s": {func(c map[string]any) {
			c["exp"] = int64(2_000_000_901)
			c["aws"].(map[string]any)["expires_at"] = int64(2_000_000_901)
			c["session_deadline"] = int64(2_000_000_901)
		}, readSessionErrInvalidToken},
		"missing session token": {func(c map[string]any) { delete(c["aws"].(map[string]any), "session_token") }, readSessionErrInvalidToken},
		"extra aws field":       {func(c map[string]any) { c["aws"].(map[string]any)["extra"] = "x" }, readSessionErrInvalidToken},
		"nil backend uuid":      {func(c map[string]any) { c["rtg"].(map[string]any)["be"] = "00000000-0000-0000-0000-000000000000" }, readSessionErrInvalidToken},
		"noncanonical inc":      {func(c map[string]any) { c["rtg"].(map[string]any)["t"] = "fixture_03" }, readSessionErrInvalidToken},
		"manifest json suffix": {func(c map[string]any) {
			c["grants"].([]map[string]any)[1]["object_key"] = "root/2c/t/fixture_3/manifest.json"
		}, readSessionErrInvalidToken},
		"manifest empty root": {func(c map[string]any) {
			c["grants"].([]map[string]any)[1]["object_key"] = "/2c/t/fixture_3/manifest"
		}, readSessionErrInvalidToken},
		"manifest no root": {func(c map[string]any) {
			c["grants"].([]map[string]any)[1]["object_key"] = "2c/t/fixture_3/manifest"
		}, readSessionErrInvalidToken},
		"manifest wrong shard": {func(c map[string]any) {
			c["grants"].([]map[string]any)[1]["object_key"] = "root/ff/t/fixture_3/manifest"
		}, readSessionErrInvalidToken},
		"manifest as prefix": {func(c map[string]any) {
			g := c["grants"].([]map[string]any)[1]
			delete(g, "object_key")
			g["prefix"] = "root/2c/t/fixture_3/"
		}, readSessionErrInvalidToken},
		"wildcard in object key": {func(c map[string]any) {
			c["grants"].([]map[string]any)[0]["object_key"] = "tables/*.parquet"
		}, readSessionErrInvalidToken},
		"wildcard in prefix": {func(c map[string]any) {
			c["grants"].([]map[string]any)[2]["prefix"] = "events/*/"
		}, readSessionErrInvalidToken},
		"both key and prefix": {func(c map[string]any) {
			c["grants"].([]map[string]any)[2]["object_key"] = "events/team_id=42/a"
		}, readSessionErrInvalidToken},
		"overlapping prefixes": {func(c map[string]any) {
			c["grants"].([]map[string]any)[3]["prefix"] = "events/team_id=42/sub/"
		}, readSessionErrInvalidToken},
		"unsorted grants": {func(c map[string]any) {
			g := c["grants"].([]map[string]any)
			g[2], g[3] = g[3], g[2]
		}, readSessionErrInvalidToken},
		"duplicate manifest grant": {func(c map[string]any) {
			g := c["grants"].([]map[string]any)
			extra := map[string]any{
				"catalog":    readSessionTestCatalog,
				"target":     readSessionTestBackend,
				"bucket":     readSessionTestBucket,
				"object_key": "zroot/2c/t/fixture_3/manifest",
				"region":     readSessionTestRegion,
				"actions":    []string{"s3:GetObject"},
			}
			c["grants"] = []map[string]any{g[0], g[1], extra, g[2], g[3]}
		}, readSessionErrInvalidToken},
		"grant region mismatch": {func(c map[string]any) {
			c["grants"].([]map[string]any)[1]["region"] = "eu-west-1"
		}, readSessionErrInvalidToken},
		"write action": {func(c map[string]any) {
			c["grants"].([]map[string]any)[1]["actions"] = []string{"s3:PutObject"}
		}, readSessionErrInvalidToken},
		"case variant claim": {func(c map[string]any) {
			c["AWS"] = c["aws"]
		}, readSessionErrInvalidToken},
	}

	for name, tc := range tokenRejections {
		t.Run(name, func(t *testing.T) {
			raw := f.token(t, tc.mutate)
			_, err := f.auth.verifyReadSession(context.Background(), raw, readSessionTestTenant, readSessionTestCatalog)
			var rerr *readSessionError
			if !errors.As(err, &rerr) || rerr.reason != tc.reason {
				t.Fatalf("case %q: got err=%v, want reason=%q", name, err, tc.reason)
			}
			// Never leak JWT or AWS secrets in errors.
			if strings.Contains(err.Error(), "ASIAEXAMPLEALICE0001") ||
				strings.Contains(err.Error(), "secret-alice") ||
				strings.Contains(err.Error(), "session-token-alice") {
				t.Fatalf("error leaked secret material: %v", err)
			}
		})
	}
}

func TestReadSessionZeroClockSkewAndTimeBounds(t *testing.T) {
	f := newReadSessionAuthFixture(t)
	raw := f.token(t, func(c map[string]any) {
		c["iat"] = int64(2_000_000_000)
		c["nbf"] = int64(2_000_000_000)
		c["exp"] = int64(2_000_000_060)
		c["aws"].(map[string]any)["expires_at"] = int64(2_000_000_060)
		c["session_deadline"] = int64(2_000_000_120)
	})

	// One second before aws.expires_at succeeds.
	f.setNow(time.Unix(2_000_000_059, 0))
	if _, err := f.auth.verifyReadSession(context.Background(), raw, readSessionTestTenant, readSessionTestCatalog); err != nil {
		t.Fatalf("expected valid one second before aws.expires_at: %v", err)
	}
	// Exact boundary now == aws.expires_at fails with zero clock skew.
	f.setNow(time.Unix(2_000_000_060, 0))
	var rerr *readSessionError
	if _, err := f.auth.verifyReadSession(context.Background(), raw, readSessionTestTenant, readSessionTestCatalog); !errors.As(err, &rerr) || rerr.reason != readSessionErrTokenExpired {
		t.Fatalf("expected token_expired at aws.expires_at, got %v", err)
	}

	// When exp < aws.expires_at, exp honors 60s skew while aws.expires_at keeps zero skew.
	rawSplit := f.token(t, func(c map[string]any) {
		c["iat"] = int64(2_000_000_000)
		c["nbf"] = int64(2_000_000_000)
		c["exp"] = int64(2_000_000_060)
		c["aws"].(map[string]any)["expires_at"] = int64(2_000_000_200)
		c["session_deadline"] = int64(2_000_000_300)
	})
	f.setNow(time.Unix(2_000_000_120, 0))
	if _, err := f.auth.verifyReadSession(context.Background(), rawSplit, readSessionTestTenant, readSessionTestCatalog); err != nil {
		t.Fatalf("expected valid at exp + 60s before aws.expires_at: %v", err)
	}
	f.setNow(time.Unix(2_000_000_121, 0))
	if _, err := f.auth.verifyReadSession(context.Background(), rawSplit, readSessionTestTenant, readSessionTestCatalog); !errors.As(err, &rerr) || rerr.reason != readSessionErrTokenExpired {
		t.Fatalf("expected token_expired at exp + 61s, got %v", err)
	}

	// Future iat skew boundary: 60s passes, 61s fails as not_yet_valid.
	f.setNow(time.Unix(2_000_000_000-60, 0))
	if _, err := f.auth.verifyReadSession(context.Background(), raw, readSessionTestTenant, readSessionTestCatalog); err != nil {
		t.Fatalf("expected valid at iat - 60s: %v", err)
	}
	f.setNow(time.Unix(2_000_000_000-61, 0))
	if _, err := f.auth.verifyReadSession(context.Background(), raw, readSessionTestTenant, readSessionTestCatalog); !errors.As(err, &rerr) || rerr.reason != readSessionErrNotYetValid {
		t.Fatalf("expected not_yet_valid at iat - 61s, got %v", err)
	}
}

func TestReadSessionCryptoAndRouteBinding(t *testing.T) {
	f := newReadSessionAuthFixture(t)

	// RS256 is refused even when the RSA key is in the mounted JWKS.
	rsHdr, _ := json.Marshal(map[string]any{"alg": "RS256", "kid": f.rsaKid, "typ": "JWT"})
	payload, _ := json.Marshal(f.baseClaims())
	rsInput := base64.RawURLEncoding.EncodeToString(rsHdr) + "." + base64.RawURLEncoding.EncodeToString(payload)
	rsSig, err := jwt.SigningMethodRS256.Sign(rsInput, f.rsaKey)
	if err != nil {
		t.Fatal(err)
	}
	rsToken := rsInput + "." + base64.RawURLEncoding.EncodeToString(rsSig)
	var rerr *readSessionError
	if _, err := f.auth.verifyReadSession(context.Background(), rsToken, readSessionTestTenant, readSessionTestCatalog); !errors.As(err, &rerr) || rerr.reason != readSessionErrUnsupportedAlg {
		t.Fatalf("RS256 should fail with unsupported_alg, got %v", err)
	}

	// Valid token whose placement or storage target does not match this Hoglite returns errUnsupportedAuthority.
	mismatches := map[string]func(map[string]any){
		"wrong backend": func(c map[string]any) {
			other := "22222222-2222-4222-8222-222222222222"
			c["rtg"].(map[string]any)["be"] = other
			c["grants"].([]map[string]any)[1]["target"] = other
		},
		"wrong tenant": func(c map[string]any) {
			c["rtg"].(map[string]any)["t"] = "fixture_4"
			c["grants"].([]map[string]any)[1]["object_key"] = manifestKey(readSessionTestPrefix, "fixture_4")
		},
		"wrong catalog": func(c map[string]any) {
			c["primary_catalog"] = "other-catalog"
			c["cat"] = map[string][]string{"other-catalog": {"read"}}
			for _, g := range c["grants"].([]map[string]any) {
				g["catalog"] = "other-catalog"
			}
			c["grants"].([]map[string]any)[0]["target"] = "other-catalog"
		},
		"wrong root prefix": func(c map[string]any) {
			c["grants"].([]map[string]any)[1]["object_key"] = manifestKey("other-root", readSessionTestTenant)
		},
		"wrong bucket": func(c map[string]any) {
			c["grants"].([]map[string]any)[1]["bucket"] = "other-owner-bucket"
			// Keep grants sorted by (bucket, key): example-customer < example-remote < other-owner-bucket.
			g := c["grants"].([]map[string]any)
			c["grants"] = []map[string]any{g[0], g[2], g[3], g[1]}
		},
		"wrong region": func(c map[string]any) {
			c["aws"].(map[string]any)["region"] = "eu-west-1"
			for _, g := range c["grants"].([]map[string]any) {
				g["region"] = "eu-west-1"
			}
		},
	}
	for name, mutate := range mismatches {
		t.Run(name, func(t *testing.T) {
			raw := f.token(t, mutate)
			if _, err := f.auth.verifyReadSession(context.Background(), raw, readSessionTestTenant, readSessionTestCatalog); !errors.Is(err, errUnsupportedAuthority) {
				t.Fatalf("mismatch %q: got %v, want errUnsupportedAuthority", name, err)
			}
		})
	}

	// Credentials redact themselves on formatting.
	rsa, err := f.auth.verifyReadSession(context.Background(), f.token(t, nil), readSessionTestTenant, readSessionTestCatalog)
	if err != nil {
		t.Fatal(err)
	}
	for _, formatted := range []string{rsa.creds.String(), rsa.creds.GoString()} {
		if strings.Contains(formatted, "ASIAEXAMPLEALICE0001") ||
			strings.Contains(formatted, "secret-alice") ||
			strings.Contains(formatted, "session-token-alice") {
			t.Fatalf("formatted credentials leaked secrets: %q", formatted)
		}
	}

	// Verify canceled context returns context.Canceled.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := f.auth.verifyReadSession(ctx, f.token(t, nil), readSessionTestTenant, readSessionTestCatalog); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled context got %v", err)
	}
}
