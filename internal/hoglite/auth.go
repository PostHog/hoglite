package hoglite

import (
	"bytes"
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/big"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

// AuthConfig enables offline verification using public keys only.
type AuthConfig struct {
	Mode, Issuer, BackendID, JWKSFile, JWKSURL, UpstreamAudience string
}

type verificationKey struct {
	key crypto.PublicKey
	alg string
}

// JWTAuth caches only public keys. The issuer is never consulted about tokens.
type JWTAuth struct {
	issuer, backend, upstream string
	fetch                     func(context.Context) ([]byte, error)
	now                       func() time.Time
	mu                        sync.Mutex
	keys                      map[string]verificationKey
	fetched, tried            time.Time
	refreshing                chan struct{}
}

const (
	jwksMaxBytes = 1 << 20
	jwksTTL      = time.Minute
	jwksMinRetry = 30 * time.Second
	maxTokenSize = 32 << 10
)

// NewJWTAuth fails boot on invalid enforcement configuration or unusable keys.
// Off preserves the internal service's legacy development behavior.
func NewJWTAuth(ctx context.Context, cfg AuthConfig) (*JWTAuth, error) {
	if cfg.Mode == "" || cfg.Mode == "off" {
		if cfg.Issuer != "" || cfg.BackendID != "" || cfg.JWKSFile != "" || cfg.JWKSURL != "" || cfg.UpstreamAudience != "" {
			log.Print("warning: JWT authentication is off despite partial authentication configuration")
		}
		return nil, nil
	}
	if cfg.Mode != "enforce" {
		return nil, errors.New("auth mode must be off or enforce")
	}
	if !absoluteHTTPURL(cfg.Issuer) {
		return nil, errors.New("auth issuer must be an absolute http(s) URL")
	}
	id, err := uuid.Parse(cfg.BackendID)
	if err != nil || id.String() != cfg.BackendID {
		return nil, errors.New("auth backend ID must be a canonical lowercase UUID")
	}
	if (cfg.JWKSFile == "") == (cfg.JWKSURL == "") {
		return nil, errors.New("auth requires exactly one JWKS file or URL")
	}
	upstream := cfg.UpstreamAudience
	if upstream == "" {
		upstream = "hoglake"
	}
	if upstream == "hoglite" || upstream == "hogtower-vend" || len(upstream) > 256 || strings.TrimSpace(upstream) != upstream {
		return nil, errors.New("invalid upstream metadata audience")
	}
	a := &JWTAuth{issuer: cfg.Issuer, backend: cfg.BackendID, upstream: upstream, now: time.Now}
	if cfg.JWKSFile != "" {
		a.fetch = func(context.Context) ([]byte, error) {
			f, err := os.Open(cfg.JWKSFile)
			if err != nil {
				return nil, err
			}
			defer f.Close()
			return boundedJWKS(f)
		}
	} else {
		if !absoluteHTTPURL(cfg.JWKSURL) {
			return nil, errors.New("auth JWKS URL must be an absolute http(s) URL")
		}
		client := &http.Client{
			Timeout: 5 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return errors.New("JWKS redirects are not supported")
			},
		}
		a.fetch = func(ctx context.Context) ([]byte, error) {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, cfg.JWKSURL, nil)
			if err != nil {
				return nil, err
			}
			resp, err := client.Do(req)
			if err != nil {
				return nil, err
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				return nil, fmt.Errorf("JWKS answered %d", resp.StatusCode)
			}
			return boundedJWKS(resp.Body)
		}
	}
	if err := a.refresh(ctx); err != nil {
		return nil, fmt.Errorf("load public JWKS: %w", err)
	}
	return a, nil
}

func absoluteHTTPURL(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && (u.Scheme == "http" || u.Scheme == "https") &&
		u.Host != "" && u.User == nil && u.Fragment == ""
}

func boundedJWKS(r io.Reader) ([]byte, error) {
	b, err := io.ReadAll(io.LimitReader(r, jwksMaxBytes+1))
	if len(b) > jwksMaxBytes {
		return nil, errors.New("JWKS too large")
	}
	return b, err
}

func (a *JWTAuth) refresh(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	b, err := a.fetch(ctx)
	if err != nil {
		return err
	}
	keys, err := parsePublicJWKS(b)
	if err != nil {
		return err
	}
	a.mu.Lock()
	a.keys, a.fetched, a.tried = keys, a.now(), a.now()
	a.mu.Unlock()
	return nil
}

func (a *JWTAuth) key(ctx context.Context, kid, alg string) (crypto.PublicKey, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	k, ok := a.keys[kid]
	now := a.now()
	if (!ok || now.Sub(a.fetched) >= jwksTTL) && a.refreshing == nil && now.Sub(a.tried) >= jwksMinRetry {
		// Bound failures and unknown-kid floods globally, not per attacker-selected kid.
		// Requests never wait for a fetch, including known-key requests during rotation.
		a.tried = now
		done := make(chan struct{})
		a.refreshing = done
		go func() {
			// Detached from the request, bounded, and never a permanent worker.
			if err := a.refresh(context.WithoutCancel(ctx)); err != nil {
				// The refresh interval bounds this warning. Never include source URLs or key material.
				log.Print("warning: public JWT key refresh failed. Cached keys retained")
			}
			a.mu.Lock()
			a.refreshing = nil
			close(done)
			a.mu.Unlock()
		}()
	}
	if !ok || k.alg != alg {
		return nil, errors.New("unknown signing key")
	}
	return k.key, nil
}

func parsePublicJWKS(raw []byte) (map[string]verificationKey, error) {
	if err := strictJSON(raw); err != nil {
		return nil, err
	}
	var set struct {
		Keys []map[string]json.RawMessage `json:"keys"`
	}
	if err := json.Unmarshal(raw, &set); err != nil {
		return nil, err
	}
	if len(set.Keys) == 0 || len(set.Keys) > 100 {
		return nil, errors.New("JWKS requires 1..100 public keys")
	}
	keys := make(map[string]verificationKey, len(set.Keys))
	for _, jwk := range set.Keys {
		text := func(name string) string {
			var s string
			_ = json.Unmarshal(jwk[name], &s)
			return s
		}
		for _, private := range []string{"d", "p", "q", "dp", "dq", "qi", "oth", "k"} {
			if _, ok := jwk[private]; ok {
				return nil, errors.New("JWKS must contain only public keys")
			}
		}
		kid, alg, use := text("kid"), text("alg"), text("use")
		// Shared public sets may include encryption or foreign-algorithm entries.
		// Skip their purpose before requiring a kid, matching Hoglake's contract.
		if _, present := jwk["use"]; present && use != "sig" {
			continue
		}
		if _, present := jwk["alg"]; present && alg != "ES256" && alg != "RS256" {
			continue
		}
		if kid == "" || len(kid) > 256 {
			return nil, errors.New("invalid signing key metadata")
		}
		if _, exists := keys[kid]; exists {
			return nil, errors.New("duplicate JWKS kid")
		}
		if ops, exists := jwk["key_ops"]; exists {
			var values []string
			if json.Unmarshal(ops, &values) != nil || len(values) != 1 || values[0] != "verify" {
				return nil, errors.New("public key must permit verification only")
			}
		}
		decode := func(name string) ([]byte, error) {
			return base64.RawURLEncoding.Strict().DecodeString(text(name))
		}
		var pub crypto.PublicKey
		switch text("kty") {
		case "EC":
			if (alg != "" && alg != "ES256") || text("crv") != "P-256" {
				return nil, errors.New("only ES256 P-256 EC keys are supported")
			}
			x, e1 := decode("x")
			y, e2 := decode("y")
			if e1 != nil || e2 != nil || len(x) != 32 || len(y) != 32 {
				return nil, errors.New("invalid EC coordinates")
			}
			point := append(append([]byte{4}, x...), y...)
			key, err := ecdsa.ParseUncompressedPublicKey(elliptic.P256(), point)
			if err != nil {
				return nil, errors.New("invalid EC public key")
			}
			pub, alg = key, "ES256"
		case "RSA":
			if alg != "" && alg != "RS256" {
				return nil, errors.New("only RS256 RSA keys are supported")
			}
			n, e1 := decode("n")
			e, e2 := decode("e")
			exponent := new(big.Int).SetBytes(e)
			modulus := new(big.Int).SetBytes(n)
			if e1 != nil || e2 != nil || modulus.BitLen() < 2048 || modulus.BitLen() > 8192 || modulus.Bit(0) == 0 ||
				!exponent.IsInt64() || exponent.Int64() < 3 || exponent.Int64() > 1<<31-1 || exponent.Bit(0) == 0 {
				return nil, errors.New("invalid RSA public key")
			}
			pub, alg = &rsa.PublicKey{N: modulus, E: int(exponent.Int64())}, "RS256"
		default:
			return nil, errors.New("unsupported JWKS key type")
		}
		keys[kid] = verificationKey{key: pub, alg: alg}
	}
	if len(keys) == 0 {
		return nil, errors.New("JWKS contains no usable signing keys")
	}
	return keys, nil
}

// strictJSON refuses duplicates and trailing values before JWT library decoding.
func strictJSON(raw []byte) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var value func(int) error
	value = func(depth int) error {
		if depth > 64 {
			return errors.New("JSON too deep")
		}
		tok, err := dec.Token()
		if err != nil {
			return err
		}
		delim, compound := tok.(json.Delim)
		if !compound {
			return nil
		}
		if delim != '{' && delim != '[' {
			return errors.New("invalid JSON")
		}
		seen := map[string]bool{}
		for dec.More() {
			if delim == '{' {
				name, err := dec.Token()
				key, ok := name.(string)
				if err != nil || !ok || seen[key] {
					return errors.New("duplicate or invalid JSON key")
				}
				seen[key] = true
			}
			if err := value(depth + 1); err != nil {
				return err
			}
		}
		_, err = dec.Token()
		return err
	}
	if err := value(0); err != nil {
		return err
	}
	if _, err := dec.Token(); err != io.EOF {
		return errors.New("trailing JSON")
	}
	return nil
}

type tokenClaims struct {
	jwt.RegisteredClaims
	Kind string              `json:"ten"`
	Org  string              `json:"org"`
	Cat  map[string][]string `json:"cat"`
	Rtg  *struct {
		Backend string `json:"b"`
		ID      string `json:"be"`
		Tenant  string `json:"t"`
	} `json:"rtg"`
}

type requestAuthority struct {
	token, tenant string
	operator      bool
	write         bool
}

type authorityKey struct{}

var errUnsupportedAuthority = errors.New("verified authority is not supported")

func (a *JWTAuth) verify(ctx context.Context, raw string) (*requestAuthority, error) {
	if len(raw) > maxTokenSize {
		return nil, errors.New("token too large")
	}
	parts := strings.Split(raw, ".")
	if len(parts) != 3 {
		return nil, errors.New("invalid compact token")
	}
	var payload map[string]json.RawMessage
	for i := 0; i < 2; i++ {
		b, err := base64.RawURLEncoding.Strict().DecodeString(parts[i])
		if err != nil || strictJSON(b) != nil {
			return nil, errors.New("invalid token JSON")
		}
		var fields map[string]json.RawMessage
		if json.Unmarshal(b, &fields) != nil || fields == nil {
			return nil, errors.New("token segments must be objects")
		}
		if i == 0 {
			if crit, ok := fields["crit"]; ok && string(crit) != "[]" {
				return nil, errors.New("unsupported critical header")
			}
			if _, ok := fields["b64"]; ok {
				return nil, errors.New("unsupported encoding header")
			}
		} else {
			payload = fields
		}
	}
	for _, name := range []string{"exp", "iat", "nbf"} {
		if b, ok := payload[name]; ok {
			var number json.Number
			if json.Unmarshal(b, &number) != nil {
				return nil, errors.New("invalid token time")
			}
			if _, err := number.Int64(); err != nil || len(b) == 0 || b[0] == '"' {
				return nil, errors.New("invalid token time")
			}
		}
	}
	claims := &tokenClaims{}
	token, err := jwt.ParseWithClaims(raw, claims, func(t *jwt.Token) (any, error) {
		kid, ok := t.Header["kid"].(string)
		if !ok || kid == "" || len(kid) > 256 {
			return nil, errors.New("kid required")
		}
		return a.key(ctx, kid, t.Method.Alg())
	}, jwt.WithValidMethods([]string{"ES256", "RS256"}), jwt.WithIssuer(a.issuer),
		jwt.WithAudience("hoglite"), jwt.WithExpirationRequired(), jwt.WithIssuedAt(),
		// Hoglake compares whole seconds and accepts the equality boundary.
		jwt.WithLeeway(60*time.Second+time.Nanosecond),
		jwt.WithTimeFunc(func() time.Time { return a.now().Truncate(time.Second) }), jwt.WithStrictDecoding())
	if err != nil || !token.Valid {
		return nil, errors.New("invalid or expired token")
	}
	// Metadata authority is never storage-vending authority, even in a signed array.
	upstream := a.upstream
	if upstream == "" {
		upstream = "hoglake"
	}
	if len(claims.Audience) != 2 ||
		!((claims.Audience[0] == upstream && claims.Audience[1] == "hoglite") ||
			(claims.Audience[0] == "hoglite" && claims.Audience[1] == upstream)) {
		return nil, errors.New("invalid metadata audiences")
	}
	if claims.Subject == "" || claims.ID == "" {
		return nil, errors.New("missing token identity")
	}
	if claims.Kind != "tenant" && claims.Kind != "operator" {
		return nil, errUnsupportedAuthority
	}
	for _, unsupported := range []string{"prd", "scope", "usr"} {
		if _, ok := payload[unsupported]; ok {
			return nil, errUnsupportedAuthority
		}
	}
	out := &requestAuthority{token: raw, operator: claims.Kind == "operator"}
	if claims.Rtg == nil && out.operator && len(claims.Cat) == 0 && claims.Org == "" {
		return out, nil // Explicit unbound operators can administer the fleet, not tenant data.
	}
	if claims.Rtg == nil || claims.Rtg.Backend != "hoglite" || claims.Rtg.ID != a.backend ||
		!validTID(claims.Rtg.Tenant) || claims.Org == "" {
		return nil, errors.New("invalid backend or tenant binding")
	}
	if len(claims.Cat) != 1 {
		return nil, errUnsupportedAuthority
	}
	var routing map[string]json.RawMessage
	if json.Unmarshal(payload["rtg"], &routing) != nil || len(routing) != 3 {
		return nil, errors.New("invalid routing fields")
	}
	out.tenant = claims.Rtg.Tenant
	operatorAdmin := false
	for catalog, scopes := range claims.Cat {
		if catalog == "" || len(scopes) == 0 {
			return nil, errUnsupportedAuthority
		}
		recognized := false
		for _, scope := range scopes {
			switch scope {
			case "read":
				recognized = true
			case "write", "maintenance":
				recognized = true
				out.write = true
			case "admin":
				recognized = true
				out.write, operatorAdmin = true, true
			}
		}
		if !recognized {
			return nil, errUnsupportedAuthority
		}
	}
	if out.operator && !operatorAdmin {
		return nil, errUnsupportedAuthority
	}
	return out, nil
}

func authority(r *http.Request) *requestAuthority {
	a, _ := r.Context().Value(authorityKey{}).(*requestAuthority)
	return a
}

// authorize runs before handlers can discover ownership or touch storage.
func (s *Server) authorize(r *http.Request) (*http.Request, int) {
	if s.auth == nil {
		return r, 0
	}
	// Only in-process subrequests inherit this private context value.
	// Every separate HTTP hop starts without it and must verify its signature.
	a := authority(r)
	if a == nil {
		values := r.Header.Values("Authorization")
		if len(values) != 1 {
			return r, http.StatusUnauthorized
		}
		fields := strings.Fields(values[0])
		if len(fields) != 2 || !strings.EqualFold(fields[0], "Bearer") {
			return r, http.StatusUnauthorized
		}
		var err error
		a, err = s.auth.verify(r.Context(), fields[1])
		if err != nil {
			if errors.Is(err, errUnsupportedAuthority) {
				return r, http.StatusForbidden
			}
			return r, http.StatusUnauthorized
		}
	}
	r = r.WithContext(context.WithValue(r.Context(), authorityKey{}, a))
	tid := r.PathValue("tid")
	switch r.Pattern {
	case "POST /v1/query-batch":
		if a.tenant == "" {
			return r, http.StatusForbidden
		}
	case "POST /v1/t/{tid}/query", "POST /v1/t/{tid}/tx":
		if a.tenant == "" || tid != a.tenant {
			return r, http.StatusForbidden
		}
	default:
		if !a.operator || (tid != "" && tid != a.tenant) || (tid == "" && a.tenant != "") {
			return r, http.StatusForbidden
		}
	}
	return r, 0
}

func forwardAuthority(dst, src *http.Request) {
	if a := authority(src); a != nil {
		dst.Header.Set("Authorization", "Bearer "+a.token)
	} else {
		// Legacy off-mode forwarding remains compatible.
		dst.Header.Set("Authorization", src.Header.Get("Authorization"))
	}
}
