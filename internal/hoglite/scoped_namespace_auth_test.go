package hoglite

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func newScopedNamespaceAuthFixture(t *testing.T, fields ...string) *authFixture {
	t.Helper()
	f := newAuthFixture(t)
	if len(fields) == 0 {
		fields = []string{"team_id"}
	}
	a, err := NewJWTAuth(context.Background(), AuthConfig{
		Mode: "enforce", Issuer: "https://issuer.test", BackendID: authTestBackend, JWKSFile: f.file,
		ScopedNamespaceEnabled: true, ScopedNamespacePredicateFields: fields,
	})
	if err != nil {
		t.Fatal(err)
	}
	a.now = f.auth.now
	a.fetched, a.tried = f.now, f.now
	f.auth = a
	t.Cleanup(func() { awaitAuthRefresh(t, a) })
	return f
}

func (f *authFixture) scopedNamespaceToken(t *testing.T, mutate func(jwt.MapClaims)) string {
	t.Helper()
	return f.token(t, func(c jwt.MapClaims) {
		c["ten"] = "scoped"
		c["usr"] = "reader@example.test"
		c["cat"] = map[string][]string{"catalog-not-the-tid": {"read"}}
		c["prd"] = map[string]map[string]string{"team_id": {"42": "read"}}
		if mutate != nil {
			mutate(c)
		}
	})
}

func TestScopedNamespaceAuthConfig(t *testing.T) {
	f := newAuthFixture(t)
	base := AuthConfig{
		Mode: "enforce", Issuer: "https://issuer.test", BackendID: authTestBackend, JWKSFile: f.file,
		ScopedNamespacePredicateFields: []string{"team_id"},
	}
	a, err := NewJWTAuth(context.Background(), base)
	if err != nil || a.scopedNamespaceEnabled {
		t.Fatal("scoped namespace verification does not default to off")
	}
	if _, err := a.verifyScopedNamespace(context.Background(), f.scopedNamespaceToken(t, nil), "opaque_1", "catalog-not-the-tid"); !errors.Is(err, errUnsupportedAuthority) {
		t.Fatal("disabled scoped verification accepts authority")
	}
	base.ScopedNamespaceEnabled = true
	for name, mutate := range map[string]func(*AuthConfig){
		"off":          func(c *AuthConfig) { c.Mode = "off" },
		"unset mode":   func(c *AuthConfig) { c.Mode = "" },
		"warn":         func(c *AuthConfig) { c.Mode = "warn" },
		"no file":      func(c *AuthConfig) { c.JWKSFile = "" },
		"URL only":     func(c *AuthConfig) { c.JWKSFile, c.JWKSURL = "", "https://keys.test" },
		"file and URL": func(c *AuthConfig) { c.JWKSURL = "https://keys.test" },
		"empty set":    func(c *AuthConfig) { c.ScopedNamespacePredicateFields = nil },
		"empty field":  func(c *AuthConfig) { c.ScopedNamespacePredicateFields = []string{""} },
		"uppercase":    func(c *AuthConfig) { c.ScopedNamespacePredicateFields = []string{"Team_id"} },
		"underscore":   func(c *AuthConfig) { c.ScopedNamespacePredicateFields = []string{"_team_id"} },
		"space":        func(c *AuthConfig) { c.ScopedNamespacePredicateFields = []string{"team id"} },
		"long":         func(c *AuthConfig) { c.ScopedNamespacePredicateFields = []string{strings.Repeat("a", 64)} },
		"duplicate":    func(c *AuthConfig) { c.ScopedNamespacePredicateFields = []string{"team_id", "team_id"} },
	} {
		t.Run(name, func(t *testing.T) {
			cfg := base
			mutate(&cfg)
			if _, err := NewJWTAuth(context.Background(), cfg); err == nil {
				t.Fatal("invalid scoped configuration starts")
			}
		})
	}
	for _, fields := range [][]string{{"a"}, {"team_id", "account_2"}, {strings.Repeat("a", 63)}} {
		cfg := base
		cfg.ScopedNamespacePredicateFields = fields
		a, err := NewJWTAuth(context.Background(), cfg)
		if err != nil || !a.scopedNamespaceEnabled || len(a.scopedNamespaceFields) != len(fields) {
			t.Fatal("valid scoped configuration fails")
		}
		fields[0] = "changed"
		if a.scopedNamespaceFields["changed"] {
			t.Fatal("caller mutation changes the configured field set")
		}
	}
}

func TestScopedNamespaceAuthority(t *testing.T) {
	f := newScopedNamespaceAuthFixture(t)
	for _, permission := range []string{"read", "write"} {
		t.Run(permission, func(t *testing.T) {
			raw := f.scopedNamespaceToken(t, func(c jwt.MapClaims) {
				c["aud"] = []string{"hoglite", "hoglake"}
				c["cat"] = map[string][]string{"catalog-not-the-tid": {permission, permission}}
				c["prd"] = map[string]map[string]string{"team_id": {"42": permission, "9223372036854775807": "read"}}
				c["scope"] = "foreign:ignored hoglake:catalog-not-the-tid:" + permission +
					" hoglake:catalog-not-the-tid:" + permission
				c["dlg"] = true
			})
			a, err := f.auth.verifyScopedNamespace(context.Background(), raw, "opaque_1", "catalog-not-the-tid")
			if err != nil {
				t.Fatal("valid scoped authority fails")
			}
			if !a.permits("team_id", 42) || !a.permits("team_id", math.MaxInt64) {
				t.Fatal("signed containment values fail")
			}
			for _, value := range []int64{0, -1, 7} {
				if a.permits("team_id", value) {
					t.Fatal("unsigned containment value passes")
				}
			}
			if a.permits("", 42) || a.permits("account_id", 42) {
				t.Fatal("unsigned containment field passes")
			}
			if _, err := f.auth.verify(context.Background(), raw); !errors.Is(err, errUnsupportedAuthority) {
				t.Fatal("scoped authority changes the legacy refusal")
			}
		})
	}
	for _, scope := range []string{"", "foreign:only", "foreign:\tignored"} {
		raw := f.scopedNamespaceToken(t, func(c jwt.MapClaims) { c["scope"] = scope })
		if _, err := f.auth.verifyScopedNamespace(context.Background(), raw, "opaque_1", "catalog-not-the-tid"); err != nil {
			t.Fatal("foreign scope changes signed catalog authority")
		}
	}
	raw := f.scopedNamespaceToken(t, func(c jwt.MapClaims) {
		c["prd"] = map[string]map[string]string{"team_id": {"*": "read"}}
	})
	a, err := f.auth.verifyScopedNamespace(context.Background(), raw, "opaque_1", "catalog-not-the-tid")
	if err != nil || !a.permits("team_id", 1) || !a.permits("team_id", math.MaxInt64) {
		t.Fatal("valid wildcard authority fails")
	}
	if a.permits("team_id", 0) || a.permits("", 42) {
		t.Fatal("wildcard grants uncontained access")
	}
	var absent *scopedNamespaceAuthority
	if absent.permits("team_id", 42) {
		t.Fatal("absent authority permits access")
	}
}

func TestScopedNamespaceMultipleFields(t *testing.T) {
	f := newScopedNamespaceAuthFixture(t, "team_id", "account_id")
	raw := f.scopedNamespaceToken(t, func(c jwt.MapClaims) {
		c["prd"] = map[string]map[string]string{
			"team_id": {"42": "read"}, "account_id": {"7": "write"},
		}
	})
	a, err := f.auth.verifyScopedNamespace(context.Background(), raw, "opaque_1", "catalog-not-the-tid")
	if err != nil || !a.permits("team_id", 42) || !a.permits("account_id", 7) {
		t.Fatal("configured generic predicates fail")
	}
	if a.permits("team_id", 7) || a.permits("account_id", 42) {
		t.Fatal("a different predicate supplies authority")
	}
	if _, err := f.auth.verifyScopedNamespace(context.Background(), f.scopedNamespaceToken(t, nil), "opaque_1", "catalog-not-the-tid"); !errors.Is(err, errUnsupportedAuthority) {
		t.Fatal("a predicate subset satisfies the configured field set")
	}
}

func TestScopedNamespaceRefusesClaims(t *testing.T) {
	f := newScopedNamespaceAuthFixture(t)
	tests := map[string]func(jwt.MapClaims){
		"tenant":          func(c jwt.MapClaims) { c["ten"] = "tenant" },
		"operator":        func(c jwt.MapClaims) { c["ten"] = "operator" },
		"missing org":     func(c jwt.MapClaims) { delete(c, "org") },
		"missing user":    func(c jwt.MapClaims) { delete(c, "usr") },
		"null user":       func(c jwt.MapClaims) { c["usr"] = nil },
		"blank user":      func(c jwt.MapClaims) { c["usr"] = " \t\n" },
		"long user":       func(c jwt.MapClaims) { c["usr"] = strings.Repeat("a", 201) },
		"long UTF16 user": func(c jwt.MapClaims) { c["usr"] = strings.Repeat("\U0001F642", 101) },
		"null delegation": func(c jwt.MapClaims) { c["dlg"] = nil },
		"text delegation": func(c jwt.MapClaims) { c["dlg"] = "true" },
		"missing routing": func(c jwt.MapClaims) { delete(c, "rtg") },
		"extra routing":   func(c jwt.MapClaims) { c["rtg"].(map[string]any)["url"] = "https://other.test" },
		"routing case": func(c jwt.MapClaims) {
			c["rtg"] = map[string]any{"B": "hoglite", "be": authTestBackend, "t": "opaque_1"}
		},
		"wrong backend": func(c jwt.MapClaims) { c["rtg"].(map[string]any)["be"] = "another-backend" },
		"wrong type":    func(c jwt.MapClaims) { c["rtg"].(map[string]any)["b"] = "postgres" },
		"wrong tenant":  func(c jwt.MapClaims) { c["rtg"].(map[string]any)["t"] = "opaque_2" },
		"wrong catalog": func(c jwt.MapClaims) { c["cat"] = map[string][]string{"another": {"read"}} },
		"two catalogs": func(c jwt.MapClaims) {
			c["cat"] = map[string][]string{"catalog-not-the-tid": {"read"}, "another": {"read"}}
		},
		"empty catalog":   func(c jwt.MapClaims) { c["cat"] = map[string][]string{"catalog-not-the-tid": {}} },
		"unknown catalog": func(c jwt.MapClaims) { c["cat"] = map[string][]string{"catalog-not-the-tid": {"future"}} },
		"mixed catalog":   func(c jwt.MapClaims) { c["cat"] = map[string][]string{"catalog-not-the-tid": {"read", "future"}} },
		"admin catalog":   func(c jwt.MapClaims) { c["cat"] = map[string][]string{"catalog-not-the-tid": {"admin"}} },
		"missing prd":     func(c jwt.MapClaims) { delete(c, "prd") },
		"null prd":        func(c jwt.MapClaims) { c["prd"] = nil },
		"empty prd":       func(c jwt.MapClaims) { c["prd"] = map[string]any{} },
		"unknown field":   func(c jwt.MapClaims) { c["prd"] = map[string]any{"account_id": map[string]string{"42": "read"}} },
		"extra field": func(c jwt.MapClaims) {
			c["prd"].(map[string]map[string]string)["account_id"] = map[string]string{"42": "read"}
		},
		"null field":       func(c jwt.MapClaims) { c["prd"] = map[string]any{"team_id": nil} },
		"empty field":      func(c jwt.MapClaims) { c["prd"] = map[string]any{"team_id": map[string]string{}} },
		"wrong field type": func(c jwt.MapClaims) { c["prd"] = map[string]any{"team_id": []string{"42"}} },
		"null scope":       func(c jwt.MapClaims) { c["scope"] = nil },
		"array scope":      func(c jwt.MapClaims) { c["scope"] = []string{"hoglake:catalog-not-the-tid:read"} },
		"wrong scope":      func(c jwt.MapClaims) { c["scope"] = "hoglake:another:read" },
		"wider scope":      func(c jwt.MapClaims) { c["scope"] = "hoglake:catalog-not-the-tid:write" },
		"extra scope":      func(c jwt.MapClaims) { c["scope"] = "hoglake:catalog-not-the-tid:read hoglake:another:read" },
		"bad scope":        func(c jwt.MapClaims) { c["scope"] = "hoglake:catalog-not-the-tid:read:extra" },
		"uppercase scope":  func(c jwt.MapClaims) { c["scope"] = "hoglake:Catalog:read" },
		"dot scope":        func(c jwt.MapClaims) { c["scope"] = "hoglake:catalog.name:read" },
		"tab scope":        func(c jwt.MapClaims) { c["scope"] = "hoglake:catalog-not-the-tid:read\tforeign:ignored" },
		"catalog case":     func(c jwt.MapClaims) { c["CAT"] = c["cat"]; delete(c, "cat") },
		"catalog alias":    func(c jwt.MapClaims) { c["CAT"] = c["cat"] },
		"kind case":        func(c jwt.MapClaims) { c["TEN"] = c["ten"]; delete(c, "ten") },
		"audience case":    func(c jwt.MapClaims) { c["AUD"] = c["aud"]; delete(c, "aud") },
	}
	for _, name := range []string{
		"profile", "aws", "primary_catalog", "remote_schemas", "version", "purpose",
		"sid", "generation", "session_deadline", "grants", "storage",
	} {
		tests["unsupported "+name] = func(c jwt.MapClaims) { c[name] = nil }
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			raw := f.scopedNamespaceToken(t, mutate)
			if _, err := f.auth.verifyScopedNamespace(context.Background(), raw, "opaque_1", "catalog-not-the-tid"); err == nil {
				t.Fatal("invalid scoped claims pass")
			}
		})
	}
}

func TestScopedNamespacePredicateGrammar(t *testing.T) {
	f := newScopedNamespaceAuthFixture(t)
	for _, value := range []string{"0", "-1", "+1", "01", "1.0", "1e1", " 1", "1 ", "9223372036854775808", "18446744073709551616", ""} {
		t.Run("value "+value, func(t *testing.T) {
			raw := f.scopedNamespaceToken(t, func(c jwt.MapClaims) {
				c["prd"] = map[string]map[string]string{"team_id": {value: "read"}}
			})
			if _, err := f.auth.verifyScopedNamespace(context.Background(), raw, "opaque_1", "catalog-not-the-tid"); err == nil {
				t.Fatal("noncanonical predicate value passes")
			}
		})
	}
	for name, values := range map[string]any{
		"mixed wildcard": map[string]string{"*": "read", "42": "write"},
		"unknown":        map[string]string{"42": "future"},
		"admin":          map[string]string{"42": "admin"},
		"array":          map[string]any{"42": []string{"read"}},
		"null":           map[string]any{"42": nil},
	} {
		t.Run(name, func(t *testing.T) {
			raw := f.scopedNamespaceToken(t, func(c jwt.MapClaims) { c["prd"] = map[string]any{"team_id": values} })
			if _, err := f.auth.verifyScopedNamespace(context.Background(), raw, "opaque_1", "catalog-not-the-tid"); err == nil {
				t.Fatal("invalid predicate permission passes")
			}
		})
	}
	for _, count := range []int{200, 201} {
		values := make(map[string]string, count)
		for i := 1; i <= count; i++ {
			values[fmt.Sprint(i)] = "read"
		}
		raw := f.scopedNamespaceToken(t, func(c jwt.MapClaims) { c["prd"] = map[string]any{"team_id": values} })
		_, err := f.auth.verifyScopedNamespace(context.Background(), raw, "opaque_1", "catalog-not-the-tid")
		if (err == nil) != (count == 200) {
			t.Fatal("predicate count limit changes")
		}
	}
}

func TestScopedNamespaceRefusalClasses(t *testing.T) {
	f := newScopedNamespaceAuthFixture(t)
	for _, tc := range []struct {
		name        string
		mutate      func(jwt.MapClaims)
		unsupported bool
	}{
		{"wrong tenant", func(c jwt.MapClaims) { c["rtg"].(map[string]any)["t"] = "opaque_2" }, true},
		{"wrong catalog", func(c jwt.MapClaims) { c["cat"] = map[string][]string{"another": {"read"}} }, true},
		{"unsupported profile", func(c jwt.MapClaims) { c["profile"] = "storage-session" }, true},
		{"wrong scope", func(c jwt.MapClaims) { c["scope"] = "hoglake:another:read" }, true},
		{"missing user", func(c jwt.MapClaims) { delete(c, "usr") }, false},
		{"malformed scope", func(c jwt.MapClaims) { c["scope"] = "hoglake:catalog-not-the-tid:admin" }, false},
		{"malformed predicate", func(c jwt.MapClaims) { c["prd"] = map[string]any{"team_id": map[string]string{"01": "read"}} }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := f.scopedNamespaceToken(t, tc.mutate)
			_, err := f.auth.verifyScopedNamespace(context.Background(), raw, "opaque_1", "catalog-not-the-tid")
			if err == nil || errors.Is(err, errUnsupportedAuthority) != tc.unsupported {
				t.Fatal("scoped refusal class changes")
			}
		})
	}
}

func TestScopedNamespaceTimes(t *testing.T) {
	f := newScopedNamespaceAuthFixture(t)
	for name, mutate := range map[string]func(jwt.MapClaims){
		"missing iat":   func(c jwt.MapClaims) { delete(c, "iat") },
		"missing exp":   func(c jwt.MapClaims) { delete(c, "exp") },
		"zero life":     func(c jwt.MapClaims) { c["exp"] = c["iat"] },
		"negative life": func(c jwt.MapClaims) { c["exp"] = f.now.Unix() - 1 },
		"long life":     func(c jwt.MapClaims) { c["exp"] = f.now.Unix() + 901 },
		"overflow":      func(c jwt.MapClaims) { c["iat"], c["exp"] = int64(math.MinInt64), int64(math.MaxInt64) },
		"null iat":      func(c jwt.MapClaims) { c["iat"] = nil },
		"float exp":     func(c jwt.MapClaims) { c["exp"] = float64(f.now.Unix()) + 0.5 },
		"text nbf":      func(c jwt.MapClaims) { c["nbf"] = "0" },
		"null nbf":      func(c jwt.MapClaims) { c["nbf"] = nil },
		"future nbf":    func(c jwt.MapClaims) { c["nbf"] = int64(math.MaxInt64) },
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := f.auth.verifyScopedNamespace(context.Background(), f.scopedNamespaceToken(t, mutate), "opaque_1", "catalog-not-the-tid"); err == nil {
				t.Fatal("invalid scoped lifetime passes")
			}
		})
	}
	for _, claim := range []string{"exp", "iat", "nbf"} {
		for _, seconds := range []int64{59, 60, 61} {
			raw := f.scopedNamespaceToken(t, func(c jwt.MapClaims) {
				switch claim {
				case "exp":
					c["exp"], c["iat"] = f.now.Unix()-seconds, f.now.Unix()-seconds-900
				case "iat":
					c["iat"], c["exp"] = f.now.Unix()+seconds, f.now.Unix()+seconds+900
				case "nbf":
					c["nbf"] = f.now.Unix() + seconds
				}
			})
			_, err := f.auth.verifyScopedNamespace(context.Background(), raw, "opaque_1", "catalog-not-the-tid")
			if (err == nil) != (seconds <= 60) {
				t.Fatalf("scoped clock boundary changes for %s at %d seconds", claim, seconds)
			}
		}
	}
	for _, user := range []string{strings.Repeat("a", 200), strings.Repeat("\U0001F642", 100)} {
		raw := f.scopedNamespaceToken(t, func(c jwt.MapClaims) {
			c["usr"] = user
			delete(c, "nbf")
		})
		if _, err := f.auth.verifyScopedNamespace(context.Background(), raw, "opaque_1", "catalog-not-the-tid"); err != nil {
			t.Fatal("valid caller or absent optional nbf fails")
		}
	}
}

func TestScopedNamespaceCryptoAndBounds(t *testing.T) {
	f := newScopedNamespaceAuthFixture(t)
	valid := f.scopedNamespaceToken(t, nil)
	parts := strings.Split(valid, ".")
	payload, _ := base64.RawURLEncoding.DecodeString(parts[1])
	duplicate := string(payload[:len(payload)-1]) + `,"prd":{"team_id":{"43":"read"}}}`
	input := parts[0] + "." + base64.RawURLEncoding.EncodeToString([]byte(duplicate))
	signature, err := jwt.SigningMethodES256.Sign(input, f.key)
	if err != nil {
		t.Fatal("cannot sign the synthetic duplicate")
	}
	for name, raw := range map[string]string{
		"malformed": "one.two",
		"padding":   parts[0] + "=." + parts[1] + "." + parts[2],
		"tampered": parts[0] + "." + base64.RawURLEncoding.EncodeToString(
			[]byte(strings.Replace(string(payload), "opaque_1", "opaque_2", 1))) + "." + parts[2],
		"signed duplicate": input + "." + base64.RawURLEncoding.EncodeToString(signature),
		"oversized":        strings.Repeat("x", scopedNamespaceMaxTokenSize+1),
		"bad audience":     f.scopedNamespaceToken(t, func(c jwt.MapClaims) { c["aud"] = []string{"hoglake", "hoglite", "hogtower-vend"} }),
		"bad issuer":       f.scopedNamespaceToken(t, func(c jwt.MapClaims) { c["iss"] = "https://other.test" }),
		"missing subject":  f.scopedNamespaceToken(t, func(c jwt.MapClaims) { delete(c, "sub") }),
		"missing jti":      f.scopedNamespaceToken(t, func(c jwt.MapClaims) { delete(c, "jti") }),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := f.auth.verifyScopedNamespace(context.Background(), raw, "opaque_1", "catalog-not-the-tid"); err == nil {
				t.Fatal("invalid signed input passes")
			}
		})
	}
	for _, size := range []int{30 << 10, 50 << 10} {
		raw := f.scopedNamespaceToken(t, func(c jwt.MapClaims) { c["padding"] = strings.Repeat("x", size) })
		if len(raw) <= maxTokenSize {
			t.Fatal("synthetic signed input does not exceed the legacy limit")
		}
		_, err := f.auth.verifyScopedNamespace(context.Background(), raw, "opaque_1", "catalog-not-the-tid")
		if (err == nil) != (len(raw) <= scopedNamespaceMaxTokenSize) {
			t.Fatal("scoped compact token limit changes")
		}
		if _, err := f.auth.verify(context.Background(), raw); err == nil || errors.Is(err, errUnsupportedAuthority) {
			t.Fatal("legacy compact token limit changes")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := f.auth.verifyScopedNamespace(ctx, valid, "opaque_1", "catalog-not-the-tid"); !errors.Is(err, context.Canceled) {
		t.Fatal("scoped verification ignores cancellation")
	}
}

func TestScopedNamespaceIssuerNeutralSignedFixture(t *testing.T) {
	raw, err := os.ReadFile("testdata/hoglake-metadata-vectors.json")
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
	if err := json.Unmarshal(raw, &file); err != nil {
		t.Fatal("cannot read the issuer-neutral fixture")
	}
	keys, err := parsePublicJWKS(file.JWKS)
	if err != nil {
		t.Fatal("cannot read fixture public keys")
	}
	now := time.Unix(file.VerifyAt, 0)
	a := &JWTAuth{
		issuer: file.Issuer, backend: authTestBackend, keys: keys,
		now: func() time.Time { return now }, fetched: now, tried: now,
		scopedNamespaceEnabled: true, scopedNamespaceFields: map[string]bool{"team_id": true},
	}
	found := false
	for _, vector := range file.Vectors {
		if vector.Name != "scoped_routed" {
			continue
		}
		found = true
		authority, err := a.verifyScopedNamespace(context.Background(), vector.Token, "t_3fa8c1d9e2_1", "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b")
		if err != nil || !authority.permits("team_id", 42) || !authority.permits("team_id", 7) || authority.permits("team_id", 43) {
			t.Fatal("issuer-neutral scoped authority differs")
		}
		if _, err := a.verify(context.Background(), vector.Token); !errors.Is(err, errUnsupportedAuthority) {
			t.Fatal("issuer-neutral scoped fixture changes the SQL refusal")
		}
	}
	if !found {
		t.Fatal("the issuer-neutral scoped fixture is absent")
	}
}
