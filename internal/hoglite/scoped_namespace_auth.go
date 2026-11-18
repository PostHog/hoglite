package hoglite

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"unicode/utf16"
)

const scopedNamespaceMaxTokenSize = 64 << 10

type scopedNamespaceAuthority struct {
	predicates map[string]map[string]string
}

func scopedNamespaceConfig(cfg AuthConfig) (map[string]bool, error) {
	if !cfg.ScopedNamespaceEnabled {
		return nil, nil
	}
	if cfg.Mode != "enforce" || cfg.JWKSFile == "" || cfg.JWKSURL != "" {
		return nil, errors.New("scoped namespace reads require enforced authentication with a JWKS file")
	}
	fields := make(map[string]bool, len(cfg.ScopedNamespacePredicateFields))
	for _, field := range cfg.ScopedNamespacePredicateFields {
		if !scopedNamespaceField(field) || fields[field] {
			return nil, errors.New("invalid scoped namespace predicate fields")
		}
		fields[field] = true
	}
	if len(fields) == 0 {
		return nil, errors.New("scoped namespace reads require predicate fields")
	}
	return fields, nil
}

func scopedNamespaceField(field string) bool {
	if len(field) == 0 || len(field) > 63 || field[0] < 'a' || field[0] > 'z' {
		return false
	}
	for _, c := range field[1:] {
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '_' {
			return false
		}
	}
	return true
}

// permits uses only the persisted containment of the selected namespace.
func (a *scopedNamespaceAuthority) permits(field string, value int64) bool {
	if a == nil || value <= 0 || !scopedNamespaceField(field) {
		return false
	}
	values := a.predicates[field]
	return scopedNamespaceRead(values["*"]) || scopedNamespaceRead(values[strconv.FormatInt(value, 10)])
}

func scopedNamespaceRead(permission string) bool {
	return permission == "read" || permission == "write"
}

func scopedNamespaceScopeCatalog(catalog string) bool {
	if len(catalog) == 0 || len(catalog) > 63 {
		return false
	}
	for i, c := range catalog {
		alphanumeric := (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')
		if !alphanumeric && (i == 0 || (c != '_' && c != '-')) {
			return false
		}
	}
	return true
}

func (a *JWTAuth) verifyScopedNamespace(ctx context.Context, raw, tenant, catalog string) (*scopedNamespaceAuthority, error) {
	if a == nil || !a.scopedNamespaceEnabled {
		return nil, errUnsupportedAuthority
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	payload, err := metadataTokenPayload(raw, scopedNamespaceMaxTokenSize)
	if err != nil {
		return nil, err
	}
	for name := range payload {
		switch strings.ToLower(name) {
		case "iss", "aud", "sub", "jti", "ten", "org", "cat", "rtg", "usr", "prd", "scope", "dlg",
			"exp", "iat", "nbf", "profile", "aws", "primary_catalog", "remote_schemas", "version",
			"purpose", "sid", "generation", "session_deadline", "grants", "storage":
			if name != strings.ToLower(name) {
				return nil, errors.New("invalid scoped claim name")
			}
		}
	}
	if err := a.scopedNamespaceTimes(payload); err != nil {
		return nil, err
	}
	claims, err := a.verifyMetadataToken(ctx, raw)
	if err != nil {
		return nil, err
	}
	if claims.Kind != "scoped" || claims.Org == "" || len(claims.Cat) != 1 {
		return nil, errUnsupportedAuthority
	}
	for _, name := range []string{
		"profile", "aws", "primary_catalog", "remote_schemas", "version", "purpose",
		"sid", "generation", "session_deadline", "grants", "storage",
	} {
		if _, present := payload[name]; present {
			return nil, errUnsupportedAuthority
		}
	}
	if delegation, present := payload["dlg"]; present {
		if value := strings.TrimSpace(string(delegation)); value != "true" && value != "false" {
			return nil, errors.New("invalid delegation claim")
		}
	}
	var user string
	if json.Unmarshal(payload["usr"], &user) != nil || strings.TrimSpace(user) == "" || len(utf16.Encode([]rune(user))) > 200 {
		return nil, errors.New("invalid caller identity")
	}
	if !validTID(tenant) || catalog == "" || claims.Rtg == nil ||
		claims.Rtg.Backend != "hoglite" || claims.Rtg.ID != a.backend || claims.Rtg.Tenant != tenant {
		return nil, errUnsupportedAuthority
	}
	var routing map[string]json.RawMessage
	if json.Unmarshal(payload["rtg"], &routing) != nil || len(routing) != 3 {
		return nil, errors.New("invalid routing fields")
	}
	for _, name := range []string{"b", "be", "t"} {
		if _, present := routing[name]; !present {
			return nil, errors.New("invalid routing fields")
		}
	}
	permissions, ok := claims.Cat[catalog]
	if !ok || len(permissions) == 0 {
		return nil, errUnsupportedAuthority
	}
	for _, permission := range permissions {
		if !scopedNamespaceRead(permission) {
			return nil, errors.New("invalid scoped catalog permission")
		}
	}
	if err := scopedNamespaceScope(payload, catalog, permissions); err != nil {
		return nil, err
	}
	var predicates map[string]map[string]string
	if json.Unmarshal(payload["prd"], &predicates) != nil || len(predicates) == 0 {
		return nil, errors.New("invalid namespace predicates")
	}
	if len(predicates) != len(a.scopedNamespaceFields) {
		return nil, errUnsupportedAuthority
	}
	for field, values := range predicates {
		if !a.scopedNamespaceFields[field] {
			return nil, errUnsupportedAuthority
		}
		if len(values) == 0 || len(values) > 200 {
			return nil, errors.New("invalid namespace predicates")
		}
		for value, permission := range values {
			if !scopedNamespaceRead(permission) {
				return nil, errors.New("invalid namespace predicate permission")
			}
			if value == "*" {
				if len(values) != 1 {
					return nil, errors.New("invalid namespace predicate wildcard")
				}
				continue
			}
			n, err := strconv.ParseInt(value, 10, 64)
			if err != nil || n <= 0 || strconv.FormatInt(n, 10) != value {
				return nil, errors.New("invalid namespace predicate value")
			}
		}
	}
	return &scopedNamespaceAuthority{predicates: predicates}, nil
}

func (a *JWTAuth) scopedNamespaceTimes(payload map[string]json.RawMessage) error {
	integer := func(name string) (int64, error) {
		var n json.Number
		if json.Unmarshal(payload[name], &n) != nil {
			return 0, errors.New("invalid scoped token time")
		}
		return n.Int64()
	}
	exp, e1 := integer("exp")
	iat, e2 := integer("iat")
	now := a.now().Unix()
	if e1 != nil || e2 != nil || exp <= iat || uint64(exp)-uint64(iat) > 900 ||
		exp < now-60 || iat > now+60 {
		return errors.New("invalid scoped token lifetime")
	}
	if _, present := payload["nbf"]; present {
		nbf, err := integer("nbf")
		if err != nil || nbf > now+60 {
			return errors.New("invalid scoped token not-before time")
		}
	}
	return nil
}

func scopedNamespaceScope(payload map[string]json.RawMessage, catalog string, permissions []string) error {
	raw, present := payload["scope"]
	if !present {
		return nil
	}
	var scope *string
	if json.Unmarshal(raw, &scope) != nil || scope == nil {
		return errors.New("invalid metadata scope")
	}
	expected := make(map[string]bool)
	for _, permission := range permissions {
		if scopedNamespaceRead(permission) {
			expected["hoglake:"+catalog+":"+permission] = true
		}
	}
	actual := make(map[string]bool)
	for _, grant := range strings.Split(*scope, " ") {
		if strings.HasPrefix(grant, "hoglake:") {
			parts := strings.Split(grant, ":")
			if len(parts) != 3 || !scopedNamespaceScopeCatalog(parts[1]) || !scopedNamespaceRead(parts[2]) {
				return errors.New("invalid metadata scope grant")
			}
			actual[grant] = true
		}
	}
	if len(actual) == 0 {
		return nil
	}
	if len(actual) != len(expected) {
		return errUnsupportedAuthority
	}
	for grant := range actual {
		if !expected[grant] {
			return errUnsupportedAuthority
		}
	}
	return nil
}
