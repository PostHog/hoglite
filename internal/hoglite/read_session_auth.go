package hoglite

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf16"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

const (
	readSessionMaxHeaderLength = 10923
	readSessionClockSkew       = int64(60)
	readSessionMaxLifetime     = uint64(900)
	readSessionMaxKeyLength    = 1024
	readSessionMaxUserUnits    = 200
	readSessionPurpose         = "user-session"
	readSessionExperimentalID  = "storage-session"
)

var (
	readSessionAudiences = map[string]bool{
		"trino-session":   true,
		"hoglake-session": true,
		"hoglite-session": true,
	}
	readSessionPositiveIntRe  = regexp.MustCompile(`^[1-9][0-9]*$`)
	readSessionRoutedTenantRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,125}_[1-9][0-9]*$`)
	readSessionBucketRe       = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)
	readSessionSegmentRe      = regexp.MustCompile(`^[A-Za-z0-9_.=-]+$`)
	readSessionRegionRe       = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}$`)
)

// ReadSessionConfig enables dedicated read-session verification on the typed namespace route.
// A nil pointer keeps the dedicated verifier disabled.
type ReadSessionConfig struct {
	Profile       string
	RemoteTargets []string
	Bucket        string
	Prefix        string
	Region        string
}

type readSessionProfile struct {
	profile       string
	remoteTargets [2]string
	bucket        string
	prefix        string
	region        string
}

type readSessionCredentials struct {
	accessKeyID     string
	secretAccessKey string
	sessionToken    string
	region          string
	bucket          string
	key             string
	expiresAt       int64
}

func (readSessionCredentials) String() string   { return "read-session-credentials(redacted)" }
func (readSessionCredentials) GoString() string { return "read-session-credentials(redacted)" }

type readSessionClaims struct {
	subject        string
	org            string
	catalog        string
	backend        string
	tenant         string
	predicates     map[string]map[string]string
	manifestBucket string
	manifestRegion string
	manifestKey    string
	creds          readSessionCredentials
}

type readSessionAuthority struct {
	authority *scopedNamespaceAuthority
	creds     readSessionCredentials
}

const (
	readSessionErrInvalidToken    = "invalid_token"
	readSessionErrInvalidAudience = "invalid_audience"
	readSessionErrTokenExpired    = "token_expired"
	readSessionErrNotYetValid     = "not_yet_valid"
	readSessionErrUnsupportedAlg  = "unsupported_alg"
	readSessionErrUnknownKey      = "unknown_key"
	readSessionErrInvalidIssuer   = "invalid_issuer"
)

type readSessionError struct {
	reason string
}

func (e *readSessionError) Error() string { return "read session refused: " + e.reason }

func readSessionConfig(cfg AuthConfig) (*readSessionProfile, error) {
	if cfg.ReadSession == nil {
		return nil, nil
	}
	rs := cfg.ReadSession
	if !cfg.ScopedNamespaceEnabled || cfg.Mode != "enforce" || cfg.JWKSFile == "" || cfg.JWKSURL != "" {
		return nil, errors.New("read sessions require scoped namespace reads with enforced JWKS file auth")
	}
	if len(cfg.ScopedNamespacePredicateFields) != 1 || cfg.ScopedNamespacePredicateFields[0] != "team_id" {
		return nil, errors.New("read sessions require team_id as the only predicate field")
	}
	if strings.TrimSpace(rs.Profile) == "" || strings.TrimSpace(rs.Profile) != rs.Profile ||
		len(rs.Profile) > 256 || rs.Profile == readSessionExperimentalID {
		return nil, errors.New("invalid read session profile")
	}
	if len(rs.RemoteTargets) != 2 {
		return nil, errors.New("read sessions require two distinct remote targets")
	}
	t0, t1 := rs.RemoteTargets[0], rs.RemoteTargets[1]
	if strings.TrimSpace(t0) == "" || strings.TrimSpace(t0) != t0 ||
		strings.TrimSpace(t1) == "" || strings.TrimSpace(t1) != t1 ||
		t0 == t1 || t0 == cfg.BackendID || t1 == cfg.BackendID {
		return nil, errors.New("read sessions require two distinct remote targets")
	}
	if !validReadSessionBucket(rs.Bucket) {
		return nil, errors.New("invalid read session control bucket")
	}
	prefix := strings.Trim(rs.Prefix, "/")
	if prefix == "" || !validReadSessionPathSegments(prefix) {
		return nil, errors.New("read sessions require a nonempty canonical control prefix")
	}
	if !readSessionRegionRe.MatchString(rs.Region) {
		return nil, errors.New("invalid read session control region")
	}
	return &readSessionProfile{
		profile:       rs.Profile,
		remoteTargets: [2]string{t0, t1},
		bucket:        rs.Bucket,
		prefix:        prefix,
		region:        rs.Region,
	}, nil
}

func validReadSessionBucket(bucket string) bool {
	return readSessionBucketRe.MatchString(bucket) && !strings.Contains(bucket, "..")
}

func validReadSessionPathSegments(path string) bool {
	if path == "" {
		return false
	}
	for _, seg := range strings.Split(path, "/") {
		if seg == "." || seg == ".." || !readSessionSegmentRe.MatchString(seg) {
			return false
		}
	}
	return true
}

func (a *JWTAuth) readSessionEnabled() bool {
	return a != nil && a.readSession != nil
}

// verifyReadSession verifies a dedicated read-session token and binds it to the route.
func (a *JWTAuth) verifyReadSession(ctx context.Context, raw, tenant, catalog string) (*readSessionAuthority, error) {
	if !a.readSessionEnabled() {
		return nil, errUnsupportedAuthority
	}
	claims, err := a.verifyReadSessionToken(ctx, raw)
	if err != nil {
		return nil, err
	}
	rs := a.readSession
	if !validTID(tenant) || catalog == "" ||
		claims.backend != a.backend || claims.tenant != tenant || claims.catalog != catalog ||
		claims.manifestBucket != rs.bucket || claims.manifestRegion != rs.region ||
		claims.creds.region != rs.region || claims.manifestKey != manifestKey(rs.prefix, tenant) {
		return nil, errUnsupportedAuthority
	}
	return &readSessionAuthority{
		authority: &scopedNamespaceAuthority{predicates: claims.predicates},
		creds:     claims.creds,
	}, nil
}

// verifyReadSessionToken checks signature and claims against the read-session contract.
func (a *JWTAuth) verifyReadSessionToken(ctx context.Context, raw string) (*readSessionClaims, error) {
	if !a.readSessionEnabled() {
		return nil, errUnsupportedAuthority
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(raw) > scopedNamespaceMaxTokenSize {
		return nil, &readSessionError{reason: readSessionErrInvalidToken}
	}
	firstDot := strings.IndexByte(raw, '.')
	if firstDot < 1 || firstDot > readSessionMaxHeaderLength {
		return nil, &readSessionError{reason: readSessionErrInvalidToken}
	}
	parts := strings.Split(raw, ".")
	if len(parts) != 3 {
		return nil, &readSessionError{reason: readSessionErrInvalidToken}
	}
	header, err := decodeStrictJSONObject(parts[0])
	if err != nil {
		return nil, &readSessionError{reason: readSessionErrInvalidToken}
	}
	if crit, ok := header["crit"]; ok && string(bytes.TrimSpace(crit)) != "[]" {
		return nil, &readSessionError{reason: readSessionErrInvalidToken}
	}
	if _, ok := header["b64"]; ok {
		return nil, &readSessionError{reason: readSessionErrInvalidToken}
	}
	alg, ok := readSessionText(header, "alg")
	if !ok {
		return nil, &readSessionError{reason: readSessionErrInvalidToken}
	}
	if alg != "ES256" {
		return nil, &readSessionError{reason: readSessionErrUnsupportedAlg}
	}
	kid, ok := readSessionText(header, "kid")
	if !ok || len(kid) > 256 {
		return nil, &readSessionError{reason: readSessionErrUnknownKey}
	}
	key, err := a.key(ctx, kid, "ES256")
	if err != nil {
		return nil, &readSessionError{reason: readSessionErrUnknownKey}
	}
	sig, err := base64.RawURLEncoding.Strict().DecodeString(parts[2])
	if err != nil || len(sig) != 64 {
		return nil, &readSessionError{reason: readSessionErrInvalidToken}
	}
	if err := jwt.SigningMethodES256.Verify(parts[0]+"."+parts[1], sig, key); err != nil {
		return nil, &readSessionError{reason: readSessionErrInvalidToken}
	}
	payload, err := decodeStrictJSONObject(parts[1])
	if err != nil {
		return nil, &readSessionError{reason: readSessionErrInvalidToken}
	}
	if hasCaseVariantClaim(payload) {
		return nil, &readSessionError{reason: readSessionErrInvalidToken}
	}
	iss, ok := readSessionText(payload, "iss")
	if !ok || iss != a.issuer {
		return nil, &readSessionError{reason: readSessionErrInvalidIssuer}
	}
	return a.readSession.validate(payload, a.now().Unix())
}

func decodeStrictJSONObject(segment string) (map[string]json.RawMessage, error) {
	b, err := base64.RawURLEncoding.Strict().DecodeString(segment)
	if err != nil || strictJSON(b) != nil {
		return nil, errors.New("invalid token JSON")
	}
	if len(bytes.TrimSpace(b)) == 0 || bytes.TrimSpace(b)[0] != '{' {
		return nil, errors.New("token segment is not an object")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(b, &fields); err != nil || fields == nil {
		return nil, errors.New("token segment is not an object")
	}
	return fields, nil
}

func hasCaseVariantClaim(payload map[string]json.RawMessage) bool {
	for name := range payload {
		lower := strings.ToLower(name)
		switch lower {
		case "iss", "aud", "sub", "usr", "jti", "ten", "org", "cat", "rtg", "prd",
			"scope", "dlg", "svc", "exp", "iat", "nbf", "profile", "version", "purpose",
			"primary_catalog", "remote_schemas", "sid", "generation", "session_deadline",
			"aws", "grants", "storage":
			if name != lower {
				return true
			}
		}
	}
	return false
}

func (p *readSessionProfile) validate(payload map[string]json.RawMessage, now int64) (*readSessionClaims, error) {
	invalid := func() (*readSessionClaims, error) {
		return nil, &readSessionError{reason: readSessionErrInvalidToken}
	}
	if !validateReadSessionAudiences(payload["aud"]) {
		return nil, &readSessionError{reason: readSessionErrInvalidAudience}
	}
	profile, ok := readSessionText(payload, "profile")
	if !ok || profile != p.profile {
		return invalid()
	}
	version, ok := readSessionPositiveInt(payload, "version")
	if !ok || version != 1 {
		return invalid()
	}
	purpose, ok := readSessionText(payload, "purpose")
	if !ok || purpose != readSessionPurpose {
		return invalid()
	}
	ten, ok := readSessionText(payload, "ten")
	if !ok || ten != "scoped" {
		return invalid()
	}
	sub, ok1 := readSessionText(payload, "sub")
	usr, ok2 := readSessionText(payload, "usr")
	if !ok1 || !ok2 || sub != usr || len(utf16.Encode([]rune(usr))) > readSessionMaxUserUnits {
		return invalid()
	}
	for _, forbidden := range []string{"dlg", "scope", "svc"} {
		if _, present := payload[forbidden]; present {
			return invalid()
		}
	}
	org, ok1 := readSessionText(payload, "org")
	_, ok2 = readSessionText(payload, "sid")
	_, ok3 := readSessionText(payload, "jti")
	if !ok1 || !ok2 || !ok3 {
		return invalid()
	}
	gen, ok := readSessionPositiveInt(payload, "generation")
	if !ok || gen != 1 {
		return invalid()
	}
	primary, ok := readSessionText(payload, "primary_catalog")
	if !ok {
		return invalid()
	}
	if !requireSingleReadMap(payload["cat"], primary) || !requireSingleReadMap(payload["remote_schemas"], "team") {
		return invalid()
	}
	teamKey, ok := validateReadSessionPredicate(payload["prd"])
	if !ok {
		return invalid()
	}
	iat, ok1 := readSessionPositiveInt(payload, "iat")
	nbf, ok2 := readSessionPositiveInt(payload, "nbf")
	exp, ok3 := readSessionPositiveInt(payload, "exp")
	deadline, ok4 := readSessionPositiveInt(payload, "session_deadline")
	if !ok1 || !ok2 || !ok3 || !ok4 {
		return invalid()
	}
	creds, ok := parseReadSessionAWS(payload["aws"])
	if !ok {
		return invalid()
	}
	if nbf > iat || iat >= exp || exp > creds.expiresAt || creds.expiresAt > deadline {
		return invalid()
	}
	if uint64(exp)-uint64(iat) > readSessionMaxLifetime {
		return invalid()
	}
	// AWS credential expiry allows zero clock skew.
	if now >= creds.expiresAt || (now > exp && now-exp > readSessionClockSkew) {
		return nil, &readSessionError{reason: readSessionErrTokenExpired}
	}
	if iat > now && (now < 0 || uint64(iat)-uint64(now) > uint64(readSessionClockSkew)) {
		return nil, &readSessionError{reason: readSessionErrNotYetValid}
	}
	backend, tenant, ok := parseReadSessionRouting(payload["rtg"])
	if !ok {
		return invalid()
	}
	manifestBucket, manifestRegion, manifestObjKey, ok := p.validateGrants(payload["grants"], primary, backend, tenant, creds.region)
	if !ok {
		return invalid()
	}
	creds.bucket = manifestBucket
	creds.key = manifestObjKey
	return &readSessionClaims{
		subject:        sub,
		org:            org,
		catalog:        primary,
		backend:        backend,
		tenant:         tenant,
		predicates:     map[string]map[string]string{"team_id": {teamKey: "read"}},
		manifestBucket: manifestBucket,
		manifestRegion: manifestRegion,
		manifestKey:    manifestObjKey,
		creds:          creds,
	}, nil
}

func validateReadSessionAudiences(raw json.RawMessage) bool {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '[' {
		return false
	}
	var items []json.RawMessage
	if json.Unmarshal(trimmed, &items) != nil || len(items) != len(readSessionAudiences) {
		return false
	}
	seen := make(map[string]bool, len(items))
	for _, item := range items {
		t := bytes.TrimSpace(item)
		if len(t) == 0 || t[0] != '"' {
			return false
		}
		var aud string
		if json.Unmarshal(t, &aud) != nil || !readSessionAudiences[aud] || seen[aud] {
			return false
		}
		seen[aud] = true
	}
	return len(seen) == len(readSessionAudiences)
}

func requireSingleReadMap(raw json.RawMessage, expectedKey string) bool {
	obj, ok := parseJSONObject(raw)
	if !ok || len(obj) != 1 {
		return false
	}
	rawScopes, present := obj[expectedKey]
	if !present {
		return false
	}
	trimmed := bytes.TrimSpace(rawScopes)
	if len(trimmed) == 0 || trimmed[0] != '[' {
		return false
	}
	var scopes []json.RawMessage
	if json.Unmarshal(trimmed, &scopes) != nil || len(scopes) != 1 {
		return false
	}
	s := bytes.TrimSpace(scopes[0])
	if len(s) == 0 || s[0] != '"' {
		return false
	}
	var word string
	return json.Unmarshal(s, &word) == nil && word == "read"
}

func validateReadSessionPredicate(raw json.RawMessage) (string, bool) {
	prd, ok := parseJSONObject(raw)
	if !ok || len(prd) != 1 {
		return "", false
	}
	teams, ok := parseJSONObject(prd["team_id"])
	if !ok || len(teams) != 1 {
		return "", false
	}
	for k, v := range teams {
		if !readSessionPositiveIntRe.MatchString(k) {
			return "", false
		}
		if n, err := strconv.ParseInt(k, 10, 64); err != nil || n <= 0 {
			return "", false
		}
		t := bytes.TrimSpace(v)
		if len(t) == 0 || t[0] != '"' {
			return "", false
		}
		var perm string
		if json.Unmarshal(t, &perm) != nil || perm != "read" {
			return "", false
		}
		return k, true
	}
	return "", false
}

func parseReadSessionAWS(raw json.RawMessage) (readSessionCredentials, bool) {
	var creds readSessionCredentials
	obj, ok := parseJSONObject(raw)
	if !ok || len(obj) != 5 {
		return creds, false
	}
	ak, ok1 := readSessionText(obj, "access_key_id")
	sk, ok2 := readSessionText(obj, "secret_access_key")
	st, ok3 := readSessionText(obj, "session_token")
	reg, ok4 := readSessionText(obj, "region")
	exp, ok5 := readSessionPositiveInt(obj, "expires_at")
	if !ok1 || !ok2 || !ok3 || !ok4 || !ok5 {
		return creds, false
	}
	return readSessionCredentials{
		accessKeyID:     ak,
		secretAccessKey: sk,
		sessionToken:    st,
		region:          reg,
		expiresAt:       exp,
	}, true
}

func parseReadSessionRouting(raw json.RawMessage) (string, string, bool) {
	obj, ok := parseJSONObject(raw)
	if !ok || len(obj) != 3 {
		return "", "", false
	}
	b, ok := readSessionText(obj, "b")
	if !ok || b != "hoglite" {
		return "", "", false
	}
	backend, ok := readSessionText(obj, "be")
	if !ok {
		return "", "", false
	}
	id, err := uuid.Parse(backend)
	if err != nil || id.String() != backend || id == uuid.Nil {
		return "", "", false
	}
	tenant, ok := readSessionText(obj, "t")
	if !ok || len(tenant) > 128 || !validTID(tenant) || !readSessionRoutedTenantRe.MatchString(tenant) || strings.Contains(tenant, "..") {
		return "", "", false
	}
	idx := strings.LastIndexByte(tenant, '_')
	if idx < 0 {
		return "", "", false
	}
	inc := tenant[idx+1:]
	if !readSessionPositiveIntRe.MatchString(inc) {
		return "", "", false
	}
	if n, err := strconv.ParseInt(inc, 10, 64); err != nil || n <= 0 {
		return "", "", false
	}
	return backend, tenant, true
}

func (p *readSessionProfile) validateGrants(raw json.RawMessage, catalog, backend, tenant, region string) (string, string, string, bool) {
	targets := map[string]bool{
		p.remoteTargets[0]: true,
		p.remoteTargets[1]: true,
		catalog:            true,
		backend:            true,
	}
	if len(targets) != 4 {
		return "", "", "", false
	}
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '[' {
		return "", "", "", false
	}
	var items []json.RawMessage
	if json.Unmarshal(trimmed, &items) != nil || len(items) < len(targets) {
		return "", "", "", false
	}
	manifestSuffix := "/" + TenantShard(tenant) + "/t/" + tenant + "/manifest"
	seen := make(map[string]bool, len(targets))
	var prevBucket, prevKey string
	var manifestBucket, manifestRegion, manifestObjKey string
	for _, item := range items {
		grant, ok := parseJSONObject(item)
		if !ok || len(grant) != 6 {
			return "", "", "", false
		}
		_, hasObjectKey := grant["object_key"]
		_, hasPrefix := grant["prefix"]
		if hasObjectKey == hasPrefix {
			return "", "", "", false
		}
		bucket, ok1 := readSessionText(grant, "bucket")
		keyField := "object_key"
		if hasPrefix {
			keyField = "prefix"
		}
		key, ok2 := readSessionText(grant, keyField)
		target, ok3 := readSessionText(grant, "target")
		grantCat, ok4 := readSessionText(grant, "catalog")
		grantReg, ok5 := readSessionText(grant, "region")
		if !ok1 || !ok2 || !ok3 || !ok4 || !ok5 {
			return "", "", "", false
		}
		if !validReadSessionBucket(bucket) || len(key) > readSessionMaxKeyLength ||
			hasPrefix != strings.HasSuffix(key, "/") ||
			!validReadSessionPathSegments(strings.TrimSuffix(key, "/")) ||
			!targets[target] || grantCat != catalog || grantReg != region ||
			!isGetObjectOnly(grant["actions"]) {
			return "", "", "", false
		}
		if target == backend {
			if hasPrefix || !strings.HasSuffix(key, manifestSuffix) || seen[target] {
				return "", "", "", false
			}
			manifestBucket, manifestRegion, manifestObjKey = bucket, grantReg, key
		}
		overlaps := strings.HasSuffix(prevKey, "/") && strings.HasPrefix(key, prevKey)
		if bucket < prevBucket || (bucket == prevBucket && (key <= prevKey || overlaps)) {
			return "", "", "", false
		}
		prevBucket, prevKey = bucket, key
		seen[target] = true
	}
	if len(seen) != len(targets) || manifestObjKey == "" {
		return "", "", "", false
	}
	return manifestBucket, manifestRegion, manifestObjKey, true
}

func isGetObjectOnly(raw json.RawMessage) bool {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '[' {
		return false
	}
	var actions []json.RawMessage
	if json.Unmarshal(trimmed, &actions) != nil || len(actions) != 1 {
		return false
	}
	a := bytes.TrimSpace(actions[0])
	if len(a) == 0 || a[0] != '"' {
		return false
	}
	var action string
	return json.Unmarshal(a, &action) == nil && action == "s3:GetObject"
}

func parseJSONObject(raw json.RawMessage) (map[string]json.RawMessage, bool) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return nil, false
	}
	var obj map[string]json.RawMessage
	if json.Unmarshal(trimmed, &obj) != nil || obj == nil {
		return nil, false
	}
	return obj, true
}

func readSessionText(obj map[string]json.RawMessage, field string) (string, bool) {
	raw, ok := obj[field]
	if !ok {
		return "", false
	}
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '"' {
		return "", false
	}
	var s string
	if json.Unmarshal(trimmed, &s) != nil || strings.TrimSpace(s) == "" {
		return "", false
	}
	return s, true
}

func readSessionPositiveInt(obj map[string]json.RawMessage, field string) (int64, bool) {
	raw, ok := obj[field]
	if !ok {
		return 0, false
	}
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] < '1' || trimmed[0] > '9' {
		return 0, false
	}
	for _, b := range trimmed {
		if b < '0' || b > '9' {
			return 0, false
		}
	}
	n, err := strconv.ParseInt(string(trimmed), 10, 64)
	if err != nil || n <= 0 {
		return 0, false
	}
	return n, true
}
