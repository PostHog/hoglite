package hoglite

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const readSessionTestPath = "/v1/t/" + readSessionTestTenant + "/metadata/v1/catalogs/" + readSessionTestCatalog + "/namespaces/reports"

type s3CallerPolicy struct {
	secretKey    string
	sessionToken string
	allowedPaths map[string]bool
}

type s3RequestRecord struct {
	accessKeyID string
	tokenDigest string
	region      string
	method      string
	path        string
	status      int
	sigValid    bool
	securityTok bool
}

// localS3Fixture verifies real AWS SigV4 signatures and enforces per-caller object policies.
type localS3Fixture struct {
	srv       *httptest.Server
	mu        sync.Mutex
	objects   map[string][]byte
	callers   map[string]s3CallerPolicy
	records   []s3RequestRecord
	onRequest func(call int, path string)
}

func newLocalS3Fixture(t *testing.T) *localS3Fixture {
	t.Helper()
	f := &localS3Fixture{
		objects: map[string][]byte{},
		callers: map[string]s3CallerPolicy{},
	}
	f.srv = httptest.NewTLSServer(http.HandlerFunc(f.serveHTTP))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *localS3Fixture) s3Config() ReadSessionS3Config {
	cli := f.srv.Client()
	cli.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return ReadSessionS3Config{
		BaseEndpoint: f.srv.URL,
		UsePathStyle: true,
		HTTPClient:   cli,
	}
}

func (f *localS3Fixture) putObject(bucket, key string, data []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objects["/"+bucket+"/"+key] = append([]byte(nil), data...)
}

func (f *localS3Fixture) setCaller(accessKeyID, secretKey, sessionToken string, allowedPaths ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	allowed := make(map[string]bool, len(allowedPaths))
	for _, p := range allowedPaths {
		allowed[p] = true
	}
	f.callers[accessKeyID] = s3CallerPolicy{
		secretKey:    secretKey,
		sessionToken: sessionToken,
		allowedPaths: allowed,
	}
}

func (f *localS3Fixture) snapshotRecords() []s3RequestRecord {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]s3RequestRecord, len(f.records))
	copy(out, f.records)
	return out
}

func (f *localS3Fixture) resetRecords() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.records = nil
}

func (f *localS3Fixture) serveHTTP(w http.ResponseWriter, r *http.Request) {
	bodyBytes, _ := io.ReadAll(r.Body)
	authHeader := r.Header.Get("Authorization")
	sessionToken := r.Header.Get("X-Amz-Security-Token")
	ak, date, region, signedHeaders, sigHex, parsed := parseSigV4Auth(authHeader)
	tokHash := sha256.Sum256([]byte(sessionToken))
	rec := s3RequestRecord{
		accessKeyID: ak,
		tokenDigest: hex.EncodeToString(tokHash[:]),
		region:      region,
		method:      r.Method,
		path:        r.URL.Path,
		securityTok: sessionToken != "",
	}

	f.mu.Lock()
	policy, knownCaller := f.callers[ak]
	hook := f.onRequest
	callNum := len(f.records) + 1
	f.mu.Unlock()

	if !parsed || !knownCaller || sessionToken != policy.sessionToken {
		rec.status = http.StatusForbidden
		f.appendRecord(rec)
		writeS3XML(w, http.StatusForbidden, "InvalidAccessKeyId", "invalid caller credentials")
		return
	}
	if !verifySigV4(r, bodyBytes, policy.secretKey, date, region, signedHeaders, sigHex) {
		rec.status = http.StatusForbidden
		f.appendRecord(rec)
		writeS3XML(w, http.StatusForbidden, "SignatureDoesNotMatch", "invalid signature")
		return
	}
	rec.sigValid = true

	if r.Method != http.MethodGet || !policy.allowedPaths[r.URL.Path] {
		rec.status = http.StatusForbidden
		f.appendRecord(rec)
		writeS3XML(w, http.StatusForbidden, "AccessDenied", "access denied")
		return
	}

	f.mu.Lock()
	data, exists := f.objects[r.URL.Path]
	if exists {
		data = append([]byte(nil), data...)
	}
	f.mu.Unlock()

	if !exists {
		rec.status = http.StatusNotFound
		f.appendRecord(rec)
		writeS3XML(w, http.StatusNotFound, "NoSuchKey", "not found")
		return
	}

	rec.status = http.StatusOK
	f.appendRecord(rec)
	if hook != nil {
		hook(callNum, r.URL.Path)
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("ETag", `"fixture-etag"`)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

func (f *localS3Fixture) appendRecord(rec s3RequestRecord) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.records = append(f.records, rec)
}

func writeS3XML(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><Error><Code>` + code + `</Code><Message>` + msg + `</Message></Error>`))
}

func parseSigV4Auth(header string) (ak, date, region string, signedHeaders []string, sigHex string, ok bool) {
	if !strings.HasPrefix(header, "AWS4-HMAC-SHA256 ") {
		return "", "", "", nil, "", false
	}
	parts := strings.Split(strings.TrimPrefix(header, "AWS4-HMAC-SHA256 "), ",")
	kv := map[string]string{}
	for _, p := range parts {
		p = strings.TrimSpace(p)
		k, v, found := strings.Cut(p, "=")
		if !found {
			return "", "", "", nil, "", false
		}
		kv[k] = v
	}
	credParts := strings.Split(kv["Credential"], "/")
	if len(credParts) != 5 || credParts[3] != "s3" || credParts[4] != "aws4_request" {
		return "", "", "", nil, "", false
	}
	sh := strings.Split(kv["SignedHeaders"], ";")
	if len(sh) == 0 || kv["Signature"] == "" {
		return "", "", "", nil, "", false
	}
	return credParts[0], credParts[1], credParts[2], sh, kv["Signature"], true
}

func verifySigV4(r *http.Request, body []byte, secret, date, region string, signedHeaders []string, sigHex string) bool {
	amzDate := r.Header.Get("X-Amz-Date")
	if len(amzDate) < 8 || amzDate[:8] != date {
		return false
	}
	payloadHash := r.Header.Get("X-Amz-Content-Sha256")
	if payloadHash == "" {
		sum := sha256.Sum256(body)
		payloadHash = hex.EncodeToString(sum[:])
	}
	canonicalURI := r.URL.EscapedPath()
	if canonicalURI == "" {
		canonicalURI = "/"
	}
	canonicalQuery := canonicalizeQuery(r.URL.Query())
	var canonicalHeaders strings.Builder
	for _, h := range signedHeaders {
		var val string
		if h == "host" {
			val = r.Host
		} else {
			vals := r.Header.Values(http.CanonicalHeaderKey(h))
			cleaned := make([]string, len(vals))
			for i, v := range vals {
				cleaned[i] = strings.Join(strings.Fields(strings.TrimSpace(v)), " ")
			}
			val = strings.Join(cleaned, ",")
		}
		canonicalHeaders.WriteString(h)
		canonicalHeaders.WriteByte(':')
		canonicalHeaders.WriteString(val)
		canonicalHeaders.WriteByte('\n')
	}
	canonicalReq := strings.Join([]string{
		r.Method,
		canonicalURI,
		canonicalQuery,
		canonicalHeaders.String(),
		strings.Join(signedHeaders, ";"),
		payloadHash,
	}, "\n")
	reqSum := sha256.Sum256([]byte(canonicalReq))
	scope := date + "/" + region + "/s3/aws4_request"
	stringToSign := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + hex.EncodeToString(reqSum[:])

	kDate := hmacSHA256([]byte("AWS4"+secret), date)
	kRegion := hmacSHA256(kDate, region)
	kService := hmacSHA256(kRegion, "s3")
	kSigning := hmacSHA256(kService, "aws4_request")
	expected := hmacSHA256(kSigning, stringToSign)
	got, err := hex.DecodeString(sigHex)
	if err != nil {
		return false
	}
	return hmac.Equal(expected, got)
}

func canonicalizeQuery(q url.Values) string {
	if len(q) == 0 {
		return ""
	}
	keys := make([]string, 0, len(q))
	for k := range q {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var parts []string
	for _, k := range keys {
		vals := append([]string(nil), q[k]...)
		sort.Strings(vals)
		for _, v := range vals {
			parts = append(parts, awsURIEncode(k)+"="+awsURIEncode(v))
		}
	}
	return strings.Join(parts, "&")
}

func awsURIEncode(s string) string {
	escaped := url.QueryEscape(s)
	escaped = strings.ReplaceAll(escaped, "+", "%20")
	escaped = strings.ReplaceAll(escaped, "%7E", "~")
	return escaped
}

func hmacSHA256(key []byte, data string) []byte {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(data))
	return mac.Sum(nil)
}

type readSessionDaemonSpy struct {
	*scopedNamespaceStore
	denyManifest atomic.Bool
}

func (s *readSessionDaemonSpy) GetManifest(ctx context.Context, tid string) (*Manifest, string, error) {
	s.manifests.Add(1)
	if s.denyManifest.Load() {
		return nil, "", errors.New("daemon manifest read is disabled")
	}
	return s.Backend.GetManifest(ctx, tid)
}

func newReadSessionOwnerHarness(t *testing.T) (*readSessionAuthFixture, *localS3Fixture, *readSessionDaemonSpy, *Manager, *Tenant, http.Handler) {
	t.Helper()
	f := newReadSessionAuthFixture(t)
	s3fx := newLocalS3Fixture(t)
	fake := NewFakeStore()
	spy := &readSessionDaemonSpy{scopedNamespaceStore: &scopedNamespaceStore{Backend: fake}}

	opts := testOpts()
	opts.Lease = 30 * time.Second
	opts.HeartbeatEvery = 5 * time.Second

	mgr := NewManager(spy, readSessionTestPrefix, "owner-1", "standard", t.TempDir(), opts, 1000, &Metrics{}, ControllerOpts{})
	t.Cleanup(mgr.ShutdownAll)

	tenant, err := mgr.Acquire(context.Background(), readSessionTestTenant, true)
	if err != nil {
		t.Fatal(err)
	}
	scopedNamespaceWrite(t, tenant, "seed-read-session",
		Statement{SQL: `CREATE TABLE meta(id INTEGER PRIMARY KEY CHECK(id=1), name TEXT NOT NULL, dropping INTEGER NOT NULL DEFAULT 0)`},
		Statement{SQL: `INSERT INTO meta(id,name) VALUES(1,?)`, Args: []any{readSessionTestCatalog}},
		Statement{SQL: `CREATE TABLE namespaces(id INTEGER PRIMARY KEY, name TEXT NOT NULL, created_snapshot INTEGER NOT NULL, dropped_snapshot INTEGER, contained_field TEXT, contained_value INTEGER)`},
		Statement{SQL: `CREATE UNIQUE INDEX namespaces_live ON namespaces(name) WHERE dropped_snapshot IS NULL`},
		Statement{SQL: `INSERT INTO namespaces VALUES(17,'reports',1,NULL,'team_id',42)`},
		Statement{SQL: `INSERT INTO namespaces VALUES(18,'other',1,NULL,'team_id',43)`},
		Statement{SQL: `INSERT INTO namespaces VALUES(19,'uncontained',1,NULL,NULL,NULL)`},
		Statement{SQL: `INSERT INTO namespaces VALUES(20,'dropped',1,2,'team_id',42)`},
	)

	mf, _, err := fake.GetManifest(context.Background(), readSessionTestTenant)
	if err != nil || mf == nil {
		t.Fatalf("seeded manifest missing: %v", err)
	}
	mfBytes, err := json.Marshal(mf)
	if err != nil {
		t.Fatal(err)
	}
	s3fx.putObject(readSessionTestBucket, manifestKey(readSessionTestPrefix, readSessionTestTenant), mfBytes)

	// Reset spy counters after setup so assertions measure only request-path calls.
	spy.manifests.Store(0)
	spy.objects.Store(0)
	spy.mutations.Store(0)

	srv := NewServer(mgr, map[string]string{"peer-1": "http://127.0.0.1:1"}).
		WithAuth(f.auth).
		WithReadSessionS3(s3fx.s3Config())
	return f, s3fx, spy, mgr, tenant, srv.Handler()
}

func TestReadSessionTwoDistinctCallersAndDualManifestRead(t *testing.T) {
	f, s3fx, spy, _, _, h := newReadSessionOwnerHarness(t)
	// Disable the daemon manifest store completely to prove zero daemon fallback.
	spy.denyManifest.Store(true)

	manifestPath := "/" + readSessionTestBucket + "/" + manifestKey(readSessionTestPrefix, readSessionTestTenant)
	s3fx.setCaller("ASIAEXAMPLEALICE0001", "secret-alice-material-only-for-test", "session-token-alice-only-for-test", manifestPath)
	s3fx.setCaller("ASIAEXAMPLEBOB000002", "secret-bob-material-only-for-test", "session-token-bob-only-for-test", manifestPath)

	aliceToken := f.token(t, nil)
	bobToken := f.token(t, func(c map[string]any) {
		c["sub"] = "example.bob"
		c["usr"] = "example.bob"
		c["jti"] = "token-b"
		c["sid"] = "session-b"
		c["aws"] = map[string]any{
			"access_key_id":     "ASIAEXAMPLEBOB000002",
			"secret_access_key": "secret-bob-material-only-for-test",
			"session_token":     "session-token-bob-only-for-test",
			"region":            readSessionTestRegion,
			"expires_at":        int64(2_000_000_060),
		}
	})

	var logBuf bytes.Buffer
	oldOut := log.Writer()
	log.SetOutput(&logBuf)
	t.Cleanup(func() { log.SetOutput(oldOut) })

	sequence := []struct {
		name    string
		token   string
		wantKey string
		wantTok string
	}{
		{"alice-1", aliceToken, "ASIAEXAMPLEALICE0001", "session-token-alice-only-for-test"},
		{"bob-1", bobToken, "ASIAEXAMPLEBOB000002", "session-token-bob-only-for-test"},
		{"alice-2", aliceToken, "ASIAEXAMPLEALICE0001", "session-token-alice-only-for-test"},
	}

	for _, step := range sequence {
		s3fx.resetRecords()
		w := scopedNamespaceCall(t, h, readSessionTestPath, "Bearer "+step.token)
		var dto scopedNamespaceDTO
		if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &dto) != nil ||
			dto.ID != 17 || dto.Name != "reports" || dto.Containment.Field != "team_id" || dto.Containment.Value != 42 {
			t.Fatalf("%s: status=%d body=%s", step.name, w.Code, w.Body.String())
		}
		if w.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("%s: response permits caching", step.name)
		}
		recs := s3fx.snapshotRecords()
		// Both pre-read and post-read ownership checks must hit S3 with that caller's SigV4 credentials.
		if len(recs) != 2 {
			t.Fatalf("%s: expected 2 uncached manifest reads, got %d", step.name, len(recs))
		}
		wantHash := sha256.Sum256([]byte(step.wantTok))
		wantDigest := hex.EncodeToString(wantHash[:])
		for i, rec := range recs {
			if !rec.sigValid || !rec.securityTok || rec.method != http.MethodGet ||
				rec.path != manifestPath || rec.region != readSessionTestRegion ||
				rec.accessKeyID != step.wantKey || rec.tokenDigest != wantDigest || rec.status != http.StatusOK {
				t.Fatalf("%s read %d: unexpected S3 record %+v", step.name, i, rec)
			}
		}
		// Ensure neither logs nor response leak JWT or AWS credentials.
		for _, secret := range []string{
			step.token, step.wantKey, step.wantTok,
			"secret-alice-material-only-for-test", "secret-bob-material-only-for-test",
		} {
			if strings.Contains(w.Body.String(), secret) || strings.Contains(logBuf.String(), secret) {
				t.Fatalf("%s: leaked secret material", step.name)
			}
		}
	}

	if spy.manifests.Load() != 0 || spy.objects.Load() != 0 || spy.mutations.Load() != 0 {
		t.Fatalf("daemon store was touched: manifests=%d objects=%d mutations=%d",
			spy.manifests.Load(), spy.objects.Load(), spy.mutations.Load())
	}
}

func TestReadSessionRefusalsAndNoAmbientFallback(t *testing.T) {
	f, s3fx, spy, _, _, h := newReadSessionOwnerHarness(t)
	manifestPath := "/" + readSessionTestBucket + "/" + manifestKey(readSessionTestPrefix, readSessionTestTenant)

	// Set ambient AWS env credentials that ARE authorized in local S3 to prove they are never used.
	s3fx.setCaller("ASIAAMBIENTENV000000", "secret-ambient-env-do-not-use", "session-ambient-env-do-not-use", manifestPath)
	t.Setenv("AWS_ACCESS_KEY_ID", "ASIAAMBIENTENV000000")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "secret-ambient-env-do-not-use")
	t.Setenv("AWS_SESSION_TOKEN", "session-ambient-env-do-not-use")
	t.Setenv("AWS_REGION", readSessionTestRegion)

	// Keep the daemon store healthy (denyManifest=false) to prove refusals never fall back to it.
	spy.denyManifest.Store(false)

	legacyMetadataToken := f.signClaims(t, map[string]any{
		"iss": "https://issuer.test",
		"aud": []string{"hoglake", "hoglite"},
		"sub": "example.alice",
		"usr": "example.alice",
		"jti": "legacy-1",
		"ten": "scoped",
		"org": "example-org",
		"iat": int64(2_000_000_000),
		"nbf": int64(2_000_000_000),
		"exp": int64(2_000_000_060),
		"cat": map[string][]string{readSessionTestCatalog: {"read"}},
		"prd": map[string]map[string]string{"team_id": {"42": "read"}},
		"rtg": map[string]any{"b": "hoglite", "be": readSessionTestBackend, "t": readSessionTestTenant},
	})

	cases := []struct {
		name       string
		bearer     string
		wantStatus int
		wantCode   string
	}{
		{"absent authorization", "", http.StatusUnauthorized, "unauthorized"},
		{"legacy metadata token", "Bearer " + legacyMetadataToken, http.StatusUnauthorized, "unauthorized"},
		{"expired aws credentials", "Bearer " + f.token(t, func(c map[string]any) {
			c["exp"] = int64(2_000_000_001)
			c["aws"].(map[string]any)["expires_at"] = int64(2_000_000_001)
		}), http.StatusUnauthorized, "unauthorized"},
		{"manifest json suffix", "Bearer " + f.token(t, func(c map[string]any) {
			c["grants"].([]map[string]any)[1]["object_key"] = "root/2c/t/fixture_3/manifest.json"
		}), http.StatusUnauthorized, "unauthorized"},
		{"mismatched tenant", "Bearer " + f.token(t, func(c map[string]any) {
			c["rtg"].(map[string]any)["t"] = "fixture_4"
			c["grants"].([]map[string]any)[1]["object_key"] = manifestKey(readSessionTestPrefix, "fixture_4")
		}), http.StatusNotFound, "not_found"},
		{"mismatched catalog", "Bearer " + f.token(t, func(c map[string]any) {
			c["primary_catalog"] = "other-catalog"
			c["cat"] = map[string][]string{"other-catalog": {"read"}}
			for _, g := range c["grants"].([]map[string]any) {
				g["catalog"] = "other-catalog"
			}
			c["grants"].([]map[string]any)[0]["target"] = "other-catalog"
		}), http.StatusNotFound, "not_found"},
		{"wrong key root", "Bearer " + f.token(t, func(c map[string]any) {
			c["grants"].([]map[string]any)[1]["object_key"] = manifestKey("other-root", readSessionTestTenant)
		}), http.StatusNotFound, "not_found"},
		{"wrong bucket", "Bearer " + f.token(t, func(c map[string]any) {
			c["grants"].([]map[string]any)[1]["bucket"] = "other-owner-bucket"
			g := c["grants"].([]map[string]any)
			c["grants"] = []map[string]any{g[0], g[2], g[3], g[1]}
		}), http.StatusNotFound, "not_found"},
		{"wrong region", "Bearer " + f.token(t, func(c map[string]any) {
			c["aws"].(map[string]any)["region"] = "eu-west-1"
			for _, g := range c["grants"].([]map[string]any) {
				g["region"] = "eu-west-1"
			}
		}), http.StatusNotFound, "not_found"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s3fx.resetRecords()
			w := scopedNamespaceCall(t, h, readSessionTestPath, tc.bearer)
			scopedNamespaceWantCode(t, w, tc.wantStatus, tc.wantCode)
			if len(s3fx.snapshotRecords()) != 0 {
				t.Fatalf("%s: refused token reached S3", tc.name)
			}
		})
	}

	// Ordinary SQL routes still refuse the dedicated read-session token.
	validToken := f.token(t, nil)
	for _, route := range []string{"/v1/t/" + readSessionTestTenant + "/query", "/v1/t/" + readSessionTestTenant + "/tx"} {
		req := httptest.NewRequest(http.MethodPost, route, strings.NewReader(`{"sql":"SELECT 1"}`))
		req.Header.Set("Authorization", "Bearer "+validToken)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		if w.Code != http.StatusUnauthorized {
			t.Fatalf("ordinary route %s returned %d, want 401", route, w.Code)
		}
	}

	if spy.manifests.Load() != 0 || spy.objects.Load() != 0 || spy.mutations.Load() != 0 {
		t.Fatalf("daemon store was touched on refusals: manifests=%d objects=%d mutations=%d",
			spy.manifests.Load(), spy.objects.Load(), spy.mutations.Load())
	}
}

func TestReadSessionSiblingAllocationAndContainmentDenied(t *testing.T) {
	f, s3fx, spy, _, _, h := newReadSessionOwnerHarness(t)
	manifestPath := "/" + readSessionTestBucket + "/" + manifestKey(readSessionTestPrefix, readSessionTestTenant)
	siblingPath := "/" + readSessionTestBucket + "/" + manifestKey(readSessionTestPrefix, "sibling_9")

	// Ambient env has access to manifestPath, and daemon store is healthy. Neither may be used.
	s3fx.setCaller("ASIAAMBIENTENV000000", "secret-ambient-env-do-not-use", "session-ambient-env-do-not-use", manifestPath)
	t.Setenv("AWS_ACCESS_KEY_ID", "ASIAAMBIENTENV000000")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "secret-ambient-env-do-not-use")
	t.Setenv("AWS_SESSION_TOKEN", "session-ambient-env-do-not-use")

	// Caller's AWS session is restricted to a sibling allocation, so S3 returns 403 AccessDenied.
	s3fx.setCaller("ASIASIBLINGCALLER001", "secret-sibling-only", "session-sibling-only", siblingPath)
	deniedCredsToken := f.token(t, func(c map[string]any) {
		c["aws"] = map[string]any{
			"access_key_id":     "ASIASIBLINGCALLER001",
			"secret_access_key": "secret-sibling-only",
			"session_token":     "session-sibling-only",
			"region":            readSessionTestRegion,
			"expires_at":        int64(2_000_000_060),
		}
	})
	s3fx.resetRecords()
	w := scopedNamespaceCall(t, h, readSessionTestPath, "Bearer "+deniedCredsToken)
	scopedNamespaceWantCode(t, w, http.StatusServiceUnavailable, "unavailable")
	recs := s3fx.snapshotRecords()
	if len(recs) != 1 || recs[0].accessKeyID != "ASIASIBLINGCALLER001" || recs[0].status != http.StatusForbidden {
		t.Fatalf("expected one 403 S3 denial for sibling credentials, got %+v", recs)
	}
	if spy.manifests.Load() != 0 {
		t.Fatalf("fell back to daemon store after S3 AccessDenied: manifests=%d", spy.manifests.Load())
	}

	// Authorized manifest credentials still enforce persisted namespace containment.
	s3fx.setCaller("ASIAEXAMPLEALICE0001", "secret-alice-material-only-for-test", "session-token-alice-only-for-test", manifestPath)
	aliceToken := f.token(t, nil)
	for _, ns := range []string{"other", "uncontained", "dropped", "missing"} {
		s3fx.resetRecords()
		path := strings.TrimSuffix(readSessionTestPath, "reports") + ns
		w := scopedNamespaceCall(t, h, path, "Bearer "+aliceToken)
		scopedNamespaceWantCode(t, w, http.StatusNotFound, "not_found")
		if len(s3fx.snapshotRecords()) != 2 {
			t.Fatalf("namespace %s: expected 2 ownership manifest reads, got %d", ns, len(s3fx.snapshotRecords()))
		}
	}
	if spy.manifests.Load() != 0 {
		t.Fatalf("daemon store was touched: manifests=%d", spy.manifests.Load())
	}
}

func TestReadSessionColdTenantUnavailableNoPeerDiscoveryOrAcquire(t *testing.T) {
	f, s3fx, spy, mgr, _, _ := newReadSessionOwnerHarness(t)

	peerHits := atomic.Int64{}
	peerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		peerHits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(peerSrv.Close)

	coldTenant := "cold_7"
	coldManifestPath := "/" + readSessionTestBucket + "/" + manifestKey(readSessionTestPrefix, coldTenant)
	coldManifest, _ := json.Marshal(&Manifest{
		Epoch: 1, Gen: 1, Instance: "inst-cold", Owner: "peer-1", OwnerAddr: "peer-1",
	})
	s3fx.putObject(readSessionTestBucket, manifestKey(readSessionTestPrefix, coldTenant), coldManifest)
	s3fx.setCaller("ASIAEXAMPLEALICE0001", "secret-alice-material-only-for-test", "session-token-alice-only-for-test", coldManifestPath)

	srv := NewServer(mgr, map[string]string{"peer-1": peerSrv.URL}).
		WithAuth(f.auth).
		WithReadSessionS3(s3fx.s3Config())

	coldToken := f.token(t, func(c map[string]any) {
		c["rtg"].(map[string]any)["t"] = coldTenant
		c["grants"].([]map[string]any)[1]["object_key"] = manifestKey(readSessionTestPrefix, coldTenant)
	})
	coldPath := "/v1/t/" + coldTenant + "/metadata/v1/catalogs/" + readSessionTestCatalog + "/namespaces/reports"
	s3fx.resetRecords()
	w := scopedNamespaceCall(t, srv.Handler(), coldPath, "Bearer "+coldToken)
	scopedNamespaceWantCode(t, w, http.StatusServiceUnavailable, "unavailable")

	if mgr.LocalTenant(coldTenant) != nil {
		t.Fatal("cold tenant was acquired on read-session request")
	}
	if peerHits.Load() != 0 {
		t.Fatalf("peer was contacted %d times for cold tenant", peerHits.Load())
	}
	if len(s3fx.snapshotRecords()) != 0 {
		t.Fatalf("cold tenant hit S3 %d times", len(s3fx.snapshotRecords()))
	}
	if spy.manifests.Load() != 0 || spy.objects.Load() != 0 || spy.mutations.Load() != 0 {
		t.Fatalf("cold tenant touched daemon store: manifests=%d objects=%d mutations=%d",
			spy.manifests.Load(), spy.objects.Load(), spy.mutations.Load())
	}
}

func TestReadSessionPostReadOwnershipLossAndMidRequestExpiry(t *testing.T) {
	t.Run("ownership changes between pre-read and post-read", func(t *testing.T) {
		f, s3fx, spy, _, tenant, h := newReadSessionOwnerHarness(t)
		manifestPath := "/" + readSessionTestBucket + "/" + manifestKey(readSessionTestPrefix, readSessionTestTenant)
		s3fx.setCaller("ASIAEXAMPLEALICE0001", "secret-alice-material-only-for-test", "session-token-alice-only-for-test", manifestPath)

		// After the first S3 manifest read succeeds, advance the manifest epoch in S3.
		s3fx.mu.Lock()
		s3fx.onRequest = func(call int, _ string) {
			if call == 1 {
				bumped, _ := json.Marshal(&Manifest{
					Epoch:    tenant.claimEpoch + 1,
					Gen:      1,
					Instance: tenant.claimInst,
					Owner:    tenant.self,
				})
				s3fx.putObject(readSessionTestBucket, manifestKey(readSessionTestPrefix, readSessionTestTenant), bumped)
			}
		}
		s3fx.mu.Unlock()

		w := scopedNamespaceCall(t, h, readSessionTestPath, "Bearer "+f.token(t, nil))
		scopedNamespaceWantCode(t, w, http.StatusServiceUnavailable, "unavailable")
		if len(s3fx.snapshotRecords()) != 2 {
			t.Fatalf("expected 2 manifest reads to detect post-read epoch loss, got %d", len(s3fx.snapshotRecords()))
		}
		if spy.manifests.Load() != 0 {
			t.Fatalf("daemon store was touched: manifests=%d", spy.manifests.Load())
		}
	})

	t.Run("credentials expire before post-read manifest check", func(t *testing.T) {
		f, s3fx, spy, _, _, h := newReadSessionOwnerHarness(t)
		manifestPath := "/" + readSessionTestBucket + "/" + manifestKey(readSessionTestPrefix, readSessionTestTenant)
		s3fx.setCaller("ASIAEXAMPLEALICE0001", "secret-alice-material-only-for-test", "session-token-alice-only-for-test", manifestPath)

		// Advance clock to aws.expires_at right after the first manifest read returns.
		s3fx.mu.Lock()
		s3fx.onRequest = func(call int, _ string) {
			if call == 1 {
				f.setNow(time.Unix(2_000_000_060, 0))
			}
		}
		s3fx.mu.Unlock()

		w := scopedNamespaceCall(t, h, readSessionTestPath, "Bearer "+f.token(t, nil))
		scopedNamespaceWantCode(t, w, http.StatusUnauthorized, "unauthorized")
		// The second manifest read must be refused BEFORE calling S3.
		if len(s3fx.snapshotRecords()) != 1 {
			t.Fatalf("expired credentials sent %d S3 calls, want 1", len(s3fx.snapshotRecords()))
		}
		if spy.manifests.Load() != 0 {
			t.Fatalf("daemon store was touched: manifests=%d", spy.manifests.Load())
		}
	})

	t.Run("expiring lease fails closed before calling S3", func(t *testing.T) {
		f, s3fx, spy, mgr, _, h := newReadSessionOwnerHarness(t)
		manifestPath := "/" + readSessionTestBucket + "/" + manifestKey(readSessionTestPrefix, readSessionTestTenant)
		s3fx.setCaller("ASIAEXAMPLEALICE0001", "secret-alice-material-only-for-test", "session-token-alice-only-for-test", manifestPath)

		// Force the owner's lease to look lost.
		mgr.leaseLost.Store(true)

		w := scopedNamespaceCall(t, h, readSessionTestPath, "Bearer "+f.token(t, nil))
		scopedNamespaceWantCode(t, w, http.StatusServiceUnavailable, "unavailable")
		if len(s3fx.snapshotRecords()) != 0 {
			t.Fatalf("expiring lease still sent %d S3 calls", len(s3fx.snapshotRecords()))
		}
		if spy.manifests.Load() != 0 {
			t.Fatalf("daemon store was touched: manifests=%d", spy.manifests.Load())
		}
	})

	t.Run("plain HTTP S3 endpoint is refused without sending caller credentials", func(t *testing.T) {
		f, _, spy, mgr, _, _ := newReadSessionOwnerHarness(t)
		spy.denyManifest.Store(true)
		var hits atomic.Int64
		plainSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hits.Add(1)
			w.WriteHeader(http.StatusOK)
		}))
		t.Cleanup(plainSrv.Close)

		srv := NewServer(mgr, nil).
			WithAuth(f.auth).
			WithReadSessionS3(ReadSessionS3Config{
				BaseEndpoint: plainSrv.URL,
				UsePathStyle: true,
			})
		w := scopedNamespaceCall(t, srv.Handler(), readSessionTestPath, "Bearer "+f.token(t, nil))
		scopedNamespaceWantCode(t, w, http.StatusServiceUnavailable, "unavailable")
		if hits.Load() != 0 {
			t.Fatalf("plain HTTP S3 endpoint received %d requests, want 0", hits.Load())
		}
	})

	t.Run("metrics and logs record caller-session manifest reads without leaking secrets", func(t *testing.T) {
		f, s3fx, spy, mgr, _, h := newReadSessionOwnerHarness(t)
		spy.denyManifest.Store(true)
		manifestPath := "/" + readSessionTestBucket + "/" + manifestKey(readSessionTestPrefix, readSessionTestTenant)
		s3fx.setCaller("ASIAEXAMPLEALICE0001", "secret-alice-material-only-for-test", "session-token-alice-only-for-test", manifestPath)

		// Compute the exact manifest byte length served by the fixture.
		s3fx.mu.Lock()
		wantBytes := len(s3fx.objects[manifestPath])
		s3fx.mu.Unlock()
		if wantBytes <= 0 {
			t.Fatalf("fixture manifest at %s is empty", manifestPath)
		}

		logCh := make(chan string, 16)
		mgr.metrics.Logger = readSessionTestLogger{ch: logCh}

		w := scopedNamespaceCall(t, h, readSessionTestPath, "Bearer "+f.token(t, nil))
		if w.Code != http.StatusOK {
			t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
		}
		if got := mgr.metrics.ReadSessionManifestReads.Load(); got != 2 {
			t.Fatalf("ReadSessionManifestReads=%d, want 2 (pre-check and post-check)", got)
		}
		if got := mgr.metrics.ReadSessionManifestErrors.Load(); got != 0 {
			t.Fatalf("ReadSessionManifestErrors=%d, want 0", got)
		}

		// Verify /metrics exposes both counters.
		mw := httptest.NewRecorder()
		NewServer(mgr, nil).MetricsHandler().ServeHTTP(mw, httptest.NewRequest(http.MethodGet, "/metrics", nil))
		metricsBody := mw.Body.String()
		if !strings.Contains(metricsBody, "hoglite_read_session_manifest_reads_total 2\n") {
			t.Fatalf("/metrics missing read counter:\n%s", metricsBody)
		}
		if !strings.Contains(metricsBody, "hoglite_read_session_manifest_errors_total 0\n") {
			t.Fatalf("/metrics missing error counter:\n%s", metricsBody)
		}

		// Verify both successful reads emit the full log line with exact byte count and zero secrets.
		wantKey := manifestKey(readSessionTestPrefix, readSessionTestTenant)
		wantLine := fmt.Sprintf(
			"read-session manifest read tenant=%s bucket=%s key=%s source=caller-read-session bytes=%d",
			readSessionTestTenant, readSessionTestBucket, wantKey, wantBytes,
		)
		var lines []string
		deadline := time.After(2 * time.Second)
		for len(lines) < 2 {
			select {
			case line := <-logCh:
				lines = append(lines, line)
			case <-deadline:
				t.Fatalf("timed out waiting for 2 log lines, got %d: %v", len(lines), lines)
			}
		}
		for i, line := range lines {
			if line != wantLine {
				t.Fatalf("log line %d = %q, want %q", i, line, wantLine)
			}
			for _, secret := range []string{"ASIAEXAMPLEALICE0001", "secret-alice-material-only-for-test", "session-token-alice-only-for-test"} {
				if strings.Contains(line, secret) {
					t.Fatalf("log line %d leaked credential material %q: %q", i, secret, line)
				}
			}
		}

		// Trigger an S3 refusal on the caller-session manifest read and confirm the error counter increments.
		s3fx.setCaller("ASIAEXAMPLEALICE0001", "wrong-secret", "wrong-session", manifestPath)
		ew := scopedNamespaceCall(t, h, readSessionTestPath, "Bearer "+f.token(t, nil))
		scopedNamespaceWantCode(t, ew, http.StatusServiceUnavailable, "unavailable")
		if got := mgr.metrics.ReadSessionManifestErrors.Load(); got != 1 {
			t.Fatalf("ReadSessionManifestErrors=%d, want 1", got)
		}
	})

	t.Run("stalled logger does not block caller-session manifest reads", func(t *testing.T) {
		f, s3fx, spy, mgr, _, h := newReadSessionOwnerHarness(t)
		spy.denyManifest.Store(true)
		manifestPath := "/" + readSessionTestBucket + "/" + manifestKey(readSessionTestPrefix, readSessionTestTenant)
		s3fx.setCaller("ASIAEXAMPLEALICE0001", "secret-alice-material-only-for-test", "session-token-alice-only-for-test", manifestPath)

		unblock := make(chan struct{})
		t.Cleanup(func() { close(unblock) })
		mgr.metrics.Logger = blockingTestLogger{unblock: unblock}

		done := make(chan int, 1)
		go func() {
			w := scopedNamespaceCall(t, h, readSessionTestPath, "Bearer "+f.token(t, nil))
			done <- w.Code
		}()
		select {
		case code := <-done:
			if code != http.StatusOK {
				t.Fatalf("status=%d, want 200", code)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("scoped read blocked on stalled logger")
		}
		if got := mgr.metrics.ReadSessionManifestReads.Load(); got != 2 {
			t.Fatalf("ReadSessionManifestReads=%d, want 2", got)
		}
	})
}

type readSessionTestLogger struct {
	ch chan string
}

func (l readSessionTestLogger) Printf(format string, args ...any) {
	l.ch <- fmt.Sprintf(format, args...)
}

type blockingTestLogger struct {
	unblock <-chan struct{}
}

func (l blockingTestLogger) Printf(string, ...any) {
	<-l.unblock
}
