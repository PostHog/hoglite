package hoglite

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const scopedNamespaceTestPath = "/v1/t/opaque_1/metadata/v1/catalogs/catalog-not-the-tid/namespaces/reports"

type scopedNamespaceStore struct {
	Backend
	manifests atomic.Int64
	objects   atomic.Int64
	mutations atomic.Int64
}

func (s *scopedNamespaceStore) GetManifest(ctx context.Context, tid string) (*Manifest, string, error) {
	s.manifests.Add(1)
	return s.Backend.GetManifest(ctx, tid)
}

func (s *scopedNamespaceStore) GetObject(ctx context.Context, key string) ([]byte, error) {
	s.objects.Add(1)
	return s.Backend.GetObject(ctx, key)
}

func (s *scopedNamespaceStore) GetObjectTo(ctx context.Context, key string, out io.Writer) error {
	s.objects.Add(1)
	return s.Backend.GetObjectTo(ctx, key, out)
}

func (s *scopedNamespaceStore) ListSegments(ctx context.Context, tid, instance string, gen int64) ([]SegMeta, error) {
	s.objects.Add(1)
	return s.Backend.ListSegments(ctx, tid, instance, gen)
}

func (s *scopedNamespaceStore) PutManifestCAS(ctx context.Context, tid string, mf *Manifest, etag string) (string, error) {
	s.mutations.Add(1)
	return s.Backend.PutManifestCAS(ctx, tid, mf, etag)
}

func scopedNamespaceTestManager(t *testing.T, store Backend, id string) *Manager {
	t.Helper()
	m := NewManager(store, "fake", id, "standard", t.TempDir(), testOpts(), 1000, &Metrics{}, ControllerOpts{})
	t.Cleanup(m.ShutdownAll)
	return m
}

func scopedNamespaceTestToken(t *testing.T, f *authFixture, value string, mutate func(jwt.MapClaims)) string {
	t.Helper()
	return f.token(t, func(c jwt.MapClaims) {
		c["ten"] = "scoped"
		c["usr"] = "0190a1b2-c3d4-7e5f-8a9b-000000000042"
		c["cat"] = map[string][]string{"catalog-not-the-tid": {"read"}}
		c["prd"] = map[string]any{"team_id": map[string]string{value: "read"}}
		if mutate != nil {
			mutate(c)
		}
	})
}

func scopedNamespaceSeed(t *testing.T, m *Manager) *Tenant {
	t.Helper()
	tenant, err := m.Acquire(context.Background(), "opaque_1", true)
	if err != nil {
		t.Fatal(err)
	}
	scopedNamespaceWrite(t, tenant, "seed",
		Statement{SQL: `CREATE TABLE meta(id INTEGER PRIMARY KEY CHECK(id=1), name TEXT NOT NULL, dropping INTEGER NOT NULL DEFAULT 0)`},
		Statement{SQL: `INSERT INTO meta(id,name) VALUES(1,?)`, Args: []any{"catalog-not-the-tid"}},
		Statement{SQL: `CREATE TABLE namespaces(id INTEGER PRIMARY KEY, name TEXT NOT NULL, created_snapshot INTEGER NOT NULL, dropped_snapshot INTEGER, contained_field TEXT, contained_value INTEGER)`},
		Statement{SQL: `CREATE UNIQUE INDEX namespaces_live ON namespaces(name) WHERE dropped_snapshot IS NULL`},
		Statement{SQL: `INSERT INTO namespaces VALUES(17,'reports',1,NULL,'team_id',42)`},
		Statement{SQL: `INSERT INTO namespaces VALUES(18,'other',1,NULL,'team_id',43)`},
		Statement{SQL: `INSERT INTO namespaces VALUES(19,'uncontained',1,NULL,NULL,NULL)`},
		Statement{SQL: `INSERT INTO namespaces VALUES(20,'team_42',1,NULL,'team_id',43)`},
		Statement{SQL: `INSERT INTO namespaces VALUES(21,'dropped',1,2,'team_id',42)`},
	)
	return tenant
}

func scopedNamespaceWrite(t *testing.T, tenant *Tenant, txid string, stmts ...Statement) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, _, err := tenant.SubmitTxCtx(ctx, TxRequest{Client: "scoped-fixture", TxID: txid, Stmts: stmts}); err != nil {
		t.Fatal(err)
	}
}

func scopedNamespaceCall(t *testing.T, h http.Handler, path, bearer string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet, path, nil)
	if bearer != "" {
		r.Header.Set("Authorization", bearer)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func scopedNamespaceWantCode(t *testing.T, w *httptest.ResponseRecorder, status int, code string) {
	t.Helper()
	if w.Code != status || w.Body.String() != `{"code":"`+code+`"}` {
		t.Fatalf("response: status=%d body=%s", w.Code, w.Body.String())
	}
}

func TestScopedNamespaceVisibility(t *testing.T) {
	f := newScopedNamespaceAuthFixture(t)
	m := scopedNamespaceTestManager(t, NewFakeStore(), "owner")
	tenant := scopedNamespaceSeed(t, m)
	h := NewServer(m, nil).WithAuth(f.auth).Handler()
	tokens := map[string]string{
		"42": "Bearer " + scopedNamespaceTestToken(t, f, "42", nil),
		"43": "Bearer " + scopedNamespaceTestToken(t, f, "43", func(c jwt.MapClaims) {
			c["usr"] = "0190a1b2-c3d4-7e5f-8a9b-000000000043"
		}),
		"*": "Bearer " + scopedNamespaceTestToken(t, f, "*", nil),
	}
	for i, value := range []string{"42", "43", "42", "43", "*"} {
		w := scopedNamespaceCall(t, h, scopedNamespaceTestPath, tokens[value])
		if value == "43" {
			scopedNamespaceWantCode(t, w, 404, "not_found")
			continue
		}
		var dto scopedNamespaceDTO
		if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &dto) != nil ||
			dto.ID != 17 || dto.Name != "reports" || dto.Containment.Field != "team_id" || dto.Containment.Value != 42 {
			t.Fatalf("authorized response %d: %d %s", i, w.Code, w.Body.String())
		}
		if w.Header().Get("Cache-Control") != "no-store" {
			t.Fatal("response permits caching")
		}
	}
	for _, namespace := range []string{"missing", "uncontained", "team_42", "dropped"} {
		for _, value := range []string{"42", "*"} {
			if namespace == "team_42" && value == "*" {
				continue
			}
			w := scopedNamespaceCall(t, h, strings.TrimSuffix(scopedNamespaceTestPath, "reports")+namespace, tokens[value])
			scopedNamespaceWantCode(t, w, 404, "not_found")
		}
	}
	scopedNamespaceWrite(t, tenant, "recreate",
		Statement{SQL: `UPDATE namespaces SET dropped_snapshot=3 WHERE id=17`},
		Statement{SQL: `INSERT INTO namespaces VALUES(22,'reports',3,NULL,'team_id',43)`})
	scopedNamespaceWantCode(t, scopedNamespaceCall(t, h, scopedNamespaceTestPath, tokens["42"]), 404, "not_found")
	w := scopedNamespaceCall(t, h, scopedNamespaceTestPath, tokens["43"])
	var dto scopedNamespaceDTO
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &dto) != nil || dto.ID != 22 || dto.Containment.Value != 43 {
		t.Fatal("read does not use the current namespace incarnation")
	}
	scopedNamespaceWrite(t, tenant, "catalog-mismatch", Statement{SQL: `UPDATE meta SET name='another-catalog' WHERE id=1`})
	scopedNamespaceWantCode(t, scopedNamespaceCall(t, h, scopedNamespaceTestPath, tokens["43"]), 404, "not_found")
	scopedNamespaceWrite(t, tenant, "catalog-dropping", Statement{SQL: `UPDATE meta SET name='catalog-not-the-tid',dropping=1 WHERE id=1`})
	scopedNamespaceWantCode(t, scopedNamespaceCall(t, h, scopedNamespaceTestPath, tokens["43"]), 404, "not_found")
}

func TestScopedNamespaceRefusesBeforeStorage(t *testing.T) {
	f := newScopedNamespaceAuthFixture(t)
	store := &scopedNamespaceStore{Backend: NewFakeStore()}
	m := scopedNamespaceTestManager(t, store, "owner")
	h := NewServer(m, nil).WithAuth(f.auth).Handler()
	tests := []struct {
		name   string
		mutate func(jwt.MapClaims)
		status int
		code   string
	}{
		{"route tenant", func(c jwt.MapClaims) { c["rtg"].(map[string]any)["t"] = "other" }, 404, "not_found"},
		{"route catalog", func(c jwt.MapClaims) { c["cat"] = map[string][]string{"other": {"read"}} }, 404, "not_found"},
		{"no read", func(c jwt.MapClaims) { c["cat"] = map[string][]string{"catalog-not-the-tid": {}} }, 404, "not_found"},
		{"backend", func(c jwt.MapClaims) { c["rtg"].(map[string]any)["be"] = "7c9e2a14-3f51-4b8d-9e60-2d5a1f8c44ab" }, 404, "not_found"},
		{"audience", func(c jwt.MapClaims) { c["aud"] = []string{"hoglake"} }, 401, "unauthorized"},
		{"expired", func(c jwt.MapClaims) { c["exp"] = f.now.Add(-61 * time.Second).Unix() }, 401, "unauthorized"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			raw := scopedNamespaceTestToken(t, f, "42", tc.mutate)
			scopedNamespaceWantCode(t, scopedNamespaceCall(t, h, scopedNamespaceTestPath, "Bearer "+raw), tc.status, tc.code)
		})
	}
	otherKey := newAuthFixture(t)
	invalid := scopedNamespaceTestToken(t, otherKey, "42", nil)
	scopedNamespaceWantCode(t, scopedNamespaceCall(t, h, scopedNamespaceTestPath, "Bearer "+invalid), 401, "unauthorized")
	scopedNamespaceWantCode(t, scopedNamespaceCall(t, h, scopedNamespaceTestPath, ""), 401, "unauthorized")
	duplicate := httptest.NewRequest(http.MethodGet, scopedNamespaceTestPath, nil)
	raw := scopedNamespaceTestToken(t, f, "42", nil)
	duplicate.Header.Add("Authorization", "Bearer "+raw)
	duplicate.Header.Add("Authorization", "Bearer "+raw)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, duplicate)
	scopedNamespaceWantCode(t, w, 401, "unauthorized")
	if store.manifests.Load() != 0 || store.objects.Load() != 0 || store.mutations.Load() != 0 || m.OpenCount() != 0 {
		t.Fatal("refused authority reaches tenant storage")
	}
}

func TestScopedNamespaceRequestShape(t *testing.T) {
	f := newScopedNamespaceAuthFixture(t)
	store := &scopedNamespaceStore{Backend: NewFakeStore()}
	m := scopedNamespaceTestManager(t, store, "owner")
	h := NewServer(m, nil).WithAuth(f.auth).Handler()
	token := "Bearer " + scopedNamespaceTestToken(t, f, "42", nil)
	for _, path := range []string{
		scopedNamespaceTestPath + "?x=1", scopedNamespaceTestPath + "?",
		scopedNamespaceTestPath + "/", scopedNamespaceTestPath + "/extra",
		strings.Replace(scopedNamespaceTestPath, "/metadata/", "//metadata/", 1),
		strings.Replace(scopedNamespaceTestPath, "/metadata/", "/./metadata/", 1),
		strings.Replace(scopedNamespaceTestPath, "/metadata/", "/%6Detadata/", 1),
		strings.TrimSuffix(scopedNamespaceTestPath, "reports") + "%72eports",
		strings.TrimSuffix(scopedNamespaceTestPath, "reports") + "bad%2Fname",
		strings.TrimSuffix(scopedNamespaceTestPath, "reports") + "bad%5Cname",
		strings.TrimSuffix(scopedNamespaceTestPath, "reports") + "bad%252Fname",
		strings.TrimSuffix(scopedNamespaceTestPath, "reports") + "%2e%2e",
		strings.TrimSuffix(scopedNamespaceTestPath, "reports") + strings.Repeat("x", scopedNamespaceTargetLimit),
	} {
		scopedNamespaceWantCode(t, scopedNamespaceCall(t, h, path, token), 400, "invalid_request")
	}
	for _, method := range []string{http.MethodHead, http.MethodPost, http.MethodPut} {
		r := httptest.NewRequest(method, scopedNamespaceTestPath, nil)
		r.Header.Set("Authorization", token)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		scopedNamespaceWantCode(t, w, 400, "invalid_request")
	}
	r := httptest.NewRequest(http.MethodGet, scopedNamespaceTestPath, strings.NewReader("{}"))
	r.Header.Set("Authorization", token)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	scopedNamespaceWantCode(t, w, 400, "invalid_request")
	scopedNamespaceWantCode(t, scopedNamespaceCall(t, h, scopedNamespaceTestPath, "Bearer "+strings.Repeat("x", scopedNamespaceTokenLimit+1)), 401, "unauthorized")
	if store.manifests.Load() != 0 || store.objects.Load() != 0 || store.mutations.Load() != 0 {
		t.Fatal("invalid request reaches tenant storage")
	}
}

func TestScopedNamespaceUsesCanonicalSegmentEncoding(t *testing.T) {
	base := strings.TrimSuffix(scopedNamespaceTestPath, "reports")
	r := httptest.NewRequest(http.MethodGet, base+"private%3A%2B%C3%A9", nil)
	target, ok := parseScopedNamespaceTarget(r)
	if !ok || target.namespace != "private:+é" {
		t.Fatal("the canonical encoding must preserve the namespace")
	}
	for _, suffix := range []string{"private:+%C3%A9", "private%3a%2b%C3%A9"} {
		r := httptest.NewRequest(http.MethodGet, base+suffix, nil)
		if _, ok := parseScopedNamespaceTarget(r); ok {
			t.Fatal("a noncanonical encoding must be refused")
		}
	}
}

func TestScopedNamespaceDoesNotInterceptLegacyTenantName(t *testing.T) {
	f := newScopedNamespaceAuthFixture(t)
	h := NewServer(nil, nil).WithAuth(f.auth).Handler()
	if got := authRequest(t, h, http.MethodPost, "/v1/t/metadata/query", "", "{}").Code; got != http.StatusUnauthorized {
		t.Fatalf("legacy query for tenant metadata: got %d, want 401", got)
	}
}

func TestScopedNamespaceDefaultOffAndMetrics(t *testing.T) {
	f := newAuthFixture(t)
	m := scopedNamespaceTestManager(t, NewFakeStore(), "owner")
	for _, server := range []*Server{NewServer(m, nil), NewServer(m, nil).WithAuth(f.auth)} {
		if w := scopedNamespaceCall(t, server.Handler(), scopedNamespaceTestPath, ""); w.Code != 404 {
			t.Fatalf("disabled route status=%d", w.Code)
		}
	}
	f.auth.scopedNamespaceEnabled = true
	server := NewServer(m, nil).WithAuth(f.auth)
	if w := scopedNamespaceCall(t, server.MetricsHandler(), scopedNamespaceTestPath, ""); w.Code != 404 {
		t.Fatal("metrics listener exposes the typed route")
	}
}

func TestScopedNamespaceNeverAcquires(t *testing.T) {
	f := newScopedNamespaceAuthFixture(t)
	store := &scopedNamespaceStore{Backend: NewFakeStore()}
	m := scopedNamespaceTestManager(t, store, "reader")
	h := NewServer(m, nil).WithAuth(f.auth).Handler()
	token := "Bearer " + scopedNamespaceTestToken(t, f, "42", nil)
	scopedNamespaceWantCode(t, scopedNamespaceCall(t, h, scopedNamespaceTestPath, token), 503, "unavailable")
	if store.objects.Load() != 0 || store.mutations.Load() != 0 || m.metrics.Acquires.Load() != 0 || m.OpenCount() != 0 {
		t.Fatal("unknown tenant causes acquisition or object reads")
	}
	if _, err := os.Stat(filepath.Join(m.baseDir, "t", "opaque_1")); !os.IsNotExist(err) {
		t.Fatal("unknown tenant creates a local directory")
	}
	owner := scopedNamespaceTestManager(t, store, "owner")
	scopedNamespaceSeed(t, owner)
	if err := owner.Release("opaque_1", ""); err != nil {
		t.Fatal(err)
	}
	beforeObjects, beforeMutations := store.objects.Load(), store.mutations.Load()
	scopedNamespaceWantCode(t, scopedNamespaceCall(t, h, scopedNamespaceTestPath, token), 503, "unavailable")
	if store.objects.Load() != beforeObjects || store.mutations.Load() != beforeMutations ||
		m.metrics.Acquires.Load() != 0 || m.OpenCount() != 0 {
		t.Fatal("cold tenant causes recovery or acquisition")
	}
}

func TestScopedNamespaceOwnershipRecheck(t *testing.T) {
	for _, change := range []string{"epoch", "instance", "owner", "lease", "cancellation"} {
		t.Run(change, func(t *testing.T) {
			f := newScopedNamespaceAuthFixture(t)
			fs := NewFakeStore()
			m := scopedNamespaceTestManager(t, fs, "owner")
			tenant := scopedNamespaceSeed(t, m)
			entered, resume := make(chan struct{}), make(chan struct{})
			tenant.readHook = func(ctx context.Context) {
				close(entered)
				select {
				case <-resume:
				case <-ctx.Done():
				}
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			r := httptest.NewRequest(http.MethodGet, scopedNamespaceTestPath, nil).WithContext(ctx)
			r.Header.Set("Authorization", "Bearer "+scopedNamespaceTestToken(t, f, "42", nil))
			w, done := httptest.NewRecorder(), make(chan struct{})
			go func() {
				NewServer(m, nil).WithAuth(f.auth).Handler().ServeHTTP(w, r)
				close(done)
			}()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("read does not start")
			}
			if change == "lease" {
				m.leaseLost.Store(true)
			} else if change == "cancellation" {
				cancel()
			} else {
				mf, etag, err := fs.GetManifest(context.Background(), "opaque_1")
				if err != nil {
					t.Fatal(err)
				}
				switch change {
				case "epoch":
					mf.Epoch++
				case "instance":
					mf.Instance = "new-incarnation"
				case "owner":
					mf.Owner = "another-owner"
				}
				if _, err := fs.PutManifestCAS(context.Background(), "opaque_1", mf, etag); err != nil {
					t.Fatal(err)
				}
			}
			close(resume)
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("read ignores cancellation")
			}
			scopedNamespaceWantCode(t, w, 503, "unavailable")
		})
	}
}

func TestScopedNamespacePeerVerifiesOriginalBearer(t *testing.T) {
	f := newScopedNamespaceAuthFixture(t)
	store := &scopedNamespaceStore{Backend: NewFakeStore()}
	owner := scopedNamespaceTestManager(t, store, "owner")
	scopedNamespaceSeed(t, owner)
	reader := scopedNamespaceTestManager(t, store, "reader")
	peerAuth, err := NewJWTAuth(context.Background(), AuthConfig{
		Mode: "enforce", Issuer: "https://issuer.test", BackendID: authTestBackend, JWKSFile: f.file,
		ScopedNamespaceEnabled: true, ScopedNamespacePredicateFields: []string{"team_id"},
	})
	if err != nil {
		t.Fatal(err)
	}
	peerAuth.scopedNamespaceEnabled = true
	t.Cleanup(func() { awaitAuthRefresh(t, peerAuth) })
	peerHandler := NewServer(owner, nil).WithAuth(peerAuth).Handler()
	bearer := "bEaReR " + scopedNamespaceTestToken(t, f, "42", nil)
	var hops atomic.Int64
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hops.Add(1)
		if r.Header.Get("Authorization") != bearer || r.Header.Get("x-hw-noproxy") != "1" ||
			r.Header.Get("Cookie") != "" || r.Header.Get("x-extra") != "" || r.Header.Get("x-hw-owner") != "" {
			t.Error("peer request changes authority or copies caller headers")
		}
		peerHandler.ServeHTTP(w, r)
	}))
	t.Cleanup(peer.Close)
	h := NewServer(reader, map[string]string{"owner": peer.URL}).WithAuth(f.auth).Handler()
	r := httptest.NewRequest(http.MethodGet, scopedNamespaceTestPath, nil)
	r.Header.Set("Authorization", bearer)
	r.Header.Set("Cookie", "not-forwarded=yes")
	r.Header.Set("x-extra", "not-forwarded")
	r.Header.Set("x-hw-owner", "not-authority")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 200 || hops.Load() != 1 || reader.OpenCount() != 0 || reader.metrics.Acquires.Load() != 0 {
		t.Fatalf("peer result: status=%d hops=%d", w.Code, hops.Load())
	}
	peerAuth.now = func() time.Time { return f.now.Add(16*time.Minute + time.Second) }
	beforeReads := store.manifests.Load()
	w = scopedNamespaceCall(t, h, scopedNamespaceTestPath, bearer)
	scopedNamespaceWantCode(t, w, 401, "unauthorized")
	// The reader discovers the peer. The peer rejects before its owner checks.
	if store.manifests.Load() != beforeReads+1 || hops.Load() != 2 {
		t.Fatal("peer does not verify before ownership discovery")
	}
}

func scopedNamespaceRemoteManifest(t *testing.T, fs *FakeStore) {
	t.Helper()
	ctx := context.Background()
	if _, err := fs.PutManifestCAS(ctx, "opaque_1", &Manifest{
		Owner: "remote-incarnation", OwnerAddr: "remote", Epoch: 1, Instance: "instance", Class: "standard",
	}, ""); err != nil {
		t.Fatal(err)
	}
	if err := fs.PutPodLease(ctx, "remote-incarnation", time.Now().Add(time.Minute).UnixMilli()); err != nil {
		t.Fatal(err)
	}
}

func TestScopedNamespacePeerRefusalsAreBounded(t *testing.T) {
	f := newScopedNamespaceAuthFixture(t)
	fs := NewFakeStore()
	scopedNamespaceRemoteManifest(t, fs)
	m := scopedNamespaceTestManager(t, fs, "reader")
	token := "Bearer " + scopedNamespaceTestToken(t, f, "42", nil)
	success := `{"namespace_id":17,"name":"reports","containment":{"field":"team_id","value":42}}`
	tests := []struct {
		name   string
		status int
		body   string
		want   int
		code   string
	}{
		{"missing", 404, `{"code":"not_found"}`, 404, "not_found"},
		{"unsupported route", 404, `404 page not found`, 503, "unavailable"},
		{"redirect", 307, success, 503, "unavailable"},
		{"owner redirect", 421, `{"owner":"private-address"}`, 503, "unavailable"},
		{"unknown field", 200, strings.TrimSuffix(success, "}") + `,"extra":"hidden"}`, 503, "unavailable"},
		{"nested field", 200, strings.Replace(success, `"value":42`, `"value":42,"org":"hidden"`, 1), 503, "unavailable"},
		{"wrong name", 200, strings.Replace(success, "reports", "other", 1), 503, "unavailable"},
		{"unauthorized mark", 200, strings.Replace(success, `"value":42`, `"value":43`, 1), 503, "unavailable"},
		{"overflow", 200, strings.Replace(success, `"namespace_id":17`, `"namespace_id":9223372036854775808`, 1), 503, "unavailable"},
		{"float", 200, strings.Replace(success, `"namespace_id":17`, `"namespace_id":17.0`, 1), 503, "unavailable"},
		{"duplicate", 200, strings.TrimSuffix(success, "}") + `,"name":"reports"}`, 503, "unavailable"},
		{"trailing", 200, success + `{}`, 503, "unavailable"},
		{"oversized", 200, strings.Repeat(" ", scopedNamespaceResponseLimit+1), 503, "unavailable"},
		{"wrong case", 200, strings.Replace(success, `"name"`, `"Name"`, 1), 503, "unavailable"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var hops atomic.Int64
			peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				hops.Add(1)
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Location", "/not-followed")
				w.Header().Set("x-hw-owner", "not-relayed")
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			}))
			defer peer.Close()
			h := NewServer(m, map[string]string{"remote": peer.URL}).WithAuth(f.auth).Handler()
			w := scopedNamespaceCall(t, h, scopedNamespaceTestPath, token)
			scopedNamespaceWantCode(t, w, tc.want, tc.code)
			if hops.Load() != 1 || w.Header().Get("Location") != "" || w.Header().Get("x-hw-owner") != "" {
				t.Fatal("peer refusal retries or discloses routing")
			}
		})
	}
	if m.OpenCount() != 0 || m.metrics.Acquires.Load() != 0 {
		t.Fatal("peer refusal falls through to acquisition")
	}
}

func TestScopedNamespacePeerCancellationAndNoSecondHop(t *testing.T) {
	f := newScopedNamespaceAuthFixture(t)
	fs := NewFakeStore()
	scopedNamespaceRemoteManifest(t, fs)
	m := scopedNamespaceTestManager(t, fs, "reader")
	entered, canceled := make(chan struct{}), make(chan struct{})
	var hops atomic.Int64
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hops.Add(1)
		close(entered)
		<-r.Context().Done()
		close(canceled)
	}))
	defer peer.Close()
	h := NewServer(m, map[string]string{"remote": peer.URL}).WithAuth(f.auth).Handler()
	token := "Bearer " + scopedNamespaceTestToken(t, f, "42", nil)
	r := httptest.NewRequest(http.MethodGet, scopedNamespaceTestPath, nil)
	r.Header.Set("Authorization", token)
	r.Header.Set("x-hw-noproxy", "1")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	scopedNamespaceWantCode(t, w, 503, "unavailable")
	if hops.Load() != 0 {
		t.Fatal("routing control allows a second hop")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r = httptest.NewRequest(http.MethodGet, scopedNamespaceTestPath, nil).WithContext(ctx)
	r.Header.Set("Authorization", token)
	w, done := httptest.NewRecorder(), make(chan struct{})
	go func() {
		h.ServeHTTP(w, r)
		close(done)
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("peer request does not start")
	}
	cancel()
	for _, ch := range []<-chan struct{}{done, canceled} {
		select {
		case <-ch:
		case <-time.After(5 * time.Second):
			t.Fatal("cancellation does not reach the peer")
		}
	}
	scopedNamespaceWantCode(t, w, 503, "unavailable")
}

func TestScopedNamespaceLegacySQLStillRefuses(t *testing.T) {
	f := newScopedNamespaceAuthFixture(t)
	store := &scopedNamespaceStore{Backend: NewFakeStore()}
	m := scopedNamespaceTestManager(t, store, "reader")
	h := NewServer(m, nil).WithAuth(f.auth).Handler()
	for _, route := range []string{"/v1/t/opaque_1/query", "/v1/t/opaque_1/tx", "/v1/query-batch"} {
		r := httptest.NewRequest(http.MethodPost, route, strings.NewReader(`{"sql":"SELECT 1"}`))
		r.Header.Set("Authorization", "Bearer "+scopedNamespaceTestToken(t, f, "42", nil))
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 403 {
			t.Fatalf("%s status=%d", route, w.Code)
		}
	}
	if store.manifests.Load() != 0 || store.objects.Load() != 0 || store.mutations.Load() != 0 {
		t.Fatal("legacy SQL refusal reaches storage")
	}
}

func TestScopedNamespaceMissingSchemaDoesNotInitialize(t *testing.T) {
	f := newScopedNamespaceAuthFixture(t)
	m := scopedNamespaceTestManager(t, NewFakeStore(), "owner")
	tenant, err := m.Acquire(context.Background(), "opaque_1", true)
	if err != nil {
		t.Fatal(err)
	}
	h := NewServer(m, nil).WithAuth(f.auth).Handler()
	token := "Bearer " + scopedNamespaceTestToken(t, f, "42", nil)
	scopedNamespaceWantCode(t, scopedNamespaceCall(t, h, scopedNamespaceTestPath, token), 404, "not_found")
	result, err := tenant.SubmitQueryCtx(context.Background(), Statement{SQL: `SELECT count(*) FROM sqlite_master WHERE name IN ('meta','namespaces')`})
	if err != nil || len(result.Rows) != 1 || fmt.Sprint(result.Rows[0][0]) != "0" {
		t.Fatal("read creates the catalog schema")
	}
}

func TestScopedNamespaceLocalCeilingAndResponseLimit(t *testing.T) {
	f := newScopedNamespaceAuthFixture(t)
	m := scopedNamespaceTestManager(t, NewFakeStore(), "owner")
	tenant := scopedNamespaceSeed(t, m)
	var hasDeadline atomic.Bool
	tenant.readHook = func(ctx context.Context) {
		deadline, ok := ctx.Deadline()
		if ok && time.Until(deadline) > 0 && time.Until(deadline) <= scopedNamespaceCeiling {
			hasDeadline.Store(true)
		}
	}
	h := NewServer(m, nil).WithAuth(f.auth).Handler()
	token := "Bearer " + scopedNamespaceTestToken(t, f, "42", nil)
	if w := scopedNamespaceCall(t, h, scopedNamespaceTestPath, token); w.Code != 200 || !hasDeadline.Load() {
		t.Fatal("local read lacks the five-second ceiling")
	}
	// The encoded target fits. JSON escaping makes the DTO exceed 4 KiB.
	name := strings.Repeat("<", 700)
	scopedNamespaceWrite(t, tenant, "large-name",
		Statement{SQL: `INSERT INTO namespaces VALUES(23,?,1,NULL,'team_id',42)`, Args: []any{name}})
	path := strings.TrimSuffix(scopedNamespaceTestPath, "reports") + strings.Repeat("%3C", 700)
	scopedNamespaceWantCode(t, scopedNamespaceCall(t, h, path, token), 503, "unavailable")
}

func TestScopedNamespaceNoLiveLoadedOwner(t *testing.T) {
	f := newScopedNamespaceAuthFixture(t)
	token := "Bearer " + scopedNamespaceTestToken(t, f, "42", nil)
	for _, mode := range []string{"unconfigured", "expired", "unloaded", "transport"} {
		t.Run(mode, func(t *testing.T) {
			fs := NewFakeStore()
			scopedNamespaceRemoteManifest(t, fs)
			m := scopedNamespaceTestManager(t, fs, "reader")
			var hops atomic.Int64
			peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				hops.Add(1)
				scopedNamespaceRefusal(w, 503)
			}))
			defer peer.Close()
			peers := map[string]string{"remote": peer.URL}
			switch mode {
			case "unconfigured":
				peers = nil
			case "expired":
				if err := fs.PutPodLease(context.Background(), "remote-incarnation", time.Now().Add(-time.Minute).UnixMilli()); err != nil {
					t.Fatal(err)
				}
			case "transport":
				peer.Close()
			}
			h := NewServer(m, peers).WithAuth(f.auth).Handler()
			scopedNamespaceWantCode(t, scopedNamespaceCall(t, h, scopedNamespaceTestPath, token), 503, "unavailable")
			if m.OpenCount() != 0 || m.metrics.Acquires.Load() != 0 {
				t.Fatal("unavailable owner causes acquisition")
			}
			wantHops := int64(0)
			if mode == "unloaded" {
				wantHops = 1
			}
			if hops.Load() != wantHops {
				t.Fatal("unavailable owner triggers an unexpected peer hop")
			}
		})
	}
}
