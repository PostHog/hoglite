package hoglite

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const (
	scopedTableTestPath = readSessionTestPath + "/tables/metrics"
	scopedScanTestPath  = scopedTableTestPath + "/scan"
	scopedTestUUID1     = "11111111-1111-4111-8111-111111111111"
	scopedTestUUID2     = "22222222-2222-4222-8222-222222222222"
	scopedTestUUIDOther = "33333333-3333-4333-8333-333333333333"
)

func newScopedTableScanHarness(t *testing.T) (*readSessionAuthFixture, *localS3Fixture, *readSessionDaemonSpy, *Manager, *Tenant, *Server) {
	return newScopedTableScanHarnessWithOpts(t, nil)
}

func newScopedTableScanHarnessWithOpts(t *testing.T, mutateOpts func(*TenantOpts)) (*readSessionAuthFixture, *localS3Fixture, *readSessionDaemonSpy, *Manager, *Tenant, *Server) {
	t.Helper()
	f := newReadSessionAuthFixture(t)
	s3fx := newLocalS3Fixture(t)
	fake := NewFakeStore()
	spy := &readSessionDaemonSpy{scopedNamespaceStore: &scopedNamespaceStore{Backend: fake}}

	opts := testOpts()
	opts.Lease = 30 * time.Second
	opts.HeartbeatEvery = 5 * time.Second
	if mutateOpts != nil {
		mutateOpts(&opts)
	}

	mgr := NewManager(spy, readSessionTestPrefix, "owner-1", "standard", t.TempDir(), opts, 1000, &Metrics{}, ControllerOpts{})
	t.Cleanup(mgr.ShutdownAll)

	tenant, err := mgr.Acquire(context.Background(), readSessionTestTenant, true)
	if err != nil {
		t.Fatal(err)
	}
	scopedNamespaceWrite(t, tenant, "seed-table-scan-schema",
		Statement{SQL: `CREATE TABLE meta(
			id INTEGER PRIMARY KEY CHECK(id = 1),
			name TEXT NOT NULL,
			next_snapshot INTEGER NOT NULL DEFAULT 1,
			earliest_snapshot_id INTEGER NOT NULL DEFAULT 0,
			dropping INTEGER NOT NULL DEFAULT 0
		)`},
		Statement{SQL: `INSERT INTO meta(id, name, next_snapshot, earliest_snapshot_id, dropping) VALUES(1, ?, 6, 1, 0)`, Args: []any{readSessionTestCatalog}},
		Statement{SQL: `CREATE TABLE namespaces(
			id INTEGER PRIMARY KEY,
			name TEXT NOT NULL,
			created_snapshot INTEGER NOT NULL,
			dropped_snapshot INTEGER,
			contained_field TEXT,
			contained_value INTEGER
		)`},
		Statement{SQL: `INSERT INTO namespaces VALUES(17, 'reports', 1, NULL, 'team_id', 42)`},
		Statement{SQL: `INSERT INTO namespaces VALUES(18, 'other', 1, NULL, 'team_id', 43)`},
		Statement{SQL: `INSERT INTO namespaces VALUES(19, 'uncontained', 1, NULL, NULL, NULL)`},
		Statement{SQL: `INSERT INTO namespaces VALUES(20, 'dropped_ns', 1, 3, 'team_id', 42)`},
		Statement{SQL: `CREATE TABLE tables(
			id INTEGER PRIMARY KEY,
			namespace_id INTEGER NOT NULL,
			namespace TEXT NOT NULL,
			name TEXT NOT NULL,
			table_uuid TEXT NOT NULL UNIQUE,
			created_snapshot INTEGER NOT NULL,
			dropped_snapshot INTEGER
		)`},
		Statement{SQL: `CREATE TABLE table_versions(
			table_id INTEGER NOT NULL,
			namespace_id INTEGER NOT NULL,
			namespace TEXT NOT NULL,
			name TEXT NOT NULL,
			begin_snapshot INTEGER NOT NULL,
			end_snapshot INTEGER,
			PRIMARY KEY(table_id, begin_snapshot)
		)`},
		Statement{SQL: `CREATE TABLE columns(
			table_id INTEGER NOT NULL,
			field_id INTEGER NOT NULL,
			parent_field_id INTEGER,
			ordinal INTEGER NOT NULL,
			name TEXT NOT NULL,
			type TEXT NOT NULL,
			type_params_json TEXT,
			nullable INTEGER NOT NULL,
			comment TEXT,
			begin_snapshot INTEGER NOT NULL,
			end_snapshot INTEGER,
			PRIMARY KEY(table_id, field_id, begin_snapshot)
		)`},
		Statement{SQL: `CREATE TABLE table_totals(
			table_id INTEGER PRIMARY KEY,
			record_count INTEGER NOT NULL,
			file_count INTEGER NOT NULL,
			file_size_bytes INTEGER NOT NULL
		)`},
		Statement{SQL: `CREATE TABLE files(
			id INTEGER PRIMARY KEY,
			table_id INTEGER NOT NULL,
			begin_snapshot INTEGER NOT NULL,
			end_snapshot INTEGER,
			record_count INTEGER NOT NULL,
			file_size_bytes INTEGER NOT NULL,
			footer_size INTEGER,
			row_id_start INTEGER NOT NULL,
			stats_state TEXT NOT NULL,
			file_format TEXT NOT NULL DEFAULT 'parquet',
			path TEXT NOT NULL,
			explicit_row_ids INTEGER NOT NULL DEFAULT 0
		)`},
		Statement{SQL: `CREATE TABLE dvs(
			data_file_id INTEGER NOT NULL,
			table_id INTEGER NOT NULL,
			delete_file_id INTEGER NOT NULL,
			path TEXT NOT NULL,
			delete_count INTEGER NOT NULL,
			file_size_bytes INTEGER NOT NULL,
			begin_snapshot INTEGER NOT NULL,
			end_snapshot INTEGER,
			epoch INTEGER NOT NULL,
			PRIMARY KEY(data_file_id, epoch)
		)`},
		// Live table metrics (id=101) under reports (ns=17), with nested columns and historical column.
		Statement{SQL: `INSERT INTO tables VALUES(101, 17, 'reports', 'metrics', ?, 1, NULL)`, Args: []any{scopedTestUUID1}},
		Statement{SQL: `INSERT INTO table_versions VALUES(101, 17, 'reports', 'metrics', 1, NULL)`},
		Statement{SQL: `INSERT INTO table_totals VALUES(101, 999, 5, 65536)`},
		Statement{SQL: `INSERT INTO columns VALUES(101, 1, NULL, 0, 'id', 'long', NULL, 0, NULL, 1, NULL)`},
		Statement{SQL: `INSERT INTO columns VALUES(101, 2, NULL, 1, 'amount', 'decimal', '{"precision":10,"scale":2}', 1, 'usd', 1, NULL)`},
		Statement{SQL: `INSERT INTO columns VALUES(101, 3, NULL, 2, 'meta', 'struct', NULL, 1, NULL, 1, NULL)`},
		Statement{SQL: `INSERT INTO columns VALUES(101, 4, 3, 0, 'tags', 'list', NULL, 1, NULL, 1, NULL)`},
		Statement{SQL: `INSERT INTO columns VALUES(101, 5, 4, 0, 'element', 'string', NULL, 0, NULL, 1, NULL)`},
		Statement{SQL: `INSERT INTO columns VALUES(101, 6, 3, 1, 'score', 'double', '{}', 1, '', 1, NULL)`},
		Statement{SQL: `INSERT INTO columns VALUES(101, 7, NULL, 3, 'retired', 'string', NULL, 1, NULL, 1, 4)`},
		// Dropped table under reports (ns=17).
		Statement{SQL: `INSERT INTO tables VALUES(102, 17, 'reports', 'dropped_tbl', ?, 1, 3)`, Args: []any{scopedTestUUID2}},
		Statement{SQL: `INSERT INTO table_versions VALUES(102, 17, 'reports', 'dropped_tbl', 1, 3)`},
		Statement{SQL: `INSERT INTO columns VALUES(102, 1, NULL, 0, 'id', 'long', NULL, 0, NULL, 1, NULL)`},
		// Table under unauthorized namespace other (ns=18).
		Statement{SQL: `INSERT INTO tables VALUES(103, 18, 'other', 'metrics', ?, 1, NULL)`, Args: []any{scopedTestUUIDOther}},
		Statement{SQL: `INSERT INTO table_versions VALUES(103, 18, 'other', 'metrics', 1, NULL)`},
		Statement{SQL: `INSERT INTO columns VALUES(103, 1, NULL, 0, 'secret_col', 'string', NULL, 1, NULL, 1, NULL)`},
		// Data files and deletion vectors for table 101 (file 12 is compacted with explicit_row_ids=1).
		Statement{SQL: `INSERT INTO files VALUES(10, 101, 1, NULL, 3, 1024, 256, 0, 'none', 'parquet', 's3://example-customer/tables/team_id=42/part-0.parquet', 0)`},
		Statement{SQL: `INSERT INTO files VALUES(11, 101, 2, 4, 5, 2048, NULL, 3, 'provided', 'parquet', 's3://example-customer/tables/team_id=42/part-1.parquet', 0)`},
		Statement{SQL: `INSERT INTO files VALUES(12, 101, 4, NULL, 7, 4096, 512, 10, 'none', 'parquet', 's3://example-customer/tables/team_id=42/part-2.parquet', 1)`},
		// Two DV epochs on file 10: epoch 1 at snap 2, epoch 2 at snap 4.
		Statement{SQL: `INSERT INTO dvs VALUES(10, 101, 501, 's3://example-customer/tables/team_id=42/dv-1.puffin', 1, 128, 2, 4, 1)`},
		Statement{SQL: `INSERT INTO dvs VALUES(10, 101, 502, 's3://example-customer/tables/team_id=42/dv-2.puffin', 2, 256, 4, NULL, 2)`},
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

	spy.manifests.Store(0)
	spy.objects.Store(0)
	spy.mutations.Store(0)

	srv := NewServer(mgr, map[string]string{"peer-1": "http://127.0.0.1:1"}).
		WithAuth(f.auth).
		WithReadSessionS3(s3fx.s3Config())
	return f, s3fx, spy, mgr, tenant, srv
}

func scopedTableScanToken(t *testing.T, f *readSessionAuthFixture, s3fx *localS3Fixture, teamID, ak, sk, st string, mutate func(map[string]any)) string {
	t.Helper()
	s3fx.setCaller(ak, sk, st, "/"+readSessionTestBucket+"/"+manifestKey(readSessionTestPrefix, readSessionTestTenant))
	return "Bearer " + f.token(t, func(c map[string]any) {
		c["prd"] = map[string]map[string]string{"team_id": {teamID: "read"}}
		c["aws"] = map[string]any{
			"access_key_id":     ak,
			"secret_access_key": sk,
			"session_token":     st,
			"region":            readSessionTestRegion,
			"expires_at":        int64(2_000_000_060),
		}
		c["grants"] = []map[string]any{
			{
				"catalog": readSessionTestCatalog,
				"target":  readSessionTestCatalog,
				"bucket":  "example-customer",
				"prefix":  "tables/team_id=" + teamID + "/",
				"region":  readSessionTestRegion,
				"actions": []string{"s3:GetObject"},
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
				"prefix":  "events/team_id=" + teamID + "/",
				"region":  readSessionTestRegion,
				"actions": []string{"s3:GetObject"},
			},
			{
				"catalog": readSessionTestCatalog,
				"target":  "remote:imports",
				"bucket":  "example-remote",
				"prefix":  "imports/team_id=" + teamID + "/",
				"region":  readSessionTestRegion,
				"actions": []string{"s3:GetObject"},
			},
		}
		if mutate != nil {
			mutate(c)
		}
	})
}

func TestScopedTableLookupHappyPathAndNestedColumns(t *testing.T) {
	f, s3fx, spy, _, _, srv := newScopedTableScanHarness(t)
	spy.denyManifest.Store(true)
	h := srv.Handler()

	token := scopedTableScanToken(t, f, s3fx, "42",
		"ASIAEXAMPLEALICE0001", "secret-alice-table", "session-alice-table", nil)
	w := scopedNamespaceCall(t, h, scopedTableTestPath, token)
	if w.Code != http.StatusOK {
		t.Fatalf("table lookup status=%d body=%s", w.Code, w.Body.String())
	}
	if w.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("Cache-Control=%q, want no-store", w.Header().Get("Cache-Control"))
	}

	// Verify totals are omitted from the raw JSON response.
	var raw map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &raw); err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"record_count", "file_count", "file_size_bytes"} {
		if _, present := raw[forbidden]; present {
			t.Fatalf("response must omit %q", forbidden)
		}
	}

	var dto scopedTableDTO
	if err := json.Unmarshal(w.Body.Bytes(), &dto); err != nil {
		t.Fatal(err)
	}
	if dto.NamespaceID != 17 || dto.Namespace != "reports" ||
		dto.Containment.Field != "team_id" || dto.Containment.Value != 42 ||
		dto.TableID != 101 || dto.TableUUID != scopedTestUUID1 ||
		dto.Name != "metrics" || dto.ReadSnapshotID != 5 {
		t.Fatalf("unexpected table dto: %+v", dto)
	}
	if len(dto.Columns) != 3 {
		t.Fatalf("got %d top-level columns, want 3 (retired column excluded)", len(dto.Columns))
	}
	if dto.Columns[0].Name != "id" || dto.Columns[0].Nullable || len(dto.Columns[0].Children) != 0 ||
		dto.Columns[0].Comment != "" || len(dto.Columns[0].TypeParams) != 0 {
		t.Fatalf("unexpected column 0: %+v", dto.Columns[0])
	}
	if dto.Columns[1].Name != "amount" || dto.Columns[1].Comment != "usd" ||
		string(dto.Columns[1].TypeParams) != `{"precision":10,"scale":2}` {
		t.Fatalf("unexpected column 1: %+v", dto.Columns[1])
	}
	// Verify nested children (struct -> list -> element, plus scalar sibling with empty type_params/comment omitted).
	metaCol := dto.Columns[2]
	if metaCol.Name != "meta" || len(metaCol.Children) != 2 {
		t.Fatalf("unexpected nested struct column: %+v", metaCol)
	}
	if metaCol.Children[0].Name != "tags" || len(metaCol.Children[0].Children) != 1 ||
		metaCol.Children[0].Children[0].Name != "element" || metaCol.Children[0].Children[0].Nullable {
		t.Fatalf("unexpected list child column: %+v", metaCol.Children[0])
	}
	if metaCol.Children[1].Name != "score" || len(metaCol.Children[1].TypeParams) != 0 || metaCol.Children[1].Comment != "" {
		t.Fatalf("empty type_params and comment should be omitted: %+v", metaCol.Children[1])
	}

	// Verify both pre-read and post-read manifest checks used the caller's credentials.
	recs := s3fx.snapshotRecords()
	if len(recs) != 2 || spy.manifests.Load() != 0 {
		t.Fatalf("manifest reads=%d daemon=%d, want 2 and 0", len(recs), spy.manifests.Load())
	}
	for i, rec := range recs {
		if !rec.sigValid || rec.accessKeyID != "ASIAEXAMPLEALICE0001" || rec.status != http.StatusOK {
			t.Fatalf("manifest read %d invalid: %+v", i, rec)
		}
	}
}

func TestScopedTableLookupRefusalsAndSQLIsolation(t *testing.T) {
	f, s3fx, spy, _, tenant, srv := newScopedTableScanHarness(t)
	spy.denyManifest.Store(true)
	h := srv.Handler()

	token := scopedTableScanToken(t, f, s3fx, "42",
		"ASIAEXAMPLEALICE0001", "secret-alice-ref", "session-alice-ref", nil)

	// Missing, dropped, uncontained, and unauthorized targets return 404.
	for _, path := range []string{
		readSessionTestPath + "/tables/missing",
		readSessionTestPath + "/tables/dropped_tbl",
		strings.Replace(scopedTableTestPath, "/namespaces/reports/", "/namespaces/other/", 1),
		strings.Replace(scopedTableTestPath, "/namespaces/reports/", "/namespaces/uncontained/", 1),
		strings.Replace(scopedTableTestPath, "/namespaces/reports/", "/namespaces/dropped_ns/", 1),
		strings.Replace(scopedTableTestPath, "/namespaces/reports/", "/namespaces/missing_ns/", 1),
	} {
		w := scopedNamespaceCall(t, h, path, token)
		scopedNamespaceWantCode(t, w, http.StatusNotFound, "not_found")
		if strings.Contains(w.Body.String(), "secret_col") || strings.Contains(w.Body.String(), "example-customer") {
			t.Fatalf("refusal leaked metadata: %s", w.Body.String())
		}
	}

	// SQL routes still refuse read-session tokens.
	if w := authRequest(t, h, http.MethodPost, "/v1/t/"+readSessionTestTenant+"/query", token, `{"sql":"SELECT 1"}`); w.Code != http.StatusUnauthorized {
		t.Fatalf("SQL /query accepted read-session token: status=%d", w.Code)
	}
	if w := authRequest(t, h, http.MethodPost, "/v1/t/"+readSessionTestTenant+"/tx", token, `{"txid":"t1","stmts":[{"sql":"SELECT 1"}]}`); w.Code != http.StatusUnauthorized {
		t.Fatalf("SQL /tx accepted read-session token: status=%d", w.Code)
	}

	// Legacy metadata token and storage-session v1 token are refused on table and scan routes.
	legacy := "Bearer " + f.signClaims(t, map[string]any{
		"iss": "https://issuer.test", "aud": []string{"hoglake"}, "sub": "u", "usr": "u", "jti": "j",
		"ten": "scoped", "org": "o", "iat": int64(2_000_000_000), "exp": int64(2_000_000_060),
		"cat": map[string][]string{readSessionTestCatalog: {"read"}},
		"prd": map[string]map[string]string{"team_id": {"42": "read"}},
		"rtg": map[string]any{"b": "hoglite", "be": readSessionTestBackend, "t": readSessionTestTenant},
	})
	scopedNamespaceWantCode(t, scopedNamespaceCall(t, h, scopedTableTestPath, legacy), http.StatusUnauthorized, "unauthorized")
	scopedNamespaceWantCode(t, scopedNamespaceCall(t, h, scopedScanTestPath+"?snapshot=5&table_uuid="+scopedTestUUID1, legacy), http.StatusUnauthorized, "unauthorized")

	v1 := "Bearer " + f.token(t, func(c map[string]any) { c["profile"] = readSessionExperimentalID })
	scopedNamespaceWantCode(t, scopedNamespaceCall(t, h, scopedTableTestPath, v1), http.StatusUnauthorized, "unauthorized")
	scopedNamespaceWantCode(t, scopedNamespaceCall(t, h, scopedScanTestPath+"?snapshot=5&table_uuid="+scopedTestUUID1, v1), http.StatusUnauthorized, "unauthorized")

	// Dropping catalog, mismatched catalog name, and expired head return 404.
	scopedNamespaceWrite(t, tenant, "expire-head", Statement{SQL: `UPDATE meta SET earliest_snapshot_id = 6 WHERE id = 1`})
	scopedNamespaceWantCode(t, scopedNamespaceCall(t, h, scopedTableTestPath, token), http.StatusNotFound, "not_found")
	scopedNamespaceWrite(t, tenant, "dropping-cat", Statement{SQL: `UPDATE meta SET earliest_snapshot_id = 1, dropping = 1 WHERE id = 1`})
	scopedNamespaceWantCode(t, scopedNamespaceCall(t, h, scopedTableTestPath, token), http.StatusNotFound, "not_found")
	scopedNamespaceWrite(t, tenant, "wrong-cat-name", Statement{SQL: `UPDATE meta SET dropping = 0, name = 'other-catalog' WHERE id = 1`})
	scopedNamespaceWantCode(t, scopedNamespaceCall(t, h, scopedTableTestPath, token), http.StatusNotFound, "not_found")
}

func TestScopedTableAndScanDefaultOffAndRequestShape(t *testing.T) {
	// Default-off when scoped namespace is off, when readSession is nil, and on MetricsHandler.
	fOff := newScopedNamespaceAuthFixture(t)
	mgrOff := scopedNamespaceTestManager(t, NewFakeStore(), "owner")
	srvOnlyLegacy := NewServer(mgrOff, nil).WithAuth(fOff.auth)
	scopedNamespaceWantCode(t, scopedNamespaceCall(t, srvOnlyLegacy.Handler(), scopedNamespaceTestPath+"/tables/metrics", ""), http.StatusNotFound, "not_found")
	scopedNamespaceWantCode(t, scopedNamespaceCall(t, srvOnlyLegacy.Handler(), scopedNamespaceTestPath+"/tables/metrics/scan?snapshot=1&table_uuid="+scopedTestUUID1, ""), http.StatusNotFound, "not_found")

	f, s3fx, _, _, _, srv := newScopedTableScanHarness(t)
	if w := scopedNamespaceCall(t, srv.MetricsHandler(), scopedTableTestPath, ""); w.Code != http.StatusNotFound {
		t.Fatalf("metrics listener exposed table route: %d", w.Code)
	}
	if w := scopedNamespaceCall(t, srv.MetricsHandler(), scopedScanTestPath+"?snapshot=5&table_uuid="+scopedTestUUID1, ""); w.Code != http.StatusNotFound {
		t.Fatalf("metrics listener exposed scan route: %d", w.Code)
	}

	h := srv.Handler()
	token := scopedTableScanToken(t, f, s3fx, "42",
		"ASIAEXAMPLEALICE0001", "secret-alice-shape", "session-alice-shape", nil)

	// Invalid request targets and query strings return 400 before touching S3.
	badTargets := []string{
		scopedTableTestPath + "?totals=false",
		scopedTableTestPath + "?",
		scopedTableTestPath + "/",
		scopedTableTestPath + "/extra",
		strings.Replace(scopedTableTestPath, "/tables/", "/%74ables/", 1),
		scopedScanTestPath,
		scopedScanTestPath + "?",
		scopedScanTestPath + "?snapshot=5",
		scopedScanTestPath + "?table_uuid=" + scopedTestUUID1,
		scopedScanTestPath + "?snapshot=-1&table_uuid=" + scopedTestUUID1,
		scopedScanTestPath + "?snapshot=01&table_uuid=" + scopedTestUUID1,
		scopedScanTestPath + "?snapshot=+1&table_uuid=" + scopedTestUUID1,
		scopedScanTestPath + "?snapshot=9223372036854775808&table_uuid=" + scopedTestUUID1,
		scopedScanTestPath + "?snapshot=5&table_uuid=11111111-1111-4111-8111-11111111111A",
		scopedScanTestPath + "?snapshot=5&table_uuid=not-a-uuid",
		scopedScanTestPath + "?snapshot=5&snapshot=5&table_uuid=" + scopedTestUUID1,
		scopedScanTestPath + "?snapshot=5&table_uuid=" + scopedTestUUID1 + "&extra=1",
		scopedScanTestPath + "?snapshot=5&&table_uuid=" + scopedTestUUID1,
		scopedScanTestPath + "?snapshot=5&table_uuid=" + scopedTestUUID1 + "&",
		scopedScanTestPath + "?snapshot=5;table_uuid=" + scopedTestUUID1,
		strings.Replace(scopedScanTestPath, "/scan", "/%73can", 1) + "?snapshot=5&table_uuid=" + scopedTestUUID1,
		scopedTableTestPath + strings.Repeat("a", scopedNamespaceTargetLimit),
	}
	for _, target := range badTargets {
		scopedNamespaceWantCode(t, scopedNamespaceCall(t, h, target, token), http.StatusBadRequest, "invalid_request")
	}
	for _, method := range []string{http.MethodHead, http.MethodPost, http.MethodPut} {
		for _, path := range []string{scopedTableTestPath, scopedScanTestPath + "?snapshot=5&table_uuid=" + scopedTestUUID1} {
			r := httptest.NewRequest(method, path, nil)
			r.Header.Set("Authorization", token)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			scopedNamespaceWantCode(t, w, http.StatusBadRequest, "invalid_request")
		}
	}
	// DELETE is invalid on scan and forbidden for a read-only token on a table.
	{
		r := httptest.NewRequest(http.MethodDelete, scopedScanTestPath+"?snapshot=5&table_uuid="+scopedTestUUID1, nil)
		r.Header.Set("Authorization", token)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		scopedNamespaceWantCode(t, w, http.StatusBadRequest, "invalid_request")
	}
	{
		r := httptest.NewRequest(http.MethodDelete, scopedTableTestPath, nil)
		r.Header.Set("Authorization", token)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		scopedNamespaceWantCode(t, w, http.StatusForbidden, "forbidden")
	}
	r := httptest.NewRequest(http.MethodGet, scopedTableTestPath, strings.NewReader("{}"))
	r.Header.Set("Authorization", token)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	scopedNamespaceWantCode(t, w, http.StatusBadRequest, "invalid_request")
	scopedNamespaceWantCode(t, scopedNamespaceCall(t, h, scopedTableTestPath, "Bearer "+strings.Repeat("x", scopedNamespaceTokenLimit+1)), http.StatusUnauthorized, "unauthorized")
	if len(s3fx.snapshotRecords()) != 0 {
		t.Fatalf("invalid requests reached S3: %d", len(s3fx.snapshotRecords()))
	}
}

func TestScopedScanHappyPathAndSnapshotPinning(t *testing.T) {
	f, s3fx, spy, _, tenant, srv := newScopedTableScanHarness(t)
	spy.denyManifest.Store(true)
	h := srv.Handler()

	token := scopedTableScanToken(t, f, s3fx, "42",
		"ASIAEXAMPLEALICE0001", "secret-alice-scan", "session-alice-scan", nil)

	// Snapshot 3: files 10 (with DV epoch 1) and 11 (null footer_size, no DV).
	w := scopedNamespaceCall(t, h, scopedScanTestPath+"?table_uuid="+scopedTestUUID1+"&snapshot=3", token)
	if w.Code != http.StatusOK {
		t.Fatalf("scan@3 status=%d body=%s", w.Code, w.Body.String())
	}
	if w.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("Cache-Control=%q, want no-store", w.Header().Get("Cache-Control"))
	}
	var dto3 scopedScanDTO
	if err := json.Unmarshal(w.Body.Bytes(), &dto3); err != nil {
		t.Fatal(err)
	}
	if dto3.NamespaceID != 17 || dto3.Namespace != "reports" ||
		dto3.TableUUID != scopedTestUUID1 || dto3.ReadSnapshotID != 3 {
		t.Fatalf("unexpected scan@3 header: %+v", dto3)
	}
	if len(dto3.Files) != 2 {
		t.Fatalf("scan@3 files=%d, want 2", len(dto3.Files))
	}
	f0 := dto3.Files[0]
	if f0.DataFile.DataFileID != 10 || f0.DataFile.FooterSize == nil || *f0.DataFile.FooterSize != 256 ||
		f0.DataFile.ExplicitRowIDs ||
		f0.DeleteFile == nil || f0.DeleteFile.DeleteFileID != 501 || f0.DeleteFile.DeleteCount != 1 {
		t.Fatalf("unexpected scan@3 file 0: %+v", f0)
	}
	f1 := dto3.Files[1]
	if f1.DataFile.DataFileID != 11 || f1.DataFile.FooterSize != nil || f1.DataFile.ExplicitRowIDs || f1.DeleteFile != nil {
		t.Fatalf("unexpected scan@3 file 1: %+v", f1)
	}

	// Snapshot 5 (head): file 10 (explicit_row_ids=false, DV epoch 2) and compacted file 12 (explicit_row_ids=true).
	w = scopedNamespaceCall(t, h, scopedScanTestPath+"?snapshot=5&table_uuid="+scopedTestUUID1, token)
	if w.Code != http.StatusOK {
		t.Fatalf("scan@5 status=%d body=%s", w.Code, w.Body.String())
	}
	raw5 := w.Body.String()
	if !strings.Contains(raw5, `"explicit_row_ids":false`) || !strings.Contains(raw5, `"explicit_row_ids":true`) {
		t.Fatalf("scan@5 must always emit explicit_row_ids bools, got body=%s", raw5)
	}
	var dto5 scopedScanDTO
	if err := json.Unmarshal(w.Body.Bytes(), &dto5); err != nil {
		t.Fatal(err)
	}
	if len(dto5.Files) != 2 || dto5.Files[0].DataFile.DataFileID != 10 ||
		dto5.Files[0].DataFile.ExplicitRowIDs ||
		dto5.Files[0].DeleteFile == nil || dto5.Files[0].DeleteFile.DeleteFileID != 502 ||
		dto5.Files[1].DataFile.DataFileID != 12 || !dto5.Files[1].DataFile.ExplicitRowIDs || dto5.Files[1].DeleteFile != nil {
		t.Fatalf("unexpected scan@5 files: %+v", dto5.Files)
	}

	// Snapshot 1: file 10 has no DV yet, and exact object_key primary grant authorizes it.
	exactTok := scopedTableScanToken(t, f, s3fx, "42",
		"ASIAEXAMPLEEXACT0001", "secret-exact-scan", "session-exact-scan", func(c map[string]any) {
			grants := c["grants"].([]map[string]any)
			grants[0] = map[string]any{
				"catalog":    readSessionTestCatalog,
				"target":     readSessionTestCatalog,
				"bucket":     "example-customer",
				"object_key": "tables/team_id=42/part-0.parquet",
				"region":     readSessionTestRegion,
				"actions":    []string{"s3:GetObject"},
			}
		})
	w = scopedNamespaceCall(t, h, scopedScanTestPath+"?snapshot=1&table_uuid="+scopedTestUUID1, exactTok)
	if w.Code != http.StatusOK {
		t.Fatalf("scan@1 exact object_key status=%d body=%s", w.Code, w.Body.String())
	}
	var dto1 scopedScanDTO
	if err := json.Unmarshal(w.Body.Bytes(), &dto1); err != nil || len(dto1.Files) != 1 || dto1.Files[0].DeleteFile != nil {
		t.Fatalf("unexpected scan@1 result: err=%v dto=%+v", err, dto1)
	}

	// Snapshot retention bounds (earliest=1, head=5): 0 and 6 both return 404.
	scopedNamespaceWantCode(t, scopedNamespaceCall(t, h, scopedScanTestPath+"?snapshot=0&table_uuid="+scopedTestUUID1, token), http.StatusNotFound, "not_found")
	scopedNamespaceWantCode(t, scopedNamespaceCall(t, h, scopedScanTestPath+"?snapshot=6&table_uuid="+scopedTestUUID1, token), http.StatusNotFound, "not_found")

	// Drop and recreate table metrics with a new UUID at snapshot 6.
	const recreatedUUID = "44444444-4444-4444-8444-444444444444"
	scopedNamespaceWrite(t, tenant, "drop-recreate-table",
		Statement{SQL: `UPDATE meta SET next_snapshot = 8 WHERE id = 1`},
		Statement{SQL: `UPDATE tables SET dropped_snapshot = 6 WHERE id = 101`},
		Statement{SQL: `UPDATE table_versions SET end_snapshot = 6 WHERE table_id = 101`},
		Statement{SQL: `INSERT INTO tables VALUES(104, 17, 'reports', 'metrics', ?, 7, NULL)`, Args: []any{recreatedUUID}},
		Statement{SQL: `INSERT INTO table_versions VALUES(104, 17, 'reports', 'metrics', 7, NULL)`},
		Statement{SQL: `INSERT INTO columns VALUES(104, 1, NULL, 0, 'id', 'long', NULL, 0, NULL, 7, NULL)`},
	)
	// Old UUID at new snapshot 7 returns 404; new UUID at old snapshot 5 returns 404.
	scopedNamespaceWantCode(t, scopedNamespaceCall(t, h, scopedScanTestPath+"?snapshot=7&table_uuid="+scopedTestUUID1, token), http.StatusNotFound, "not_found")
	scopedNamespaceWantCode(t, scopedNamespaceCall(t, h, scopedScanTestPath+"?snapshot=5&table_uuid="+recreatedUUID, token), http.StatusNotFound, "not_found")
	// New UUID at snapshot 7 succeeds with an empty files array (not null).
	w = scopedNamespaceCall(t, h, scopedScanTestPath+"?snapshot=7&table_uuid="+recreatedUUID, token)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"files":[]`) {
		t.Fatalf("recreated empty scan status=%d body=%s", w.Code, w.Body.String())
	}
	// Old UUID at historical snapshot 5 still succeeds while retained.
	w = scopedNamespaceCall(t, h, scopedScanTestPath+"?snapshot=5&table_uuid="+scopedTestUUID1, token)
	if w.Code != http.StatusOK {
		t.Fatalf("historical scan@5 status=%d body=%s", w.Code, w.Body.String())
	}
}

func TestScopedScanStorageGrantEnforcementAndRedaction(t *testing.T) {
	f, s3fx, spy, _, tenant, srv := newScopedTableScanHarness(t)
	spy.denyManifest.Store(true)
	h := srv.Handler()

	token := scopedTableScanToken(t, f, s3fx, "42",
		"ASIAEXAMPLEALICE0001", "secret-alice-grant", "session-alice-grant", nil)

	checkForbiddenRedacted := func(t *testing.T, path string, secrets ...string) {
		t.Helper()
		w := scopedNamespaceCall(t, h, path, token)
		scopedNamespaceWantCode(t, w, http.StatusForbidden, "forbidden")
		body := w.Body.String()
		for _, secret := range secrets {
			if strings.Contains(body, secret) {
				t.Fatalf("403 body leaked %q: %s", secret, body)
			}
		}
	}

	// Valid S3 object names with characters like parentheses under an authorized prefix are accepted.
	scopedNamespaceWrite(t, tenant, "valid-s3-object-chars",
		Statement{SQL: `UPDATE files SET path = 's3://example-customer/tables/team_id=42/part(1).parquet' WHERE id = 12`},
	)
	w := scopedNamespaceCall(t, h, scopedScanTestPath+"?snapshot=5&table_uuid="+scopedTestUUID1, token)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "part(1).parquet") {
		t.Fatalf("valid S3 object key with parentheses failed: status=%d body=%s", w.Code, w.Body.String())
	}

	// Data file outside primary-catalog grant (wrong prefix, wrong bucket, traversal, empty segment, control char, non-s3, remote target, manifest).
	badDataPaths := []struct {
		name string
		path string
	}{
		{"wrong_team_prefix", "s3://example-customer/tables/team_id=43/part-0.parquet"},
		{"wrong_bucket", "s3://other-bucket/tables/team_id=42/part-0.parquet"},
		{"traversal", "s3://example-customer/tables/team_id=42/../team_id=43/part-0.parquet"},
		{"dot_segment", "s3://example-customer/tables/team_id=42/./part-0.parquet"},
		{"empty_segment", "s3://example-customer/tables/team_id=42//part-0.parquet"},
		{"control_char", "s3://example-customer/tables/team_id=42/part\x00.parquet"},
		{"non_s3_scheme", "https://example-customer/tables/team_id=42/part-0.parquet"},
		{"remote_target_grant_only", "s3://example-remote/events/team_id=42/part-0.parquet"},
		{"manifest_grant_only", "s3://" + readSessionTestBucket + "/" + manifestKey(readSessionTestPrefix, readSessionTestTenant)},
	}
	for i, tc := range badDataPaths {
		t.Run(tc.name, func(t *testing.T) {
			scopedNamespaceWrite(t, tenant, fmt.Sprintf("bad-data-%d", i),
				Statement{SQL: `UPDATE files SET path = ? WHERE id = 12`, Args: []any{tc.path}},
			)
			checkForbiddenRedacted(t, scopedScanTestPath+"?snapshot=5&table_uuid="+scopedTestUUID1,
				tc.path, "example-customer", "other-bucket", "example-remote", "ASIAEXAMPLEALICE0001", "secret-alice-grant", "session-alice-grant")
		})
	}

	// Restore valid data file path, then point DV epoch 2 outside the signed primary-catalog grant.
	scopedNamespaceWrite(t, tenant, "bad-dv-path",
		Statement{SQL: `UPDATE files SET path = 's3://example-customer/tables/team_id=42/part-2.parquet' WHERE id = 12`},
		Statement{SQL: `UPDATE dvs SET path = 's3://example-customer/tables/team_id=99/dv-2.puffin' WHERE data_file_id = 10 AND epoch = 2`},
	)
	checkForbiddenRedacted(t, scopedScanTestPath+"?snapshot=5&table_uuid="+scopedTestUUID1,
		"team_id=99", "dv-2.puffin", "example-customer", "ASIAEXAMPLEALICE0001")
	// Snapshot 3 still uses DV epoch 1, which remains within the grant.
	if w := scopedNamespaceCall(t, h, scopedScanTestPath+"?snapshot=3&table_uuid="+scopedTestUUID1, token); w.Code != http.StatusOK {
		t.Fatalf("scan@3 with valid DV epoch 1 failed: status=%d body=%s", w.Code, w.Body.String())
	}
}

func TestScopedTableAndScanDistinctCallersAndManifestChecks(t *testing.T) {
	f, s3fx, spy, _, tenant, srv := newScopedTableScanHarness(t)
	spy.denyManifest.Store(true)
	h := srv.Handler()

	// Seed a file under team 43's table (id=103, ns=18 'other').
	scopedNamespaceWrite(t, tenant, "seed-other-file",
		Statement{SQL: `INSERT INTO files VALUES(20, 103, 1, NULL, 2, 512, 64, 0, 'none', 'parquet', 's3://example-customer/tables/team_id=43/part-0.parquet', 0)`},
	)

	aliceTok := scopedTableScanToken(t, f, s3fx, "42",
		"ASIAEXAMPLEALICE0001", "secret-alice-dual", "session-alice-dual", nil)
	bobTok := scopedTableScanToken(t, f, s3fx, "43",
		"ASIAEXAMPLEBOB000002", "secret-bob-dual", "session-bob-dual", nil)

	otherTablePath := strings.Replace(scopedTableTestPath, "/namespaces/reports/", "/namespaces/other/", 1)
	otherScanPath := otherTablePath + "/scan?snapshot=5&table_uuid=" + scopedTestUUIDOther

	// Alice reads reports; Bob reads other; neither can read the other's table or scan.
	if w := scopedNamespaceCall(t, h, scopedTableTestPath, aliceTok); w.Code != http.StatusOK {
		t.Fatalf("alice table status=%d body=%s", w.Code, w.Body.String())
	}
	if w := scopedNamespaceCall(t, h, otherTablePath, bobTok); w.Code != http.StatusOK {
		t.Fatalf("bob table status=%d body=%s", w.Code, w.Body.String())
	}
	if w := scopedNamespaceCall(t, h, scopedScanTestPath+"?snapshot=5&table_uuid="+scopedTestUUID1, aliceTok); w.Code != http.StatusOK {
		t.Fatalf("alice scan status=%d body=%s", w.Code, w.Body.String())
	}
	if w := scopedNamespaceCall(t, h, otherScanPath, bobTok); w.Code != http.StatusOK {
		t.Fatalf("bob scan status=%d body=%s", w.Code, w.Body.String())
	}
	scopedNamespaceWantCode(t, scopedNamespaceCall(t, h, otherScanPath, aliceTok), http.StatusNotFound, "not_found")
	scopedNamespaceWantCode(t, scopedNamespaceCall(t, h, scopedScanTestPath+"?snapshot=5&table_uuid="+scopedTestUUID1, bobTok), http.StatusNotFound, "not_found")

	recs := s3fx.snapshotRecords()
	// 6 requests * 2 manifest checks each = 12 caller-signed S3 reads; 0 daemon reads.
	if len(recs) != 12 || spy.manifests.Load() != 0 {
		t.Fatalf("manifest reads=%d daemon=%d, want 12 and 0", len(recs), spy.manifests.Load())
	}
	// Verify alternating caller credentials on pre-read and post-read pairs.
	wantAKs := []string{
		"ASIAEXAMPLEALICE0001", "ASIAEXAMPLEALICE0001",
		"ASIAEXAMPLEBOB000002", "ASIAEXAMPLEBOB000002",
		"ASIAEXAMPLEALICE0001", "ASIAEXAMPLEALICE0001",
		"ASIAEXAMPLEBOB000002", "ASIAEXAMPLEBOB000002",
		"ASIAEXAMPLEALICE0001", "ASIAEXAMPLEALICE0001",
		"ASIAEXAMPLEBOB000002", "ASIAEXAMPLEBOB000002",
	}
	for i, rec := range recs {
		if !rec.sigValid || rec.accessKeyID != wantAKs[i] || rec.status != http.StatusOK {
			t.Fatalf("record %d got ak=%s sig=%v status=%d, want ak=%s", i, rec.accessKeyID, rec.sigValid, rec.status, wantAKs[i])
		}
	}
}

func TestScopedTableAndScanExpiryOwnershipLossAndColdTenant(t *testing.T) {
	t.Run("zero_skew_aws_expiry", func(t *testing.T) {
		f, s3fx, spy, _, _, srv := newScopedTableScanHarness(t)
		spy.denyManifest.Store(true)
		h := srv.Handler()
		token := scopedTableScanToken(t, f, s3fx, "42",
			"ASIAEXAMPLEALICE0001", "secret-alice-exp", "session-alice-exp", nil)
		// Exact expiry boundary (zero skew) refuses before calling S3.
		f.setNow(time.Unix(2_000_000_060, 0))
		scopedNamespaceWantCode(t, scopedNamespaceCall(t, h, scopedTableTestPath, token), http.StatusUnauthorized, "unauthorized")
		scopedNamespaceWantCode(t, scopedNamespaceCall(t, h, scopedScanTestPath+"?snapshot=5&table_uuid="+scopedTestUUID1, token), http.StatusUnauthorized, "unauthorized")
		if len(s3fx.snapshotRecords()) != 0 {
			t.Fatalf("expired credentials reached S3: %d", len(s3fx.snapshotRecords()))
		}
	})

	t.Run("mid_request_expiry_on_post_read", func(t *testing.T) {
		f, s3fx, spy, _, _, srv := newScopedTableScanHarness(t)
		spy.denyManifest.Store(true)
		h := srv.Handler()
		token := scopedTableScanToken(t, f, s3fx, "42",
			"ASIAEXAMPLEALICE0001", "secret-alice-mid", "session-alice-mid", nil)
		var calls atomic.Int32
		s3fx.mu.Lock()
		s3fx.onRequest = func(_ int, _ string) {
			if calls.Add(1) == 1 {
				f.setNow(time.Unix(2_000_000_060, 0))
			}
		}
		s3fx.mu.Unlock()
		scopedNamespaceWantCode(t, scopedNamespaceCall(t, h, scopedScanTestPath+"?snapshot=5&table_uuid="+scopedTestUUID1, token), http.StatusUnauthorized, "unauthorized")
		if len(s3fx.snapshotRecords()) != 1 {
			t.Fatalf("expected 1 S3 read before post-read expiry refusal, got %d", len(s3fx.snapshotRecords()))
		}
	})

	t.Run("ownership_loss_between_pre_and_post_read", func(t *testing.T) {
		f, s3fx, spy, _, _, srv := newScopedTableScanHarness(t)
		spy.denyManifest.Store(true)
		h := srv.Handler()
		token := scopedTableScanToken(t, f, s3fx, "42",
			"ASIAEXAMPLEALICE0001", "secret-alice-own", "session-alice-own", nil)
		var calls atomic.Int32
		s3fx.mu.Lock()
		s3fx.onRequest = func(_ int, _ string) {
			if calls.Add(1) == 1 {
				stale, _ := json.Marshal(&Manifest{Owner: "owner-2", Epoch: 99, Instance: "other-inst"})
				s3fx.objects["/"+readSessionTestBucket+"/"+manifestKey(readSessionTestPrefix, readSessionTestTenant)] = stale
			}
		}
		s3fx.mu.Unlock()
		scopedNamespaceWantCode(t, scopedNamespaceCall(t, h, scopedTableTestPath, token), http.StatusServiceUnavailable, "unavailable")
		if len(s3fx.snapshotRecords()) != 2 {
			t.Fatalf("expected 2 manifest checks (pre + post), got %d", len(s3fx.snapshotRecords()))
		}
	})

	t.Run("cold_tenant_unavailable_no_acquire", func(t *testing.T) {
		f, s3fx, spy, mgr, _, srv := newScopedTableScanHarness(t)
		spy.denyManifest.Store(true)
		h := srv.Handler()
		const coldTenant = "cold_9"
		coldTablePath := "/v1/t/" + coldTenant + "/metadata/v1/catalogs/" + readSessionTestCatalog + "/namespaces/reports/tables/metrics"
		coldScanPath := coldTablePath + "/scan?snapshot=1&table_uuid=" + scopedTestUUID1
		coldTok := scopedTableScanToken(t, f, s3fx, "42",
			"ASIAEXAMPLECOLD00001", "secret-cold", "session-cold", func(c map[string]any) {
				c["rtg"] = map[string]any{"b": "hoglite", "be": readSessionTestBackend, "t": coldTenant}
				grants := c["grants"].([]map[string]any)
				grants[1]["object_key"] = manifestKey(readSessionTestPrefix, coldTenant)
			})
		beforeAcquires := mgr.metrics.Acquires.Load()
		scopedNamespaceWantCode(t, scopedNamespaceCall(t, h, coldTablePath, coldTok), http.StatusServiceUnavailable, "unavailable")
		scopedNamespaceWantCode(t, scopedNamespaceCall(t, h, coldScanPath, coldTok), http.StatusServiceUnavailable, "unavailable")
		if mgr.metrics.Acquires.Load() != beforeAcquires || mgr.LocalTenant(coldTenant) != nil || len(s3fx.snapshotRecords()) != 0 {
			t.Fatal("cold tenant triggered acquisition or S3 manifest call")
		}
	})
}

func TestScopedTableAndScanBoundsAndColdSchema(t *testing.T) {
	t.Run("scan_file_limit_1001_refused", func(t *testing.T) {
		f, s3fx, spy, _, tenant, srv := newScopedTableScanHarness(t)
		spy.denyManifest.Store(true)
		h := srv.Handler()
		token := scopedTableScanToken(t, f, s3fx, "42",
			"ASIAEXAMPLEALICE0001", "secret-alice-lim", "session-alice-lim", nil)
		// Insert 1001 visible files under a new table (id=201).
		const bigUUID = "55555555-5555-4555-8555-555555555555"
		stmts := []Statement{
			{SQL: `INSERT INTO tables VALUES(201, 17, 'reports', 'many_files', ?, 1, NULL)`, Args: []any{bigUUID}},
			{SQL: `INSERT INTO table_versions VALUES(201, 17, 'reports', 'many_files', 1, NULL)`},
		}
		for i := 1; i <= 1001; i++ {
			stmts = append(stmts, Statement{
				SQL:  `INSERT INTO files VALUES(?, 201, 1, NULL, 1, 64, 16, ?, 'none', 'parquet', ?, 0)`,
				Args: []any{1000 + i, i, fmt.Sprintf("s3://example-customer/tables/team_id=42/f-%04d.parquet", i)},
			})
		}
		scopedNamespaceWrite(t, tenant, "seed-1001-files", stmts...)
		path := readSessionTestPath + "/tables/many_files/scan?snapshot=5&table_uuid=" + bigUUID
		scopedNamespaceWantCode(t, scopedNamespaceCall(t, h, path, token), http.StatusServiceUnavailable, "unavailable")
	})

	t.Run("table_response_over_256kib_refused", func(t *testing.T) {
		f, s3fx, spy, _, tenant, srv := newScopedTableScanHarness(t)
		spy.denyManifest.Store(true)
		h := srv.Handler()
		token := scopedTableScanToken(t, f, s3fx, "42",
			"ASIAEXAMPLEALICE0001", "secret-alice-big", "session-alice-big", nil)
		bigComment := strings.Repeat("c", 260<<10)
		scopedNamespaceWrite(t, tenant, "oversize-col",
			Statement{SQL: `UPDATE columns SET comment = ? WHERE table_id = 101 AND field_id = 1`, Args: []any{bigComment}},
		)
		scopedNamespaceWantCode(t, scopedNamespaceCall(t, h, scopedTableTestPath, token), http.StatusServiceUnavailable, "unavailable")
	})

	t.Run("scan_response_over_1mib_refused", func(t *testing.T) {
		f, s3fx, spy, _, tenant, srv := newScopedTableScanHarness(t)
		spy.denyManifest.Store(true)
		h := srv.Handler()
		token := scopedTableScanToken(t, f, s3fx, "42",
			"ASIAEXAMPLEALICE0001", "secret-alice-1mb", "session-alice-1mb", nil)
		// Insert 950 files with long (~1020-byte) keys so JSON exceeds 1 MiB while file count <= 1000.
		const bigScanUUID = "66666666-6666-4666-8666-666666666666"
		pad := strings.Repeat("p", 950)
		stmts := []Statement{
			{SQL: `INSERT INTO tables VALUES(202, 17, 'reports', 'huge_scan', ?, 1, NULL)`, Args: []any{bigScanUUID}},
			{SQL: `INSERT INTO table_versions VALUES(202, 17, 'reports', 'huge_scan', 1, NULL)`},
		}
		for i := 1; i <= 950; i++ {
			stmts = append(stmts, Statement{
				SQL:  `INSERT INTO files VALUES(?, 202, 1, NULL, 1, 64, 16, ?, 'none', 'parquet', ?, 0)`,
				Args: []any{3000 + i, i, fmt.Sprintf("s3://example-customer/tables/team_id=42/%s-%04d.parquet", pad, i)},
			})
		}
		scopedNamespaceWrite(t, tenant, "seed-oversize-scan", stmts...)
		path := readSessionTestPath + "/tables/huge_scan/scan?snapshot=5&table_uuid=" + bigScanUUID
		scopedNamespaceWantCode(t, scopedNamespaceCall(t, h, path, token), http.StatusServiceUnavailable, "unavailable")
	})

	t.Run("missing_schema_returns_404_without_init", func(t *testing.T) {
		f := newReadSessionAuthFixture(t)
		s3fx := newLocalS3Fixture(t)
		fake := NewFakeStore()
		opts := testOpts()
		opts.Lease = 30 * time.Second
		mgr := NewManager(fake, readSessionTestPrefix, "owner-1", "standard", t.TempDir(), opts, 1000, &Metrics{}, ControllerOpts{})
		t.Cleanup(mgr.ShutdownAll)
		if _, err := mgr.Acquire(context.Background(), readSessionTestTenant, true); err != nil {
			t.Fatal(err)
		}
		mf, _, err := fake.GetManifest(context.Background(), readSessionTestTenant)
		if err != nil || mf == nil {
			t.Fatalf("manifest missing: %v", err)
		}
		mfBytes, _ := json.Marshal(mf)
		s3fx.putObject(readSessionTestBucket, manifestKey(readSessionTestPrefix, readSessionTestTenant), mfBytes)
		srv := NewServer(mgr, nil).WithAuth(f.auth).WithReadSessionS3(s3fx.s3Config())
		token := scopedTableScanToken(t, f, s3fx, "42",
			"ASIAEXAMPLEALICE0001", "secret-alice-empty", "session-alice-empty", nil)
		scopedNamespaceWantCode(t, scopedNamespaceCall(t, srv.Handler(), scopedTableTestPath, token), http.StatusNotFound, "not_found")
		scopedNamespaceWantCode(t, scopedNamespaceCall(t, srv.Handler(), scopedScanTestPath+"?snapshot=1&table_uuid="+scopedTestUUID1, token), http.StatusNotFound, "not_found")
	})

	t.Run("denied_table_over_sqlite_read_budget_returns_404", func(t *testing.T) {
		f, s3fx, spy, _, tenant, srv := newScopedTableScanHarnessWithOpts(t, func(o *TenantOpts) {
			o.MaxResultRows = 5
		})
		spy.denyManifest.Store(true)
		h := srv.Handler()

		// Populate denied table 103 (namespace other, team_id=43) with 12 columns and 12 files, exceeding MaxResultRows=5.
		var stmts []Statement
		for i := 2; i <= 12; i++ {
			stmts = append(stmts, Statement{
				SQL:  `INSERT INTO columns VALUES(103, ?, NULL, ?, ?, 'string', NULL, 1, NULL, 1, NULL)`,
				Args: []any{i, i - 1, fmt.Sprintf("col_%d", i)},
			})
		}
		for i := 1; i <= 12; i++ {
			stmts = append(stmts, Statement{
				SQL:  `INSERT INTO files VALUES(?, 103, 1, NULL, 1, 64, 16, ?, 'none', 'parquet', ?, 0)`,
				Args: []any{4000 + i, i, fmt.Sprintf("s3://example-customer/tables/team_id=43/f-%02d.parquet", i)},
			})
		}
		scopedNamespaceWrite(t, tenant, "seed-over-budget-denied-table", stmts...)

		aliceTok := scopedTableScanToken(t, f, s3fx, "42",
			"ASIAEXAMPLEALICE0001", "secret-alice-budget", "session-alice-budget", nil)
		bobTok := scopedTableScanToken(t, f, s3fx, "43",
			"ASIAEXAMPLEBOB000002", "secret-bob-budget", "session-bob-budget", nil)

		otherTablePath := strings.Replace(scopedTableTestPath, "/namespaces/reports/", "/namespaces/other/", 1)
		otherScanPath := otherTablePath + "/scan?snapshot=5&table_uuid=" + scopedTestUUIDOther

		// Confirm the table genuinely exceeds the SQLite result budget for its authorized caller.
		scopedNamespaceWantCode(t, scopedNamespaceCall(t, h, otherTablePath, bobTok), http.StatusServiceUnavailable, "unavailable")
		scopedNamespaceWantCode(t, scopedNamespaceCall(t, h, otherScanPath, bobTok), http.StatusServiceUnavailable, "unavailable")

		// Unauthorized caller receives 404 (not 503) because SQL filters namespace containment first.
		scopedNamespaceWantCode(t, scopedNamespaceCall(t, h, otherTablePath, aliceTok), http.StatusNotFound, "not_found")
		scopedNamespaceWantCode(t, scopedNamespaceCall(t, h, otherScanPath, aliceTok), http.StatusNotFound, "not_found")
	})

	t.Run("scan_sorts_bounded_files_by_row_id_start_and_id", func(t *testing.T) {
		f, s3fx, spy, _, tenant, srv := newScopedTableScanHarness(t)
		spy.denyManifest.Store(true)
		h := srv.Handler()
		token := scopedTableScanToken(t, f, s3fx, "42",
			"ASIAEXAMPLEALICE0001", "secret-alice-sort", "session-alice-sort", nil)

		const sortUUID = "77777777-7777-4777-8777-777777777777"
		scopedNamespaceWrite(t, tenant, "seed-unsorted-files",
			Statement{SQL: `INSERT INTO tables VALUES(203, 17, 'reports', 'unsorted_files', ?, 1, NULL)`, Args: []any{sortUUID}},
			Statement{SQL: `INSERT INTO table_versions VALUES(203, 17, 'reports', 'unsorted_files', 1, NULL)`},
			Statement{SQL: `INSERT INTO files VALUES(50, 203, 1, NULL, 1, 64, 16, 100, 'none', 'parquet', 's3://example-customer/tables/team_id=42/f50.parquet', 0)`},
			Statement{SQL: `INSERT INTO files VALUES(30, 203, 1, NULL, 1, 64, 16, 20, 'none', 'parquet', 's3://example-customer/tables/team_id=42/f30.parquet', 1)`},
			Statement{SQL: `INSERT INTO files VALUES(20, 203, 1, NULL, 1, 64, 16, 20, 'none', 'parquet', 's3://example-customer/tables/team_id=42/f20.parquet', 0)`},
			Statement{SQL: `INSERT INTO files VALUES(40, 203, 1, NULL, 1, 64, 16, 5, 'none', 'parquet', 's3://example-customer/tables/team_id=42/f40.parquet', 1)`},
		)

		// Verify SQLite materializes bounded files before joining dvs and does not use a temp B-tree sort.
		planArgs := []any{
			readSessionTestCatalog, int64(5), int64(5),
			"reports", "team_id", int64(42), int64(5), int64(5),
			"unsorted_files", sortUUID,
			int64(5), int64(5), int64(5), int64(5),
			int64(5), int64(5), int64(5), int64(5), int64(5), int64(5),
		}
		planRes, _, err := tenant.SubmitTxCtx(context.Background(), TxRequest{
			Stmts: []Statement{{SQL: "EXPLAIN QUERY PLAN " + scopedScanFilesSQL, Args: planArgs}},
		})
		if err != nil || len(planRes) != 1 {
			t.Fatalf("explain query plan failed: err=%v", err)
		}
		var planText strings.Builder
		for _, r := range planRes[0].Rows {
			fmt.Fprintln(&planText, r...)
		}
		if !strings.Contains(planText.String(), "MATERIALIZE bounded") || strings.Contains(planText.String(), "USE TEMP B-TREE FOR ORDER BY") {
			t.Fatalf("unexpected query plan:\n%s", planText.String())
		}

		path := readSessionTestPath + "/tables/unsorted_files/scan?snapshot=5&table_uuid=" + sortUUID
		w := scopedNamespaceCall(t, h, path, token)
		if w.Code != http.StatusOK {
			t.Fatalf("unsorted scan status=%d body=%s", w.Code, w.Body.String())
		}
		var dto scopedScanDTO
		if err := json.Unmarshal(w.Body.Bytes(), &dto); err != nil {
			t.Fatal(err)
		}
		wantIDs := []int64{40, 20, 30, 50}
		if len(dto.Files) != len(wantIDs) {
			t.Fatalf("got %d files, want %d", len(dto.Files), len(wantIDs))
		}
		for i, wantID := range wantIDs {
			if dto.Files[i].DataFile.DataFileID != wantID {
				t.Fatalf("file[%d] id=%d, want %d (order=%+v)", i, dto.Files[i].DataFile.DataFileID, wantID, dto.Files)
			}
		}
	})

	t.Run("list namespaces and tables with read-only and writable sessions", func(t *testing.T) {
		f, s3fx, spy, _, _, srv := newScopedTableScanHarness(t)
		spy.denyManifest.Store(true)
		h := srv.Handler()
		readOnlyTok := scopedTableScanToken(t, f, s3fx, "42",
			"ASIAEXAMPLEALICE0001", "secret-alice-list", "session-alice-list", nil)
		writableTok := scopedTableScanToken(t, f, s3fx, "42",
			"ASIAEXAMPLEALICE0002", "secret-alice-write", "session-alice-write", func(c map[string]any) {
				c["cat"] = map[string]any{readSessionTestCatalog: []string{"read", "write"}}
				c["prd"] = map[string]any{"team_id": map[string]string{"42": "write"}}
				grants := c["grants"].([]map[string]any)
				grants[0]["actions"] = readSessionWriteActions
			})
		base := "/v1/t/" + readSessionTestTenant + "/metadata/v1/catalogs/" + readSessionTestCatalog
		for name, tok := range map[string]string{"read-only": readOnlyTok, "writable": writableTok} {
			t.Run(name, func(t *testing.T) {
				w := scopedNamespaceCall(t, h, base+"/namespaces", tok)
				if w.Code != http.StatusOK {
					t.Fatalf("list namespaces status=%d body=%s", w.Code, w.Body.String())
				}
				var nsList scopedNamespacesDTO
				if err := json.Unmarshal(w.Body.Bytes(), &nsList); err != nil {
					t.Fatal(err)
				}
				var gotNames []string
				for _, ns := range nsList.Namespaces {
					if ns.Containment.Field != "team_id" || ns.Containment.Value != 42 {
						t.Fatalf("unexpected containment %+v", ns)
					}
					gotNames = append(gotNames, ns.Name)
				}
				if len(gotNames) != 1 || gotNames[0] != "reports" {
					t.Fatalf("unexpected namespaces: %v (want [reports], excluding seeded sibling 'other')", gotNames)
				}
				// Keyset pagination with ?after=reports returns an empty next page.
				w = scopedNamespaceCall(t, h, base+"/namespaces?after=reports", tok)
				if w.Code != http.StatusOK {
					t.Fatalf("list namespaces after status=%d body=%s", w.Code, w.Body.String())
				}
				var afterNs scopedNamespacesDTO
				if err := json.Unmarshal(w.Body.Bytes(), &afterNs); err != nil {
					t.Fatal(err)
				}
				if len(afterNs.Namespaces) != 0 || afterNs.NextAfter != "" {
					t.Fatalf("unexpected paginated namespaces: %+v", afterNs)
				}

				w = scopedNamespaceCall(t, h, base+"/namespaces/reports/tables", tok)
				if w.Code != http.StatusOK {
					t.Fatalf("list tables status=%d body=%s", w.Code, w.Body.String())
				}
				var tblList scopedTablesDTO
				if err := json.Unmarshal(w.Body.Bytes(), &tblList); err != nil {
					t.Fatal(err)
				}
				if tblList.Namespace != "reports" || len(tblList.Tables) == 0 {
					t.Fatalf("unexpected tables response: %+v", tblList)
				}
				// Keyset pagination on tables with ?after=<first> advances past it.
				w = scopedNamespaceCall(t, h, base+"/namespaces/reports/tables?after="+tblList.Tables[0].Name, tok)
				if w.Code != http.StatusOK {
					t.Fatalf("list tables after status=%d body=%s", w.Code, w.Body.String())
				}
				var afterTbl scopedTablesDTO
				if err := json.Unmarshal(w.Body.Bytes(), &afterTbl); err != nil {
					t.Fatal(err)
				}
				if len(afterTbl.Tables) >= len(tblList.Tables) {
					t.Fatalf("expected fewer tables after %q, got %+v", tblList.Tables[0].Name, afterTbl.Tables)
				}
				// Unknown query parameters are rejected with 400.
				scopedNamespaceWantCode(t, scopedNamespaceCall(t, h, base+"/namespaces?limit=10", tok), http.StatusBadRequest, "invalid_request")

				// Seeded sibling team-43 namespace ("other") must return 404.
				scopedNamespaceWantCode(t, scopedNamespaceCall(t, h, base+"/namespaces/other/tables", tok), http.StatusNotFound, "not_found")
				// Existing typed read routes also admit writable sessions.
				if w := scopedNamespaceCall(t, h, readSessionTestPath, tok); w.Code != http.StatusOK {
					t.Fatalf("namespace GET status=%d body=%s", w.Code, w.Body.String())
				}
				if w := scopedNamespaceCall(t, h, readSessionTestPath+"/tables/"+tblList.Tables[0].Name, tok); w.Code != http.StatusOK {
					t.Fatalf("table GET status=%d body=%s", w.Code, w.Body.String())
				}
			})
		}
	})
}
