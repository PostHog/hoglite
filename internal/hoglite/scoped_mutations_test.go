package hoglite

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func scopedMutationCall(t *testing.T, h http.Handler, method, path, token string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var reader *bytes.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		reader = bytes.NewReader(b)
	}
	var req *http.Request
	if reader != nil {
		req = httptest.NewRequest(method, path, reader)
		req.Header.Set("Content-Type", "application/json")
	} else {
		req = httptest.NewRequest(method, path, nil)
	}
	req.RequestURI = path
	if token != "" {
		if !strings.HasPrefix(token, "Bearer ") {
			token = "Bearer " + token
		}
		req.Header.Set("Authorization", token)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}

func upgradeScopedMutationCatalog(t *testing.T, tenant *Tenant, withLegacyContainmentIndex bool) {
	t.Helper()
	stmts := []Statement{
		{SQL: `ALTER TABLE meta ADD COLUMN next_namespace_id INTEGER NOT NULL DEFAULT 100`},
		{SQL: `ALTER TABLE meta ADD COLUMN next_table_id INTEGER NOT NULL DEFAULT 300`},
		{SQL: `ALTER TABLE meta ADD COLUMN next_file_id INTEGER NOT NULL DEFAULT 500`},
		{SQL: `ALTER TABLE meta ADD COLUMN schema_version INTEGER NOT NULL DEFAULT 1`},
	}
	if withLegacyContainmentIndex {
		stmts = append(stmts, Statement{
			SQL: `CREATE UNIQUE INDEX IF NOT EXISTS namespaces_live_containment ON namespaces(contained_field, contained_value) WHERE dropped_snapshot IS NULL AND contained_field IS NOT NULL`,
		})
	}
	stmts = append(stmts,
		Statement{SQL: `ALTER TABLE tables ADD COLUMN comment TEXT`},
		Statement{SQL: `ALTER TABLE tables ADD COLUMN next_row_id INTEGER NOT NULL DEFAULT 0`},
		Statement{SQL: `ALTER TABLE tables ADD COLUMN next_field_id INTEGER NOT NULL DEFAULT 1`},
		Statement{SQL: `ALTER TABLE tables ADD COLUMN schema_version INTEGER NOT NULL DEFAULT 0`},
		Statement{SQL: `ALTER TABLE table_versions ADD COLUMN comment TEXT`},
		Statement{SQL: `CREATE TABLE IF NOT EXISTS table_totals(table_id INTEGER PRIMARY KEY, record_count INTEGER NOT NULL DEFAULT 0, file_count INTEGER NOT NULL DEFAULT 0, file_size_bytes INTEGER NOT NULL DEFAULT 0)`},
		Statement{SQL: `CREATE TABLE IF NOT EXISTS snapshots(id INTEGER PRIMARY KEY, ts TEXT NOT NULL, author TEXT, message TEXT, schema_version INTEGER NOT NULL)`},
		Statement{SQL: `CREATE TABLE IF NOT EXISTS changes(snapshot_id INTEGER NOT NULL, kind TEXT NOT NULL, object_id INTEGER NOT NULL)`},
		Statement{SQL: `CREATE TABLE IF NOT EXISTS views(id INTEGER PRIMARY KEY, namespace_id INTEGER NOT NULL, namespace TEXT NOT NULL, name TEXT NOT NULL, view_uuid TEXT NOT NULL UNIQUE, dialect TEXT NOT NULL, sql_text TEXT NOT NULL, created_snapshot INTEGER NOT NULL, dropped_snapshot INTEGER)`},
		Statement{SQL: `CREATE TABLE IF NOT EXISTS removals(id INTEGER PRIMARY KEY, path TEXT NOT NULL, file_kind TEXT NOT NULL, reason TEXT NOT NULL, queued_snapshot INTEGER NOT NULL, queued_at TEXT NOT NULL, drained_at TEXT)`},
		Statement{SQL: `CREATE TABLE IF NOT EXISTS table_layouts(table_id INTEGER NOT NULL, begin_snapshot INTEGER NOT NULL, end_snapshot INTEGER, object_prefix_field_id INTEGER NOT NULL, PRIMARY KEY(table_id, begin_snapshot))`},
		Statement{SQL: `CREATE TABLE IF NOT EXISTS partition_specs(table_id INTEGER NOT NULL, spec_id INTEGER NOT NULL, fields_json TEXT NOT NULL, begin_snapshot INTEGER NOT NULL, end_snapshot INTEGER, PRIMARY KEY(table_id, spec_id))`},
	)
	scopedNamespaceWrite(t, tenant, "upgrade-to-full-catalog-ddl", stmts...)
}

func TestScopedMutationsLifecycleAndSecurity(t *testing.T) {
	f, s3fx, spy, _, tenant, srv := newScopedTableScanHarness(t)
	spy.denyManifest.Store(true)
	// Upgrade the minimal read fixture to full catalog DDL including the legacy unique containment index.
	upgradeScopedMutationCatalog(t, tenant, true)
	// Configure optional post-neutral patterns: reserved_ro (all mutations blocked) and managed_team_<digits> (schema create/drop blocked).
	roRes, err := compileNamespacePatterns([]string{`^reserved_ro$`})
	if err != nil {
		t.Fatal(err)
	}
	protRes, err := compileNamespacePatterns([]string{`^managed_team_[0-9]+$`})
	if err != nil {
		t.Fatal(err)
	}
	srv.auth.readSession.readOnlyNamespaces = roRes
	srv.auth.readSession.protectedSchemaLifecycle = protRes

	h := srv.Handler()
	base := "/v1/t/" + readSessionTestTenant + "/metadata/v1/catalogs/" + readSessionTestCatalog

	readOnlyTok := scopedTableScanToken(t, f, s3fx, "42",
		"ASIAEXAMPLEALICE0010", "secret-ro", "session-ro", nil)
	team42WriteTok := scopedTableScanToken(t, f, s3fx, "42",
		"ASIAEXAMPLEALICE0011", "secret-w42", "session-w42", func(c map[string]any) {
			c["cat"] = map[string]any{readSessionTestCatalog: []string{"read", "write"}}
			c["prd"] = map[string]any{"team_id": map[string]string{"42": "write"}}
			grants := c["grants"].([]map[string]any)
			grants[0]["prefix"] = "tables/team_id=42/"
			grants[0]["actions"] = readSessionWriteActions
		})
	team43WriteTok := scopedTableScanToken(t, f, s3fx, "43",
		"ASIAEXAMPLEBOB000012", "secret-w43", "session-w43", func(c map[string]any) {
			c["cat"] = map[string]any{readSessionTestCatalog: []string{"read", "write"}}
			c["prd"] = map[string]any{"team_id": map[string]string{"43": "write"}}
			grants := c["grants"].([]map[string]any)
			grants[0]["prefix"] = "tables/team_id=43/"
			grants[0]["actions"] = readSessionWriteActions
		})

	// 1. Read-only session is refused (403) on every mutation route.
	scopedNamespaceWantCode(t, scopedMutationCall(t, h, http.MethodPost, base+"/namespaces", readOnlyTok, createNamespaceReq{Name: "scratch"}), http.StatusForbidden, "forbidden")
	scopedNamespaceWantCode(t, scopedMutationCall(t, h, http.MethodDelete, base+"/namespaces/reports", readOnlyTok, nil), http.StatusForbidden, "forbidden")
	scopedNamespaceWantCode(t, scopedMutationCall(t, h, http.MethodPost, base+"/namespaces/reports/tables", readOnlyTok, createTableReq{
		Name: "t1", Columns: []createTableColumnReq{{Name: "id", Type: "long"}},
	}), http.StatusForbidden, "forbidden")
	scopedNamespaceWantCode(t, scopedMutationCall(t, h, http.MethodDelete, base+"/namespaces/reports/tables/events", readOnlyTok, nil), http.StatusForbidden, "forbidden")

	// 2. Reserved & protected namespaces are refused (403).
	for _, blocked := range []string{"team_42", "information_schema", "reserved_ro", "managed_team_42"} {
		scopedNamespaceWantCode(t, scopedMutationCall(t, h, http.MethodPost, base+"/namespaces", team42WriteTok, createNamespaceReq{Name: blocked}), http.StatusForbidden, "forbidden")
		scopedNamespaceWantCode(t, scopedMutationCall(t, h, http.MethodDelete, base+"/namespaces/"+blocked, team42WriteTok, nil), http.StatusForbidden, "forbidden")
	}
	for _, blocked := range []string{"team_42", "information_schema", "reserved_ro"} {
		scopedNamespaceWantCode(t, scopedMutationCall(t, h, http.MethodPost, base+"/namespaces/"+blocked+"/tables", team42WriteTok, createTableReq{
			Name: "t1", Columns: []createTableColumnReq{{Name: "id", Type: "long"}},
		}), http.StatusForbidden, "forbidden")
	}

	// 3. Team 42 creates a SECOND local schema (proving multi-schema containment works alongside seeded "reports").
	w := scopedMutationCall(t, h, http.MethodPost, base+"/namespaces", team42WriteTok, createNamespaceReq{Name: "scratch"})
	if w.Code != http.StatusCreated {
		t.Fatalf("create namespace status=%d body=%s", w.Code, w.Body.String())
	}
	var nsDTO scopedNamespaceDTO
	if err := json.Unmarshal(w.Body.Bytes(), &nsDTO); err != nil {
		t.Fatal(err)
	}
	if nsDTO.Name != "scratch" || nsDTO.Containment.Field != "team_id" || nsDTO.Containment.Value != 42 {
		t.Fatalf("unexpected created namespace: %+v", nsDTO)
	}
	// Duplicate create returns 409 Conflict.
	scopedNamespaceWantCode(t, scopedMutationCall(t, h, http.MethodPost, base+"/namespaces", team42WriteTok, createNamespaceReq{Name: "scratch"}), http.StatusConflict, "conflict")

	// 4. Cross-team isolation: team 43 cannot see, drop, or create tables in team 42's "scratch".
	scopedNamespaceWantCode(t, scopedMutationCall(t, h, http.MethodDelete, base+"/namespaces/scratch", team43WriteTok, nil), http.StatusNotFound, "not_found")
	scopedNamespaceWantCode(t, scopedMutationCall(t, h, http.MethodPost, base+"/namespaces/scratch/tables", team43WriteTok, createTableReq{
		Name: "evil", Columns: []createTableColumnReq{{Name: "id", Type: "long"}},
	}), http.StatusNotFound, "not_found")

	// 5. Team 42 creates a table in "scratch" (and reserved _hog* columns are rejected with 400).
	scopedNamespaceWantCode(t, scopedMutationCall(t, h, http.MethodPost, base+"/namespaces/scratch/tables", team42WriteTok, createTableReq{
		Name: "bad_col", Columns: []createTableColumnReq{{Name: "_hog_row_id", Type: "long"}},
	}), http.StatusBadRequest, "invalid_request")
	w = scopedMutationCall(t, h, http.MethodPost, base+"/namespaces/scratch/tables", team42WriteTok, createTableReq{
		Name: "metrics",
		Columns: []createTableColumnReq{
			{Name: "k", Type: "string"},
			{Name: "v", Type: "long"},
		},
	})
	if w.Code != http.StatusCreated {
		t.Fatalf("create table status=%d body=%s", w.Code, w.Body.String())
	}
	var tblDTO scopedTableDTO
	if err := json.Unmarshal(w.Body.Bytes(), &tblDTO); err != nil {
		t.Fatal(err)
	}
	if tblDTO.Name != "metrics" || tblDTO.Namespace != "scratch" || len(tblDTO.Columns) != 2 {
		t.Fatalf("unexpected created table: %+v", tblDTO)
	}
	// Non-empty schema drop returns 409 Conflict.
	scopedNamespaceWantCode(t, scopedMutationCall(t, h, http.MethodDelete, base+"/namespaces/scratch", team42WriteTok, nil), http.StatusConflict, "conflict")

	// 6. Out-of-scope file URI on COMMIT is refused (403) without changing the snapshot.
	scopedNamespaceWantCode(t, scopedMutationCall(t, h, http.MethodPost, base+"/namespaces/scratch/tables/metrics/commit", team42WriteTok, commitTableReq{
		TableUUID:      tblDTO.TableUUID,
		BaseSnapshotID: tblDTO.ReadSnapshotID,
		AddFiles: []commitAddFileReq{{
			Path: "s3://example-customer/tables/team_id=43/stolen.parquet", RecordCount: 1, FileSizeBytes: 64,
		}},
	}), http.StatusForbidden, "forbidden")

	// 7. Stale base_snapshot_id or wrong table_uuid returns 409 Conflict.
	scopedNamespaceWantCode(t, scopedMutationCall(t, h, http.MethodPost, base+"/namespaces/scratch/tables/metrics/commit", team42WriteTok, commitTableReq{
		TableUUID:      tblDTO.TableUUID,
		BaseSnapshotID: tblDTO.ReadSnapshotID - 1,
		AddFiles: []commitAddFileReq{{
			Path: "s3://example-customer/tables/team_id=42/stale.parquet", RecordCount: 1, FileSizeBytes: 64,
		}},
	}), http.StatusConflict, "conflict")
	scopedNamespaceWantCode(t, scopedMutationCall(t, h, http.MethodPost, base+"/namespaces/scratch/tables/metrics/commit", team42WriteTok, commitTableReq{
		TableUUID:      "99999999-9999-4999-8999-999999999999",
		BaseSnapshotID: tblDTO.ReadSnapshotID,
		AddFiles: []commitAddFileReq{{
			Path: "s3://example-customer/tables/team_id=42/wronguuid.parquet", RecordCount: 1, FileSizeBytes: 64,
		}},
	}), http.StatusConflict, "conflict")

	// Undrained removal path on COMMIT is refused (409 Conflict).
	scopedNamespaceWrite(t, tenant, "seed-undrained-removal",
		Statement{SQL: `INSERT INTO removals(id, path, file_kind, reason, queued_snapshot, queued_at, drained_at) VALUES(1, 's3://example-customer/tables/team_id=42/queued.parquet', 'data', 'trino_upload', 1, '2026-01-01T00:00:00Z', NULL)`},
	)
	scopedNamespaceWantCode(t, scopedMutationCall(t, h, http.MethodPost, base+"/namespaces/scratch/tables/metrics/commit", team42WriteTok, commitTableReq{
		TableUUID:      tblDTO.TableUUID,
		BaseSnapshotID: tblDTO.ReadSnapshotID,
		AddFiles: []commitAddFileReq{{
			Path: "s3://example-customer/tables/team_id=42/queued.parquet", RecordCount: 1, FileSizeBytes: 64,
		}},
	}), http.StatusConflict, "conflict")

	// 8. Valid INSERT commit succeeds and is visible via typed GET scan.
	w = scopedMutationCall(t, h, http.MethodPost, base+"/namespaces/scratch/tables/metrics/commit", team42WriteTok, commitTableReq{
		TableUUID:      tblDTO.TableUUID,
		BaseSnapshotID: tblDTO.ReadSnapshotID,
		AddFiles: []commitAddFileReq{{
			Path: "s3://example-customer/tables/team_id=42/scratch-part-1.parquet", RecordCount: 3, FileSizeBytes: 512,
		}},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("commit insert status=%d body=%s", w.Code, w.Body.String())
	}
	var commitResp commitTableResp
	if err := json.Unmarshal(w.Body.Bytes(), &commitResp); err != nil {
		t.Fatal(err)
	}
	if commitResp.SnapshotID <= tblDTO.ReadSnapshotID || commitResp.FilesAdded != 1 {
		t.Fatalf("unexpected commit resp: %+v", commitResp)
	}

	// Read back via typed scan GET.
	scanPath := fmt.Sprintf("%s/namespaces/scratch/tables/metrics/scan?snapshot=%d&table_uuid=%s", base, commitResp.SnapshotID, tblDTO.TableUUID)
	w = scopedNamespaceCall(t, h, scanPath, readOnlyTok)
	if w.Code != http.StatusOK {
		t.Fatalf("scan after commit status=%d body=%s", w.Code, w.Body.String())
	}
	var scanDTO scopedScanDTO
	if err := json.Unmarshal(w.Body.Bytes(), &scanDTO); err != nil {
		t.Fatal(err)
	}
	if len(scanDTO.Files) != 1 || scanDTO.Files[0].DataFile.Path != "s3://example-customer/tables/team_id=42/scratch-part-1.parquet" {
		t.Fatalf("unexpected scan after insert: %+v", scanDTO)
	}

	// 9. Valid DELETE (DV) commit attaches deletion vector to data_file_id.
	dataFileID := scanDTO.Files[0].DataFile.DataFileID
	// Read-only session COMMIT is refused (403), and out-of-scope DV URI is refused (403).
	scopedNamespaceWantCode(t, scopedMutationCall(t, h, http.MethodPost, base+"/namespaces/scratch/tables/metrics/commit", readOnlyTok, commitTableReq{
		TableUUID: tblDTO.TableUUID, BaseSnapshotID: commitResp.SnapshotID,
		AddDeleteFiles: []commitAddDeleteFileReq{{
			DataFileID: dataFileID, Path: "s3://example-customer/tables/team_id=42/dv-ro.puffin", DeleteCount: 1, FileSizeBytes: 64,
		}},
	}), http.StatusForbidden, "forbidden")
	scopedNamespaceWantCode(t, scopedMutationCall(t, h, http.MethodPost, base+"/namespaces/scratch/tables/metrics/commit", team42WriteTok, commitTableReq{
		TableUUID: tblDTO.TableUUID, BaseSnapshotID: commitResp.SnapshotID,
		AddDeleteFiles: []commitAddDeleteFileReq{{
			DataFileID: dataFileID, Path: "s3://example-customer/tables/team_id=43/dv-bad.puffin", DeleteCount: 1, FileSizeBytes: 64,
		}},
	}), http.StatusForbidden, "forbidden")
	// Over-count DV (delete_count > record_count=3) is refused (409).
	scopedNamespaceWantCode(t, scopedMutationCall(t, h, http.MethodPost, base+"/namespaces/scratch/tables/metrics/commit", team42WriteTok, commitTableReq{
		TableUUID: tblDTO.TableUUID, BaseSnapshotID: commitResp.SnapshotID,
		AddDeleteFiles: []commitAddDeleteFileReq{{
			DataFileID: dataFileID, Path: "s3://example-customer/tables/team_id=42/dv-over.puffin", DeleteCount: 4, FileSizeBytes: 64,
		}},
	}), http.StatusConflict, "conflict")

	w = scopedMutationCall(t, h, http.MethodPost, base+"/namespaces/scratch/tables/metrics/commit", team42WriteTok, commitTableReq{
		TableUUID:      tblDTO.TableUUID,
		BaseSnapshotID: commitResp.SnapshotID,
		AddDeleteFiles: []commitAddDeleteFileReq{{
			DataFileID: dataFileID, Path: "s3://example-customer/tables/team_id=42/dv-1.puffin", DeleteCount: 2, FileSizeBytes: 128,
		}},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("commit delete status=%d body=%s", w.Code, w.Body.String())
	}
	var dvCommit commitTableResp
	if err := json.Unmarshal(w.Body.Bytes(), &dvCommit); err != nil {
		t.Fatal(err)
	}
	if dvCommit.DeletesAdded != 1 {
		t.Fatalf("unexpected dv commit resp: %+v", dvCommit)
	}
	// Shrinking replacement DV (delete_count=1 < existing 2) is refused (409) to prevent row resurrection.
	scopedNamespaceWantCode(t, scopedMutationCall(t, h, http.MethodPost, base+"/namespaces/scratch/tables/metrics/commit", team42WriteTok, commitTableReq{
		TableUUID:      tblDTO.TableUUID,
		BaseSnapshotID: dvCommit.SnapshotID,
		AddDeleteFiles: []commitAddDeleteFileReq{{
			DataFileID: dataFileID, Path: "s3://example-customer/tables/team_id=42/dv-shrink.puffin", DeleteCount: 1, FileSizeBytes: 64,
		}},
	}), http.StatusConflict, "conflict")

	// 10. DROP TABLE and DROP SCHEMA reject mismatched identity fences (409) and succeed with matching fences.
	scopedNamespaceWantCode(t, scopedMutationCall(t, h, http.MethodDelete, base+"/namespaces/scratch/tables/metrics?table_uuid=99999999-9999-4999-8999-999999999999", team42WriteTok, nil), http.StatusConflict, "conflict")
	w = scopedMutationCall(t, h, http.MethodDelete, base+"/namespaces/scratch/tables/metrics?table_uuid="+tblDTO.TableUUID, team42WriteTok, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("drop table status=%d body=%s", w.Code, w.Body.String())
	}
	scopedNamespaceWantCode(t, scopedMutationCall(t, h, http.MethodDelete, fmt.Sprintf("%s/namespaces/scratch?namespace_id=%d", base, nsDTO.ID+999), team42WriteTok, nil), http.StatusConflict, "conflict")
	w = scopedMutationCall(t, h, http.MethodDelete, fmt.Sprintf("%s/namespaces/scratch?namespace_id=%d", base, nsDTO.ID), team42WriteTok, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("drop namespace status=%d body=%s", w.Code, w.Body.String())
	}
	// Dropped namespace is now 404.
	scopedNamespaceWantCode(t, scopedNamespaceCall(t, h, base+"/namespaces/scratch", team42WriteTok), http.StatusNotFound, "not_found")
}

func TestScopedCommitTableLevelFence(t *testing.T) {
	f, s3fx, spy, _, tenant, srv := newScopedTableScanHarness(t)
	spy.denyManifest.Store(true)
	upgradeScopedMutationCatalog(t, tenant, false)
	team42WriteTok := scopedTableScanToken(t, f, s3fx, "42",
		"ASIAEXAMPLEALICE0021", "secret-w42", "session-w42", func(c map[string]any) {
			c["cat"] = map[string]any{readSessionTestCatalog: []string{"read", "write"}}
			c["prd"] = map[string]any{"team_id": map[string]string{"42": "write"}}
			grants := c["grants"].([]map[string]any)
			grants[0]["prefix"] = "tables/team_id=42/"
			grants[0]["actions"] = readSessionWriteActions
		})
	base := "/v1/t/" + readSessionTestTenant + "/metadata/v1/catalogs/" + readSessionTestCatalog
	h := srv.Handler()

	// Create namespace scratch and two tables: alpha and beta.
	w := scopedMutationCall(t, h, http.MethodPost, base+"/namespaces", team42WriteTok, createNamespaceReq{Name: "scratch"})
	if w.Code != http.StatusCreated {
		t.Fatalf("create namespace status=%d body=%s", w.Code, w.Body.String())
	}
	w = scopedMutationCall(t, h, http.MethodPost, base+"/namespaces/scratch/tables", team42WriteTok, createTableReq{
		Name:    "alpha",
		Columns: []createTableColumnReq{{Name: "id", Type: "long"}, {Name: "extra", Type: "string"}},
	})
	if w.Code != http.StatusCreated {
		t.Fatalf("create alpha status=%d body=%s", w.Code, w.Body.String())
	}
	var alphaDTO scopedTableDTO
	if err := json.Unmarshal(w.Body.Bytes(), &alphaDTO); err != nil {
		t.Fatal(err)
	}
	alphaBase := alphaDTO.ReadSnapshotID

	// Creating beta advances the catalog-wide head snapshot, but does NOT touch alpha.
	w = scopedMutationCall(t, h, http.MethodPost, base+"/namespaces/scratch/tables", team42WriteTok, createTableReq{
		Name:    "beta",
		Columns: []createTableColumnReq{{Name: "id", Type: "long"}},
	})
	if w.Code != http.StatusCreated {
		t.Fatalf("create beta status=%d body=%s", w.Code, w.Body.String())
	}
	var betaDTO scopedTableDTO
	if err := json.Unmarshal(w.Body.Bytes(), &betaDTO); err != nil {
		t.Fatal(err)
	}

	// 1. Append to alpha with alphaBase (older than catalog head) succeeds because alpha DDL did not change.
	w = scopedMutationCall(t, h, http.MethodPost, base+"/namespaces/scratch/tables/alpha/commit", team42WriteTok, commitTableReq{
		TableUUID:      alphaDTO.TableUUID,
		BaseSnapshotID: alphaBase,
		AddFiles: []commitAddFileReq{{
			Path: "s3://example-customer/tables/team_id=42/alpha-1.parquet", RecordCount: 5, FileSizeBytes: 512,
		}},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("commit to alpha after unrelated beta DDL failed: status=%d body=%s", w.Code, w.Body.String())
	}

	// 2. Concurrent append to the SAME busy table alpha from the same alphaBase ALSO succeeds (append-only fences on DDL only).
	w = scopedMutationCall(t, h, http.MethodPost, base+"/namespaces/scratch/tables/alpha/commit", team42WriteTok, commitTableReq{
		TableUUID:      alphaDTO.TableUUID,
		BaseSnapshotID: alphaBase,
		AddFiles: []commitAddFileReq{{
			Path: "s3://example-customer/tables/team_id=42/alpha-2.parquet", RecordCount: 4, FileSizeBytes: 256,
		}},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("concurrent append to busy table alpha failed: status=%d body=%s", w.Code, w.Body.String())
	}
	var alphaCommit1 commitTableResp
	if err := json.Unmarshal(w.Body.Bytes(), &alphaCommit1); err != nil {
		t.Fatal(err)
	}

	// 2b. Replaying the exact same append path (e.g. retry after lost response) MUST fail with 409
	// instead of double-registering the Parquet file.
	scopedNamespaceWantCode(t, scopedMutationCall(t, h, http.MethodPost, base+"/namespaces/scratch/tables/alpha/commit", team42WriteTok, commitTableReq{
		TableUUID:      alphaDTO.TableUUID,
		BaseSnapshotID: alphaBase,
		AddFiles: []commitAddFileReq{{
			Path: "s3://example-customer/tables/team_id=42/alpha-1.parquet", RecordCount: 5, FileSizeBytes: 512,
		}},
	}), http.StatusConflict, "conflict")

	// 3. Committing to beta advances catalog head again, but alpha is still untouched since alphaCommit1.
	w = scopedMutationCall(t, h, http.MethodPost, base+"/namespaces/scratch/tables/beta/commit", team42WriteTok, commitTableReq{
		TableUUID:      betaDTO.TableUUID,
		BaseSnapshotID: betaDTO.ReadSnapshotID,
		AddFiles: []commitAddFileReq{{
			Path: "s3://example-customer/tables/team_id=42/beta-1.parquet", RecordCount: 2, FileSizeBytes: 256,
		}},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("commit to beta status=%d body=%s", w.Code, w.Body.String())
	}

	// 4. Look up alpha's file ids, then commit a DV to alpha using alphaCommit1.SnapshotID (older than catalog head).
	w = scopedNamespaceCall(t, h, fmt.Sprintf("%s/namespaces/scratch/tables/alpha/scan?snapshot=%d&table_uuid=%s", base, alphaCommit1.SnapshotID, alphaDTO.TableUUID), team42WriteTok)
	if w.Code != http.StatusOK {
		t.Fatalf("scan alpha status=%d body=%s", w.Code, w.Body.String())
	}
	var scanDTO scopedScanDTO
	if err := json.Unmarshal(w.Body.Bytes(), &scanDTO); err != nil {
		t.Fatal(err)
	}
	if len(scanDTO.Files) != 2 {
		t.Fatalf("expected 2 files in alpha, got %d", len(scanDTO.Files))
	}
	dataFileID1 := scanDTO.Files[0].DataFile.DataFileID
	dataFileID2 := scanDTO.Files[1].DataFile.DataFileID

	w = scopedMutationCall(t, h, http.MethodPost, base+"/namespaces/scratch/tables/alpha/commit", team42WriteTok, commitTableReq{
		TableUUID:      alphaDTO.TableUUID,
		BaseSnapshotID: alphaCommit1.SnapshotID,
		AddDeleteFiles: []commitAddDeleteFileReq{{
			DataFileID: dataFileID1, Path: "s3://example-customer/tables/team_id=42/alpha-dv-1.puffin", DeleteCount: 1, FileSizeBytes: 64,
		}},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("DV commit to alpha after unrelated beta commit failed: status=%d body=%s", w.Code, w.Body.String())
	}
	var alphaCommit2 commitTableResp
	if err := json.Unmarshal(w.Body.Bytes(), &alphaCommit2); err != nil {
		t.Fatal(err)
	}

	// 5. Stale DV commit to alpha using alphaCommit1.SnapshotID AFTER alphaCommit2 touched alpha MUST fail with 409.
	scopedNamespaceWantCode(t, scopedMutationCall(t, h, http.MethodPost, base+"/namespaces/scratch/tables/alpha/commit", team42WriteTok, commitTableReq{
		TableUUID:      alphaDTO.TableUUID,
		BaseSnapshotID: alphaCommit1.SnapshotID,
		AddDeleteFiles: []commitAddDeleteFileReq{{
			DataFileID: dataFileID2, Path: "s3://example-customer/tables/team_id=42/alpha-dv-stale.puffin", DeleteCount: 1, FileSizeBytes: 64,
		}},
	}), http.StatusConflict, "conflict")

	// 6. End-only file retirement (without adding any replacement) conflicts with a stale DV commit from alphaCommit2.
	retireSnap := alphaCommit2.SnapshotID + 1
	scopedNamespaceWrite(t, tenant, "retire-file-end-only",
		Statement{SQL: `UPDATE meta SET next_snapshot = ? WHERE id = 1`, Args: []any{retireSnap + 1}},
		Statement{SQL: `UPDATE files SET end_snapshot = ? WHERE id = ?`, Args: []any{retireSnap, dataFileID2}},
	)
	scopedNamespaceWantCode(t, scopedMutationCall(t, h, http.MethodPost, base+"/namespaces/scratch/tables/alpha/commit", team42WriteTok, commitTableReq{
		TableUUID:      alphaDTO.TableUUID,
		BaseSnapshotID: alphaCommit2.SnapshotID,
		AddDeleteFiles: []commitAddDeleteFileReq{{
			DataFileID: dataFileID1, Path: "s3://example-customer/tables/team_id=42/alpha-dv-after-retire.puffin", DeleteCount: 2, FileSizeBytes: 64,
		}},
	}), http.StatusConflict, "conflict")

	// 7. End-only column drop (without adding any new column) conflicts with a stale append from retireSnap.
	dropColSnap := retireSnap + 1
	scopedNamespaceWrite(t, tenant, "drop-column-end-only",
		Statement{SQL: `UPDATE meta SET next_snapshot = ? WHERE id = 1`, Args: []any{dropColSnap + 1}},
		Statement{SQL: `UPDATE columns SET end_snapshot = ? WHERE table_id = (SELECT id FROM tables WHERE table_uuid = ?) AND name = 'extra'`, Args: []any{dropColSnap, alphaDTO.TableUUID}},
	)
	scopedNamespaceWantCode(t, scopedMutationCall(t, h, http.MethodPost, base+"/namespaces/scratch/tables/alpha/commit", team42WriteTok, commitTableReq{
		TableUUID:      alphaDTO.TableUUID,
		BaseSnapshotID: retireSnap,
		AddFiles: []commitAddFileReq{{
			Path: "s3://example-customer/tables/team_id=42/alpha-after-col-drop.parquet", RecordCount: 1, FileSizeBytes: 128,
		}},
	}), http.StatusConflict, "conflict")

	// 8. Expired base_snapshot_id (< meta.earliest_snapshot_id) MUST fail with 409.
	// Use beta (which has no intervening DDL) so this assertion isolates the retention-floor check.
	scopedNamespaceWrite(t, tenant, "advance-retention-floor",
		Statement{SQL: `UPDATE meta SET earliest_snapshot_id = ? WHERE id = 1`, Args: []any{dropColSnap}},
	)
	scopedNamespaceWantCode(t, scopedMutationCall(t, h, http.MethodPost, base+"/namespaces/scratch/tables/beta/commit", team42WriteTok, commitTableReq{
		TableUUID:      betaDTO.TableUUID,
		BaseSnapshotID: betaDTO.ReadSnapshotID,
		AddFiles: []commitAddFileReq{{
			Path: "s3://example-customer/tables/team_id=42/beta-expired.parquet", RecordCount: 1, FileSizeBytes: 128,
		}},
	}), http.StatusConflict, "conflict")

	// 9. Future base_snapshot_id (> catalog head) and pre-creation base_snapshot_id (< created_snapshot) MUST fail with 409.
	scopedNamespaceWantCode(t, scopedMutationCall(t, h, http.MethodPost, base+"/namespaces/scratch/tables/alpha/commit", team42WriteTok, commitTableReq{
		TableUUID:      alphaDTO.TableUUID,
		BaseSnapshotID: dropColSnap + 99,
		AddFiles: []commitAddFileReq{{
			Path: "s3://example-customer/tables/team_id=42/alpha-future.parquet", RecordCount: 1, FileSizeBytes: 128,
		}},
	}), http.StatusConflict, "conflict")
	scopedNamespaceWantCode(t, scopedMutationCall(t, h, http.MethodPost, base+"/namespaces/scratch/tables/alpha/commit", team42WriteTok, commitTableReq{
		TableUUID:      alphaDTO.TableUUID,
		BaseSnapshotID: alphaBase - 1,
		AddFiles: []commitAddFileReq{{
			Path: "s3://example-customer/tables/team_id=42/alpha-precreate.parquet", RecordCount: 1, FileSizeBytes: 128,
		}},
	}), http.StatusConflict, "conflict")
}
