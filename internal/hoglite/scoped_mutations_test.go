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

func TestScopedMutationsLifecycleAndSecurity(t *testing.T) {
	f, s3fx, spy, _, tenant, srv := newScopedTableScanHarness(t)
	spy.denyManifest.Store(true)
	// Upgrade the minimal read fixture to full catalog DDL including the legacy unique containment index.
	scopedNamespaceWrite(t, tenant, "upgrade-to-full-catalog-ddl",
		Statement{SQL: `ALTER TABLE meta ADD COLUMN next_namespace_id INTEGER NOT NULL DEFAULT 100`},
		Statement{SQL: `ALTER TABLE meta ADD COLUMN next_table_id INTEGER NOT NULL DEFAULT 300`},
		Statement{SQL: `ALTER TABLE meta ADD COLUMN next_file_id INTEGER NOT NULL DEFAULT 500`},
		Statement{SQL: `ALTER TABLE meta ADD COLUMN schema_version INTEGER NOT NULL DEFAULT 1`},
		Statement{SQL: `CREATE UNIQUE INDEX IF NOT EXISTS namespaces_live_containment ON namespaces(contained_field, contained_value) WHERE dropped_snapshot IS NULL AND contained_field IS NOT NULL`},
		Statement{SQL: `ALTER TABLE tables ADD COLUMN comment TEXT`},
		Statement{SQL: `ALTER TABLE tables ADD COLUMN next_row_id INTEGER NOT NULL DEFAULT 0`},
		Statement{SQL: `ALTER TABLE tables ADD COLUMN next_field_id INTEGER NOT NULL DEFAULT 1`},
		Statement{SQL: `ALTER TABLE tables ADD COLUMN schema_version INTEGER NOT NULL DEFAULT 0`},
		Statement{SQL: `ALTER TABLE table_versions ADD COLUMN comment TEXT`},
		Statement{SQL: `CREATE TABLE IF NOT EXISTS table_totals(table_id INTEGER PRIMARY KEY, record_count INTEGER NOT NULL DEFAULT 0, file_count INTEGER NOT NULL DEFAULT 0, file_size_bytes INTEGER NOT NULL DEFAULT 0)`},
		Statement{SQL: `CREATE TABLE IF NOT EXISTS snapshots(id INTEGER PRIMARY KEY, ts TEXT NOT NULL, author TEXT, message TEXT, schema_version INTEGER NOT NULL)`},
		Statement{SQL: `CREATE TABLE IF NOT EXISTS changes(snapshot_id INTEGER NOT NULL, kind TEXT NOT NULL, object_id INTEGER NOT NULL)`},
	)
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

	// 5. Team 42 creates a table in "scratch".
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

	// 8. Valid INSERT commit succeeds and is visible via typed GET scan.
	w = scopedMutationCall(t, h, http.MethodPost, base+"/namespaces/scratch/tables/metrics/commit", team42WriteTok, commitTableReq{
		TableUUID:      tblDTO.TableUUID,
		BaseSnapshotID: tblDTO.ReadSnapshotID,
		AddFiles: []commitAddFileReq{{
			Path: "s3://example-customer/tables/team_id=42/part-1.parquet", RecordCount: 3, FileSizeBytes: 512,
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
	if len(scanDTO.Files) != 1 || scanDTO.Files[0].DataFile.Path != "s3://example-customer/tables/team_id=42/part-1.parquet" {
		t.Fatalf("unexpected scan after insert: %+v", scanDTO)
	}

	// 9. Valid DELETE (DV) commit attaches deletion vector to data_file_id.
	dataFileID := scanDTO.Files[0].DataFile.DataFileID
	w = scopedMutationCall(t, h, http.MethodPost, base+"/namespaces/scratch/tables/metrics/commit", team42WriteTok, commitTableReq{
		TableUUID:      tblDTO.TableUUID,
		BaseSnapshotID: commitResp.SnapshotID,
		AddDeleteFiles: []commitAddDeleteFileReq{{
			DataFileID: dataFileID, Path: "s3://example-customer/tables/team_id=42/dv-1.puffin", DeleteCount: 1, FileSizeBytes: 128,
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

	// 10. DROP TABLE and DROP SCHEMA succeed cleanly.
	w = scopedMutationCall(t, h, http.MethodDelete, base+"/namespaces/scratch/tables/metrics", team42WriteTok, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("drop table status=%d body=%s", w.Code, w.Body.String())
	}
	w = scopedMutationCall(t, h, http.MethodDelete, base+"/namespaces/scratch", team42WriteTok, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("drop namespace status=%d body=%s", w.Code, w.Body.String())
	}
	// Dropped namespace is now 404.
	scopedNamespaceWantCode(t, scopedNamespaceCall(t, h, base+"/namespaces/scratch", team42WriteTok), http.StatusNotFound, "not_found")
}
