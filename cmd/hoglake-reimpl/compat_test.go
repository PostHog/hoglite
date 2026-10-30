package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"hoglite/internal/hoglite"
)

// harness runs hoglake-reimpl over an in-process hoglite on a FakeStore.
type harness struct {
	t   *testing.T
	url string
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	return newHarnessOpts(t, func(*hoglite.TenantOpts) {})
}

// newHarnessOpts is newHarness with adjusted hoglite tenant options.
func newHarnessOpts(t *testing.T, adjust func(*hoglite.TenantOpts)) *harness {
	t.Helper()
	opts := hoglite.DefaultOpts()
	opts.BatchWindow = time.Millisecond
	opts.IdleRelease = 0
	adjust(&opts)
	mgr := hoglite.NewManager(hoglite.NewFakeStore(), "fake", "r1", "standard", t.TempDir(), opts, 100, &hoglite.Metrics{}, hoglite.ControllerOpts{})
	hlSrv := httptest.NewServer(hoglite.NewServer(mgr, map[string]string{}).Handler())
	t.Cleanup(hlSrv.Close)
	*hlURL, *whaleURL = hlSrv.URL, hlSrv.URL
	maxInFlight = 8
	compat := httptest.NewServer(newMux())
	t.Cleanup(compat.Close)
	return &harness{t: t, url: compat.URL + "/v1"}
}

// do sends a request and decodes the JSON answer.
func (h *harness) do(method, path string, body any) (int, any) {
	h.t.Helper()
	var rdr *bytes.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rdr = bytes.NewReader(b)
	} else {
		rdr = bytes.NewReader(nil)
	}
	req, _ := http.NewRequest(method, h.url+path, rdr)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		h.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	var out any
	json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

// must asserts the status and returns the object body.
func (h *harness) must(want int, method, path string, body any) map[string]any {
	h.t.Helper()
	code, out := h.do(method, path, body)
	if code != want {
		h.t.Fatalf("%s %s: got %d want %d: %v", method, path, code, want, out)
	}
	m, _ := out.(map[string]any)
	return m
}

func (h *harness) list(path string) []any {
	h.t.Helper()
	code, out := h.do("GET", path, nil)
	if code != 200 {
		h.t.Fatalf("GET %s: %d %v", path, code, out)
	}
	l, _ := out.([]any)
	return l
}

func num(v any) int64 { f, _ := v.(float64); return int64(f) }

var cols = []map[string]any{{"name": "id", "type": "long", "nullable": false}, {"name": "v", "type": "double"}}

func (h *harness) setup(cat string) {
	h.must(201, "POST", "/catalogs", map[string]any{"name": cat, "data_path": "s3://b/" + cat + "/"})
	h.must(201, "POST", "/catalogs/"+cat+"/namespaces", map[string]any{"name": "ns"})
}

func (h *harness) createTable(cat, name string) map[string]any {
	return h.must(201, "POST", "/catalogs/"+cat+"/namespaces/ns/tables", map[string]any{"name": name, "columns": cols})
}

var pathSeq int

func appendBody(tbl string, n int, rows int64) map[string]any {
	var files []map[string]any
	for i := 0; i < n; i++ {
		pathSeq++
		files = append(files, map[string]any{
			"path": fmt.Sprintf("s3://b/f%d.parquet", pathSeq), "record_count": rows,
			"file_size_bytes": 100, "footer_size": 10, "column_stats": []any{},
		})
	}
	return map[string]any{"appends": []any{map[string]any{"namespace": "ns", "table": tbl, "files": files}}}
}

func dvBody(tbl string, fileID, count, read int64) map[string]any {
	pathSeq++
	return map[string]any{"read_snapshot": read, "deletes": []any{map[string]any{
		"namespace": "ns", "table": tbl, "files": []any{map[string]any{
			"data_file_id": fileID, "path": fmt.Sprintf("s3://b/dv%d.puffin", pathSeq),
			"delete_count": count, "file_size_bytes": 10}}}}}
}

func TestDeletionVectors(t *testing.T) {
	h := newHarness(t)
	h.setup("dv")
	h.createTable("dv", "t")
	h.must(200, "POST", "/catalogs/dv/commit", appendBody("t", 2, 100))
	files := h.list("/catalogs/dv/namespaces/ns/tables/t/files")
	if len(files) != 2 {
		t.Fatalf("files: %v", files)
	}
	f1 := num(files[0].(map[string]any)["data_file_id"])
	f2 := num(files[1].(map[string]any)["data_file_id"])
	base := num(h.must(200, "GET", "/catalogs/dv", nil)["head_snapshot_id"])

	// First vector on f1.
	s1 := num(h.must(200, "POST", "/catalogs/dv/commit", dvBody("t", f1, 1, base))["snapshot_id"])
	// A disjoint file at the OLD read snapshot does not conflict.
	h.must(200, "POST", "/catalogs/dv/commit", dvBody("t", f2, 1, base))
	// Same file at the old read snapshot: superseded since the read.
	if m := h.must(409, "POST", "/catalogs/dv/commit", dvBody("t", f1, 2, base)); m["error"] != "commit_conflict" {
		t.Fatalf("stale DV: %v", m)
	}
	// Shrinking is a validation error.
	h.must(422, "POST", "/catalogs/dv/commit", dvBody("t", f1, 0, s1+1))
	// Past record_count is a validation error.
	h.must(422, "POST", "/catalogs/dv/commit", dvBody("t", f1, 101, s1+1))
	// Unknown file.
	h.must(422, "POST", "/catalogs/dv/commit", dvBody("t", 999, 1, s1+1))
	// Deletes need a read_snapshot.
	b := dvBody("t", f1, 2, s1)
	delete(b, "read_snapshot")
	h.must(422, "POST", "/catalogs/dv/commit", b)
	// Growth at a fresh read succeeds.
	h.must(200, "POST", "/catalogs/dv/commit", dvBody("t", f1, 5, s1+1))

	plan := h.list("/catalogs/dv/namespaces/ns/tables/t/scan")
	got := map[int64]int64{}
	for _, sf := range plan {
		m := sf.(map[string]any)
		id := num(m["data_file"].(map[string]any)["data_file_id"])
		if d, ok := m["delete_file"].(map[string]any); ok {
			got[id] = num(d["delete_count"])
		}
	}
	if got[f1] != 5 || got[f2] != 1 {
		t.Fatalf("live DVs: %v", got)
	}
	// Pinned before any vector: no DVs.
	plan = h.list(fmt.Sprintf("/catalogs/dv/namespaces/ns/tables/t/scan?snapshot=%d", base))
	for _, sf := range plan {
		if _, ok := sf.(map[string]any)["delete_file"]; ok {
			t.Fatalf("pinned scan shows a DV: %v", sf)
		}
	}
	// The feed carries every vector registration in the window, including
	// supersessions (history-preserving dvs; newest epoch wins reads).
	ch := h.must(200, "GET", fmt.Sprintf("/catalogs/dv/namespaces/ns/tables/t/changes?from_snapshot=%d", base), nil)
	if n := len(ch["delete_files"].([]any)); n != 3 {
		t.Fatalf("delete_files in window: %d (want 3: two vectors, one supersession)", n)
	}
}

func TestDropRecreate(t *testing.T) {
	h := newHarness(t)
	h.setup("dd")
	old := h.createTable("dd", "t")
	h.must(200, "POST", "/catalogs/dd/commit", appendBody("t", 1, 10))
	pre := num(h.must(200, "GET", "/catalogs/dd", nil)["head_snapshot_id"])
	h.must(422, "DELETE", "/catalogs/dd/namespaces/ns/tables/t", nil)
	h.must(200, "DELETE", "/catalogs/dd/namespaces/ns/tables/t?author=a&message=m", nil)
	h.must(404, "DELETE", "/catalogs/dd/namespaces/ns/tables/t?author=a&message=m", nil)
	h.must(404, "GET", "/catalogs/dd/namespaces/ns/tables/t/files", nil)
	h.must(404, "POST", "/catalogs/dd/commit", appendBody("t", 1, 10))
	// Time travel before the drop still sees the files.
	if n := len(h.list(fmt.Sprintf("/catalogs/dd/namespaces/ns/tables/t/files?snapshot=%d", pre))); n != 1 {
		t.Fatalf("pinned files: %d", n)
	}
	if n := len(h.list(fmt.Sprintf("/catalogs/dd/namespaces/ns/tables?snapshot=%d", pre))); n != 1 {
		t.Fatalf("pinned listing: %d", n)
	}
	if n := len(h.list("/catalogs/dd/namespaces/ns/tables")); n != 0 {
		t.Fatalf("head listing after drop: %d", n)
	}
	// Recreate: a new incarnation, empty, with its own lineage.
	nu := h.createTable("dd", "t")
	if nu["table_uuid"] == old["table_uuid"] {
		t.Fatal("recreate reused the uuid")
	}
	// idempotent create: a retried create returns the live table, not 409
	h.must(201, "POST", "/catalogs/dd/namespaces/ns/tables", map[string]any{"name": "t", "columns": cols})
	if n := len(h.list("/catalogs/dd/namespaces/ns/tables/t/files")); n != 0 {
		t.Fatalf("recreated table has %d files", n)
	}
	// The old incarnation guard is refused, typed.
	b := appendBody("t", 1, 10)
	b["appends"].([]any)[0].(map[string]any)["expected_table_uuid"] = old["table_uuid"]
	if m := h.must(409, "POST", "/catalogs/dd/commit", b); m["error"] != "table_recreated" {
		t.Fatalf("guard: %v", m)
	}
	// A read_snapshot from before the recreate is DDL since the read.
	b = appendBody("t", 1, 10)
	b["read_snapshot"] = pre
	if m := h.must(409, "POST", "/catalogs/dd/commit", b); m["error"] != "ddl_since_read_snapshot" {
		t.Fatalf("ddl: %v", m)
	}
	b = appendBody("t", 2, 10)
	b["appends"].([]any)[0].(map[string]any)["expected_table_uuid"] = nu["table_uuid"]
	h.must(200, "POST", "/catalogs/dd/commit", b)
	files := h.list("/catalogs/dd/namespaces/ns/tables/t/files")
	if len(files) != 2 || num(files[0].(map[string]any)["row_id_start"]) != 0 || num(files[1].(map[string]any)["row_id_start"]) != 10 {
		t.Fatalf("recreated lineage: %v", files)
	}
	l := h.list("/catalogs/dd/namespaces/ns/tables")
	if len(l) != 1 || num(l[0].(map[string]any)["file_count"]) != 2 || num(l[0].(map[string]any)["snapshot_count"]) != 2 {
		t.Fatalf("listing: %v", l)
	}
	h.must(404, "GET", "/catalogs/dd/namespaces/nope/tables", nil)
}

func TestChangesAndOffsets(t *testing.T) {
	h := newHarness(t)
	h.setup("cf")
	tb := h.createTable("cf", "t")
	start := num(tb["snapshot_id"])
	for i := 0; i < 5; i++ {
		h.must(200, "POST", "/catalogs/cf/commit", appendBody("t", 1, 3))
	}
	head := num(h.must(200, "GET", "/catalogs/cf", nil)["head_snapshot_id"])
	ch := h.must(200, "GET", fmt.Sprintf("/catalogs/cf/namespaces/ns/tables/t/changes?from_snapshot=%d&to_snapshot=%d", head-2, head), nil)
	if n := len(ch["files"].([]any)); n != 2 || ch["table_uuid"] != tb["table_uuid"] {
		t.Fatalf("window: %v", ch)
	}
	ch = h.must(200, "GET", fmt.Sprintf("/catalogs/cf/namespaces/ns/tables/t/changes?from_snapshot=%d", start), nil)
	if n := len(ch["files"].([]any)); n != 5 {
		t.Fatalf("full window: %d", n)
	}
	h.must(422, "GET", fmt.Sprintf("/catalogs/cf/namespaces/ns/tables/t/changes?from_snapshot=%d", head+1), nil)

	u := tb["table_uuid"].(string)
	p := "/catalogs/cf/consumers/c1/offsets/" + u
	h.must(404, "GET", p, nil)
	h.must(200, "PUT", p, map[string]any{"snapshot_id": head - 1})
	h.must(200, "PUT", p, map[string]any{"snapshot_id": head - 1}) // idempotent
	h.must(200, "PUT", p, map[string]any{"snapshot_id": head})
	// regression below the committed snapshot: 409 offset_regression
	h.must(409, "PUT", p, map[string]any{"snapshot_id": head - 2})
	h.must(422, "PUT", p, map[string]any{"snapshot_id": head + 5})
	h.must(404, "PUT", "/catalogs/cf/consumers/c1/offsets/nope", map[string]any{"snapshot_id": 1})
	l := h.list("/catalogs/cf/consumers/c1/offsets")
	if len(l) != 1 || num(l[0].(map[string]any)["committed_snapshot"]) != head {
		t.Fatalf("offsets: %v", l)
	}
	// Offsets allocate no snapshot.
	if got := num(h.must(200, "GET", "/catalogs/cf", nil)["head_snapshot_id"]); got != head {
		t.Fatalf("offset commit moved head %d -> %d", head, got)
	}
}

func TestCommitReplayAndTotals(t *testing.T) {
	h := newHarness(t)
	h.setup("rp")
	h.createTable("rp", "t")
	b := appendBody("t", 1, 7)
	s1 := num(h.must(200, "POST", "/catalogs/rp/commit", b)["snapshot_id"])
	s2 := num(h.must(200, "POST", "/catalogs/rp/commit", b)["snapshot_id"])
	if s1 != s2 {
		t.Fatalf("replay minted a new snapshot: %d vs %d", s1, s2)
	}
	tb := h.must(200, "GET", "/catalogs/rp/namespaces/ns/tables/t", nil)
	if num(tb["record_count"]) != 7 || num(tb["read_snapshot_id"]) != s1 {
		t.Fatalf("table: %v", tb)
	}
	tb = h.must(200, "GET", "/catalogs/rp/namespaces/ns/tables/t?totals=false", nil)
	if _, ok := tb["record_count"]; ok {
		t.Fatalf("totals=false returned totals: %v", tb)
	}
	cols := tb["columns"].([]any)
	if num(cols[1].(map[string]any)["field_id"]) != 2 {
		t.Fatalf("field ids: %v", cols)
	}
}

func TestTotalsMaintainedAcrossCommits(t *testing.T) {
	h := newHarness(t)
	h.setup("tt")
	h.createTable("tt", "t")
	// three commits: 2 files/10 rows, 3 files/30 rows, 1 file/5 rows
	h.must(200, "POST", "/catalogs/tt/commit", appendBody("t", 2, 5))
	h.must(200, "POST", "/catalogs/tt/commit", appendBody("t", 3, 10))
	h.must(200, "POST", "/catalogs/tt/commit", appendBody("t", 1, 5))
	tb := h.must(200, "GET", "/catalogs/tt/namespaces/ns/tables/t", nil)
	if num(tb["file_count"]) != 6 || num(tb["record_count"]) != 45 {
		t.Fatalf("totals: %v", tb)
	}
	if num(tb["file_size_bytes"]) != 600 {
		t.Fatalf("byte totals: %v", tb)
	}
	// a second table keeps its own row
	h.createTable("tt", "u")
	h.must(200, "POST", "/catalogs/tt/commit", map[string]any{"appends": []any{map[string]any{
		"namespace": "ns", "table": "u", "files": []any{map[string]any{
			"path": "s3://b/u1.parquet", "record_count": 99, "file_size_bytes": 7,
			"footer_size": 1, "column_stats": []any{}}}}}})
	tu := h.must(200, "GET", "/catalogs/tt/namespaces/ns/tables/u", nil)
	if num(tu["file_count"]) != 1 || num(tu["record_count"]) != 99 {
		t.Fatalf("second table totals: %v", tu)
	}
	// t unchanged
	tb = h.must(200, "GET", "/catalogs/tt/namespaces/ns/tables/t", nil)
	if num(tb["file_count"]) != 6 {
		t.Fatalf("t totals changed by u's commit: %v", tb)
	}
}

func TestListFilesKeysetPagination(t *testing.T) {
	h := newHarness(t)
	h.setup("ks")
	h.createTable("ks", "t")
	h.must(200, "POST", "/catalogs/ks/commit", appendBody("t", 10, 1))
	// page 1: 4 files
	p1 := h.list("/catalogs/ks/namespaces/ns/tables/t/files?limit=4")
	if len(p1) != 4 {
		t.Fatalf("page 1: %d files", len(p1))
	}
	// keyset from the last id of page 1
	last := p1[len(p1)-1].(map[string]any)
	after := num(last["data_file_id"])
	p2 := h.list(fmt.Sprintf("/catalogs/ks/namespaces/ns/tables/t/files?limit=4&after=%d", after))
	if len(p2) != 4 {
		t.Fatalf("page 2: %d files", len(p2))
	}
	// no overlap between pages
	ids := map[int64]bool{}
	for _, f := range p1 {
		ids[num(f.(map[string]any)["data_file_id"])] = true
	}
	for _, f := range p2 {
		id := num(f.(map[string]any)["data_file_id"])
		if ids[id] {
			t.Fatalf("file %d on both pages", id)
		}
	}
	// last page: 2 files
	last2 := p2[len(p2)-1].(map[string]any)
	p3 := h.list(fmt.Sprintf("/catalogs/ks/namespaces/ns/tables/t/files?limit=4&after=%d", num(last2["data_file_id"])))
	if len(p3) != 2 {
		t.Fatalf("page 3: %d files", len(p3))
	}
	// offset still works for compat
	po := h.list("/catalogs/ks/namespaces/ns/tables/t/files?limit=4&offset=8")
	if len(po) != 2 {
		t.Fatalf("offset page: %d files", len(po))
	}
}
