package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"hoglite/internal/hoglite"
)

// populatePaging builds a table whose listings need many pages: single
// file commits, multi-file commits (a page ends inside a snapshot), one
// commit with vectors for several files in descending file order, and
// superseding vectors. It is deterministic, so two fresh harnesses get
// the same ids and paths. It returns a snapshot in the middle.
func populatePaging(h *harness) (mid int64) {
	pathSeq = 0
	h.setup("pg")
	h.createTable("pg", "t")
	for i := 0; i < 8; i++ {
		h.must(200, "POST", "/catalogs/pg/commit", appendBody("t", 1, 10))
	}
	for i := 0; i < 3; i++ {
		h.must(200, "POST", "/catalogs/pg/commit", appendBody("t", 9, 10))
	}
	mid = num(h.must(200, "GET", "/catalogs/pg", nil)["head_snapshot_id"])
	// two commits with vectors in descending file order: 30, 28, ..., 2
	for _, hi := range []int64{30, 14} {
		var dfs []any
		for id := hi; id > hi-16 && id >= 2; id -= 2 {
			pathSeq++
			dfs = append(dfs, map[string]any{"data_file_id": id, "path": fmt.Sprintf("s3://b/dv%d.puffin", pathSeq),
				"delete_count": 1, "file_size_bytes": 10})
		}
		h.must(200, "POST", "/catalogs/pg/commit", map[string]any{"read_snapshot": mid,
			"deletes": []any{map[string]any{"namespace": "ns", "table": "t", "files": dfs}}})
	}
	for _, id := range []int64{4, 30, 31} {
		head := num(h.must(200, "GET", "/catalogs/pg", nil)["head_snapshot_id"])
		h.must(200, "POST", "/catalogs/pg/commit", dvBody("t", id, 2, head))
	}
	h.must(200, "POST", "/catalogs/pg/commit", appendBody("t", 5, 10))
	return mid
}

// pagingViews fetches every paged listing of populatePaging's table.
func pagingViews(h *harness, mid int64) map[string]any {
	base := "/catalogs/pg/namespaces/ns/tables/t/"
	out := map[string]any{}
	for _, p := range []string{"files", "scan", fmt.Sprintf("scan?snapshot=%d", mid),
		fmt.Sprintf("files?snapshot=%d", mid), "scan?snapshot=999999",
		"changes?from_snapshot=0", fmt.Sprintf("changes?from_snapshot=%d", mid),
		fmt.Sprintf("changes?from_snapshot=1&to_snapshot=%d", mid)} {
		code, body := h.do("GET", base+p, nil)
		if code != 200 {
			h.t.Fatalf("GET %s: %d %v", p, code, body)
		}
		if m, ok := body.(map[string]any); ok {
			delete(m, "table_uuid") // random per harness
		}
		out[p] = body
	}
	return out
}

// Listings that grow with the table work through the adapter when one
// hoglite result can hold only a few of their rows: the adapter pages
// internally and returns the same response as with an unlimited hoglite.
func TestPagedListingsUnderLowResultCap(t *testing.T) {
	saved := *pageRows
	t.Cleanup(func() { *pageRows = saved })

	// reference: no limits, one page per listing
	*pageRows = 1 << 20
	ref := newHarnessOpts(t, func(o *hoglite.TenantOpts) { o.MaxResultRows, o.MaxResultBytes = -1, -1 })
	mid := populatePaging(ref)
	want := pagingViews(ref, mid)
	// the reference itself matches the unpaged SQL of the old handlers
	checkAgainstOldSQL(t, want, mid)

	// 20 rows and 16 KiB per hoglite result; the page size starts above
	// both and must shrink on 413
	*pageRows = 50
	capped := newHarnessOpts(t, func(o *hoglite.TenantOpts) { o.MaxResultRows, o.MaxResultBytes = 20, 16<<10 })
	if populatePaging(capped) != mid {
		t.Fatal("populate not deterministic")
	}
	got := pagingViews(capped, mid)
	for k := range want {
		if !reflect.DeepEqual(got[k], want[k]) {
			a, _ := json.Marshal(want[k])
			b, _ := json.Marshal(got[k])
			t.Fatalf("%s differs under the cap:\nwant %s\ngot  %s", k, a, b)
		}
	}
	if n := len(got["files"].([]any)); n != 40 {
		t.Fatalf("files: %d, want 40", n)
	}
	// an explicit page still answers directly
	if n := len(capped.list("/catalogs/pg/namespaces/ns/tables/t/files?limit=10")); n != 10 {
		t.Fatalf("explicit page: %d", n)
	}
	// a missing table is still 404
	capped.must(404, "GET", "/catalogs/pg/namespaces/ns/tables/nope/scan", nil)
	capped.must(404, "GET", "/catalogs/pg/namespaces/ns/tables/nope/files", nil)
}

// checkAgainstOldSQL compares the reference listings with the single
// statements of the handlers before internal paging (ids and order).
func checkAgainstOldSQL(t *testing.T, views map[string]any, mid int64) {
	t.Helper()
	q := func(sql string, args ...any) [][]int64 {
		res, err := hlQuery("pg", sql, args...)
		if err != nil {
			t.Fatal(err)
		}
		var out [][]int64
		for _, r := range res.Rows {
			var ids []int64
			for _, v := range r {
				if v == nil {
					ids = append(ids, -1)
				} else {
					ids = append(ids, i64(v))
				}
			}
			out = append(out, ids)
		}
		return out
	}
	ids := func(l []any, key ...string) [][]int64 {
		var out [][]int64
		for _, e := range l {
			m := e.(map[string]any)
			var r []int64
			for _, k := range key {
				switch v := m[k].(type) {
				case map[string]any: // nested file / vector
					r = append(r, num(v["data_file_id"]))
				case nil:
					r = append(r, -1)
				default:
					r = append(r, num(v))
				}
			}
			out = append(out, r)
		}
		return out
	}
	dvID := func(l []any) [][]int64 {
		var out [][]int64
		for _, e := range l {
			m := e.(map[string]any)
			r := []int64{num(m["data_file"].(map[string]any)["data_file_id"]), -1}
			if d, ok := m["delete_file"].(map[string]any); ok {
				r[1] = num(d["delete_file_id"])
			}
			out = append(out, r)
		}
		return out
	}
	eq := func(name string, got, want [][]int64) {
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("%s: handler %v, old SQL %v", name, got, want)
		}
	}
	eq("files", ids(views["files"].([]any), "data_file_id"), q(atSnap+`SELECT f.id
		FROM s, tables t LEFT JOIN files f NOT INDEXED ON f.table_id = t.id AND f.begin_snapshot <= s.id
		WHERE`+tableAt+`ORDER BY f.id`, nil, "ns", "t"))
	oldScan := atSnap + `SELECT f.id, d.delete_file_id
		FROM s, tables t LEFT JOIN files f ON f.table_id = t.id AND f.begin_snapshot <= s.id
		LEFT JOIN dvs d ON d.data_file_id = f.id AND d.begin_snapshot <= s.id
			AND d.epoch = (SELECT MAX(d2.epoch) FROM dvs d2
				WHERE d2.data_file_id = f.id AND d2.begin_snapshot <= s.id)
		WHERE` + tableAt + `ORDER BY f.begin_snapshot, f.row_id_start, f.id`
	eq("scan", dvID(views["scan"].([]any)), q(oldScan, nil, "ns", "t"))
	eq("scan@mid", dvID(views[fmt.Sprintf("scan?snapshot=%d", mid)].([]any)), q(oldScan, mid, "ns", "t"))
	ch := views[fmt.Sprintf("changes?from_snapshot=%d", mid)].(map[string]any)
	to := num(ch["to_snapshot"])
	eq("changes files", ids(ch["files"].([]any), "data_file_id"), q(`SELECT f.id FROM files f
		WHERE f.table_id=1 AND f.begin_snapshot > ? AND f.begin_snapshot <= ?
		ORDER BY f.begin_snapshot, f.row_id_start, f.id`, mid, to))
	eq("changes dvs", ids(ch["delete_files"].([]any), "delete_file_id", "data_file_id"), q(`SELECT d.delete_file_id, d.data_file_id FROM dvs d
		WHERE d.table_id=1 AND d.begin_snapshot > ? AND d.begin_snapshot <= ?
		ORDER BY d.begin_snapshot, d.data_file_id, d.epoch`, mid, to))
	if n := len(ch["delete_files"].([]any)); n != 18 {
		t.Fatalf("delete_files in window: %d, want 18", n)
	}
}

// commitMidway runs commit once, right after the n-th hoglite /query
// response of the next listing has arrived.
type commitMidway struct {
	base   http.RoundTripper
	n      atomic.Int32
	fired  atomic.Bool
	commit func()
}

func (c *commitMidway) RoundTrip(r *http.Request) (*http.Response, error) {
	resp, err := c.base.RoundTrip(r)
	if strings.HasSuffix(r.URL.Path, "/query") && c.n.Add(-1) == 0 && c.fired.CompareAndSwap(false, true) {
		c.commit()
	}
	return resp, err
}

// All pages of one listing read the same snapshot: a commit between two
// pages (new files, new vectors) does not show up in it, also when the
// requested snapshot lies beyond head.
func TestPagedListingIsConsistent(t *testing.T) {
	saved := *pageRows
	t.Cleanup(func() { *pageRows = saved })
	*pageRows = 7
	h := newHarnessOpts(t, func(o *hoglite.TenantOpts) { o.MaxResultRows = 20 })
	populatePaging(h)
	tr := &commitMidway{base: client.Transport}
	client.Transport = tr
	t.Cleanup(func() { client.Transport = tr.base })
	base := "/catalogs/pg/namespaces/ns/tables/t/"
	for i, p := range []string{"scan", "files", "scan?snapshot=999999", "changes?from_snapshot=0"} {
		before := pagingView(h, base+p)
		// /query 1 resolves the table (changes: table and window),
		// /query 2 is the first page
		tr.n.Store(2)
		tr.fired.Store(false)
		tr.commit = func() {
			head := num(h.must(200, "POST", "/catalogs/pg/commit", appendBody("t", 3, 10))["snapshot_id"])
			h.must(200, "POST", "/catalogs/pg/commit", dvBody("t", int64(1+2*i), 3, head))
		}
		during := pagingView(h, base+p)
		if !tr.fired.Load() {
			t.Fatalf("%s: no commit between pages", p)
		}
		if !reflect.DeepEqual(before, during) {
			a, _ := json.Marshal(before)
			b, _ := json.Marshal(during)
			t.Fatalf("%s changed by a commit between its pages:\nbefore %s\nduring %s", p, a, b)
		}
		if after := pagingView(h, base+p); reflect.DeepEqual(after, before) {
			t.Fatalf("%s: the commit is not visible afterwards", p)
		}
	}
}

func pagingView(h *harness, path string) any {
	code, body := h.do("GET", path, nil)
	if code != 200 {
		h.t.Fatalf("GET %s: %d %v", path, code, body)
	}
	return body
}
