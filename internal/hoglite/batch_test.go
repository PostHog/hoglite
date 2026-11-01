package hoglite

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
)

// seedTenant creates tid on replica and writes one row naming it.
func (c *testCluster) seedTenant(t *testing.T, replica, tid string) {
	t.Helper()
	if code, _, _ := c.do(t, replica, "PUT", "/v1/tenants/"+tid, map[string]any{}); code != 200 {
		t.Fatalf("create %s on %s: %d", tid, replica, code)
	}
	tx := map[string]any{"client": "c", "txid": "seed-" + tid, "statements": []map[string]any{
		{"sql": `CREATE TABLE meta(id INTEGER PRIMARY KEY, name TEXT)`},
		{"sql": `INSERT INTO meta VALUES(1, ?)`, "args": []any{tid}},
	}}
	if code, _, _ := c.do(t, replica, "POST", "/v1/t/"+tid+"/tx", tx); code != 200 {
		t.Fatalf("seed %s: %d", tid, code)
	}
}

// batch posts queries to replica's /v1/query-batch and decodes the answers.
func (c *testCluster) batch(t *testing.T, replica string, queries []map[string]any) (int, []BatchAnswer) {
	t.Helper()
	code, out, _ := c.do(t, replica, "POST", "/v1/query-batch", map[string]any{"queries": queries})
	if code != 200 {
		return code, nil
	}
	raw, _ := json.Marshal(out["results"])
	var answers []BatchAnswer
	if err := json.Unmarshal(raw, &answers); err != nil {
		t.Fatalf("decode answers: %v", err)
	}
	return code, answers
}

func metaName(t *testing.T, a BatchAnswer) string {
	t.Helper()
	var res QueryResult
	if err := json.Unmarshal(a.Body, &res); err != nil || len(res.Rows) != 1 {
		t.Fatalf("answer body %s: %v", a.Body, err)
	}
	return res.Rows[0][0].(string)
}

// Every item gets /query's answer, in request order, and one item's failure
// does not fail the others.
func TestQueryBatchAnswersEachItemLikeQuery(t *testing.T) {
	c := newTestCluster(t, "a", "b")
	c.seedTenant(t, "a", "ta")
	c.seedTenant(t, "b", "tb")
	sel := "SELECT name FROM meta WHERE id = 1"
	code, answers := c.batch(t, "a", []map[string]any{
		{"tenant": "tb", "sql": sel},
		{"tenant": "missing", "sql": sel},
		{"tenant": "ta", "sql": sel},
		{"tenant": "ta", "sql": "INSERT INTO meta VALUES(2, 'x')"},
		{"tenant": "bad$tenant", "sql": sel},
		{"tenant": "tb", "sql": "SELECT name FROM meta WHERE id = ?", "args": []any{1}},
	})
	if code != 200 || len(answers) != 6 {
		t.Fatalf("batch: %d, %d answers", code, len(answers))
	}
	if answers[0].Status != 200 || metaName(t, answers[0]) != "tb" {
		t.Fatalf("item 0 (owned by the peer): %d %s", answers[0].Status, answers[0].Body)
	}
	if answers[1].Status != 404 {
		t.Fatalf("item 1 (no such tenant): %d %s", answers[1].Status, answers[1].Body)
	}
	if answers[2].Status != 200 || metaName(t, answers[2]) != "ta" {
		t.Fatalf("item 2 (local): %d %s", answers[2].Status, answers[2].Body)
	}
	if answers[3].Status == 200 {
		t.Fatalf("item 3: a write must be refused as on /query, got 200 %s", answers[3].Body)
	}
	if answers[4].Status != 400 {
		t.Fatalf("item 4 (bad tenant id): %d %s", answers[4].Status, answers[4].Body)
	}
	if answers[5].Status != 200 || metaName(t, answers[5]) != "tb" {
		t.Fatalf("item 5 (args): %d %s", answers[5].Status, answers[5].Body)
	}
	// The refused write wrote nothing.
	_, out, _ := c.do(t, "a", "POST", "/v1/t/ta/query", map[string]any{"sql": "SELECT COUNT(*) FROM meta"})
	if n := out["rows"].([]any)[0].([]any)[0].(float64); n != 1 {
		t.Fatalf("rows after a refused batch write: %v", n)
	}
}

// Items owned by one peer cost ONE hop to that peer, not one per item.
func TestQueryBatchSendsOneSubBatchPerPeer(t *testing.T) {
	c := newTestCluster(t, "a", "b")
	const n = 20
	var queries []map[string]any
	for i := 0; i < n; i++ {
		tid := fmt.Sprintf("t%02d", i)
		c.seedTenant(t, "b", tid)
		queries = append(queries, map[string]any{"tenant": tid, "sql": "SELECT name FROM meta WHERE id = 1"})
	}
	proxiedBefore := c.mgrs["a"].metrics.Proxied.Load()
	batchesBefore := c.mgrs["b"].metrics.QueryBatches.Load()
	queriesBefore := c.mgrs["b"].metrics.Queries.Load()
	code, answers := c.batch(t, "a", queries)
	if code != 200 || len(answers) != n {
		t.Fatalf("batch: %d, %d answers", code, len(answers))
	}
	for i, a := range answers {
		if a.Status != 200 || metaName(t, a) != fmt.Sprintf("t%02d", i) {
			t.Fatalf("item %d: %d %s", i, a.Status, a.Body)
		}
	}
	if got := c.mgrs["a"].metrics.Proxied.Load() - proxiedBefore; got != 1 {
		t.Fatalf("hops from a for %d items owned by b: %d, want 1", n, got)
	}
	if got := c.mgrs["b"].metrics.QueryBatches.Load() - batchesBefore; got != 1 {
		t.Fatalf("sub-batches b received: %d, want 1", got)
	}
	if got := c.mgrs["b"].metrics.Queries.Load() - queriesBefore; got != n {
		t.Fatalf("queries b ran: %d, want %d", got, n)
	}
	if c.mgrs["a"].LocalTenant("t00") != nil {
		t.Fatal("a acquired a tenant b owns")
	}
}

// A tenant nobody owns (released) is acquired by the pod the batch reached,
// exactly as a /query would acquire it.
func TestQueryBatchAcquiresAReleasedTenant(t *testing.T) {
	c := newTestCluster(t, "a", "b")
	c.seedTenant(t, "b", "idle")
	if code, _, _ := c.do(t, "b", "POST", "/v1/t/idle/release", nil); code != 200 {
		t.Fatalf("release: %d", code)
	}
	code, answers := c.batch(t, "a", []map[string]any{{"tenant": "idle", "sql": "SELECT name FROM meta WHERE id = 1"}})
	if code != 200 || answers[0].Status != 200 || metaName(t, answers[0]) != "idle" {
		t.Fatalf("batch over a released tenant: %d %+v", code, answers)
	}
	if c.mgrs["a"].LocalTenant("idle") == nil {
		t.Fatal("the released tenant was not acquired where the batch landed")
	}
}

// A peer's sub-batch (x-hw-noproxy) answers what the peer owns and 421s the
// rest; the pod that sent it re-routes those items itself.
func TestQueryBatchSubBatchAnswersOnlyOwnedTenants(t *testing.T) {
	c := newTestCluster(t, "a", "b")
	c.seedTenant(t, "a", "ta")
	c.seedTenant(t, "b", "tb")
	body := map[string]any{"queries": []map[string]any{
		{"tenant": "ta", "sql": "SELECT name FROM meta WHERE id = 1"},
		{"tenant": "tb", "sql": "SELECT name FROM meta WHERE id = 1"},
	}}
	raw, _ := json.Marshal(body)
	srv := NewServer(c.mgrs["b"], map[string]string{"a": c.servers["a"].URL})
	req := newJSONRequest(t, "/v1/query-batch", raw)
	req.Header.Set("x-hw-noproxy", "1")
	rec := &bufferedResponse{header: map[string][]string{}}
	srv.Handler().ServeHTTP(rec, req)
	var got batchResponse
	if rec.status != 200 || json.Unmarshal(rec.buf.Bytes(), &got) != nil || len(got.Results) != 2 {
		t.Fatalf("sub-batch: %d %s", rec.status, rec.buf.String())
	}
	if got.Results[0].Status != 421 {
		t.Fatalf("a tenant the peer does not own: %d %s", got.Results[0].Status, got.Results[0].Body)
	}
	if got.Results[1].Status != 200 || metaName(t, got.Results[1]) != "tb" {
		t.Fatalf("a tenant the peer owns: %d %s", got.Results[1].Status, got.Results[1].Body)
	}
}

func TestQueryBatchBounds(t *testing.T) {
	c := newTestCluster(t, "a")
	if code, _ := c.batch(t, "a", []map[string]any{}); code != 400 {
		t.Fatalf("empty batch: %d", code)
	}
	queries := make([]map[string]any, maxBatchQueries+1)
	for i := range queries {
		queries[i] = map[string]any{"tenant": "x", "sql": "SELECT 1"}
	}
	if code, _ := c.batch(t, "a", queries); code != 400 {
		t.Fatalf("oversized batch: %d", code)
	}
}

func newJSONRequest(t *testing.T, path string, body []byte) *http.Request {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	return req
}
