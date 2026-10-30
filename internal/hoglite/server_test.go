package hoglite

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type testCluster struct {
	fs      *FakeStore
	servers map[string]*httptest.Server
	mgrs    map[string]*Manager
}

func newTestCluster(t *testing.T, ids ...string) *testCluster {
	t.Helper()
	c := &testCluster{fs: NewFakeStore(), servers: map[string]*httptest.Server{}, mgrs: map[string]*Manager{}}
	// first pass: managers
	for _, id := range ids {
		c.mgrs[id] = NewManager(c.fs, "fake", id, "standard", t.TempDir(), testOpts(), 1000, &Metrics{}, ControllerOpts{})
	}
	// second pass: servers with peer tables pointing at each other
	// (httptest URLs assigned lazily; use a two-phase trick)
	urls := map[string]string{}
	for _, id := range ids {
		c.servers[id] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// resolve handler lazily so peers map is complete
			srv := NewServer(c.mgrs[id], urls)
			srv.Handler().ServeHTTP(w, r)
		}))
		t.Cleanup(c.servers[id].Close)
	}
	for id, srv := range c.servers {
		urls[id] = srv.URL
	}
	return c
}

func (c *testCluster) do(t *testing.T, replica, method, path string, body any) (int, map[string]any, http.Header) {
	t.Helper()
	var rdr *bytes.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rdr = bytes.NewReader(b)
	} else {
		rdr = bytes.NewReader(nil)
	}
	req, _ := http.NewRequest(method, c.servers[replica].URL+path, rdr)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s via %s: %v", method, path, replica, err)
	}
	defer resp.Body.Close()
	var out map[string]any
	json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out, resp.Header
}

func TestProxyRouting(t *testing.T) {
	c := newTestCluster(t, "a", "b")
	// create on a
	code, _, _ := c.do(t, "a", "PUT", "/v1/tenants/pt", map[string]any{})
	if code != 200 {
		t.Fatalf("create: %d", code)
	}
	// write via b (non-owner) - must proxy to a
	tx := map[string]any{"client": "c", "txid": "p1", "statements": []map[string]any{
		{"sql": `CREATE TABLE kv(k INTEGER PRIMARY KEY, v TEXT)`},
	}}
	code, _, hdr := c.do(t, "b", "POST", "/v1/t/pt/tx", tx)
	if code != 200 {
		t.Fatalf("proxied write failed: %d", code)
	}
	if hdr.Get("x-hw-owner") != c.mgrs["a"].ID() {
		t.Fatalf("owner header = %q, want %q", hdr.Get("x-hw-owner"), c.mgrs["a"].ID())
	}
	tx2 := map[string]any{"client": "c", "txid": "p2", "statements": []map[string]any{
		{"sql": `INSERT INTO kv VALUES(1, 'via-proxy')`},
	}}
	if code, _, _ := c.do(t, "b", "POST", "/v1/t/pt/tx", tx2); code != 200 {
		t.Fatalf("proxied insert: %d", code)
	}
	// read via b as well (proxy again), then via a: both see the row
	for _, r := range []string{"a", "b"} {
		code, out, _ := c.do(t, r, "POST", "/v1/t/pt/query",
			map[string]any{"sql": `SELECT v FROM kv WHERE k = 1`})
		if code != 200 {
			t.Fatalf("query via %s: %d", r, code)
		}
		rows := out["rows"].([]any)
		if len(rows) != 1 || rows[0].([]any)[0] != "via-proxy" {
			t.Fatalf("read via %s: %+v", r, out)
		}
	}
	// proxy counter on b
	if c.mgrs["b"].metrics.Proxied.Load() < 3 {
		t.Fatalf("expected proxied requests, got %d", c.mgrs["b"].metrics.Proxied.Load())
	}
}

func TestProxyAfterFailover(t *testing.T) {
	c := newTestCluster(t, "a", "b")
	c.do(t, "a", "PUT", "/v1/tenants/pt", map[string]any{})
	tx := func(id string) map[string]any {
		return map[string]any{"client": "c", "txid": id, "statements": []map[string]any{
			{"sql": `INSERT INTO kv VALUES(1, 'x') ON CONFLICT(k) DO UPDATE SET v=excluded.v`},
		}}
	}
	c.do(t, "a", "POST", "/v1/t/pt/tx", map[string]any{"client": "c", "txid": "s",
		"statements": []map[string]any{{"sql": `CREATE TABLE kv(k INTEGER PRIMARY KEY, v TEXT)`}}})
	c.do(t, "a", "POST", "/v1/t/pt/tx", tx("w1"))

	// crash the WHOLE replica a: HTTP server dead, lease heartbeats stop,
	// actors die with no flush or release (kill -9 semantics)
	c.servers["a"].Close()
	delete(c.servers, "a")
	m := c.mgrs["a"]
	m.KillLease()

	// write via b: lease still live in manifest -> proxy to dead a fails ->
	// b waits out the lease, acquires, serves
	start := time.Now()
	code, _, hdr := c.do(t, "b", "POST", "/v1/t/pt/tx", tx("w2"))
	if code != 200 {
		t.Fatalf("post-failover write: %d", code)
	}
	if time.Since(start) < testOpts().Lease {
		t.Fatal("b took over before lease expiry")
	}
	if hdr.Get("x-hw-owner") != c.mgrs["b"].ID() {
		t.Fatalf("owner header = %q, want %q", hdr.Get("x-hw-owner"), c.mgrs["b"].ID())
	}
	// w1 (acked before the crash) is present
	code, out, _ := c.do(t, "b", "POST", "/v1/t/pt/query", map[string]any{"sql": `SELECT COUNT(*) FROM kv`})
	if code != 200 || out["rows"].([]any)[0].([]any)[0].(float64) != 1 {
		t.Fatalf("pre-crash write lost: %d %+v", code, out)
	}
}

func TestNotFoundAndWrongTenant(t *testing.T) {
	c := newTestCluster(t, "a")
	code, _, _ := c.do(t, "a", "POST", "/v1/t/nope/query", map[string]any{"sql": "SELECT 1"})
	if code != 404 {
		t.Fatalf("want 404, got %d", code)
	}
	code, _, _ = c.do(t, "a", "POST", "/v1/t/bad%24tenant/query", map[string]any{"sql": "SELECT 1"})
	if code != 400 {
		t.Fatalf("want 400, got %d", code)
	}
}

func TestCreateIdempotentAndRereate(t *testing.T) {
	c := newTestCluster(t, "a")
	for i := 0; i < 2; i++ {
		// second create on an existing tenant should fail or be idempotent;
		// our create path CASes create-only -> second create must not wipe data
		code, _, _ := c.do(t, "a", "PUT", "/v1/tenants/dup", map[string]any{})
		if i == 0 && code != 200 {
			t.Fatalf("first create: %d", code)
		}
	}
	tx := map[string]any{"client": "c", "txid": "d1", "statements": []map[string]any{
		{"sql": `CREATE TABLE kv(k INTEGER PRIMARY KEY)`}, {"sql": `INSERT INTO kv VALUES(1)`},
	}}
	if code, _, _ := c.do(t, "a", "POST", "/v1/t/dup/tx", tx); code != 200 {
		t.Fatal("write failed")
	}
	code, _, _ := c.do(t, "a", "PUT", "/v1/tenants/dup", map[string]any{})
	// second create after data: must NOT destroy. We accept 200 (already
	// exists, no-op) but never a wiped db. Verify data survives either way.
	_ = code
	_, out, _ := c.do(t, "a", "POST", "/v1/t/dup/query", map[string]any{"sql": `SELECT COUNT(*) FROM kv`})
	rows := out["rows"].([]any)
	if rows[0].([]any)[0].(float64) != 1 {
		t.Fatalf("second create wiped data: %+v", out)
	}
	fmt.Println("double-create safety ok")
}

func TestDeleteTenant(t *testing.T) {
	c := newTestCluster(t, "a")
	c.do(t, "a", "PUT", "/v1/tenants/del", map[string]any{})
	code, _, _ := c.do(t, "a", "DELETE", "/v1/tenants/del", nil)
	if code != 200 {
		t.Fatalf("delete: %d", code)
	}
	mf, _, _ := c.fs.GetManifest(context.Background(), "del")
	if mf != nil {
		t.Fatal("manifest survived delete")
	}
}

func TestReleaseRoutesToOwner(t *testing.T) {
	c := newTestCluster(t, "a", "b")
	if code, out, _ := c.do(t, "a", "PUT", "/v1/tenants/rt", map[string]any{}); code != 200 {
		t.Fatalf("create: %d %v", code, out)
	}
	// Release via the NON-owner must route to the owner, exactly like /tx:
	// a lifecycle caller behind a Service cannot know which pod owns the
	// tenant, and a 409 here made multi-pod catalog drops mostly fail.
	code, out, _ := c.do(t, "b", "POST", "/v1/t/rt/release", nil)
	if code != 200 {
		t.Fatalf("release via non-owner: %d %v", code, out)
	}
	// The release really happened on the owner: ownership is gone, so a
	// delete anywhere proceeds without the live-owner refusal.
	code, out, _ = c.do(t, "b", "DELETE", "/v1/tenants/rt", nil)
	if code != 200 {
		t.Fatalf("delete after routed release: %d %v", code, out)
	}
	// A release with no live owner anywhere is idempotently OK, so a
	// retried drop does not have to distinguish "released by the owner"
	// from "already released".
	if code, out, _ := c.do(t, "b", "POST", "/v1/t/rt/release", nil); code != 200 {
		t.Fatalf("release of unowned tenant: %d %v", code, out)
	}
}
