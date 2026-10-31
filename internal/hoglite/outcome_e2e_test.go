package hoglite

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// End-to-end tests for the /tx outcome taxonomy: each drives a real
// ambiguous condition in the engine (a commit applied locally whose
// durability is then lost track of) and asserts the HTTP answer is 504
// outcome_unknown, never a refusal, and that a retry with the same
// client+txid lands exactly once.

type httpAnswer struct {
	code int
	body map[string]any
	err  error
}

// post sends one request without touching *testing.T (safe off the test
// goroutine).
func (c *testCluster) post(replica, method, path string, body any) httpAnswer {
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
		return httpAnswer{err: err}
	}
	defer resp.Body.Close()
	var out map[string]any
	json.NewDecoder(resp.Body).Decode(&out)
	return httpAnswer{code: resp.StatusCode, body: out}
}

func txBody(txid string, stmts ...Statement) map[string]any {
	return map[string]any{"client": "c", "txid": txid, "statements": stmts}
}

// asyncTx starts a /tx and returns the channel its answer arrives on.
func (c *testCluster) asyncTx(replica, tid, txid string, stmts ...Statement) chan httpAnswer {
	ch := make(chan httpAnswer, 1)
	go func() { ch <- c.post(replica, "POST", "/v1/t/"+tid+"/tx", txBody(txid, stmts...)) }()
	return ch
}

func await(t *testing.T, ch chan httpAnswer, d time.Duration, what string) httpAnswer {
	t.Helper()
	select {
	case a := <-ch:
		if a.err != nil {
			t.Fatalf("%s: %v", what, a.err)
		}
		return a
	case <-time.After(d):
		t.Fatalf("%s: no answer within %s", what, d)
	}
	return httpAnswer{}
}

// segGate holds the next segment PUT inside the fake store (the commit is
// applied locally; its durability point is held) until open is called.
// land decides whether the held PUT then stores the object.
type segGate struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func holdNextSegment(fs *FakeStore, land bool) *segGate {
	g := &segGate{entered: make(chan struct{}), release: make(chan struct{})}
	var first atomic.Bool
	fs.FailPutSegment = func(string) bool {
		if !first.CompareAndSwap(false, true) {
			return false
		}
		close(g.entered)
		<-g.release
		return !land
	}
	return g
}

func (g *segGate) awaitHeld(t *testing.T) {
	t.Helper()
	select {
	case <-g.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("commit never reached its segment PUT")
	}
}

func (g *segGate) open() { g.once.Do(func() { close(g.release) }) }

func assertOutcomeUnknown(t *testing.T, a httpAnswer, cause string) {
	t.Helper()
	if a.code != StatusOutcomeUnknown || a.body["code"] != CodeOutcomeUnknown {
		t.Fatalf("want %d %s, got %d %v", StatusOutcomeUnknown, CodeOutcomeUnknown, a.code, a.body)
	}
	if msg, _ := a.body["error"].(string); !strings.Contains(msg, cause) {
		t.Fatalf("504 for the wrong reason: want %q in %q", cause, msg)
	}
}

func setupTenant(t *testing.T, c *testCluster, replica, tid string) {
	t.Helper()
	if code, body, _ := c.do(t, replica, "PUT", "/v1/tenants/"+tid, map[string]any{}); code != 200 {
		t.Fatalf("create %s: %d %v", tid, code, body)
	}
	if a := c.post(replica, "POST", "/v1/t/"+tid+"/tx", txBody("schema",
		Statement{SQL: `CREATE TABLE IF NOT EXISTS kv(k INTEGER PRIMARY KEY, v TEXT)`})); a.code != 200 {
		t.Fatalf("schema: %d %v", a.code, a.body)
	}
	if a := c.post(replica, "POST", "/v1/t/"+tid+"/tx", txBody("seed", insertStmt(1, "durable"))); a.code != 200 {
		t.Fatalf("seed: %d %v", a.code, a.body)
	}
}

// retryOnce resubmits txid through the Go API on m and returns its dup
// flag; the tenant must then hold exactly want rows.
func retryOnce(t *testing.T, m *Manager, tid, txid string, k int64, want int64) bool {
	t.Helper()
	var dup bool
	deadline := time.Now().Add(10 * time.Second)
	for {
		ten, err := m.Acquire(context.Background(), tid, false)
		if err == nil {
			_, dup, err = ten.SubmitTx(TxRequest{Client: "c", TxID: txid, Stmts: []Statement{insertStmt(k, "retry")}})
		}
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("retry of %s never succeeded: %v", txid, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if n := countRows(t, m, tid); n != want {
		t.Fatalf("after retrying %s: %d rows, want %d (applied twice or lost)", txid, n, want)
	}
	return dup
}

// A pod-wide fail-stop after a heartbeat gap ends every applied-but-
// unacked commit. It used to answer 422 (a refusal the writer never
// retries) although the commit may be durable.
func TestTxOutcomeUnknownOnFailStopAfterHeartbeatGap(t *testing.T) {
	c := newTestCluster(t, "a")
	ma := c.mgrs["a"]
	setupTenant(t, c, "a", "fs")

	// the held PUT will fail when released: the commit never ships
	gate := holdNextSegment(c.fs, false)
	defer gate.open()
	tx := c.asyncTx("a", "fs", "fs-2", insertStmt(2, "in-flight"))
	gate.awaitHeld(t)
	// heartbeats stop landing until the write deadline has passed, then
	// the heal detects the gap and fail-stops every tenant
	ma.SetPartitioned(true)
	deadline := time.Now().Add(10 * time.Second)
	for !ma.leaseExpiring() && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond)
	ma.SetPartitioned(false)
	assertOutcomeUnknown(t, await(t, tx, 10*time.Second, "parked tx"), "pod lease write deadline passed")

	gate.open()
	// the held PUT never stored anything, so the commit is not in S3: the
	// retry applies it, once
	if dup := retryOnce(t, ma, "fs", "fs-2", 2, 2); dup {
		t.Fatal("retry claimed a dedup hit for a commit that never shipped")
	}
}

// A lease generation move (a fail-stop sweep the tenant was born before)
// fences the ack of a commit whose segment IS durable. The answer must be
// unknown, and the retry finds the replayed commit.
func TestTxOutcomeUnknownOnLeaseGenerationMove(t *testing.T) {
	c := newTestCluster(t, "a")
	ma := c.mgrs["a"]
	setupTenant(t, c, "a", "lg")

	gate := holdNextSegment(c.fs, true) // the held PUT lands when released
	defer gate.open()
	tx := c.asyncTx("a", "lg", "lg-2", insertStmt(2, "in-flight"))
	gate.awaitHeld(t)
	ma.leaseGen.Add(1) // the generation moves under the tenant
	gate.open()
	assertOutcomeUnknown(t, await(t, tx, 10*time.Second, "fenced tx"), "after lease margin")

	if dup := retryOnce(t, ma, "lg", "lg-2", 2, 2); !dup {
		t.Fatal("the segment landed, so the retry must be a dedup hit")
	}
}

// A segment PUT that lands in S3 but reports failure, until the lease
// lapses: the tenant fails with the commit's bytes durable. A new owner
// replays them; the client must hear "unknown", and its retry must be a
// dedup hit, not a second commit.
func TestTxOutcomeUnknownWhenLandedSegmentReportsError(t *testing.T) {
	c := newTestCluster(t, "a", "b")
	ma, mb := c.mgrs["a"], c.mgrs["b"]
	setupTenant(t, c, "a", "ls")

	var armed atomic.Bool
	armed.Store(true)
	c.fs.LatePutSegment = func(key string) bool { return armed.Load() }
	ma.SetPartitioned(true)
	tx := c.asyncTx("a", "ls", "ls-2", insertStmt(2, "landed"))
	assertOutcomeUnknown(t, await(t, tx, 15*time.Second, "tx"), "segment put")
	armed.Store(false)

	waitLeaseExpiry()
	if dup := retryOnce(t, mb, "ls", "ls-2", 2, 2); !dup {
		t.Fatal("the landed segment was replayed, so the retry must be a dedup hit")
	}
}

// newTestClusterOpts is newTestCluster with tenant options of the test's
// choosing.
func newTestClusterOpts(t *testing.T, opts TenantOpts, ids ...string) *testCluster {
	t.Helper()
	c := &testCluster{fs: NewFakeStore(), servers: map[string]*httptest.Server{}, mgrs: map[string]*Manager{}}
	for _, id := range ids {
		c.mgrs[id] = NewManager(c.fs, "fake", id, "standard", t.TempDir(), opts, 1000, &Metrics{}, ControllerOpts{})
	}
	urls := map[string]string{}
	for _, id := range ids {
		c.servers[id] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			NewServer(c.mgrs[id], urls).Handler().ServeHTTP(w, r)
		}))
		t.Cleanup(c.servers[id].Close)
	}
	for id, srv := range c.servers {
		urls[id] = srv.URL
	}
	return c
}

// A checkpoint that fails after a commit was applied, while that commit's
// durability was to come from the checkpoint itself. A checkpoint first
// cuts and drains every pending commit into a segment; a commit stays
// pending through the base upload and manifest CAS only when the
// in-flight cap deferred its cut. That is the case driven here: one
// segment PUT is held at the cap of 1, a second commit applies behind it,
// and a release starts the checkpoint whose base step then fails.
func TestTxOutcomeUnknownOnCheckpointFailure(t *testing.T) {
	for _, tc := range []struct {
		name, cause string
		arm         func(fs *FakeStore)
	}{
		{"base put", "checkpoint put", func(fs *FakeStore) {
			fs.FailPutBase = func(string) bool { return true }
		}},
		{"manifest cas", "checkpoint cas", func(fs *FakeStore) {
			fs.FailCAS = func() bool { return true }
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := testOpts()
			opts.Inflight = 1
			opts.MaxSegments = 1 << 20 // no threshold checkpoint: the release is the only one
			opts.CheckpointByte = 1 << 40
			c := newTestClusterOpts(t, opts, "a")
			ma := c.mgrs["a"]
			tid := "ck" + strings.ReplaceAll(tc.name, " ", "")
			setupTenant(t, c, "a", tid)

			gate := holdNextSegment(c.fs, true)
			defer gate.open()
			first := c.asyncTx("a", tid, tid+"-2", insertStmt(2, "held"))
			gate.awaitHeld(t) // its segment holds the only in-flight slot
			commits := ma.metrics.Commits.Load()
			deferred := c.asyncTx("a", tid, tid+"-3", insertStmt(3, "deferred"))
			for ma.metrics.Commits.Load() == commits {
				time.Sleep(5 * time.Millisecond) // applied, cut deferred by the cap
			}
			tc.arm(c.fs)
			release := make(chan httpAnswer, 1)
			go func() { release <- c.post("a", "POST", "/v1/t/"+tid+"/release", nil) }()
			// the release's checkpoint holds the drain semaphore once it is
			// past its own (deferred) cut and about to drain
			for len(ma.ckptSem) == 0 {
				time.Sleep(time.Millisecond)
			}
			gate.open() // the drain completes; the base step then fails

			if a := await(t, first, 15*time.Second, "first"); a.code != 200 {
				t.Fatalf("the drained commit is durable and must be acked: %d %v", a.code, a.body)
			}
			assertOutcomeUnknown(t, await(t, deferred, 15*time.Second, "deferred"), tc.cause)
			<-release

			c.fs.FailPutBase, c.fs.FailCAS = nil, nil
			retryOnce(t, ma, tid, tid+"-3", 3, 3)
		})
	}
}

// The deterministic refusals stay final and distinguishable: a guard miss
// is 409 expect_failed, a refused statement set 422 refused, and neither
// changed anything.
func TestTxRefusalsStayFinal(t *testing.T) {
	c := newTestCluster(t, "a")
	setupTenant(t, c, "a", "rf")
	one := int64(1)
	g := c.post("a", "POST", "/v1/t/rf/tx", txBody("g2",
		insertStmt(5, "x"), Statement{SQL: `UPDATE kv SET v='y' WHERE k=99`, ExpectAffected: &one}))
	if g.code != 409 || g.body["code"] != CodeExpectFailed {
		t.Fatalf("guard miss: %d %v", g.code, g.body)
	}
	dupKey := c.post("a", "POST", "/v1/t/rf/tx", txBody("r", insertStmt(6, "x"), insertStmt(1, "dup")))
	if dupKey.code != 422 || dupKey.body["code"] != CodeRefused {
		t.Fatalf("constraint violation: %d %v", dupKey.code, dupKey.body)
	}
	syntax := c.post("a", "POST", "/v1/t/rf/tx", txBody("s", Statement{SQL: `INSRT INTO kv VALUES(7,'x')`}))
	if syntax.code != 422 || syntax.body["code"] != CodeRefused {
		t.Fatalf("syntax error: %d %v", syntax.code, syntax.body)
	}
	forbidden := c.post("a", "POST", "/v1/t/rf/tx", txBody("p", insertStmt(8, "x"), Statement{SQL: `PRAGMA journal_mode=DELETE`}))
	if forbidden.code != 422 || forbidden.body["code"] != CodeRefused {
		t.Fatalf("forbidden statement class: %d %v", forbidden.code, forbidden.body)
	}
	if n := countRows(t, c.mgrs["a"], "rf"); n != 1 {
		t.Fatalf("a refused transaction changed the tenant: %d rows", n)
	}
}
