package hoglite

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

func testOpts() TenantOpts {
	o := DefaultOpts()
	o.Lease = 2 * time.Second
	o.HeartbeatEvery = 200 * time.Millisecond
	o.BatchWindow = 2 * time.Millisecond
	o.CheckpointByte = 64 << 10 // small: exercise checkpoints
	o.MaxSegments = 8
	o.SkewMargin = 300 * time.Millisecond
	o.PutTimeout = time.Second
	return o
}

func newTestManager(t *testing.T, fs *FakeStore, id string) *Manager {
	t.Helper()
	dir := t.TempDir()
	return NewManager(fs, "fake", id, "standard", dir, testOpts(), 1000, &Metrics{}, ControllerOpts{})
}

func mustCreate(t *testing.T, m *Manager, tid string) {
	t.Helper()
	if _, err := m.Acquire(context.Background(), tid, true); err != nil {
		t.Fatalf("create %s: %v", tid, err)
	}
}

func mustTx(t *testing.T, m *Manager, tid, client, txid string, stmts ...Statement) []QueryResult {
	t.Helper()
	ten, err := m.Acquire(context.Background(), tid, false)
	if err != nil {
		t.Fatalf("acquire %s: %v", tid, err)
	}
	res, _, err := ten.SubmitTx(TxRequest{Client: client, TxID: txid, Stmts: stmts})
	if err != nil {
		t.Fatalf("tx %s/%s: %v", tid, txid, err)
	}
	return res
}

func mustQuery(t *testing.T, m *Manager, tid, sql string, args ...any) QueryResult {
	t.Helper()
	ten, err := m.Acquire(context.Background(), tid, false)
	if err != nil {
		t.Fatalf("acquire %s: %v", tid, err)
	}
	res, err := ten.SubmitQuery(Statement{SQL: sql, Args: args})
	if err != nil {
		t.Fatalf("query %q: %v", sql, err)
	}
	return res
}

func makeSchema(t *testing.T, m *Manager, tid string) {
	t.Helper()
	mustTx(t, m, tid, "c", "schema",
		Statement{SQL: `CREATE TABLE IF NOT EXISTS kv(k INTEGER PRIMARY KEY, v TEXT)`})
}

func insertStmt(k int64, v string) Statement {
	return Statement{SQL: `INSERT INTO kv(k, v) VALUES(?, ?)`, Args: []any{k, v}}
}

func countRows(t *testing.T, m *Manager, tid string) int64 {
	t.Helper()
	res := mustQuery(t, m, tid, `SELECT COUNT(*) FROM kv`)
	if len(res.Rows) != 1 || len(res.Rows[0]) != 1 {
		t.Fatalf("bad count result: %+v", res)
	}
	return res.Rows[0][0].(int64)
}

func TestBasicWriteRead(t *testing.T) {
	fs := NewFakeStore()
	m := newTestManager(t, fs, "a")
	mustCreate(t, m, "t1")
	makeSchema(t, m, "t1")
	mustTx(t, m, "t1", "c", "i1", insertStmt(1, "hello"), insertStmt(2, "world"))
	res := mustQuery(t, m, "t1", `SELECT v FROM kv WHERE k = ?`, 2)
	if len(res.Rows) != 1 || res.Rows[0][0] != "world" {
		t.Fatalf("unexpected: %+v", res)
	}
	if res.Cols[0] != "v" {
		t.Fatalf("bad cols: %v", res.Cols)
	}
}

func TestDedupRetry(t *testing.T) {
	fs := NewFakeStore()
	m := newTestManager(t, fs, "a")
	mustCreate(t, m, "t1")
	makeSchema(t, m, "t1")
	mustTx(t, m, "t1", "c", "x1", insertStmt(1, "a"))
	ten, _ := m.Acquire(context.Background(), "t1", false)
	res, dup, err := ten.SubmitTx(TxRequest{Client: "c", TxID: "x1", Stmts: []Statement{insertStmt(1, "a")}})
	if err != nil || !dup {
		t.Fatalf("expected dup hit, got dup=%v err=%v", dup, err)
	}
	if len(res) != 1 {
		t.Fatalf("expected recorded result, got %+v", res)
	}
	if n := countRows(t, m, "t1"); n != 1 {
		t.Fatalf("applied twice: %d rows", n)
	}
	// different txid, same client: applies
	mustTx(t, m, "t1", "c", "x2", insertStmt(2, "b"))
	if n := countRows(t, m, "t1"); n != 2 {
		t.Fatalf("new txid must apply: %d rows", n)
	}
}

func TestGroupCommitBatch(t *testing.T) {
	fs := NewFakeStore()
	fs.Latency = 5 * time.Millisecond // make batching observable
	m := newTestManager(t, fs, "a")
	mustCreate(t, m, "t1")
	makeSchema(t, m, "t1")

	const N = 50
	var wg sync.WaitGroup
	errs := make(chan error, N)
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ten, err := m.Acquire(context.Background(), "t1", false)
			if err != nil {
				errs <- err
				return
			}
			_, _, err = ten.SubmitTx(TxRequest{
				Client: "c", TxID: fmt.Sprintf("b%d", i),
				Stmts: []Statement{insertStmt(int64(i), "v")},
			})
			if err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Fatal(e)
	}
	if n := countRows(t, m, "t1"); n != N {
		t.Fatalf("want %d rows, got %d", N, n)
	}
	mf2, _, _ := fs.GetManifest(context.Background(), "t1")
	segs, _ := fs.ListSegments(context.Background(), "t1", mf2.Instance, mf2.Gen)
	t.Logf("segments for %d commits + schema: %d (batching works if < %d)", N, len(segs), N/2)
}

func TestCheckpointReload(t *testing.T) {
	fs := NewFakeStore()
	m := newTestManager(t, fs, "a")
	mustCreate(t, m, "t1")
	makeSchema(t, m, "t1")
	big := strings.Repeat("x", 4000)
	for i := 0; i < 40; i++ { // 40*4KB = 160KB > 64KB checkpoint threshold
		mustTx(t, m, "t1", "c", fmt.Sprintf("c%d", i), insertStmt(int64(i), big))
	}
	mf, _, _ := fs.GetManifest(context.Background(), "t1")
	if mf.Gen < 3 {
		t.Fatalf("expected checkpoints to advance gen, got %d", mf.Gen)
	}
	if err := m.Release("t1", ""); err != nil {
		t.Fatal(err)
	}
	// fresh manager (new local dir) re-acquires from the store
	m2 := newTestManager(t, fs, "b")
	if n := countRows(t, m2, "t1"); n != 40 {
		t.Fatalf("after reload: want 40, got %d", n)
	}
	// old generation objects must be collected eventually
	time.Sleep(300 * time.Millisecond)
	for _, k := range fs.ObjectKeys() {
		if strings.Contains(k, "/wal/000001/") || strings.Contains(k, "/base/000001-") {
			t.Fatalf("stale object survived: %s", k)
		}
	}
}

func TestCrashRecovery(t *testing.T) {
	fs := NewFakeStore()
	m := newTestManager(t, fs, "a")
	mustCreate(t, m, "t1")
	makeSchema(t, m, "t1")
	const N = 20
	for i := 0; i < N; i++ {
		mustTx(t, m, "t1", "c", fmt.Sprintf("k%d", i), insertStmt(int64(i), "v"))
	}
	// simulate pod death: lease heartbeats stop and every actor dies with
	// no flush or release (kill -9 semantics)
	m.KillLease()

	// lease is still held by "a"; manager b must wait it out
	m2 := newTestManager(t, fs, "b")
	start := time.Now()
	if n := countRows(t, m2, "t1"); n != N {
		t.Fatalf("after crash recovery: want %d, got %d", N, n)
	}
	if d := time.Since(start); d < testOpts().Lease {
		t.Fatalf("acquired before lease expiry (%v) - unsafe", d)
	}
	// unacked-but-durable case: insert directly via a fresh owner, then crash
	// mid-pipeline is covered by TestSegmentGapStopsReplay.
}

func TestSegmentGapStopsReplay(t *testing.T) {
	fs := NewFakeStore()
	m := newTestManager(t, fs, "a")
	mustCreate(t, m, "t1")
	makeSchema(t, m, "t1")
	mustTx(t, m, "t1", "c", "g1", insertStmt(1, "one"))
	// flush pending and learn segment keys
	mf, _, _ := fs.GetManifest(context.Background(), "t1")
	segs, _ := fs.ListSegments(context.Background(), "t1", mf.Instance, mf.Gen)
	if len(segs) == 0 {
		t.Skip("no segments yet")
	}
	// write a second batch, then delete the first segment -> gap
	mustTx(t, m, "t1", "c", "g2", insertStmt(2, "two"))
	segs, _ = fs.ListSegments(context.Background(), "t1", mf.Instance, mf.Gen)
	if len(segs) < 2 {
		t.Skip("batching merged segments; gap test needs 2")
	}
	// delete the second segment (offset > 0) to create an un-acked tail loss,
	// then verify replay keeps the first batch only
	target := segs[len(segs)-1]
	if target.Offset == 0 {
		t.Skip("unexpected layout")
	}
	fs.DeleteObjects(context.Background(), target.Key)
	m.KillLease()

	time.Sleep(testOpts().Lease + testOpts().SkewMargin + 100*time.Millisecond)
	m2 := newTestManager(t, fs, "b")
	res := mustQuery(t, m2, "t1", `SELECT COUNT(*) FROM kv WHERE v = 'one'`)
	if res.Rows[0][0].(int64) != 1 {
		t.Fatalf("first batch must survive: %+v", res)
	}
	res = mustQuery(t, m2, "t1", `SELECT COUNT(*) FROM kv WHERE v = 'two'`)
	if res.Rows[0][0].(int64) != 0 {
		t.Fatalf("tail after gap must not apply: %+v", res)
	}
}

func TestFencing(t *testing.T) {
	fs := NewFakeStore()
	ma := newTestManager(t, fs, "a")
	mustCreate(t, ma, "t1")
	makeSchema(t, ma, "t1")
	mustTx(t, ma, "t1", "c", "f1", insertStmt(1, "by-a"))

	ta := ma.LocalTenant("t1")
	if ta == nil {
		t.Fatal("a should own t1")
	}
	// partition a: pod lease heartbeats stop landing, the lease lapses
	ma.SetPartitioned(true)

	mb := newTestManager(t, fs, "b")
	if n := countRows(t, mb, "t1"); n != 1 { // waits out the lease, takes over
		t.Fatalf("b should see a's committed row, got %d", n)
	}
	mustTx(t, mb, "t1", "c", "f2", insertStmt(2, "by-b"))

	// partition heals: a's next heartbeat lands after a lapse, so every
	// tenant it still holds is fail-stopped
	ma.SetPartitioned(false)
	deadline := time.Now().Add(3 * time.Second)
	for ta.Alive() && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if ta.Alive() {
		t.Fatal("zombie owner never detected fencing")
	}
	_, _, err := ta.SubmitTx(TxRequest{Client: "c", TxID: "f3", Stmts: []Statement{insertStmt(3, "zombie")}})
	if err == nil {
		t.Fatal("zombie write accepted after fencing")
	}
	res := mustQuery(t, mb, "t1", `SELECT COUNT(*) FROM kv WHERE v = 'zombie'`)
	if res.Rows[0][0].(int64) != 0 {
		t.Fatalf("zombie writes leaked: %+v", res)
	}
	if n := countRows(t, mb, "t1"); n != 2 {
		t.Fatalf("want 2 rows (a-pre-fence + b), got %d", n)
	}
}

func TestEviction(t *testing.T) {
	fs := NewFakeStore()
	dir := t.TempDir()
	m := NewManager(fs, "fake", "a", "standard", dir, testOpts(), 3, &Metrics{}, ControllerOpts{})
	for i := 0; i < 6; i++ {
		tid := fmt.Sprintf("t%d", i)
		mustCreate(t, m, tid)
		makeSchema(t, m, tid)
		mustTx(t, m, tid, "c", "e1", insertStmt(int64(i), "v"))
		time.Sleep(5 * time.Millisecond)
	}
	// sweeper evicts above maxOpen=3
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		m.mu.Lock()
		n := len(m.tenants)
		m.mu.Unlock()
		if n <= 3 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	m.mu.Lock()
	n := len(m.tenants)
	m.mu.Unlock()
	if n > 3 {
		t.Fatalf("eviction did not run: %d open", n)
	}
	// all tenants still readable
	for i := 0; i < 6; i++ {
		if got := countRows(t, m, fmt.Sprintf("t%d", i)); got != 1 {
			t.Fatalf("t%d: want 1 row, got %d", i, got)
		}
	}
}

func TestConcurrentTenants(t *testing.T) {
	fs := NewFakeStore()
	m := newTestManager(t, fs, "a")
	const T = 30
	var wg sync.WaitGroup
	errs := make(chan error, T*4)
	for i := 0; i < T; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tid := fmt.Sprintf("ct%d", i)
			if _, err := m.Acquire(context.Background(), tid, true); err != nil {
				errs <- fmt.Errorf("create %s: %w", tid, err)
				return
			}
			ten, err := m.Acquire(context.Background(), tid, false)
			if err != nil {
				errs <- err
				return
			}
			if _, _, err := ten.SubmitTx(TxRequest{Client: "c", TxID: "s",
				Stmts: []Statement{{SQL: `CREATE TABLE IF NOT EXISTS kv(k INTEGER PRIMARY KEY, v TEXT)`}}}); err != nil {
				errs <- err
				return
			}
			if _, _, err := ten.SubmitTx(TxRequest{Client: "c", TxID: "i",
				Stmts: []Statement{insertStmt(int64(i), "v")}}); err != nil {
				errs <- err
				return
			}
			res, err := ten.SubmitQuery(Statement{SQL: `SELECT k FROM kv`})
			if err != nil {
				errs <- err
				return
			}
			if len(res.Rows) != 1 || res.Rows[0][0].(int64) != int64(i) {
				errs <- fmt.Errorf("%s: cross-tenant bleed or loss: %+v", tid, res.Rows)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Fatal(e)
	}
}

func TestHandoff(t *testing.T) {
	fs := NewFakeStore()
	ma := newTestManager(t, fs, "a")
	mustCreate(t, ma, "t1")
	makeSchema(t, ma, "t1")
	mustTx(t, ma, "t1", "c", "h1", insertStmt(1, "before-handoff"))
	if err := ma.Release("t1", "whale"); err != nil {
		t.Fatal(err)
	}
	// manifest must be free and reclassed
	mf, _, err := fs.GetManifest(context.Background(), "t1")
	if err != nil || mf.Owner != "" || mf.Class != "whale" {
		t.Fatalf("bad manifest after release: %+v err=%v", mf, err)
	}
	// standard-class manager must refuse
	mb := newTestManager(t, fs, "b")
	if _, err := mb.Acquire(context.Background(), "t1", false); !errors.Is(err, ErrWrongClass) {
		t.Fatalf("want ErrWrongClass, got %v", err)
	}
	// whale-class manager acquires and sees the data
	dir := t.TempDir()
	mc := NewManager(fs, "fake", "c", "whale", dir, testOpts(), 100, &Metrics{}, ControllerOpts{})
	if n := countRows(t, mc, "t1"); n != 1 {
		t.Fatalf("handoff lost data: %d rows", n)
	}
}

func TestSQLSurface(t *testing.T) {
	fs := NewFakeStore()
	m := newTestManager(t, fs, "a")
	mustCreate(t, m, "t1")
	// DDL + DML + read in ONE tx
	res := mustTx(t, m, "t1", "c", "s1",
		Statement{SQL: `CREATE TABLE files(id INTEGER PRIMARY KEY, name TEXT NOT NULL,
			ended INTEGER NOT NULL DEFAULT 0, meta TEXT)`},
		Statement{SQL: `CREATE INDEX files_live ON files(name) WHERE ended = 0`},
		Statement{SQL: `INSERT INTO files(id, name, meta) VALUES(?, ?, ?) RETURNING id`,
			Args: []any{1, "a.parquet", `{"rows": 5}`}},
		Statement{SQL: `WITH batch(x) AS (SELECT value FROM json_each(?))
			INSERT INTO files(id, name) SELECT x, 'bulk' FROM batch RETURNING id`,
			Args: []any{`[2,3,4]`}},
		Statement{SQL: `SELECT COUNT(*) FROM files`},
	)
	if len(res) != 5 {
		t.Fatalf("want 5 results, got %d", len(res))
	}
	// RETURNING propagated
	if res[2].Rows[0][0].(int64) != 1 {
		t.Fatalf("RETURNING lost: %+v", res[2])
	}
	// json_each bulk insert worked
	if res[4].Rows[0][0].(int64) != 4 {
		t.Fatalf("bulk insert wrong: %+v", res[4])
	}
	// FK cascade support (catalog drop relies on it)
	mustTx(t, m, "t1", "c", "s2",
		Statement{SQL: `CREATE TABLE parent(id INTEGER PRIMARY KEY)`},
		Statement{SQL: `CREATE TABLE child(id INTEGER PRIMARY KEY,
			pid INTEGER NOT NULL REFERENCES parent(id) ON DELETE CASCADE)`},
		Statement{SQL: `INSERT INTO parent VALUES(1)`},
		Statement{SQL: `INSERT INTO child VALUES(10, 1)`},
		Statement{SQL: `DELETE FROM parent WHERE id = 1`},
	)
	if n := mustQuery(t, m, "t1", `SELECT COUNT(*) FROM child`).Rows[0][0].(int64); n != 0 {
		t.Fatalf("cascade failed: %d", n)
	}
}

func TestCrashBeforeAnyFlush(t *testing.T) {
	// Tenant created, never written, owner killed -> new owner sees empty db.
	fs := NewFakeStore()
	m := newTestManager(t, fs, "a")
	mustCreate(t, m, "t1")
	m.KillLease()
	time.Sleep(testOpts().Lease + testOpts().SkewMargin + 100*time.Millisecond)
	m2 := newTestManager(t, fs, "b")
	ten, err := m2.Acquire(context.Background(), "t1", false)
	if err != nil {
		t.Fatal(err)
	}
	res, err := ten.SubmitQuery(Statement{SQL: `SELECT 1`})
	if err != nil {
		t.Fatal(err)
	}
	if res.Rows[0][0].(int64) != 1 {
		t.Fatal("expected open db")
	}
}

func TestManyTenantsOneReplica(t *testing.T) {
	if testing.Short() {
		t.Skip("slow")
	}
	fs := NewFakeStore()
	m := newTestManager(t, fs, "a")
	const T = 300
	start := time.Now()
	for i := 0; i < T; i++ {
		tid := fmt.Sprintf("mt%04d", i)
		mustCreate(t, m, tid)
		makeSchema(t, m, tid)
		mustTx(t, m, tid, "c", "i", insertStmt(int64(i), "v"))
	}
	t.Logf("created+loaded %d tenants in %v (%.1f ms/tenant)", T, time.Since(start),
		float64(time.Since(start).Milliseconds())/T)
	// spot check
	if n := countRows(t, m, "mt0299"); n != 1 {
		t.Fatalf("mt0299: %d", n)
	}
	ids, _ := fs.ListTenantIDs(context.Background())
	if len(ids) != T {
		t.Fatalf("store has %d tenants, want %d", len(ids), T)
	}
}
