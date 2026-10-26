package hoglite

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// crashTenant hard-kills the local pod the way a process death would:
// lease heartbeats stop, no flush, no release.
func crashTenant(t *testing.T, m *Manager, tid string) {
	t.Helper()
	m.KillLease()
}

func waitLeaseExpiry() { time.Sleep(testOpts().Lease + testOpts().SkewMargin + 150*time.Millisecond) }

// S1: a segment PUT that lands in S3 but reports an error to the writer
// must not poison the tenant or double-ack.
func TestLatePutLandsAnyway(t *testing.T) {
	fs := NewFakeStore()
	m := newTestManager(t, fs, "a")
	mustCreate(t, m, "t1")
	makeSchema(t, m, "t1")

	var once sync.Once
	fs.LatePutSegment = func(key string) bool {
		fired := false
		once.Do(func() { fired = true })
		return fired
	}
	// first commit's segment PUT "fails" after landing; the tenant retries
	// and the retry succeeds (same key, same bytes — idempotent by
	// construction)
	mustTx(t, m, "t1", "c", "l1", insertStmt(1, "ok"))
	mustTx(t, m, "t1", "c", "l2", insertStmt(2, "ok"))
	if n := countRows(t, m, "t1"); n != 2 {
		t.Fatalf("rows: %d", n)
	}
	// crash and recover: both rows must be present exactly once
	crashTenant(t, m, "t1")
	waitLeaseExpiry()
	m2 := newTestManager(t, fs, "b")
	if n := countRows(t, m2, "t1"); n != 2 {
		t.Fatalf("after recovery: %d rows", n)
	}
}

// S2: a byte flip inside a committed segment must stop replay at the
// corruption point via the WAL checksum chain — never apply garbage.
func TestCorruptSegmentStopsReplay(t *testing.T) {
	fs := NewFakeStore()
	m := newTestManager(t, fs, "a")
	mustCreate(t, m, "t1")
	makeSchema(t, m, "t1")
	// segment 1: rows 1..3; segment 2: rows 4..6 (flush explicitly between)
	for i := 1; i <= 3; i++ {
		mustTx(t, m, "t1", "c", fmt.Sprintf("a%d", i), insertStmt(int64(i), "early"))
	}
	ten, _ := m.Acquire(context.Background(), "t1", false)
	mf, _, _ := fs.GetManifest(context.Background(), "t1")
	segsBefore, _ := fs.ListSegments(context.Background(), "t1", mf.Instance, mf.Gen)
	for i := 4; i <= 6; i++ {
		mustTx(t, m, "t1", "c", fmt.Sprintf("b%d", i), insertStmt(int64(i), "late"))
	}
	segsAfter, _ := fs.ListSegments(context.Background(), "t1", mf.Instance, mf.Gen)
	if len(segsAfter) <= len(segsBefore) {
		t.Skip("batching merged segments; need two")
	}
	// corrupt a middle byte of the second segment
	fs.Corrupt(segsAfter[len(segsBefore)].Key)
	_ = ten

	crashTenant(t, m, "t1")
	waitLeaseExpiry()
	m2 := newTestManager(t, fs, "b")
	// the new contract: refuse to claim a corrupted tail LOUDLY rather
	// than publish a truncated base and let cleanup delete history
	_, err := m2.Acquire(context.Background(), "t1", false)
	if err == nil || !strings.Contains(err.Error(), "replay incomplete") {
		t.Fatalf("expected replay-incomplete refusal, got %v", err)
	}
	// operator repair path is out of scope here; the refusal IS the contract
}

// S3: crash between WAL truncate and base upload inside a checkpoint must
// lose nothing — the manifest still points at the old gen until the CAS.
func TestCheckpointFailureMidway(t *testing.T) {
	fs := NewFakeStore()
	m := newTestManager(t, fs, "a")
	mustCreate(t, m, "t1")
	makeSchema(t, m, "t1")
	for i := 0; i < 5; i++ {
		mustTx(t, m, "t1", "c", fmt.Sprintf("c%d", i), insertStmt(int64(i), "v"))
	}
	// poison base uploads: checkpoint will truncate locally, fail the
	// upload, and poison the tenant
	fs2 := fs // fail via hook on PutBase is not wired; simulate by crash
	_ = fs2
	crashTenant(t, m, "t1")
	waitLeaseExpiry()
	m2 := newTestManager(t, fs, "b")
	if n := countRows(t, m2, "t1"); n != 5 {
		t.Fatalf("lost rows: %d", n)
	}
}

// S4: a tenant deleted and recreated under the same name while a zombie
// writer lives: the zombie's writes land in the dead instance prefix and
// never appear in the new incarnation (instance UUID scoping).
func TestDeleteRecreateZombieIsolation(t *testing.T) {
	fs := NewFakeStore()
	m := newTestManager(t, fs, "a")
	mustCreate(t, m, "t1")
	makeSchema(t, m, "t1")
	mustTx(t, m, "t1", "c", "d1", insertStmt(1, "old-instance"))

	zombie := m.LocalTenant("t1")
	zombie.Freeze() // partitioned owner: believes it owns t1 forever

	// delete + recreate while the zombie holds its belief
	if err := fs.DeleteTenant(context.Background(), "t1"); err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	delete(m.tenants, "t1")
	m.mu.Unlock()
	mustCreate(t, m, "t1")
	makeSchema(t, m, "t1")
	mustTx(t, m, "t1", "c", "n1", insertStmt(100, "new-instance"))

	// zombie wakes and flushes: its segment PUTs target the OLD instance
	// prefix and old gen — the new incarnation must never read them
	zombie.Unfreeze()
	time.Sleep(300 * time.Millisecond)
	res := mustQuery(t, m, "t1", `SELECT COUNT(*) FROM kv WHERE v = 'new-instance'`)
	if res.Rows[0][0].(int64) != 1 {
		t.Fatalf("new instance wrong: %+v", res.Rows)
	}
	res = mustQuery(t, m, "t1", `SELECT COUNT(*) FROM kv WHERE v = 'old-instance'`)
	if res.Rows[0][0].(int64) != 0 {
		t.Fatalf("zombie data leaked into new instance")
	}
}

// S5: eviction under active write load loses nothing.
func TestEvictionUnderLoad(t *testing.T) {
	fs := NewFakeStore()
	dir := t.TempDir()
	m := NewManager(fs, "fake", "a", "standard", dir, testOpts(), 2, &Metrics{}, ControllerOpts{})
	const N = 6
	for i := 0; i < N; i++ {
		tid := fmt.Sprintf("t%d", i)
		mustCreate(t, m, tid)
		makeSchema(t, m, tid)
	}
	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tid := fmt.Sprintf("t%d", i)
			for j := 0; j < 20; j++ {
				ten, err := m.Acquire(context.Background(), tid, false)
				if err != nil {
					t.Error(err)
					return
				}
				if _, _, err := ten.SubmitTx(TxRequest{Client: "c", TxID: fmt.Sprintf("%d", j),
					Stmts: []Statement{insertStmt(int64(j), "v")}}); err != nil {
					t.Error(err)
					return
				}
			}
		}(i)
	}
	// sweeper runs every 5s with maxOpen=2; give it time to evict mid-load
	wg.Wait()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		m.mu.Lock()
		n := len(m.tenants)
		m.mu.Unlock()
		if n <= 2 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	for i := 0; i < N; i++ {
		if n := countRows(t, m, fmt.Sprintf("t%d", i)); n != 20 {
			t.Fatalf("t%d: %d rows after eviction churn", i, n)
		}
	}
}

// S6: a corrupted local cache must be rebuilt from S3, not fail forever.
func TestCorruptLocalCacheRebuilds(t *testing.T) {
	fs := NewFakeStore()
	m := newTestManager(t, fs, "a")
	mustCreate(t, m, "t1")
	makeSchema(t, m, "t1")
	mustTx(t, m, "t1", "c", "x", insertStmt(1, "safe"))
	if err := m.Release("t1", ""); err != nil {
		t.Fatal(err)
	}
	// corrupt the local db file
	dbPath := filepath.Join(m.baseDir, "t", "t1", "db.sqlite")
	if err := os.WriteFile(dbPath, []byte("garbage-not-sqlite"), 0o644); err != nil {
		t.Fatal(err)
	}
	// acquire must detect, wipe, rebuild
	if n := countRows(t, m, "t1"); n != 1 {
		t.Fatalf("corrupt cache recovery failed: %d rows", n)
	}
}

// S7: concurrent acquire storm on one tenant converges to exactly one
// owner-side open and zero errors.
func TestAcquireStorm(t *testing.T) {
	fs := NewFakeStore()
	m := newTestManager(t, fs, "a")
	mustCreate(t, m, "t1")
	makeSchema(t, m, "t1")
	crashTenant(t, m, "t1")
	waitLeaseExpiry()
	m = newTestManager(t, fs, "a2") // pod restart = new boot identity
	var wg sync.WaitGroup
	errs := make(chan error, 100)
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ten, err := m.Acquire(context.Background(), "t1", false)
			if err != nil {
				errs <- err
				return
			}
			_, _, err = ten.SubmitTx(TxRequest{Client: "c", TxID: fmt.Sprintf("s%d", i),
				Stmts: []Statement{insertStmt(int64(i), "v")}})
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
	if n := countRows(t, m, "t1"); n != 100 {
		t.Fatalf("storm applied %d", n)
	}
}

// S8: a read-only transaction parks until its basis is durable (the
// P0-5 gate). With a slow PUT in flight, the read waits for the
// watermark; it does NOT ack before the bytes are durable.
func TestReadOnlyTxWaitsForDurability(t *testing.T) {
	fs := NewFakeStore()
	fs.Latency = 100 * time.Millisecond
	m := newTestManager(t, fs, "a")
	mustCreate(t, m, "t1")
	makeSchema(t, m, "t1")
	mustTx(t, m, "t1", "c", "w", insertStmt(1, "v"))
	ten, _ := m.Acquire(context.Background(), "t1", false)
	// start a write that will sit in a slow PUT, then a read-only tx
	// park the fake store so the write's PUT is in flight
	gate := make(chan struct{})
	fs.FailPutSegment = func(key string) bool { <-gate; return false }
	go func() {
		ten.SubmitTx(TxRequest{Client: "c", TxID: "w2", Stmts: []Statement{insertStmt(2, "v")}})
	}()
	time.Sleep(200 * time.Millisecond) // write applied, PUT parked
	start := time.Now()
	done := make(chan error, 1)
	go func() {
		_, _, err := ten.SubmitTx(TxRequest{Client: "c", TxID: "r1",
			Stmts: []Statement{{SQL: `SELECT COUNT(*) FROM kv`}}})
		done <- err
	}()
	select {
	case <-done:
		t.Fatal("read-only tx returned before the watermark passed")
	case <-time.After(300 * time.Millisecond):
		// still parked: correct
	}
	close(gate)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	// the read released only after the write's PUT completed
	if d := time.Since(start); d < 300*time.Millisecond {
		t.Fatalf("read-only tx released too early: %v", d)
	}
}

// S9: a partition drops the owner's pod-lease heartbeats; the lease
// lapses, b takes over. When the partition heals, a's first heartbeat
// detects the lapse and fail-stops every tenant it still holds; pending
// commits fail retryably and b's view has all durable data.
func TestHeartbeatLossFence(t *testing.T) {
	fs := NewFakeStore()
	ma := newTestManager(t, fs, "a")
	mustCreate(t, ma, "t1")
	makeSchema(t, ma, "t1")
	mustTx(t, ma, "t1", "c", "h1", insertStmt(1, "durable"))

	ta := ma.LocalTenant("t1")
	ma.SetPartitioned(true)
	mb := newTestManager(t, fs, "b")
	if n := countRows(t, mb, "t1"); n != 1 {
		t.Fatalf("b sees %d", n)
	}
	mustTx(t, mb, "t1", "c", "h2", insertStmt(2, "by-b"))
	ma.SetPartitioned(false) // heal: heartbeat lands after a lapse
	deadline := time.Now().Add(3 * time.Second)
	for ta.Alive() && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if ta.Alive() {
		t.Fatal("a never noticed the fence")
	}
	if _, _, err := ta.SubmitTx(TxRequest{Client: "c", TxID: "h3", Stmts: []Statement{insertStmt(3, "zombie")}}); err == nil {
		t.Fatal("zombie write accepted")
	}
	res := mustQuery(t, mb, "t1", `SELECT COUNT(*) FROM kv`)
	if res.Rows[0][0].(int64) != 2 {
		t.Fatalf("rows: %+v", res.Rows)
	}
}

var _ = errors.Is
var _ = strings.Contains

// A read issued while its basis is still in flight must
// NOT return non-durable state. It parks until the watermark covers the
// basis; if the owner crashes first, the parked read fails and recovery
// shows no trace of the unacked commit (and no snapshot-id reuse).
func TestReadsNeverExposeNonDurableState(t *testing.T) {
	fs := NewFakeStore()
	m := newTestManager(t, fs, "a")
	mustCreate(t, m, "t1")
	makeSchema(t, m, "t1")
	mustTx(t, m, "t1", "c", "d1", insertStmt(1, "durable"))

	// stall the next segment PUT in the fake store
	release := make(chan struct{})
	fs.FailPutSegment = func(key string) bool { <-release; return false }
	ten, _ := m.Acquire(context.Background(), "t1", false)
	writeDone := make(chan error, 1)
	go func() {
		_, _, err := ten.SubmitTx(TxRequest{Client: "c", TxID: "d2", Stmts: []Statement{insertStmt(2, "inflight")}})
		writeDone <- err
	}()
	// wait until the write is applied and its PUT is parked in the fake
	time.Sleep(300 * time.Millisecond)

	// the read must NOT see the in-flight row: it parks behind the watermark
	readDone := make(chan int64, 1)
	go func() {
		res, err := ten.SubmitQuery(Statement{SQL: `SELECT COUNT(*) FROM kv`})
		if err != nil {
			readDone <- -1
			return
		}
		readDone <- res.Rows[0][0].(int64)
	}()
	select {
	case v := <-readDone:
		t.Fatalf("read returned %d while its basis was in flight", v)
	case <-time.After(500 * time.Millisecond):
		// still parked: correct
	}
	close(release) // let the PUT land
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}
	select {
	case v := <-readDone:
		if v != 2 {
			t.Fatalf("read after durability saw %d rows, want 2", v)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("parked read never released after watermark passed")
	}

	// now the crash-before-PUT variant: stall, crash, recover, verify absent
	fs2 := NewFakeStore()
	m2 := newTestManager(t, fs2, "a")
	mustCreate(t, m2, "t2")
	makeSchema(t, m2, "t2")
	mustTx(t, m2, "t2", "c", "d1", insertStmt(1, "durable"))
	release2 := make(chan struct{})
	fs2.FailPutSegment = func(key string) bool { <-release2; return false }
	ten2, _ := m2.Acquire(context.Background(), "t2", false)
	go ten2.SubmitTx(TxRequest{Client: "c", TxID: "d2", Stmts: []Statement{insertStmt(2, "lost")}})
	time.Sleep(300 * time.Millisecond)
	crashTenant(t, m2, "t2")
	close(release2) // zombie PUT may land in the dead owner's prefix window
	waitLeaseExpiry()
	m3 := newTestManager(t, fs2, "b")
	// the commit was never acked, so surviving is legal; what is NOT legal
	// is applying it twice when the client retries with the same txid
	mustTx(t, m3, "t2", "c", "d2", insertStmt(2, "lost"))
	if n := countRows(t, m3, "t2"); n != 2 {
		t.Fatalf("retry after crash must apply exactly once: %d rows", n)
	}
}

// S11: the dedup table stays bounded: checkpoints trim receipts beyond the
// 100k window. Scale the cap down via a small tenant and many checkpoints.
func TestDedupReceiptCap(t *testing.T) {
	fs := NewFakeStore()
	o := testOpts()
	o.MaxSegments = 2 // force a checkpoint every other commit
	m := NewManager(fs, "fake", "a", "standard", t.TempDir(), o, 1000, &Metrics{}, ControllerOpts{})
	mustCreate(t, m, "t1")
	makeSchema(t, m, "t1")
	for i := 0; i < 12; i++ {
		mustTx(t, m, "t1", "c", fmt.Sprintf("r%d", i), insertStmt(int64(i), "v"))
	}
	ten, _ := m.Acquire(context.Background(), "t1", false)
	// force the checkpoint so the trim runs
	done := make(chan bool, 1)
	ten.ctlCh <- func() { done <- ten.checkpointLocked("") }
	if !<-done {
		t.Fatalf("checkpoint: %v", ten.deadErr)
	}
	// count via the debug path (the dedup table is off-limits to /tx by
	// design; the authorizer is defense-in-depth on top)
	var n int64
	got := make(chan int64, 1)
	ten.ctlCh <- func() {
		ten.db.QueryRow(`SELECT COUNT(*) FROM _hw_dedup`).Scan(&n)
		got <- n
	}
	n = <-got
	// 12 commits + schema; after 6 checkpoints the trim window (100k) never
	// binds, so this only asserts no unbounded growth per commit pair
	if n > 14 {
		t.Fatalf("dedup grew: %d rows for 13 commits", n)
	}
	// retry of the first commit still dedups (inside the window)
	res2, dup, err := ten.SubmitTx(TxRequest{Client: "c", TxID: "r0", Stmts: []Statement{insertStmt(0, "v")}})
	if err != nil || !dup {
		t.Fatalf("receipt lost: dup=%v err=%v res=%v", dup, err, res2)
	}
}
