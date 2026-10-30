package hoglite

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"
)

// slowSQL never finishes on its own: all its work happens inside the first
// step, so only an interrupt (context cancel) ends it.
const slowSQL = `WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c) SELECT count(*) FROM c`

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// onActor runs f on the tenant actor and returns its result.
func onActor[T any](ten *Tenant, f func() T) T {
	ch := make(chan T, 1)
	ten.ctlCh <- func() { ch <- f() }
	return <-ch
}

func checkpointNow(ten *Tenant) bool {
	return onActor(ten, func() bool { return ten.checkpointLocked("") })
}

type readRes struct {
	res QueryResult
	err error
}

func goQuery(ctx context.Context, ten *Tenant, sql string) chan readRes {
	ch := make(chan readRes, 1)
	go func() {
		res, err := ten.SubmitQueryCtx(ctx, Statement{SQL: sql})
		ch <- readRes{res, err}
	}()
	return ch
}

func recvRead(t *testing.T, ch chan readRes, d time.Duration) readRes {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(d):
		t.Fatalf("read did not return within %v", d)
	}
	return readRes{}
}

// blockingHook parks every worker after its pin until gate closes. It
// ignores cancellation on purpose (a read stuck between two rows).
func blockingHook() (hook func(context.Context), pinned chan struct{}, gate chan struct{}) {
	pinned = make(chan struct{}, 16)
	gate = make(chan struct{})
	return func(context.Context) { pinned <- struct{}{}; <-gate }, pinned, gate
}

func waitPinned(t *testing.T, pinned chan struct{}) {
	t.Helper()
	select {
	case <-pinned:
	case <-time.After(5 * time.Second):
		t.Fatal("read never pinned")
	}
}

func newReadTestManager(t *testing.T, fs *FakeStore, o TenantOpts, met *Metrics) *Manager {
	t.Helper()
	return NewManager(fs, "fake", "a", "standard", t.TempDir(), o, 1000, met, ControllerOpts{})
}

// A slow query must not stall the actor: writes commit while it runs, and
// canceling the request stops the query and returns its slots.
func TestSlowReadDoesNotBlockWrites(t *testing.T) {
	fs := NewFakeStore()
	met := &Metrics{}
	m := newReadTestManager(t, fs, testOpts(), met)
	mustCreate(t, m, "t1")
	makeSchema(t, m, "t1")
	ten, _ := m.Acquire(context.Background(), "t1", false)

	ckBefore := met.Checkpoints.Load()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	read := goQuery(ctx, ten, slowSQL)
	waitFor(t, "slow read pinned", func() bool { return ten.readers.count() == 1 })

	for i := int64(1); i <= 20; i++ {
		t0 := time.Now()
		mustTx(t, m, "t1", "c", "w"+string(rune('a'+i)), insertStmt(i, "v"))
		if d := time.Since(t0); d > 2*time.Second {
			t.Fatalf("write %d took %v behind a slow read", i, d)
		}
	}
	select {
	case r := <-read:
		t.Fatalf("slow read ended early: %+v", r)
	default:
	}
	// 20 commits pass MaxSegments: the optional checkpoint waits for a
	// moment without open snapshots instead of stalling these writes
	if met.Checkpoints.Load() != ckBefore {
		t.Fatalf("size-triggered checkpoint drained a running read")
	}
	if ten.readers.count() != 1 {
		t.Fatalf("slow read no longer running")
	}
	cancel()
	t0 := time.Now()
	if r := recvRead(t, read, 5*time.Second); !errors.Is(r.err, context.Canceled) {
		t.Fatalf("canceled read: %v", r.err)
	}
	// the interrupt really stopped the query and returned its slots
	waitFor(t, "reader drained", func() bool {
		return ten.readers.count() == 0 && met.QueriesInflight.Load() == 0 && len(ten.readSlots) == 0
	})
	t.Logf("slow read stopped %v after cancel", time.Since(t0))
	// with the read gone, the next commit runs the deferred checkpoint
	mustTx(t, m, "t1", "c", "after", insertStmt(100, "v"))
	waitFor(t, "deferred checkpoint", func() bool { return met.Checkpoints.Load() > ckBefore })
	if n := countRows(t, m, "t1"); n != 21 {
		t.Fatalf("rows: %d", n)
	}
}

// The snapshot is pinned on the actor at the read's basis. A worker that
// begins its transaction only when it starts would read a newer commit
// that is not durable yet (here: one whose segment PUT never lands).
func TestReadSnapshotPinnedAtBasis(t *testing.T) {
	fs := NewFakeStore()
	m := newTestManager(t, fs, "a")
	mustCreate(t, m, "t1")
	makeSchema(t, m, "t1")
	mustTx(t, m, "t1", "c", "d1", insertStmt(1, "durable"))
	ten, _ := m.Acquire(context.Background(), "t1", false)

	hook, pinned, gate := blockingHook()
	ten.readHook = hook
	read := goQuery(context.Background(), ten, `SELECT COUNT(*) FROM kv`)
	waitPinned(t, pinned) // basis = d1, durable

	// d2 applies on the actor but its segment PUT stalls: not durable
	release := make(chan struct{})
	fs.FailPutSegment = func(string) bool { <-release; return false }
	writeDone := make(chan error, 1)
	go func() {
		_, _, err := ten.SubmitTx(TxRequest{Client: "c", TxID: "d2", Stmts: []Statement{insertStmt(2, "inflight")}})
		writeDone <- err
	}()
	waitFor(t, "d2 applied", func() bool {
		return onActor(ten, func() bool {
			end, _ := ten.currentCommitEnd()
			return end > ten.durableOff
		})
	})

	close(gate) // the worker runs its query only now
	r := recvRead(t, read, 3*time.Second)
	if r.err != nil {
		t.Fatal(r.err)
	}
	if got := r.res.Rows[0][0].(int64); got != 1 {
		t.Fatalf("read saw %d rows: it observed the non-durable d2", got)
	}
	select {
	case err := <-writeDone:
		t.Fatalf("d2 acked while its PUT is stalled: %v", err)
	default:
	}

	// Second half: a read pinned AFTER d2 sees it, so its result must wait
	// for durability. A crash before that fails the read.
	ten.readHook = nil
	read2 := goQuery(context.Background(), ten, `SELECT COUNT(*) FROM kv`)
	select {
	case r := <-read2:
		t.Fatalf("read over a non-durable basis returned: %+v", r)
	case <-time.After(500 * time.Millisecond):
	}
	ten.Crash()
	if r := recvRead(t, read2, 3*time.Second); r.err == nil {
		t.Fatalf("read over a lost basis succeeded: %+v", r.res)
	}
	close(release)
	<-writeDone
}

// A checkpoint waits for an open snapshot instead of failing TRUNCATE.
func TestCheckpointWaitsForInflightRead(t *testing.T) {
	fs := NewFakeStore()
	met := &Metrics{}
	o := testOpts()
	o.ReadDrainWait = 3 * time.Second
	m := newReadTestManager(t, fs, o, met)
	mustCreate(t, m, "t1")
	makeSchema(t, m, "t1")
	mustTx(t, m, "t1", "c", "d1", insertStmt(1, "v"))
	ten, _ := m.Acquire(context.Background(), "t1", false)
	ckBefore := met.Checkpoints.Load()

	hook, pinned, gate := blockingHook()
	ten.readHook = hook
	read := goQuery(context.Background(), ten, `SELECT COUNT(*) FROM kv`)
	waitPinned(t, pinned)

	ckDone := make(chan bool, 1)
	go func() { ckDone <- checkpointNow(ten) }()
	select {
	case ok := <-ckDone:
		t.Fatalf("checkpoint finished under an open snapshot: %v", ok)
	case <-time.After(300 * time.Millisecond):
	}
	close(gate)
	if r := recvRead(t, read, 3*time.Second); r.err != nil || r.res.Rows[0][0].(int64) != 1 {
		t.Fatalf("read: %+v", r)
	}
	select {
	case ok := <-ckDone:
		if !ok {
			t.Fatalf("checkpoint failed: %v", ten.deadErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("checkpoint never finished")
	}
	if met.Checkpoints.Load() != ckBefore+1 || !ten.Alive() {
		t.Fatalf("checkpoint did not run: %d -> %d alive=%v", ckBefore, met.Checkpoints.Load(), ten.Alive())
	}
	mustTx(t, m, "t1", "c", "d2", insertStmt(2, "v"))
	if n := countRows(t, m, "t1"); n != 2 {
		t.Fatalf("rows: %d", n)
	}
}

// A read that runs past the drain grace is canceled with ErrReadPreempted
// and the checkpoint proceeds.
func TestCheckpointPreemptsSlowRead(t *testing.T) {
	fs := NewFakeStore()
	met := &Metrics{}
	o := testOpts()
	o.ReadDrainWait = 200 * time.Millisecond
	m := newReadTestManager(t, fs, o, met)
	mustCreate(t, m, "t1")
	makeSchema(t, m, "t1")
	mustTx(t, m, "t1", "c", "d1", insertStmt(1, "v"))
	ten, _ := m.Acquire(context.Background(), "t1", false)
	ckBefore := met.Checkpoints.Load()

	read := goQuery(context.Background(), ten, slowSQL)
	waitFor(t, "slow read pinned", func() bool { return ten.readers.count() == 1 })
	t0 := time.Now()
	if !checkpointNow(ten) {
		t.Fatalf("checkpoint failed: %v", ten.deadErr)
	}
	if d := time.Since(t0); d > 3*time.Second {
		t.Fatalf("checkpoint took %v", d)
	}
	if met.Checkpoints.Load() != ckBefore+1 {
		t.Fatal("checkpoint skipped instead of preempting")
	}
	if r := recvRead(t, read, 3*time.Second); !errors.Is(r.err, ErrReadPreempted) {
		t.Fatalf("preempted read: %v", r.err)
	}
	if n := countRows(t, m, "t1"); n != 1 || !ten.Alive() {
		t.Fatalf("rows %d alive %v", n, ten.Alive())
	}
}

// A read that ignores cancellation makes the checkpoint skip safely (no
// TRUNCATE under a reader, no poison) and back off.
func TestCheckpointSkipsWhenReadDoesNotStop(t *testing.T) {
	fs := NewFakeStore()
	met := &Metrics{}
	o := testOpts()
	o.ReadDrainWait = 100 * time.Millisecond
	m := newReadTestManager(t, fs, o, met)
	mustCreate(t, m, "t1")
	makeSchema(t, m, "t1")
	mustTx(t, m, "t1", "c", "d1", insertStmt(1, "v"))
	ten, _ := m.Acquire(context.Background(), "t1", false)
	ckBefore := met.Checkpoints.Load()
	genBefore := onActor(ten, func() int64 { return ten.mf.Gen })

	hook, pinned, gate := blockingHook()
	ten.readHook = hook
	read := goQuery(context.Background(), ten, `SELECT COUNT(*) FROM kv`)
	waitPinned(t, pinned)
	if !checkpointNow(ten) {
		t.Fatalf("skip must not fail the tenant: %v", ten.deadErr)
	}
	if met.Checkpoints.Load() != ckBefore || onActor(ten, func() int64 { return ten.mf.Gen }) != genBefore {
		t.Fatal("checkpoint ran with an open snapshot")
	}
	if onActor(ten, func() bool { return !time.Now().Before(ten.ckptDeferUntil) }) {
		t.Fatal("no back-off after a skipped checkpoint")
	}
	close(gate)
	if r := recvRead(t, read, 3*time.Second); !errors.Is(r.err, ErrReadPreempted) {
		t.Fatalf("read after preemption: %+v", r)
	}
	ten.readHook = nil
	if !checkpointNow(ten) || met.Checkpoints.Load() != ckBefore+1 {
		t.Fatalf("checkpoint after the read ended: %v", ten.deadErr)
	}
	mustTx(t, m, "t1", "c", "d2", insertStmt(2, "v"))
	if n := countRows(t, m, "t1"); n != 2 {
		t.Fatalf("rows: %d", n)
	}
}

// Graceful stop cancels a running read cleanly; recovery is unaffected.
func TestStopWithInflightRead(t *testing.T) {
	fs := NewFakeStore()
	met := &Metrics{}
	o := testOpts()
	o.ReadDrainWait = 200 * time.Millisecond
	m := newReadTestManager(t, fs, o, met)
	mustCreate(t, m, "t1")
	makeSchema(t, m, "t1")
	mustTx(t, m, "t1", "c", "d1", insertStmt(1, "v"))
	ten, _ := m.Acquire(context.Background(), "t1", false)

	read := goQuery(context.Background(), ten, slowSQL)
	waitFor(t, "slow read pinned", func() bool { return ten.readers.count() == 1 })
	t0 := time.Now()
	ten.Stop(true, "")
	if d := time.Since(t0); d > 3*time.Second {
		t.Fatalf("stop took %v", d)
	}
	r := recvRead(t, read, 3*time.Second)
	if !errors.Is(r.err, ErrClosed) && !errors.Is(r.err, ErrReadPreempted) {
		t.Fatalf("read across stop: %v", r.err)
	}
	waitFor(t, "roDB closed", func() bool { return ten.roDB.Stats().OpenConnections == 0 })
	if met.QueriesInflight.Load() != 0 {
		t.Fatalf("pod slots leaked: %d", met.QueriesInflight.Load())
	}
	if n := countRows(t, m, "t1"); n != 1 {
		t.Fatalf("rows after re-acquire: %d", n)
	}
}

// Crash under a read that ignores cancellation: the read fails cleanly,
// and roDB stays open until that read has ended.
func TestCrashWithStuckRead(t *testing.T) {
	fs := NewFakeStore()
	met := &Metrics{}
	o := testOpts()
	o.ReadDrainWait = 100 * time.Millisecond
	m := newReadTestManager(t, fs, o, met)
	mustCreate(t, m, "t1")
	makeSchema(t, m, "t1")
	ten, _ := m.Acquire(context.Background(), "t1", false)

	ran := make(chan error, 1)
	pinned := make(chan struct{}, 1)
	gate := make(chan struct{})
	ten.readHook = func(ctx context.Context) {
		pinned <- struct{}{}
		<-gate
		// the pinned transaction must still be usable: roDB is not closed
		// under a running read
		ran <- ten.roDB.PingContext(context.Background())
	}
	read := goQuery(context.Background(), ten, `SELECT COUNT(*) FROM kv`)
	waitPinned(t, pinned)
	t0 := time.Now()
	ten.Crash()
	if d := time.Since(t0); d > 3*time.Second {
		t.Fatalf("crash took %v", d)
	}
	if r := recvRead(t, read, 3*time.Second); r.err == nil {
		t.Fatal("read on a crashed tenant succeeded")
	}
	if n := ten.roDB.Stats().OpenConnections; n != 1 {
		t.Fatalf("roDB connections under a running read: %d", n)
	}
	close(gate)
	if err := <-ran; err != nil {
		t.Fatal(err)
	}
	waitFor(t, "roDB closed after the last read", func() bool { return ten.roDB.Stats().OpenConnections == 0 })
	// the late reader closed the last connection; it must not unlink the
	// WAL by path (a re-acquire may own that path by now)
	if _, err := os.Stat(ten.walPath); err != nil {
		t.Fatalf("late reader close removed the WAL: %v", err)
	}
	waitFor(t, "slots returned", func() bool { return met.QueriesInflight.Load() == 0 })
}

// The pod limiter bounds queries across tenants; a waiting query ends with
// its context (deadline or cancel).
func TestPodQueryLimiter(t *testing.T) {
	fs := NewFakeStore()
	met := &Metrics{}
	o := testOpts()
	o.PodMaxQueries = 2
	m := newReadTestManager(t, fs, o, met)
	var tens []*Tenant
	hook, pinned, gate := blockingHook()
	for _, id := range []string{"t1", "t2"} {
		mustCreate(t, m, id)
		makeSchema(t, m, id)
		ten, _ := m.Acquire(context.Background(), id, false)
		ten.readHook = hook
		tens = append(tens, ten)
	}
	r1 := goQuery(context.Background(), tens[0], `SELECT COUNT(*) FROM kv`)
	r2 := goQuery(context.Background(), tens[1], `SELECT COUNT(*) FROM kv`)
	waitPinned(t, pinned)
	waitPinned(t, pinned)
	if n := met.QueriesInflight.Load(); n != 2 {
		t.Fatalf("inflight: %d", n)
	}

	// deadline while waiting
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	r3 := goQuery(ctx, tens[1], `SELECT 1`)
	waitFor(t, "third query waiting", func() bool { return met.QueriesWaiting.Load() == 1 })
	if r := recvRead(t, r3, 3*time.Second); !errors.Is(r.err, context.DeadlineExceeded) {
		t.Fatalf("saturated query: %v", r.err)
	}

	// client goes away while waiting
	ctx4, cancel4 := context.WithCancel(context.Background())
	r4 := goQuery(ctx4, tens[0], `SELECT 1`)
	waitFor(t, "fourth query waiting", func() bool { return met.QueriesWaiting.Load() == 1 })
	cancel4()
	if r := recvRead(t, r4, time.Second); !errors.Is(r.err, context.Canceled) {
		t.Fatalf("canceled waiter: %v", r.err)
	}
	waitFor(t, "no waiters", func() bool { return met.QueriesWaiting.Load() == 0 })
	if n := met.QueriesInflight.Load(); n != 2 {
		t.Fatalf("inflight after waiters left: %d", n)
	}

	close(gate)
	for _, ch := range []chan readRes{r1, r2} {
		if r := recvRead(t, ch, 3*time.Second); r.err != nil {
			t.Fatal(r.err)
		}
	}
	waitFor(t, "slots returned", func() bool { return met.QueriesInflight.Load() == 0 })
	for _, ten := range tens {
		ten.readHook = nil
	}
	// a waiter is admitted once a slot frees
	mustQuery(t, m, "t1", `SELECT 1`)
}

// MaxReaders bounds one tenant; its waiters do not hold pod slots.
func TestTenantReaderCap(t *testing.T) {
	fs := NewFakeStore()
	met := &Metrics{}
	o := testOpts()
	o.MaxReaders = 1
	m := newReadTestManager(t, fs, o, met)
	mustCreate(t, m, "t1")
	makeSchema(t, m, "t1")
	ten, _ := m.Acquire(context.Background(), "t1", false)
	hook, pinned, gate := blockingHook()
	ten.readHook = hook
	r1 := goQuery(context.Background(), ten, `SELECT COUNT(*) FROM kv`)
	waitPinned(t, pinned)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	r2 := goQuery(ctx, ten, `SELECT 1`)
	if r := recvRead(t, r2, 3*time.Second); !errors.Is(r.err, context.DeadlineExceeded) {
		t.Fatalf("second reader: %v", r.err)
	}
	if met.QueriesInflight.Load() != 1 || met.QueriesWaiting.Load() != 0 {
		t.Fatalf("tenant waiter touched the pod limiter: inflight %d waiting %d",
			met.QueriesInflight.Load(), met.QueriesWaiting.Load())
	}
	close(gate)
	if r := recvRead(t, r1, 3*time.Second); r.err != nil {
		t.Fatal(r.err)
	}
}

// Read-only /tx runs on the reader path: it does not block writes, reads
// its pinned snapshot, and keeps its guards.
func TestReadOnlyTxOffActor(t *testing.T) {
	fs := NewFakeStore()
	m := newTestManager(t, fs, "a")
	mustCreate(t, m, "t1")
	makeSchema(t, m, "t1")
	mustTx(t, m, "t1", "c", "d1", insertStmt(1, "v"))
	ten, _ := m.Acquire(context.Background(), "t1", false)
	hook, pinned, gate := blockingHook()
	ten.readHook = hook
	one := int64(1)
	type txRes struct {
		res []QueryResult
		err error
	}
	done := make(chan txRes, 1)
	go func() {
		res, _, err := ten.SubmitTx(TxRequest{Client: "c", TxID: "ro1", Stmts: []Statement{
			{SQL: `SELECT COUNT(*) FROM kv`, ExpectScalarInt: &one},
			{SQL: `SELECT v FROM kv WHERE k = 1`},
		}})
		done <- txRes{res, err}
	}()
	waitPinned(t, pinned)
	t0 := time.Now()
	mustTx(t, m, "t1", "c", "d2", insertStmt(2, "v"))
	if d := time.Since(t0); d > 2*time.Second {
		t.Fatalf("write behind a read-only tx took %v", d)
	}
	close(gate)
	var r txRes
	select {
	case r = <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("read-only tx never returned")
	}
	if r.err != nil || len(r.res) != 2 || r.res[1].Rows[0][0] != "v" {
		t.Fatalf("read-only tx: %+v %v", r.res, r.err)
	}
	ten.readHook = nil
	_, _, err := ten.SubmitTx(TxRequest{Client: "c", TxID: "ro2", Stmts: []Statement{
		{SQL: `SELECT COUNT(*) FROM kv`, ExpectScalarInt: &one}}})
	var ef *ExpectFailedError
	if !errors.As(err, &ef) || ef.Index != 0 {
		t.Fatalf("guard on the reader path: %v", err)
	}
}
