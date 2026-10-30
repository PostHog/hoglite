package hoglite

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Tests for delta checkpoints (S2) together with reads off the actor (Q1),
// the memory guards (Q3) and usage accounting (Q4).

// deltaTestOpts turns off the size triggers, so only the test checkpoints.
func deltaTestOpts() TenantOpts {
	o := testOpts()
	o.CheckpointByte = 1 << 30
	o.MaxSegments = 1 << 20
	// the bases of these tests are tiny: allow long chains of deltas
	o.DeltaMaxRatio = 1000
	return o
}

func tenantManifest(ten *Tenant) Manifest {
	return onActor(ten, func() Manifest { return *ten.mf })
}

// newDeltaTenant creates t1 with rows 1..n and checks that the next
// checkpoint can be a delta: a chain generation with a base.
func newDeltaTenant(t *testing.T, m *Manager, n int) *Tenant {
	t.Helper()
	mustCreate(t, m, "t1")
	makeSchema(t, m, "t1")
	for i := 1; i <= n; i++ {
		mustTx(t, m, "t1", "c", fmt.Sprint("r", i), insertStmt(int64(i), strings.Repeat("v", 3000)))
	}
	ten := m.LocalTenant("t1")
	if mf := tenantManifest(ten); mf.Format != manifestFormatChain || mf.BaseSize <= 0 {
		t.Fatalf("not a chain generation: %+v", mf)
	}
	return ten
}

// expectDelta checks that exactly one checkpoint ran since before and that
// it published a delta on the same chain.
func expectDelta(t *testing.T, met *Metrics, ten *Tenant, before Manifest, deltas0, rebases0 int64) {
	t.Helper()
	mf := tenantManifest(ten)
	if mf.Gen != before.Gen+1 || mf.BaseKey != before.BaseKey || len(mf.Deltas) != len(before.Deltas)+1 {
		t.Fatalf("not a delta checkpoint: before %+v, after %+v", before, mf)
	}
	if met.DeltaCheckpoints.Load() != deltas0+1 || met.Rebases.Load() != rebases0 {
		t.Fatalf("delta checkpoints %d -> %d, rebases %d -> %d",
			deltas0, met.DeltaCheckpoints.Load(), rebases0, met.Rebases.Load())
	}
}

// recoverRows takes the tenant over on a new manager (from S3 only) and
// returns its row count.
func recoverRows(t *testing.T, m *Manager, fs Backend, o TenantOpts) int64 {
	t.Helper()
	m.KillLease()
	m2 := NewManager(fs, "fake", "a", "standard", t.TempDir(), o, 1000, &Metrics{}, ControllerOpts{})
	t.Cleanup(m2.ShutdownAll)
	return countRows(t, m2, "t1")
}

// A delta checkpoint runs only after the read drain. A read that ends in
// the grace is waited for, a read past it is preempted, and a read that
// ignores the cancel makes the checkpoint skip. The fold never meets an
// open snapshot, so no case fails the tenant on busy, and recovery has
// every row.
func TestDeltaCheckpointWithPinnedRead(t *testing.T) {
	for _, mode := range []string{"wait", "preempt", "skip"} {
		t.Run(mode, func(t *testing.T) {
			fs := NewFakeStore()
			met := &Metrics{}
			o := deltaTestOpts()
			switch mode {
			case "wait":
				o.ReadDrainWait = 3 * time.Second
			case "preempt":
				o.ReadDrainWait = 200 * time.Millisecond
			case "skip":
				o.ReadDrainWait = 100 * time.Millisecond
			}
			m := newReadTestManager(t, fs, o, met)
			ten := newDeltaTenant(t, m, 3)
			before := tenantManifest(ten)
			ck0, d0, r0 := met.Checkpoints.Load(), met.DeltaCheckpoints.Load(), met.Rebases.Load()

			var read chan readRes
			var gate chan struct{}
			if mode == "preempt" {
				read = goQuery(context.Background(), ten, slowSQL)
				waitFor(t, "slow read pinned", func() bool { return ten.readers.count() == 1 })
			} else {
				var hook func(context.Context)
				var pinned chan struct{}
				hook, pinned, gate = blockingHook()
				ten.readHook = hook
				read = goQuery(context.Background(), ten, `SELECT COUNT(*) FROM kv`)
				waitPinned(t, pinned)
			}
			// writes go on while the snapshot is open; the checkpoint folds them
			for i := 4; i <= 6; i++ {
				mustTx(t, m, "t1", "c", fmt.Sprint("r", i), insertStmt(int64(i), strings.Repeat("w", 3000)))
			}

			switch mode {
			case "wait":
				ckDone := make(chan bool, 1)
				go func() { ckDone <- checkpointNow(ten) }()
				select {
				case ok := <-ckDone:
					t.Fatalf("checkpoint finished under an open snapshot: %v", ok)
				case <-time.After(300 * time.Millisecond):
				}
				close(gate)
				// the read sees its pinned snapshot, not the later commits
				if r := recvRead(t, read, 3*time.Second); r.err != nil || r.res.Rows[0][0].(int64) != 3 {
					t.Fatalf("pinned read: %+v", r)
				}
				if ok := <-ckDone; !ok {
					t.Fatalf("checkpoint failed: %v", ten.deadErr)
				}
				expectDelta(t, met, ten, before, d0, r0)
			case "preempt":
				if !checkpointNow(ten) {
					t.Fatalf("checkpoint failed: %v", ten.deadErr)
				}
				if r := recvRead(t, read, 3*time.Second); !errors.Is(r.err, ErrReadPreempted) {
					t.Fatalf("read: %+v", r)
				}
				expectDelta(t, met, ten, before, d0, r0)
			case "skip":
				size0, _, sub0 := walState(t, ten)
				if !checkpointNow(ten) {
					t.Fatalf("skip must not fail the tenant: %v", ten.deadErr)
				}
				mf := tenantManifest(ten)
				if met.Checkpoints.Load() != ck0 || met.DeltaCheckpoints.Load() != d0 ||
					mf.Gen != before.Gen || len(mf.Deltas) != len(before.Deltas) {
					t.Fatalf("checkpoint ran with an open snapshot: %+v", mf)
				}
				// nothing was folded: the WAL still holds every shipped byte
				if size, _, sub := walState(t, ten); size != size0 || sub != sub0 || size < sub {
					t.Fatalf("WAL changed by a skipped checkpoint: size %d -> %d, shipped %d -> %d", size0, size, sub0, sub)
				}
				close(gate)
				if r := recvRead(t, read, 3*time.Second); !errors.Is(r.err, ErrReadPreempted) {
					t.Fatalf("read after preemption: %+v", r)
				}
				ten.readHook = nil
				if !checkpointNow(ten) {
					t.Fatalf("checkpoint after the read ended: %v", ten.deadErr)
				}
				expectDelta(t, met, ten, before, d0, r0)
			}
			if !ten.Alive() || met.Checkpoints.Load() != ck0+1 {
				t.Fatalf("alive=%v checkpoints %d -> %d (%v)", ten.Alive(), ck0, met.Checkpoints.Load(), ten.deadErr)
			}
			if n := recoverRows(t, m, fs, o); n != 6 {
				t.Fatalf("rows after recovery: %d", n)
			}
		})
	}
}

// segHoldStore holds segment PUTs while hold is armed.
type segHoldStore struct {
	*FakeStore
	armed   atomic.Bool
	entered chan string
	release chan struct{}
}

func (s *segHoldStore) PutSegment(ctx context.Context, key string, data []byte) error {
	if s.armed.Load() {
		select {
		case s.entered <- key:
		default:
		}
		select {
		case <-s.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return s.FakeStore.PutSegment(ctx, key, data)
}

// A read pinned on a commit that is not durable yet parks until a delta
// checkpoint makes that commit durable. The commit never gets a segment:
// the inflight cap deferred its cut, and the delta carries it. The read
// returns the pinned state, and recovery from S3 has the commit.
func TestDeltaCheckpointReleasesParkedRead(t *testing.T) {
	fs := &segHoldStore{FakeStore: NewFakeStore(), entered: make(chan string, 4), release: make(chan struct{})}
	met := &Metrics{}
	o := deltaTestOpts()
	o.Inflight = 1
	m := NewManager(fs, "fake", "a", "standard", t.TempDir(), o, 1000, met, ControllerOpts{})
	ten := newDeltaTenant(t, m, 2)
	before := tenantManifest(ten)
	d0, r0 := met.DeltaCheckpoints.Load(), met.Rebases.Load()

	fs.armed.Store(true)
	txDone := make(chan error, 2)
	submit := func(k int64) {
		go func() {
			_, _, err := ten.SubmitTx(TxRequest{Client: "c", TxID: fmt.Sprint("p", k),
				Stmts: []Statement{insertStmt(k, "parked")}})
			txDone <- err
		}()
	}
	submit(3)
	select {
	case <-fs.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("segment PUT never started")
	}
	submit(4) // its cut waits for the inflight slot
	// the first commit waits in its segment, the second in pending
	waitFor(t, "deferred commit", func() bool {
		return onActor(ten, func() bool { return len(ten.pending) == 1 && len(ten.segs) == 1 })
	})
	segPuts0 := met.SegPuts.Load()
	read := goQuery(context.Background(), ten, `SELECT COUNT(*) FROM kv`)
	waitFor(t, "parked read", func() bool { return onActor(ten, func() int { return len(ten.readWaits) }) == 1 })

	ckDone := make(chan bool, 1)
	go func() { ckDone <- checkpointNow(ten) }()
	time.Sleep(100 * time.Millisecond) // the checkpoint waits in the segment drain
	select {
	case r := <-read:
		t.Fatalf("read released before its basis was durable: %+v", r)
	default:
	}
	close(fs.release)
	if ok := <-ckDone; !ok {
		t.Fatalf("checkpoint failed: %v", ten.deadErr)
	}
	fs.armed.Store(false)
	if r := recvRead(t, read, 3*time.Second); r.err != nil || r.res.Rows[0][0].(int64) != 4 {
		t.Fatalf("parked read: %+v", r)
	}
	for i := 0; i < 2; i++ {
		if err := <-txDone; err != nil {
			t.Fatalf("commit: %v", err)
		}
	}
	expectDelta(t, met, ten, before, d0, r0)
	// segPuts0 already counts the held segment of the first commit
	if n := met.SegPuts.Load() - segPuts0; n != 0 {
		t.Fatalf("%d more segments; the deferred commit must reach S3 only in the delta", n)
	}
	if n := recoverRows(t, m, fs, o); n != 4 {
		t.Fatalf("rows after recovery: %d", n)
	}
}

// SQLITE_NOMEM in the PASSIVE step of a delta checkpoint follows the Q3
// skip rule: nothing was folded, the WAL reaches the shipped offset, the
// tenant stays open, and the next checkpoint publishes the delta.
func TestDeltaCheckpointNoMemInPassiveSkips(t *testing.T) {
	fs := NewFakeStore()
	met := &Metrics{Logger: tLogger{t}}
	o := deltaTestOpts()
	m := newReadTestManager(t, fs, o, met)
	ten := newDeltaTenant(t, m, 20)
	waitFor(t, "acks", func() bool { _, end, sub := walState(t, ten); return end == sub })
	before := tenantManifest(ten)
	ck0, d0, r0, oom0 := met.Checkpoints.Load(), met.DeltaCheckpoints.Load(), met.Rebases.Load(), met.OutOfMemory.Load()
	size0, _, _ := walState(t, ten)

	var folded atomic.Bool
	ten.foldHook = func() { folded.Store(true) }
	restore := withHeapLimitAt(t, 64<<10)
	ok := checkpointNow(ten)
	restore()
	if !ok || !ten.Alive() {
		t.Fatalf("checkpoint under the heap limit: ok=%v alive=%v (%v)", ok, ten.Alive(), ten.deadErr)
	}
	if folded.Load() {
		t.Fatal("PASSIVE succeeded under the heap limit: the test did not reach its case")
	}
	if met.Checkpoints.Load() != ck0 || met.OutOfMemory.Load() == oom0 {
		t.Fatalf("checkpoints %d -> %d, out of memory %d: the limit did not stop the checkpoint",
			ck0, met.Checkpoints.Load(), met.OutOfMemory.Load())
	}
	if mf := tenantManifest(ten); mf.Gen != before.Gen || len(mf.Deltas) != len(before.Deltas) {
		t.Fatalf("manifest changed by a skipped checkpoint: %+v", mf)
	}
	if size, _, sub := walState(t, ten); size != size0 || size < sub {
		t.Fatalf("WAL %d -> %d bytes, shipped %d", size0, size, sub)
	}
	mustTx(t, m, "t1", "c", "after", insertStmt(21, "after"))
	ten.foldHook = nil
	if !checkpointNow(ten) {
		t.Fatalf("checkpoint after the limit: %v", ten.deadErr)
	}
	expectDelta(t, met, ten, before, d0, r0)
	if n := recoverRows(t, m, fs, o); n != 21 {
		t.Fatalf("rows after recovery: %d", n)
	}
}

// SQLITE_NOMEM in the TRUNCATE step after PASSIVE folded the WAL must not
// skip. SQLite counts the WAL as folded, so the next write could restart
// it under the shipped offsets. The tenant fails, publishes nothing, and
// recovery from S3 has every acked row.
func TestDeltaCheckpointNoMemAfterFoldFails(t *testing.T) {
	// lookaside serves small allocations outside the heap limit
	hits := noLookasideForTest(t)
	fs := NewFakeStore()
	met := &Metrics{Logger: tLogger{t}}
	o := deltaTestOpts()
	m := newReadTestManager(t, fs, o, met)
	ten := newDeltaTenant(t, m, 20)
	if hits() == 0 {
		t.Fatal("setup: lookaside hook never matched")
	}
	waitFor(t, "acks", func() bool { _, end, sub := walState(t, ten); return end == sub })
	before := tenantManifest(ten)
	ck0 := met.Checkpoints.Load()

	hard, soft := SQLiteHeapLimits()
	t.Cleanup(func() { SetSQLiteHeapLimits(hard, soft) })
	var folded atomic.Bool
	ten.foldHook = func() {
		folded.Store(true)
		SetSQLiteHeapLimits(1, 0)
	}
	ok := checkpointNow(ten)
	SetSQLiteHeapLimits(hard, soft)
	if !folded.Load() {
		t.Fatal("PASSIVE did not run")
	}
	if ok || ten.Alive() {
		t.Fatalf("a NOMEM after the fold was skipped: ok=%v alive=%v", ok, ten.Alive())
	}
	var fe *walFoldedError
	if !errors.Is(ten.deadErr, ErrOutOfMemory) || !errors.As(ten.deadErr, &fe) {
		t.Fatalf("unexpected failure: %v", ten.deadErr)
	}
	if met.Checkpoints.Load() != ck0 {
		t.Fatal("the failed checkpoint counted")
	}
	if mf, _, err := fs.GetManifest(context.Background(), "t1"); err != nil || mf.Gen != before.Gen {
		t.Fatalf("manifest moved: %+v %v", mf, err)
	}
	if n := recoverRows(t, m, fs, o); n != 20 {
		t.Fatalf("rows after recovery: %d", n)
	}
}

// slowCkptStore delays checkpoint uploads and manifest CAS calls.
type slowCkptStore struct {
	*FakeStore
	d atomic.Int64
}

func (s *slowCkptStore) PutBase(ctx context.Context, key string, r io.Reader, size int64) error {
	time.Sleep(time.Duration(s.d.Load()))
	return s.FakeStore.PutBase(ctx, key, r, size)
}

func (s *slowCkptStore) PutManifestCAS(ctx context.Context, tid string, mf *Manifest, etag string) (string, error) {
	time.Sleep(time.Duration(s.d.Load()))
	return s.FakeStore.PutManifestCAS(ctx, tid, mf, etag)
}

// Usage accounting charges every checkpoint form to the tenant's
// checkpoint kind: a delta, a checkpoint with nothing new (no upload), and
// a rebase. The pod totals get the same charge.
func TestUsageChargesDeltaCheckpoints(t *testing.T) {
	fs := &slowCkptStore{FakeStore: NewFakeStore()}
	met := &Metrics{}
	o := usageOpts()
	o.DeltaMaxRatio = 1000
	m := NewManager(fs, "fake", "a", "standard", t.TempDir(), o, 1000, met, ControllerOpts{})
	defer m.ShutdownAll()
	ten := newDeltaTenant(t, m, 3)
	const d = 30 * time.Millisecond
	fs.d.Store(int64(d))

	for i, form := range []string{"delta", "empty", "rebase"} {
		switch form {
		case "delta", "rebase":
			mustTx(t, m, "t1", "c", form, insertStmt(int64(10+i), form))
		}
		if form == "rebase" {
			onActor(ten, func() bool { ten.opts.DeltaMaxChain = len(ten.mf.Deltas); return true })
		}
		local, pod := ten.usage.load(), met.usage.totals()
		d0, r0 := met.DeltaCheckpoints.Load(), met.Rebases.Load()
		before := tenantManifest(ten)
		if !checkpointNow(ten) {
			t.Fatalf("%s checkpoint: %v", form, ten.deadErr)
		}
		mf := tenantManifest(ten)
		switch form {
		case "delta":
			expectDelta(t, met, ten, before, d0, r0)
		case "empty":
			if met.DeltaCheckpoints.Load() != d0+1 || len(mf.Deltas) != len(before.Deltas) || mf.BaseKey != before.BaseKey {
				t.Fatalf("empty checkpoint: %+v", mf)
			}
		case "rebase":
			if met.Rebases.Load() != r0+1 || len(mf.Deltas) != 0 || mf.BaseKey == before.BaseKey {
				t.Fatalf("rebase: %+v", mf)
			}
		}
		own := usageDelta(ten.usage.load(), local)
		all := usageDelta(met.usage.totals(), pod)
		// the empty checkpoint uploads nothing but still does its CAS
		if own.Ops[kindCheckpoint] != 1 || own.ExecNs[kindCheckpoint] < int64(d) {
			t.Fatalf("%s checkpoint charged %+v", form, own)
		}
		if all.Ops[kindCheckpoint] != 1 || all.ExecNs[kindCheckpoint] != own.ExecNs[kindCheckpoint] {
			t.Fatalf("%s checkpoint: pod %+v, tenant %+v", form, all, own)
		}
	}
}
