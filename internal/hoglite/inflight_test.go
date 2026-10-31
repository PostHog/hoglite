package hoglite

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The hoglite_inflight_segments gauge (Metrics.InflightSegs) counts the
// segment PUTs that are running. It feeds hoglite_saturation, which the
// autoscaler sums: a gauge that only grows scales the fleet out on load
// that does not exist and never lets it scale back in. These tests drive
// every way a segment PUT can end and check that the gauge falls back to
// its true value (0 when idle) and never goes below zero.

// segGateStore holds each segment PUT while armed until the test sends
// one token on gate.
type segGateStore struct {
	*FakeStore
	armed   atomic.Bool
	entered chan string
	gate    chan struct{}
}

func newSegGateStore() *segGateStore {
	return &segGateStore{FakeStore: NewFakeStore(), entered: make(chan string, 16), gate: make(chan struct{})}
}

func (s *segGateStore) PutSegment(ctx context.Context, key string, data []byte) error {
	if s.armed.Load() {
		select {
		case s.entered <- key:
		default:
		}
		select {
		case <-s.gate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return s.FakeStore.PutSegment(ctx, key, data)
}

// gaugeWatch samples a gauge until stopped and records its minimum.
type gaugeWatch struct {
	min  atomic.Int64
	stop chan struct{}
	wg   sync.WaitGroup
}

func watchGauge(g *atomic.Int64) *gaugeWatch {
	w := &gaugeWatch{stop: make(chan struct{})}
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		for {
			if v := g.Load(); v < w.min.Load() {
				w.min.Store(v)
			}
			select {
			case <-w.stop:
				return
			case <-time.After(200 * time.Microsecond):
			}
		}
	}()
	return w
}

func (w *gaugeWatch) done(t *testing.T, name string) {
	t.Helper()
	close(w.stop)
	w.wg.Wait()
	if m := w.min.Load(); m < 0 {
		t.Fatalf("%s went negative: min %d", name, m)
	}
}

// waitGauge waits until the gauge settles at want; on timeout it reports
// the value it is stuck at.
func waitGauge(t *testing.T, what string, g *atomic.Int64, want int64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for g.Load() != want {
		if time.Now().After(deadline) {
			t.Fatalf("%s: InflightSegs stuck at %d, want %d", what, g.Load(), want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// The confirmed leak: a checkpoint that finds a segment in flight drains
// it (checkpointLocked) instead of the actor loop (onSegDone). Each round
// holds one segment PUT, starts a checkpoint that must wait for it, then
// lets the PUT land. The gauge reads 1 while the PUT runs (the real
// in-flight count is not masked) and 0 once the tenant is idle again.
func TestInflightSegsCheckpointDrainReturnsToZero(t *testing.T) {
	fs := newSegGateStore()
	met := &Metrics{}
	m := NewManager(fs, "fake", "a", "standard", t.TempDir(), deltaTestOpts(), 1000, met, ControllerOpts{})
	defer m.ShutdownAll()
	mustCreate(t, m, "t1")
	makeSchema(t, m, "t1")
	ten := m.LocalTenant("t1")
	waitGauge(t, "after schema", &met.InflightSegs, 0)

	w := watchGauge(&met.InflightSegs)
	ck0 := met.Checkpoints.Load()
	const rounds = 5
	for r := 0; r < rounds; r++ {
		fs.armed.Store(true)
		txDone := make(chan error, 1)
		go func() {
			_, _, err := ten.SubmitTx(TxRequest{Client: "c", TxID: fmt.Sprint("d", r),
				Stmts: []Statement{insertStmt(int64(r+1), "v")}})
			txDone <- err
		}()
		select {
		case <-fs.entered:
		case <-time.After(5 * time.Second):
			t.Fatal("segment PUT never started")
		}
		if n := met.InflightSegs.Load(); n != 1 {
			t.Fatalf("round %d: InflightSegs = %d with one PUT running, want 1", r, n)
		}
		ckDone := make(chan bool, 1)
		go func() { ckDone <- checkpointNow(ten) }()
		time.Sleep(20 * time.Millisecond) // the checkpoint waits in its segment drain
		fs.armed.Store(false)
		fs.gate <- struct{}{}
		if ok := <-ckDone; !ok {
			t.Fatalf("round %d: checkpoint failed: %v", r, ten.deadErr)
		}
		if err := <-txDone; err != nil {
			t.Fatalf("round %d: commit: %v", r, err)
		}
		waitGauge(t, fmt.Sprintf("idle after round %d", r), &met.InflightSegs, 0)
	}
	w.done(t, "InflightSegs")
	if n := met.Checkpoints.Load() - ck0; n != rounds {
		t.Fatalf("%d checkpoints, want %d", n, rounds)
	}
	if n := onActor(ten, func() int { return len(ten.sem) }); n != 0 {
		t.Fatalf("%d inflight slots still held on an idle tenant", n)
	}
}

// The same leak under a bench-like load: concurrent writers on several
// tenants with S3 latency, size-triggered checkpoints that find segments
// in flight, then idle. Saturation must fall back to its latency term
// alone; the inflight term must be 0.
func TestInflightSegsIdleAfterLoadWithCheckpoints(t *testing.T) {
	fs := NewFakeStore()
	fs.Latency = 3 * time.Millisecond
	met := &Metrics{}
	o := testOpts()
	o.CheckpointByte = 32 << 10
	o.MaxSegments = 4
	m := NewManagerSat(fs, "fake", "a", "standard", t.TempDir(), o, 1000, met, ControllerOpts{}, 500e3, 128)
	defer m.ShutdownAll()

	w := watchGauge(&met.InflightSegs)
	const tenants, writers, perWriter = 3, 4, 40
	var wg sync.WaitGroup
	errs := make(chan error, tenants*writers)
	for i := 0; i < tenants; i++ {
		tid := fmt.Sprint("load", i)
		mustCreate(t, m, tid)
		makeSchema(t, m, tid)
		for j := 0; j < writers; j++ {
			wg.Add(1)
			go func(tid string, j int) {
				defer wg.Done()
				for k := 0; k < perWriter; k++ {
					ten, err := m.Acquire(context.Background(), tid, false)
					if err == nil {
						_, _, err = ten.SubmitTx(TxRequest{Client: fmt.Sprint("w", j), TxID: fmt.Sprint(k),
							Stmts: []Statement{insertStmt(int64(j*perWriter+k), strings.Repeat("x", 1500))}})
					}
					if err != nil {
						errs <- fmt.Errorf("%s w%d tx %d: %w", tid, j, k, err)
						return
					}
				}
			}(tid, j)
		}
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if met.Checkpoints.Load() == 0 {
		t.Fatal("the load never checkpointed")
	}
	waitGauge(t, "idle after load", &met.InflightSegs, 0)
	w.done(t, "InflightSegs")
	lat := float64(met.CommitEwmaUs.Load()) / 500e3
	if s := m.Saturation(); s > lat+1e-9 {
		t.Fatalf("idle saturation %.4f above its latency term %.4f: the inflight term leaked", s, lat)
	}
}

// A tenant that dies with segment PUTs in flight never reads their
// outcome: failQueued drops t.segs and the actor is gone. The gauge must
// still fall when those PUTs end, or every poisoned or crashed tenant
// leaves its in-flight count behind for the life of the pod.
func TestInflightSegsDeadTenantReturnsToZero(t *testing.T) {
	for _, tc := range []struct {
		name string
		kill func(m *Manager, ten *Tenant)
	}{
		{"poison", func(m *Manager, ten *Tenant) { ten.Poison(fmt.Errorf("test poison")) }},
		{"crash", func(m *Manager, ten *Tenant) { ten.Crash() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs := newSegGateStore()
			met := &Metrics{}
			m := NewManager(fs, "fake", "a", "standard", t.TempDir(), deltaTestOpts(), 1000, met, ControllerOpts{})
			defer m.ShutdownAll()
			mustCreate(t, m, "t1")
			makeSchema(t, m, "t1")
			ten := m.LocalTenant("t1")
			waitGauge(t, "after schema", &met.InflightSegs, 0)
			w := watchGauge(&met.InflightSegs)

			fs.armed.Store(true)
			const held = 3
			for k := 0; k < held; k++ {
				go func(k int) {
					_, _, _ = ten.SubmitTx(TxRequest{Client: "c", TxID: fmt.Sprint("x", k),
						Stmts: []Statement{insertStmt(int64(k+1), "v")}})
				}(k)
				select {
				case <-fs.entered:
				case <-time.After(5 * time.Second):
					t.Fatal("segment PUT never started")
				}
			}
			if n := met.InflightSegs.Load(); n != held {
				t.Fatalf("InflightSegs = %d with %d PUTs running, want %d", n, held, held)
			}
			tc.kill(m, ten)
			if ten.Alive() {
				t.Fatal("tenant survived the kill")
			}
			// the PUTs still run: the gauge must not drop before they end
			if n := met.InflightSegs.Load(); n != held {
				t.Fatalf("InflightSegs = %d right after the kill with %d PUTs running, want %d", n, held, held)
			}
			fs.armed.Store(false)
			for k := 0; k < held; k++ {
				fs.gate <- struct{}{}
			}
			waitGauge(t, "after the dead tenant's PUTs ended", &met.InflightSegs, 0)
			w.done(t, "InflightSegs")
		})
	}
}

// A checkpoint whose drain gets a failed segment (the lease lapsed while
// the PUT was retrying) poisons the tenant. Both the failed segment and
// any other PUT still running at that moment must leave the gauge.
func TestInflightSegsDrainFailureReturnsToZero(t *testing.T) {
	fs := newSegGateStore()
	met := &Metrics{}
	o := deltaTestOpts()
	m := NewManager(fs, "fake", "a", "standard", t.TempDir(), o, 1000, met, ControllerOpts{})
	defer m.ShutdownAll()
	mustCreate(t, m, "t1")
	makeSchema(t, m, "t1")
	ten := m.LocalTenant("t1")
	waitGauge(t, "after schema", &met.InflightSegs, 0)
	w := watchGauge(&met.InflightSegs)

	fs.armed.Store(true)
	const held = 2
	for k := 0; k < held; k++ {
		go func(k int) {
			_, _, _ = ten.SubmitTx(TxRequest{Client: "c", TxID: fmt.Sprint("f", k),
				Stmts: []Statement{insertStmt(int64(k+1), "v")}})
		}(k)
		select {
		case <-fs.entered:
		case <-time.After(5 * time.Second):
			t.Fatal("segment PUT never started")
		}
	}
	ckDone := make(chan bool, 1)
	go func() { ckDone <- checkpointNow(ten) }()
	time.Sleep(20 * time.Millisecond) // the checkpoint waits in its segment drain
	// heartbeats stop landing: the held PUTs time out at the write fence
	// and give up once the lease is expiring
	m.SetPartitioned(true)
	select {
	case ok := <-ckDone:
		if ok {
			t.Fatal("checkpoint succeeded without its segments")
		}
	case <-time.After(4 * o.Lease):
		t.Fatal("checkpoint drain never saw the failed segment")
	}
	if ten.Alive() {
		t.Fatal("tenant survived a failed drain")
	}
	waitGauge(t, "after a failed drain", &met.InflightSegs, 0)
	w.done(t, "InflightSegs")
}
