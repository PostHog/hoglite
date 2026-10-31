package hoglite

import (
	"context"
	"sync"
	"testing"
	"time"
)

// Durability stalls: a WAL prefix whose segment PUTs neither land nor
// fail, or fail forever, while the owner is otherwise healthy. The
// contract under test is the existing one: a commit is acknowledged only
// once every byte before it is durable, so a prefix that never becomes
// durable means no commit behind it is ever acknowledged, and a reader
// never sees it. A fail-stop during the stall ends every waiting commit in
// an error, and recovery holds a WAL prefix of what was applied, never a
// gap, never a commit twice.

// stallGate holds every segment PUT of a FakeStore. While held, a PUT
// blocks; open lets held and later PUTs proceed, failing (not landing)
// when fail is set; heal makes later PUTs land again.
type stallGate struct {
	mu      sync.Mutex
	gate    chan struct{}
	fail    bool
	entered int
}

func stallSegmentPuts(t *testing.T, fs *FakeStore) *stallGate {
	return stallPuts(t, fs, false)
}

// stallPuts with firstOnly holds only the first segment PUT; every later
// one lands at once.
func stallPuts(t *testing.T, fs *FakeStore, firstOnly bool) *stallGate {
	g := &stallGate{gate: make(chan struct{})}
	fs.FailPutSegment = func(key string) bool {
		g.mu.Lock()
		g.entered++
		if firstOnly && g.entered > 1 {
			g.mu.Unlock()
			return false
		}
		ch := g.gate
		g.mu.Unlock()
		<-ch
		g.mu.Lock()
		defer g.mu.Unlock()
		return g.fail
	}
	t.Cleanup(func() { g.open(false) })
	return g
}

func (g *stallGate) open(fail bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	select {
	case <-g.gate:
	default:
		g.fail = fail
		close(g.gate)
	}
}

func (g *stallGate) heal() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.fail = false
}

func (g *stallGate) puts() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.entered
}

type writeRes struct {
	res []QueryResult
	dup bool
	err error
}

func goWrite(ten *Tenant, txid string, stmts ...Statement) chan writeRes {
	ch := make(chan writeRes, 1)
	go func() {
		res, dup, err := ten.SubmitTx(TxRequest{Client: "c", TxID: txid, Stmts: stmts})
		ch <- writeRes{res, dup, err}
	}()
	return ch
}

func noWriteReply(t *testing.T, what string, ch chan writeRes, d time.Duration) {
	t.Helper()
	select {
	case r := <-ch:
		t.Fatalf("%s answered during the stall: %+v", what, r)
	case <-time.After(d):
	}
}

func recvWrite(t *testing.T, what string, ch chan writeRes, d time.Duration) writeRes {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(d):
		t.Fatalf("%s did not answer within %v", what, d)
	}
	return writeRes{}
}

// appliedPast waits until ten has applied commits past off and returns
// the applied end and the durable watermark.
func appliedPast(t *testing.T, ten *Tenant, off int64) (applied, durable int64) {
	t.Helper()
	waitFor(t, "a commit applied past the durable watermark", func() bool {
		return onActor(ten, func() bool {
			applied, _ = ten.currentCommitEnd()
			durable = ten.durableOff
			return applied > off && applied > durable
		})
	})
	return applied, durable
}

func kvKeys(t *testing.T, m *Manager, tid string) map[int64]string {
	t.Helper()
	out := map[int64]string{}
	for _, r := range mustQuery(t, m, tid, `SELECT k, v FROM kv`).Rows {
		out[r[0].(int64)] = r[1].(string)
	}
	return out
}

// stalledPair applies N then W on a fresh tenant with every segment PUT
// held, and checks that neither is acknowledged, the watermark does not
// move, and an ordinary read of the applied state parks.
func stalledPair(t *testing.T) (fs *FakeStore, m *Manager, ten *Tenant, g *stallGate,
	n, w chan writeRes, read chan readRes, wEnd int64) {
	t.Helper()
	fs = NewFakeStore()
	m = newTestManager(t, fs, "a")
	mustCreate(t, m, "t1")
	makeSchema(t, m, "t1")
	ten, _ = m.Acquire(context.Background(), "t1", false)
	g = stallSegmentPuts(t, fs)
	n = goWrite(ten, "N", insertStmt(1, "N"))
	nEnd, durable := appliedPast(t, ten, 0)
	w = goWrite(ten, "W", insertStmt(2, "W"))
	wEnd, _ = appliedPast(t, ten, nEnd)
	read = goQuery(context.Background(), ten, `SELECT COUNT(*) FROM kv`)
	noWriteReply(t, "N", n, 300*time.Millisecond)
	noWriteReply(t, "W", w, 0)
	noReply(t, "ordinary read", read, 0)
	if d := onActor(ten, func() int64 { return ten.durableOff }); d != durable {
		t.Fatalf("watermark moved during the stall: %d -> %d", durable, d)
	}
	return fs, m, ten, g, n, w, read, wEnd
}

// A stall that ends: both commits are acknowledged, and only once the
// watermark covers them.
func TestStallReleaseAcksOnlyWhenDurable(t *testing.T) {
	_, m, ten, g, n, w, read, wEnd := stalledPair(t)
	g.open(false)
	for _, c := range []struct {
		what string
		ch   chan writeRes
	}{{"N", n}, {"W", w}} {
		if r := recvWrite(t, c.what, c.ch, 5*time.Second); r.err != nil {
			t.Fatalf("%s after the stall: %v", c.what, r.err)
		}
	}
	if d := onActor(ten, func() int64 { return ten.durableOff }); d < wEnd {
		t.Fatalf("W acknowledged at watermark %d, its commit ends at %d", d, wEnd)
	}
	if r := recvRead(t, read, 5*time.Second); r.err != nil || r.res.Rows[0][0].(int64) != 2 {
		t.Fatalf("parked read after the stall: %+v", r)
	}
	if got := countRows(t, m, "t1"); got != 2 {
		t.Fatalf("rows %d, want 2", got)
	}
}

// A permanently poisoned prefix: every PUT fails while the lease stays
// healthy. The owner retries for as long as it holds the lease, so
// nothing behind the prefix is answered at all; once it fail-stops, every
// waiting commit ends in an error.
func TestStallPoisonNeverAcks(t *testing.T) {
	_, m, ten, g, n, w, read, _ := stalledPair(t)
	nEnd := onActor(ten, func() int64 { return ten.durableOff })
	g.open(true)
	waitFor(t, "repeated failed PUTs", func() bool { return g.puts() >= 5 })
	noWriteReply(t, "N on a poisoned prefix", n, time.Second)
	noWriteReply(t, "W on a poisoned prefix", w, 0)
	noReply(t, "ordinary read on a poisoned prefix", read, 0)
	if d := onActor(ten, func() int64 { return ten.durableOff }); d != nEnd {
		t.Fatalf("watermark moved on a poisoned prefix: %d -> %d", nEnd, d)
	}
	crashTenant(t, m, "t1")
	for _, c := range []struct {
		what string
		ch   chan writeRes
	}{{"N", n}, {"W", w}} {
		if r := recvWrite(t, c.what, c.ch, 10*time.Second); r.err == nil {
			t.Fatalf("%s acknowledged on a poisoned prefix: %+v", c.what, r)
		}
	}
	if r := recvRead(t, read, 5*time.Second); r.err == nil {
		t.Fatalf("ordinary read returned non-durable state: %+v", r.res)
	}
}

// Fail-stop during the stall. The dead owner acknowledges neither commit,
// whether its held PUTs then fail or land late (a zombie PUT). Recovery
// holds a WAL prefix of what was applied (W only with N), and retries of
// both txids on the new owner land each commit exactly once.
func TestStallFailStopAcksNothing(t *testing.T) {
	for _, tc := range []struct {
		name  string
		land  bool
		exact bool // recovery must hold nothing (no PUT ever landed)
	}{{"held puts fail", false, true}, {"held puts land late", true, false}} {
		t.Run(tc.name, func(t *testing.T) {
			fs, m, _, g, n, w, read, _ := stalledPair(t)
			crashTenant(t, m, "t1")
			g.open(!tc.land)
			for _, c := range []struct {
				what string
				ch   chan writeRes
			}{{"N", n}, {"W", w}} {
				if r := recvWrite(t, c.what, c.ch, 10*time.Second); r.err == nil {
					t.Fatalf("%s acknowledged by a dead owner: %+v", c.what, r)
				}
			}
			if r := recvRead(t, read, 5*time.Second); r.err == nil {
				t.Fatalf("ordinary read returned non-durable state: %+v", r.res)
			}
			g.heal()

			waitLeaseExpiry()
			mb := newTestManager(t, fs, "b")
			got := kvKeys(t, mb, "t1")
			if _, hasW := got[2]; hasW {
				if _, hasN := got[1]; !hasN {
					t.Fatalf("recovery holds W without N: %v", got)
				}
			}
			if tc.exact && len(got) != 0 {
				t.Fatalf("recovery holds commits whose PUTs never landed: %v", got)
			}
			mustTx(t, mb, "t1", "c", "N", insertStmt(1, "N"))
			mustTx(t, mb, "t1", "c", "W", insertStmt(2, "W"))
			if final := kvKeys(t, mb, "t1"); len(final) != 2 || final[1] != "N" || final[2] != "W" {
				t.Fatalf("after retries: %v, want each commit exactly once", final)
			}
			t.Logf("recovered before retries: %v", got)
		})
	}
}

// Segments complete out of order: N's PUT hangs, W's (a later segment)
// lands. W is durable on its own but must not be acknowledged while a
// byte before it is not: acks follow WAL order, not PUT completion.
func TestStallLaterSegmentLandsFirst(t *testing.T) {
	fs := NewFakeStore()
	m := newTestManager(t, fs, "a")
	mustCreate(t, m, "t1")
	makeSchema(t, m, "t1")
	ten, _ := m.Acquire(context.Background(), "t1", false)
	g := stallPuts(t, fs, true)
	n := goWrite(ten, "N", insertStmt(1, "N"))
	nEnd, durable := appliedPast(t, ten, 0)
	waitFor(t, "N's segment PUT in flight", func() bool { return g.puts() >= 1 })
	w := goWrite(ten, "W", insertStmt(2, "W"))
	appliedPast(t, ten, nEnd)
	waitFor(t, "W's segment PUT landed", func() bool { return g.puts() >= 2 })
	noWriteReply(t, "W behind a held earlier segment", w, 300*time.Millisecond)
	noWriteReply(t, "N", n, 0)
	if d := onActor(ten, func() int64 { return ten.durableOff }); d != durable {
		t.Fatalf("watermark moved past a held segment: %d -> %d", durable, d)
	}
	g.open(false)
	for _, c := range []struct {
		what string
		ch   chan writeRes
	}{{"N", n}, {"W", w}} {
		if r := recvWrite(t, c.what, c.ch, 5*time.Second); r.err != nil {
			t.Fatalf("%s after release: %v", c.what, r.err)
		}
	}
	if got := countRows(t, m, "t1"); got != 2 {
		t.Fatalf("rows %d, want 2", got)
	}
}

// The owner's lease lapses while its prefix is poisoned (a partition: no
// kill, heartbeats just stop landing). The PUT loop gives up at the lease
// fence; that failure must end the incarnation, not count as durable.
func TestStallPoisonThenLeaseLapse(t *testing.T) {
	_, m, _, g, n, w, read, _ := stalledPair(t)
	m.SetPartitioned(true)
	t.Cleanup(func() { m.SetPartitioned(false) })
	g.open(true)
	for _, c := range []struct {
		what string
		ch   chan writeRes
	}{{"N", n}, {"W", w}} {
		if r := recvWrite(t, c.what, c.ch, 3*(testOpts().Lease+testOpts().SkewMargin)); r.err == nil {
			t.Fatalf("%s acknowledged after its PUT gave up: %+v", c.what, r)
		}
	}
	if r := recvRead(t, read, 5*time.Second); r.err == nil {
		t.Fatalf("ordinary read returned non-durable state: %+v", r.res)
	}
}
