package hoglite

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// Tests for the bounds on read results that wait for durability
// (reads.go). A stalled segment PUT keeps every new read over the
// unshipped commit parked.

// parkSQL returns the row count and a 1000-byte blob. Its result estimate
// is 1202 bytes: 64 + 2 x (24 + 1) for the result and its column names,
// 24 + 2 x 32 + 1000 for the row.
const (
	parkSQL   = `SELECT count(*) AS n, zeroblob(1000) AS b FROM kv`
	parkBytes = 1202
)

// parkTenants opens tenants with the kv schema and one durable row each.
func parkTenants(t *testing.T, o TenantOpts, tids ...string) (*FakeStore, *Metrics, []*Tenant) {
	t.Helper()
	fs := NewFakeStore()
	met := &Metrics{}
	m := newReadTestManager(t, fs, o, met)
	t.Cleanup(m.ShutdownAll)
	var tens []*Tenant
	for _, tid := range tids {
		ten, err := m.Acquire(context.Background(), tid, true)
		if err != nil {
			t.Fatal(err)
		}
		makeSchema(t, m, tid)
		mustTx(t, m, tid, "c", "d1", insertStmt(1, "durable"))
		tens = append(tens, ten)
	}
	return fs, met, tens
}

// stallSegments blocks every segment PUT until the returned func runs.
func stallSegments(t *testing.T, fs *FakeStore) (release func()) {
	gate := make(chan struct{})
	var once sync.Once
	release = func() { once.Do(func() { close(gate) }) }
	t.Cleanup(release)
	fs.FailPutSegment = func(string) bool { <-gate; return false }
	return release
}

// unshippedWrite commits row k whose segment PUT stalls, and waits until
// the commit is applied but not durable.
func unshippedWrite(t *testing.T, ten *Tenant, k int64) chan error {
	t.Helper()
	ch := make(chan error, 1)
	go func() {
		_, _, err := ten.SubmitTx(TxRequest{Client: "c", TxID: fmt.Sprint("u", k), Stmts: []Statement{insertStmt(k, "unshipped")}})
		ch <- err
	}()
	waitFor(t, "unshipped write", func() bool {
		return onActor(ten, func() bool { end, _ := ten.currentCommitEnd(); return end > ten.durableOff })
	})
	return ch
}

// goPark submits parkSQL as a /query or as a read-only /tx.
func goPark(ctx context.Context, ten *Tenant, tx bool) chan readRes {
	if !tx {
		return goQuery(ctx, ten, parkSQL)
	}
	ch := make(chan readRes, 1)
	go func() {
		res, _, err := ten.SubmitTxCtx(ctx, TxRequest{Client: "c", TxID: "ro", Stmts: []Statement{{SQL: parkSQL}}})
		if err != nil {
			ch <- readRes{err: err}
			return
		}
		ch <- readRes{res: res[0]}
	}()
	return ch
}

func parkedCount(ten *Tenant) int {
	return onActor(ten, func() int { return len(ten.readWaits) })
}

// wantCount checks a successful parkSQL result.
func wantCount(t *testing.T, what string, r readRes, n int64) {
	t.Helper()
	if r.err != nil {
		t.Fatalf("%s: %v", what, r.err)
	}
	if got := r.res.Rows[0][0].(int64); got != n {
		t.Fatalf("%s: count %d, want %d", what, got, n)
	}
}

// noReply checks that ch stays empty for d.
func noReply(t *testing.T, what string, ch chan readRes, d time.Duration) {
	t.Helper()
	select {
	case r := <-ch:
		t.Fatalf("%s returned during the stall: %+v", what, r)
	case <-time.After(d):
	}
}

// allReleased waits until no slot, parked result or budget share is left.
func allReleased(t *testing.T, met *Metrics, tens ...*Tenant) {
	t.Helper()
	waitFor(t, "slots and park budget returned", func() bool {
		for _, ten := range tens {
			if len(ten.readSlots) != 0 || ten.readers.count() != 0 {
				return false
			}
			if ten.Alive() && parkedCount(ten) != 0 {
				return false
			}
		}
		return met.ParkedReads.Load() == 0 && met.ParkedBytes.Load() == 0 &&
			met.ParkWaiting.Load() == 0 && met.QueriesInflight.Load() == 0
	})
}

// While a segment PUT stalls, a tenant parks at most MaxReaders results.
// Further reads wait for a reader slot without executing, and end with
// their context. After the stall all waiting reads return the durable
// state.
func TestParkedReadsBoundedPerTenant(t *testing.T) {
	for _, tx := range []bool{false, true} {
		t.Run(map[bool]string{false: "query", true: "readonly-tx"}[tx], func(t *testing.T) {
			o := testOpts()
			o.MaxReaders = 2
			fs, met, tens := parkTenants(t, o, "t1")
			ten := tens[0]
			release := stallSegments(t, fs)
			wr := unshippedWrite(t, ten, 2)

			r1, r2 := goPark(context.Background(), ten, tx), goPark(context.Background(), ten, tx)
			waitFor(t, "two parked results", func() bool { return parkedCount(ten) == 2 })
			if met.ParkedReads.Load() != 2 || met.ParkedBytes.Load() != 2*parkBytes {
				t.Fatalf("gauges: parked %d, bytes %d", met.ParkedReads.Load(), met.ParkedBytes.Load())
			}
			// parked results keep their tenant slots, not their pod slots
			if len(ten.readSlots) != 2 || met.QueriesInflight.Load() != 0 {
				t.Fatalf("slots: tenant %d, pod %d", len(ten.readSlots), met.QueriesInflight.Load())
			}

			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer cancel()
			r3 := goPark(ctx, ten, tx)
			r4, r5 := goPark(context.Background(), ten, tx), goPark(context.Background(), ten, tx)
			if r := recvRead(t, r3, 3*time.Second); !errors.Is(r.err, context.DeadlineExceeded) {
				t.Fatalf("read past its deadline: %+v", r)
			}
			noReply(t, "waiting read", r4, 200*time.Millisecond)
			// the extra reads did not execute: no snapshot, no result
			if n := parkedCount(ten); n != 2 {
				t.Fatalf("%d parked results, want at most MaxReaders=2", n)
			}
			if ten.readers.count() != 0 || met.ParkedReads.Load() != 2 || met.ParkedBytes.Load() != 2*parkBytes {
				t.Fatalf("extra reads ran: readers %d, parked %d, bytes %d",
					ten.readers.count(), met.ParkedReads.Load(), met.ParkedBytes.Load())
			}

			release()
			if err := <-wr; err != nil {
				t.Fatal(err)
			}
			for i, ch := range []chan readRes{r1, r2, r4, r5} {
				wantCount(t, fmt.Sprint("read ", i), recvRead(t, ch, 5*time.Second), 2)
			}
			allReleased(t, met, ten)
		})
	}
}

// A caller that gives up while its result is parked drops the result and
// frees its tenant slot and budget share during the stall. The next read
// then parks and returns the durable state after the stall.
func TestParkedReadCancelDropsResult(t *testing.T) {
	for _, tx := range []bool{false, true} {
		t.Run(map[bool]string{false: "query", true: "readonly-tx"}[tx], func(t *testing.T) {
			o := testOpts()
			o.MaxReaders = 2
			fs, met, tens := parkTenants(t, o, "t1")
			ten := tens[0]
			release := stallSegments(t, fs)
			wr := unshippedWrite(t, ten, 2)

			ctx1, cancel1 := context.WithCancel(context.Background())
			ctx2, cancel2 := context.WithCancel(context.Background())
			r1, r2 := goPark(ctx1, ten, tx), goPark(ctx2, ten, tx)
			waitFor(t, "two parked results", func() bool { return parkedCount(ten) == 2 })
			cancel1()
			cancel2()
			for _, ch := range []chan readRes{r1, r2} {
				if r := recvRead(t, ch, 3*time.Second); !errors.Is(r.err, context.Canceled) {
					t.Fatalf("canceled read: %+v", r)
				}
			}
			// still stalled: the drop does not wait for durability
			allReleased(t, met, ten)
			select {
			case err := <-wr:
				t.Fatalf("write acked during the stall: %v", err)
			default:
			}

			r3 := goPark(context.Background(), ten, tx)
			waitFor(t, "new parked result", func() bool { return parkedCount(ten) == 1 })
			release()
			if err := <-wr; err != nil {
				t.Fatal(err)
			}
			wantCount(t, "read after the stall", recvRead(t, r3, 5*time.Second), 2)
			allReleased(t, met, ten)
		})
	}
}

// The pod park budget bounds parked results across tenants. A finished
// read that does not fit waits with its pod slot, and its caller can give
// up. Reads with a durable basis need no budget and still run. After the
// stall every waiting read returns the durable state.
func TestParkBudgetPodWide(t *testing.T) {
	o := testOpts()
	o.PodMaxQueries = 8
	o.PodMaxParkedBytes = 2*parkBytes + 100 // two results fit
	fs, met, tens := parkTenants(t, o, "t1", "t2", "t3")
	t1, t2, t3 := tens[0], tens[1], tens[2]
	release := stallSegments(t, fs)
	wr1, wr2 := unshippedWrite(t, t1, 2), unshippedWrite(t, t2, 2)

	a, b := goPark(context.Background(), t1, false), goPark(context.Background(), t2, false)
	waitFor(t, "one parked result per tenant", func() bool { return parkedCount(t1) == 1 && parkedCount(t2) == 1 })
	if met.ParkedBytes.Load() != 2*parkBytes {
		t.Fatalf("park budget in use: %d", met.ParkedBytes.Load())
	}

	// t1 has free reader slots, but the budget is full
	c := goPark(context.Background(), t1, false)
	waitFor(t, "read waiting for budget", func() bool { return met.ParkWaiting.Load() == 1 })
	if met.QueriesInflight.Load() != 1 || parkedCount(t1) != 1 || met.ParkedBytes.Load() != 2*parkBytes {
		t.Fatalf("waiting read: pod slots %d, parked %d, bytes %d",
			met.QueriesInflight.Load(), parkedCount(t1), met.ParkedBytes.Load())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	d := goPark(ctx, t2, false)
	if r := recvRead(t, d, 3*time.Second); !errors.Is(r.err, context.DeadlineExceeded) {
		t.Fatalf("read past its deadline: %+v", r)
	}
	waitFor(t, "timed-out waiter gone", func() bool {
		return met.ParkWaiting.Load() == 1 && met.QueriesInflight.Load() == 1 && len(t2.readSlots) == 1
	})

	// a durable basis needs no budget
	rctx, rcancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer rcancel()
	wantCount(t, "durable read", recvRead(t, goPark(rctx, t3, false), 3*time.Second), 1)
	noReply(t, "budget waiter", c, 100*time.Millisecond)

	release()
	for _, wr := range []chan error{wr1, wr2} {
		if err := <-wr; err != nil {
			t.Fatal(err)
		}
	}
	for i, ch := range []chan readRes{a, b, c} {
		wantCount(t, fmt.Sprint("read ", i), recvRead(t, ch, 5*time.Second), 2)
	}
	allReleased(t, met, t1, t2, t3)
}

// A crash fails parked results and reads that wait for budget, and
// returns all their slots and budget shares.
func TestParkedReadsReleasedOnCrash(t *testing.T) {
	o := testOpts()
	o.MaxReaders = 3
	o.PodMaxParkedBytes = 2*parkBytes + 100
	fs, met, tens := parkTenants(t, o, "t1")
	ten := tens[0]
	release := stallSegments(t, fs)
	wr := unshippedWrite(t, ten, 2)

	r1, r2 := goPark(context.Background(), ten, false), goPark(context.Background(), ten, true)
	waitFor(t, "two parked results", func() bool { return parkedCount(ten) == 2 })
	r3 := goPark(context.Background(), ten, false)
	waitFor(t, "read waiting for budget", func() bool { return met.ParkWaiting.Load() == 1 })

	ten.Crash()
	for i, ch := range []chan readRes{r1, r2, r3} {
		if r := recvRead(t, ch, 3*time.Second); r.err == nil {
			t.Fatalf("read %d over a lost basis succeeded: %+v", i, r.res)
		}
	}
	allReleased(t, met, ten)
	if met.ParkedReads.Load() != 0 {
		t.Fatalf("parked reads after crash: %d", met.ParkedReads.Load())
	}
	release()
	if err := <-wr; err == nil {
		t.Fatal("write acked after crash")
	}
}

// The park budget grants shares in FIFO order, cuts a share above the
// budget to the budget, and gives a canceled waiter's place to the next.
func TestParkBudget(t *testing.T) {
	met := &Metrics{}
	l := newQueryLimiter(TenantOpts{PodMaxQueries: 4, PodMaxParkedBytes: 100}, met)
	bg, done := context.Background(), make(chan struct{})
	reserve := func(ctx context.Context, n int64) chan int64 {
		ch := make(chan int64, 1)
		go func() {
			got, err := l.reservePark(ctx, done, n)
			if err != nil {
				got = -1
			}
			ch <- got
		}()
		return ch
	}
	get := func(ch chan int64) int64 {
		t.Helper()
		select {
		case n := <-ch:
			return n
		case <-time.After(3 * time.Second):
			t.Fatal("reservation did not return")
		}
		return 0
	}
	if n := get(reserve(bg, 60)); n != 60 {
		t.Fatalf("first share: %d", n)
	}
	// 50 does not fit; 10 would, but must not pass the waiter
	ctx, cancel := context.WithCancel(bg)
	big := reserve(ctx, 50)
	waitFor(t, "waiter", func() bool { return met.ParkWaiting.Load() == 1 })
	small := reserve(bg, 10)
	waitFor(t, "second waiter", func() bool { return met.ParkWaiting.Load() == 2 })
	// the head gives up: the next waiter fits now
	cancel()
	if n := get(big); n != -1 {
		t.Fatalf("canceled waiter got %d", n)
	}
	if n := get(small); n != 10 {
		t.Fatalf("share behind a canceled waiter: %d", n)
	}
	l.releasePark(60)
	// above the budget: cut to 100, granted once the budget is empty
	huge := reserve(bg, 1000)
	waitFor(t, "huge waiter", func() bool { return met.ParkWaiting.Load() == 1 })
	l.releasePark(10)
	if n := get(huge); n != 100 {
		t.Fatalf("cut share: %d", n)
	}
	// a closed tenant ends the wait
	closed := reserve(bg, 1)
	waitFor(t, "closed waiter", func() bool { return met.ParkWaiting.Load() == 1 })
	close(done)
	if n := get(closed); n != -1 {
		t.Fatalf("waiter of a closed tenant got %d", n)
	}
	l.releasePark(100)
	if met.ParkedBytes.Load() != 0 || met.ParkWaiting.Load() != 0 || l.parkUsed != 0 || len(l.parkQ) != 0 {
		t.Fatalf("budget left: gauge %d, used %d, waiting %d/%d",
			met.ParkedBytes.Load(), l.parkUsed, met.ParkWaiting.Load(), len(l.parkQ))
	}
	// defaults: n x MaxResultBytes; negative turns the budget off
	if got := podParkBudget(TenantOpts{}, 8); got != 8*defaultMaxResultBytes {
		t.Fatalf("default budget %d", got)
	}
	if got := podParkBudget(TenantOpts{MaxResultBytes: 1 << 20}, 8); got != 8<<20 {
		t.Fatalf("budget from MaxResultBytes %d", got)
	}
	if got := podParkBudget(TenantOpts{PodMaxParkedBytes: -1}, 8); got != 0 {
		t.Fatalf("budget off: %d", got)
	}
}
