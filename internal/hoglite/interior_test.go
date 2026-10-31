package hoglite

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// Tests for interior reads (reads.go, "Interior reads").
//
// The model is an OCC counter: ctr.v is the state a caller reads, and a
// write decided on the read guards that v still has the value it read
// (ExpectScalarInt), bumps it, and records the decision in kv. This is
// the shape of a hoglake commit: plan on a read, then one guarded write.

func interiorOpts(o *TenantOpts) { o.InteriorReads = true }

// ctrSchema creates the counter at v=0, durable.
func ctrSchema(t *testing.T, m *Manager, tid string) {
	t.Helper()
	makeSchema(t, m, tid)
	mustTx(t, m, tid, "c", "ctr",
		Statement{SQL: `CREATE TABLE ctr(id INTEGER PRIMARY KEY, v INTEGER NOT NULL)`},
		Statement{SQL: `INSERT INTO ctr(id, v) VALUES(1, 0)`})
}

const ctrRead = `SELECT v FROM ctr WHERE id = 1`

// bumpReq is the write decided on a read that saw v=seen: guard v, bump
// it, record the decision in kv under key seen+1.
func bumpReq(txid string, seen int64, what, incarnation string) TxRequest {
	return TxRequest{Client: "c", TxID: txid, Incarnation: incarnation, Stmts: []Statement{
		{SQL: ctrRead, ExpectScalarInt: &seen},
		{SQL: `UPDATE ctr SET v = v + 1 WHERE id = 1`},
		insertStmt(seen+1, what),
	}}
}

type txRes struct {
	res []QueryResult
	dup bool
	err error
}

func goTx(ten *Tenant, req TxRequest) chan txRes {
	ch := make(chan txRes, 1)
	go func() {
		res, dup, err := ten.SubmitTx(req)
		ch <- txRes{res, dup, err}
	}()
	return ch
}

// interiorRead takes an interior read of ctr.v and requires it to return
// within d.
func interiorRead(t *testing.T, ten *Tenant, d time.Duration) (int64, string) {
	t.Helper()
	type r struct {
		res []QueryResult
		inc string
		err error
	}
	ch := make(chan r, 1)
	go func() {
		res, inc, err := ten.SubmitInteriorReadCtx(context.Background(),
			TxRequest{Client: "c", TxID: "read:i", Interior: true, Stmts: []Statement{{SQL: ctrRead}}})
		ch <- r{res, inc, err}
	}()
	select {
	case got := <-ch:
		if got.err != nil {
			t.Fatalf("interior read: %v", got.err)
		}
		return got.res[0].Rows[0][0].(int64), got.inc
	case <-time.After(d):
		t.Fatalf("interior read did not return within %v", d)
	}
	return 0, ""
}

// interiorGate stalls segment PUTs. While held, a PUT blocks; release lets
// the held and later PUTs land, fail makes them fail without landing.
type interiorGate struct {
	mu      sync.Mutex
	gate    chan struct{}
	fail    bool
	entered []string
}

func holdSegments(t *testing.T, fs *FakeStore) *interiorGate {
	g := &interiorGate{gate: make(chan struct{})}
	fs.FailPutSegment = func(key string) bool {
		g.mu.Lock()
		g.entered = append(g.entered, key)
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

func (g *interiorGate) open(fail bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	select {
	case <-g.gate:
	default:
		g.fail = fail
		close(g.gate)
	}
}

// heal lets every later PUT land again: the store is back for the next
// owner, after the dead one's held PUTs have failed.
func (g *interiorGate) heal() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.fail = false
}

func (g *interiorGate) keys() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]string(nil), g.entered...)
}

// appliedNotDurable waits until ten has applied commits past its durable
// watermark and returns both offsets.
func appliedNotDurable(t *testing.T, ten *Tenant) (applied, durable int64) {
	t.Helper()
	waitFor(t, "a commit applied but not durable", func() bool {
		return onActor(ten, func() bool {
			applied, _ = ten.currentCommitEnd()
			durable = ten.durableOff
			return applied > durable
		})
	})
	return applied, durable
}

func noTxReply(t *testing.T, what string, ch chan txRes, d time.Duration) {
	t.Helper()
	select {
	case r := <-ch:
		t.Fatalf("%s answered during the stall: %+v", what, r)
	case <-time.After(d):
	}
}

func recvTx(t *testing.T, what string, ch chan txRes, d time.Duration) txRes {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(d):
		t.Fatalf("%s did not answer within %v", what, d)
	}
	return txRes{}
}

func ctrValue(t *testing.T, m *Manager, tid string) int64 {
	t.Helper()
	return mustQuery(t, m, tid, ctrRead).Rows[0][0].(int64)
}

func kvValues(t *testing.T, m *Manager, tid string) map[int64]string {
	t.Helper()
	out := map[int64]string{}
	for _, r := range mustQuery(t, m, tid, `SELECT k, v FROM kv`).Rows {
		out[r[0].(int64)] = r[1].(string)
	}
	return out
}

// The ordering claim, end to end. Commit N is applied but not durable. An
// interior read sees it, and W is decided on that read (guard v==1) and
// fenced to the reader's incarnation. Then the owner fail-stops and N's
// segment never lands.
//
//   - W is never acknowledged: its caller gets an error, not a result.
//   - Recovery has neither N nor W: the outcome a reader can observe does
//     not depend on the lost commit.
//   - A retry of W (same txid, same token) on the new owner is refused as
//     not applied, also once a different commit M' has brought v back to
//     1, so that W's guard alone would pass (ABA on the counter).
//   - Control: the same W without the fence DOES apply against M', which
//     is the hole the fence closes.
func TestInteriorReadWriteNotAckedWhenPrefixLost(t *testing.T) {
	fs := NewFakeStore()
	ma, meta := newTestManagerOpts(t, fs, "a", interiorOpts)
	mustCreate(t, ma, "t1")
	ctrSchema(t, ma, "t1")
	ten, _ := ma.Acquire(context.Background(), "t1", false)

	g := holdSegments(t, fs)
	n := goTx(ten, bumpReq("N", 0, "N", ""))
	nEnd, durableBefore := appliedNotDurable(t, ten)

	// the interior read sees N at once; an ordinary read of the same state
	// parks behind it
	parkedBefore := meta.ParkedReadsTotal.Load()
	seen, inc := interiorRead(t, ten, 2*time.Second)
	if seen != 1 {
		t.Fatalf("interior read saw v=%d, want 1 (N applied)", seen)
	}
	if inc == "" || inc != ten.incarnation {
		t.Fatalf("incarnation token %q, want %q", inc, ten.incarnation)
	}
	if meta.InteriorReads.Load() != 1 || meta.ParkedReadsTotal.Load() != parkedBefore {
		t.Fatalf("interior read: interior %d, parked %d->%d", meta.InteriorReads.Load(),
			parkedBefore, meta.ParkedReadsTotal.Load())
	}
	plain := goQuery(context.Background(), ten, ctrRead)

	// W decided on the interior read, fenced to its incarnation: applied
	// after N, so its ack waits for N
	w := goTx(ten, bumpReq("W", seen, "W decided on N", inc))
	waitFor(t, "W applied", func() bool {
		return onActor(ten, func() bool { end, _ := ten.currentCommitEnd(); return end > nEnd })
	})
	noTxReply(t, "W", w, 300*time.Millisecond)
	noTxReply(t, "N", n, 0)
	noReply(t, "ordinary read", plain, 0)
	if d := onActor(ten, func() int64 { return ten.durableOff }); d != durableBefore {
		t.Fatalf("watermark moved during the stall: %d -> %d", durableBefore, d)
	}

	// fail-stop: the pod dies, and the held segments fail without landing
	crashTenant(t, ma, "t1")
	g.open(true)
	if r := recvTx(t, "W", w, 5*time.Second); r.err == nil {
		t.Fatalf("W was acknowledged although N was lost: %+v", r)
	}
	if r := recvTx(t, "N", n, 5*time.Second); r.err == nil {
		t.Fatalf("N was acknowledged although it was lost: %+v", r)
	}
	if r := recvRead(t, plain, 5*time.Second); r.err == nil {
		t.Fatalf("ordinary read returned non-durable state: %+v", r.res)
	}
	g.heal()

	waitLeaseExpiry()
	mb, metb := newTestManagerOpts(t, fs, "b", interiorOpts)
	if v, kv := ctrValue(t, mb, "t1"), kvValues(t, mb, "t1"); v != 0 || len(kv) != 0 {
		t.Fatalf("recovery holds lost state: v=%d kv=%v", v, kv)
	}
	tb, _ := mb.Acquire(context.Background(), "t1", false)
	if tb.incarnation == inc {
		t.Fatal("new owner reuses the incarnation token")
	}

	// the retry of W is refused, not applied
	if _, _, err := tb.SubmitTx(bumpReq("W", seen, "W decided on N", inc)); !errors.Is(err, errIncarnationChanged) {
		t.Fatalf("retry of W on the new owner: %v, want errIncarnationChanged", err)
	}
	if code, ae := txErrorResponse(errIncarnationChanged, false, "b"); code != 412 || ae.Code != CodeIncarnationChanged {
		t.Fatalf("wire answer %d %+v", code, ae)
	}

	// ABA: M' brings v back to 1 with different content
	mustTx(t, mb, "t1", "c", "M'", bumpReq("M'", 0, "M'", "").Stmts...)
	if _, _, err := tb.SubmitTx(bumpReq("W", seen, "W decided on N", inc)); !errors.Is(err, errIncarnationChanged) {
		t.Fatalf("retry of W after M': %v, want errIncarnationChanged", err)
	}
	if metb.IncarnationFenced.Load() != 2 {
		t.Fatalf("fenced count %d, want 2", metb.IncarnationFenced.Load())
	}
	if v, kv := ctrValue(t, mb, "t1"), kvValues(t, mb, "t1"); v != 1 || len(kv) != 1 || kv[1] != "M'" {
		t.Fatalf("after M': v=%d kv=%v", v, kv)
	}

	// control: unfenced, W's guard passes against M' and W applies on a
	// state it was never decided on
	if _, _, err := tb.SubmitTx(bumpReq("W", seen, "W decided on N", "")); err != nil {
		t.Fatalf("control: unfenced W should pass its guard by ABA: %v", err)
	}
	if kv := kvValues(t, mb, "t1"); kv[2] != "W decided on N" {
		t.Fatalf("control: kv=%v", kv)
	}
}

// When the lost-looking prefix did land (the PUTs reached S3 after the
// owner died), the new owner replays N and W together, and a retry of W
// with the old token answers with W's receipt: the fence never refuses a
// durable commit, so the retry cannot apply it twice or report it lost.
func TestInteriorFenceAnswersReplayedCommit(t *testing.T) {
	fs := NewFakeStore()
	ma, _ := newTestManagerOpts(t, fs, "a", interiorOpts)
	mustCreate(t, ma, "t1")
	ctrSchema(t, ma, "t1")
	ten, _ := ma.Acquire(context.Background(), "t1", false)

	g := holdSegments(t, fs)
	n := goTx(ten, bumpReq("N", 0, "N", ""))
	nEnd, _ := appliedNotDurable(t, ten)
	seen, inc := interiorRead(t, ten, 2*time.Second)
	w := goTx(ten, bumpReq("W", seen, "W decided on N", inc))
	waitFor(t, "W applied", func() bool {
		return onActor(ten, func() bool { end, _ := ten.currentCommitEnd(); return end > nEnd })
	})
	// wait until W's bytes are in a held PUT too
	waitFor(t, "W's segment cut", func() bool {
		return onActor(ten, func() bool { end, _ := ten.currentCommitEnd(); return ten.submittedOff == end })
	})
	crashTenant(t, ma, "t1")
	if r := recvTx(t, "W", w, 5*time.Second); r.err == nil {
		t.Fatalf("W acknowledged by a dead owner: %+v", r)
	}
	recvTx(t, "N", n, 5*time.Second)
	g.open(false) // the zombie PUTs land
	waitFor(t, "held segments landed", func() bool {
		keys := g.keys()
		if len(keys) == 0 {
			return false
		}
		fs.mu.Lock()
		defer fs.mu.Unlock()
		for _, k := range keys {
			if _, ok := fs.objects[k]; !ok {
				return false
			}
		}
		return true
	})

	waitLeaseExpiry()
	mb, metb := newTestManagerOpts(t, fs, "b", interiorOpts)
	if v, kv := ctrValue(t, mb, "t1"), kvValues(t, mb, "t1"); v != 2 || kv[1] != "N" || kv[2] != "W decided on N" {
		t.Fatalf("replay: v=%d kv=%v, want N and W", v, kv)
	}
	tb, _ := mb.Acquire(context.Background(), "t1", false)
	res, dup, err := tb.SubmitTx(bumpReq("W", seen, "W decided on N", inc))
	if err != nil || !dup || len(res) != 3 {
		t.Fatalf("retry of a replayed W: res=%v dup=%v err=%v, want its receipt", res, dup, err)
	}
	if metb.IncarnationFenced.Load() != 0 {
		t.Fatalf("a durable W was fenced")
	}
	if v := ctrValue(t, mb, "t1"); v != 2 {
		t.Fatalf("W applied twice: v=%d", v)
	}
}

// A prefix that never becomes durable: W, decided on an interior read of
// it, is never acknowledged. Two shapes of permanent stall:
//
//   - stall: the PUT neither lands nor fails while the lease holds. W
//     waits as long as the stall lasts (interior reads keep answering
//     meanwhile); once the prefix lands, W is acknowledged only after
//     N's bytes are durable.
//   - poison: every PUT of the prefix fails (S3 refuses it) while the
//     lease stays healthy. The owner retries the PUT for as long as it
//     holds the lease, so W is never answered at all — not acknowledged,
//     the watermark never passes N — and once the owner fail-stops, W
//     ends with an error, never a result.
func TestInteriorReadStalledPrefixNeverAcks(t *testing.T) {
	t.Run("stall", func(t *testing.T) {
		fs := NewFakeStore()
		m, _ := newTestManagerOpts(t, fs, "a", interiorOpts)
		mustCreate(t, m, "t1")
		ctrSchema(t, m, "t1")
		ten, _ := m.Acquire(context.Background(), "t1", false)
		g := holdSegments(t, fs)
		n := goTx(ten, bumpReq("N", 0, "N", ""))
		nEnd, _ := appliedNotDurable(t, ten)
		seen, inc := interiorRead(t, ten, 2*time.Second)

		// W's ack must find N durable: record the watermark W is acked at
		var ackedAt int64 = -1
		w := make(chan txRes, 1)
		go func() {
			res, dup, err := ten.SubmitTx(bumpReq("W", seen, "W", inc))
			if err == nil {
				ackedAt = onActor(ten, func() int64 { return ten.durableOff })
			}
			w <- txRes{res, dup, err}
		}()
		for i := 0; i < 5; i++ {
			noTxReply(t, "W", w, 200*time.Millisecond)
			if v, _ := interiorRead(t, ten, time.Second); v != 2 {
				t.Fatalf("interior read during the stall saw v=%d, want 2", v)
			}
		}
		g.open(false)
		if r := recvTx(t, "N", n, 5*time.Second); r.err != nil {
			t.Fatal(r.err)
		}
		if r := recvTx(t, "W", w, 5*time.Second); r.err != nil {
			t.Fatal(r.err)
		}
		if ackedAt <= nEnd {
			t.Fatalf("W acknowledged at watermark %d, N ends at %d", ackedAt, nEnd)
		}
	})
	t.Run("poison", func(t *testing.T) {
		fs := NewFakeStore()
		m, _ := newTestManagerOpts(t, fs, "a", interiorOpts)
		mustCreate(t, m, "t1")
		ctrSchema(t, m, "t1")
		ten, _ := m.Acquire(context.Background(), "t1", false)
		g := holdSegments(t, fs)
		n := goTx(ten, bumpReq("N", 0, "N", ""))
		nEnd, _ := appliedNotDurable(t, ten)
		seen, inc := interiorRead(t, ten, 2*time.Second)
		w := goTx(ten, bumpReq("W", seen, "W", inc))
		noTxReply(t, "W", w, 200*time.Millisecond)
		g.open(true) // every PUT from now on fails
		// poisoned for long enough that the PUT has failed and been retried
		// many times over (50..800ms backoff): still no answer for W or N
		waitFor(t, "repeated failed PUTs", func() bool { return len(g.keys()) >= 5 })
		noTxReply(t, "W on a poisoned prefix", w, time.Second)
		noTxReply(t, "N on a poisoned prefix", n, 0)
		if d := onActor(ten, func() int64 { return ten.durableOff }); d >= nEnd {
			t.Fatalf("watermark %d passed N's end %d on a poisoned prefix", d, nEnd)
		}
		// fail-stop: the owner gives up; W and N end in errors
		crashTenant(t, m, "t1")
		r := recvTx(t, "W", w, 10*time.Second)
		if r.err == nil {
			t.Fatalf("W acknowledged on a poisoned prefix: %+v", r)
		}
		if r := recvTx(t, "N", n, 5*time.Second); r.err == nil {
			t.Fatalf("N acknowledged on a poisoned prefix")
		}
		waitFor(t, "the tenant dead after fail-stop", func() bool { return !ten.Alive() })
	})
}

// Regression against over-application: with interior reads on, every
// read that does not opt in through the interior entry point still parks
// (/query, read-only /tx), and the opt-in cannot ride the ordinary path.
// With the engine option off, an interior request parks too and gets no
// token, so a fenced write never follows a non-durable read.
func TestNonInteriorReadsStillPark(t *testing.T) {
	fs := NewFakeStore()
	m, met := newTestManagerOpts(t, fs, "a", interiorOpts)
	mustCreate(t, m, "t1")
	ctrSchema(t, m, "t1")
	ten, _ := m.Acquire(context.Background(), "t1", false)
	g := holdSegments(t, fs)
	n := goTx(ten, bumpReq("N", 0, "N", ""))
	appliedNotDurable(t, ten)

	q := goQuery(context.Background(), ten, ctrRead)
	ro := goPark(context.Background(), ten, true)
	// an interior read on the same tenant, between and after them
	if v, _ := interiorRead(t, ten, 2*time.Second); v != 1 {
		t.Fatalf("interior read saw %d", v)
	}
	waitFor(t, "two parked reads", func() bool { return parkedCount(ten) == 2 })
	qAfter := goQuery(context.Background(), ten, ctrRead)
	noReply(t, "/query", q, 300*time.Millisecond)
	noReply(t, "read-only /tx", ro, 0)
	noReply(t, "/query after the interior read", qAfter, 0)
	if met.InteriorReads.Load() != 1 {
		t.Fatalf("interior reads %d, want 1", met.InteriorReads.Load())
	}

	// the flag cannot ride the ordinary entry points
	if _, _, err := ten.SubmitTxCtx(context.Background(),
		TxRequest{Client: "c", TxID: "x", Interior: true, Stmts: []Statement{{SQL: ctrRead}}}); !errors.Is(err, ErrInteriorMisuse) {
		t.Fatalf("interior through SubmitTxCtx: %v", err)
	}
	// nor be put on a write
	if _, _, err := ten.SubmitInteriorReadCtx(context.Background(),
		TxRequest{Client: "c", TxID: "x", Interior: true, Stmts: []Statement{insertStmt(9, "x")}}); !errors.Is(err, ErrInteriorMisuse) {
		t.Fatalf("interior write: %v", err)
	}
	// nor be implied by the entry point alone
	if _, _, err := ten.SubmitInteriorReadCtx(context.Background(),
		TxRequest{Client: "c", TxID: "x", Stmts: []Statement{{SQL: ctrRead}}}); !errors.Is(err, ErrInteriorMisuse) {
		t.Fatalf("interior entry point without the flag: %v", err)
	}

	g.open(false)
	recvTx(t, "N", n, 5*time.Second)
	for what, ch := range map[string]chan readRes{"/query": q, "read-only /tx": ro, "/query after": qAfter} {
		if r := recvRead(t, ch, 5*time.Second); r.err != nil {
			t.Fatalf("%s: %v", what, r.err)
		}
	}

	t.Run("engine option off", func(t *testing.T) {
		fs := NewFakeStore()
		m, met := newTestManagerOpts(t, fs, "a", func(*TenantOpts) {})
		mustCreate(t, m, "t1")
		ctrSchema(t, m, "t1")
		ten, _ := m.Acquire(context.Background(), "t1", false)
		g := holdSegments(t, fs)
		n := goTx(ten, bumpReq("N", 0, "N", ""))
		appliedNotDurable(t, ten)
		type r struct {
			v   int64
			inc string
			err error
		}
		ch := make(chan r, 1)
		go func() {
			res, inc, err := ten.SubmitInteriorReadCtx(context.Background(),
				TxRequest{Client: "c", TxID: "read:i", Interior: true, Stmts: []Statement{{SQL: ctrRead}}})
			if err != nil {
				ch <- r{err: err}
				return
			}
			ch <- r{res[0].Rows[0][0].(int64), inc, nil}
		}()
		waitFor(t, "the interior request parked", func() bool { return parkedCount(ten) == 1 })
		select {
		case got := <-ch:
			t.Fatalf("interior request with the option off returned during the stall: %+v", got)
		case <-time.After(300 * time.Millisecond):
		}
		g.open(false)
		recvTx(t, "N", n, 5*time.Second)
		got := <-ch
		if got.err != nil || got.v != 1 || got.inc != "" {
			t.Fatalf("option off: %+v, want v=1 after the stall and no token", got)
		}
		if met.InteriorReads.Load() != 0 {
			t.Fatalf("option off served %d interior reads", met.InteriorReads.Load())
		}
	})
}

// The fence is per tenant: the token of one tenant (a registry tenant,
// say) never admits a write to another. Ordering is per tenant WAL, so a
// read of tenant A orders nothing on tenant B.
func TestInteriorTokenIsPerTenant(t *testing.T) {
	fs := NewFakeStore()
	m, _ := newTestManagerOpts(t, fs, "a", interiorOpts)
	for _, tid := range []string{"reg", "cat"} {
		mustCreate(t, m, tid)
		ctrSchema(t, m, tid)
	}
	reg, _ := m.Acquire(context.Background(), "reg", false)
	cat, _ := m.Acquire(context.Background(), "cat", false)
	seen, inc := interiorRead(t, reg, 2*time.Second)
	if _, _, err := cat.SubmitTx(bumpReq("W", seen, "W", inc)); !errors.Is(err, errIncarnationChanged) {
		t.Fatalf("registry token on the catalog tenant: %v", err)
	}
	// and the catalog's own token admits it
	_, own := interiorRead(t, cat, 2*time.Second)
	if _, _, err := cat.SubmitTx(bumpReq("W", seen, "W", own)); err != nil {
		t.Fatal(err)
	}
}

// The HTTP shape: an interior read returns its token, a write cannot be
// interior (400), a fenced write in another incarnation is 412
// incarnation_changed, and /query ignores the field and parks.
func TestInteriorHTTP(t *testing.T) {
	fs := NewFakeStore()
	m, _ := newTestManagerOpts(t, fs, "a", interiorOpts)
	srv := httptest.NewServer(NewServer(m, nil).Handler())
	t.Cleanup(srv.Close)
	c := &testCluster{fs: fs, servers: map[string]*httptest.Server{"a": srv}, mgrs: map[string]*Manager{"a": m}}
	mustCreate(t, m, "t1")
	ctrSchema(t, m, "t1")

	code, body, _ := c.do(t, "a", http.MethodPost, "/v1/t/t1/tx", map[string]any{
		"client": "c", "txid": "read:1", "interior": true,
		"statements": []map[string]any{{"sql": ctrRead}},
	})
	inc, _ := body["incarnation"].(string)
	if code != 200 || inc == "" {
		t.Fatalf("interior read: %d %v", code, body)
	}
	code, body, _ = c.do(t, "a", http.MethodPost, "/v1/t/t1/tx", map[string]any{
		"client": "c", "txid": "w:1", "interior": true,
		"statements": []map[string]any{{"sql": "INSERT INTO kv(k, v) VALUES(5, 'x')"}},
	})
	if code != 400 {
		t.Fatalf("interior write: %d %v", code, body)
	}
	code, body, _ = c.do(t, "a", http.MethodPost, "/v1/t/t1/tx", map[string]any{
		"client": "c", "txid": "w:2", "incarnation": "t1/not-this-one",
		"statements": []map[string]any{{"sql": "INSERT INTO kv(k, v) VALUES(5, 'x')"}},
	})
	if code != 412 || body["code"] != CodeIncarnationChanged {
		t.Fatalf("fenced write in another incarnation: %d %v", code, body)
	}
	code, body, _ = c.do(t, "a", http.MethodPost, "/v1/t/t1/tx", map[string]any{
		"client": "c", "txid": "w:3", "incarnation": inc,
		"statements": []map[string]any{{"sql": "INSERT INTO kv(k, v) VALUES(5, 'x')"}},
	})
	if code != 200 {
		t.Fatalf("fenced write in its incarnation: %d %v", code, body)
	}

	// /query has no interior form: the field is ignored and the read parks
	ten, _ := m.Acquire(context.Background(), "t1", false)
	g := holdSegments(t, fs)
	n := goTx(ten, bumpReq("N", 0, "N", ""))
	appliedNotDurable(t, ten)
	q := make(chan int, 1)
	go func() {
		code, _, _ := c.do(t, "a", http.MethodPost, "/v1/t/t1/query", map[string]any{"sql": ctrRead, "interior": true})
		q <- code
	}()
	select {
	case code := <-q:
		t.Fatalf("/query with interior returned during the stall: %d", code)
	case <-time.After(300 * time.Millisecond):
	}
	g.open(false)
	recvTx(t, "N", n, 5*time.Second)
	if code := <-q; code != 200 {
		t.Fatalf("/query: %d", code)
	}
	_ = fmt.Sprint
}
