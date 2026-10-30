package hoglite

// Read path: reads execute on worker goroutines, not on the tenant actor.
//
// The actor keeps the durable-read gate. It only decides the basis of a
// read and pins a read snapshot at that basis (pinRead). A worker then runs
// the statements on the pinned snapshot (readWorker). A slow query no
// longer stalls writes, segment acks, heartbeats or checkpoints of its
// tenant, so it cannot trip the actor watchdog either.
//
// Admission happens on the caller goroutine before the actor sees the read:
// first one per-tenant reader slot (MaxReaders, equal to the roDB pool
// size), then one pod-wide query slot (PodMaxQueries). Both waits end with
// the request context. The slots return when the query has finished, not
// when its result is released by the durable gate.

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync"
	"time"
)

// ErrReadPreempted is returned to a read that a checkpoint canceled after
// the drain grace. The read is safe to retry.
var ErrReadPreempted = errors.New("read canceled by checkpoint; retry")

// readResult carries a worker outcome back to the actor when the basis was
// not yet durable at pin time.
type readResult struct {
	w     *readWork
	basis int64
	gen   int64 // manifest gen of the basis offsets
	out   readOutcome
}

// pinnedRead is one read snapshot held open on a roDB connection.
type pinnedRead struct {
	w       *readWork
	ctx     context.Context
	cancel  context.CancelCauseFunc
	conn    *sql.Conn
	db      uintptr // sqlite handle of conn for interrupts; 0 = none
	tx      *sql.Tx
	ar      *activeRead
	basis   int64
	gen     int64
	durable bool // basis <= durableOff at pin time
}

// submitRead admits w, queues it for the actor and waits for its outcome.
func (t *Tenant) submitRead(ctx context.Context, w *readWork) readOutcome {
	if ctx == nil {
		ctx = context.Background()
	}
	w.ctx = ctx
	w.reply = make(chan readOutcome, 1)
	// tenant slot first: a tenant that already uses all its reader slots
	// must not also hold pod slots while it waits for its own
	select {
	case t.readSlots <- struct{}{}:
	case <-ctx.Done():
		return readOutcome{err: ctx.Err()}
	case <-t.done:
		return readOutcome{err: t.deadError()}
	}
	if err := t.qlim.acquire(ctx, t.done); err != nil {
		<-t.readSlots
		if errors.Is(err, ErrClosed) {
			err = t.deadError()
		}
		return readOutcome{err: err}
	}
	// The queue can be full while the actor is busy (checkpoint upload).
	// A canceled or timed-out caller must not wait here with its slots: w
	// was not queued, so this goroutine still owns them.
	select {
	case t.readCh <- w:
	case <-ctx.Done():
		t.releaseSlots(w)
		return readOutcome{err: ctx.Err()}
	case <-t.done:
		t.releaseSlots(w)
		return readOutcome{err: t.deadError()}
	}
	select {
	case o := <-w.reply:
		return o
	case <-ctx.Done():
		// still queued: take it back and return the slots here. Otherwise
		// the actor or the worker owns it; the canceled ctx stops the
		// query and the owner returns the slots.
		if w.taken.CompareAndSwap(false, true) {
			t.releaseSlots(w)
		}
		return readOutcome{err: ctx.Err()}
	case <-t.done:
		select {
		case o := <-w.reply:
			return o
		default:
		}
		// a send that raced the close can sit in readCh forever
		if w.taken.CompareAndSwap(false, true) {
			t.releaseSlots(w)
		}
		return readOutcome{err: t.deadError()}
	}
}

// releaseSlots returns the admission slots of w. Idempotent.
func (t *Tenant) releaseSlots(w *readWork) {
	t.releasePodSlot(w)
	if w.released.CompareAndSwap(false, true) {
		<-t.readSlots
	}
}

// releasePodSlot returns only the pod-wide slot of w. Idempotent.
func (t *Tenant) releasePodSlot(w *readWork) {
	if w.podReleased.CompareAndSwap(false, true) {
		t.qlim.release()
	}
}

// replyRead answers a read that never reached a worker.
func (t *Tenant) replyRead(w *readWork, o readOutcome) {
	t.releaseSlots(w)
	w.reply <- o
}

// deliverRead hands a computed outcome to the submitter.
func (t *Tenant) deliverRead(w *readWork, o readOutcome) {
	if o.err == nil {
		t.metrics.Queries.Add(1)
	}
	w.reply <- o
}

// pinRead fixes the read snapshot of w at basis and starts its worker.
// Actor-loop only, called right after basis = currentCommitEnd().
//
// Why the result never exposes applied-but-not-durable state:
//
//  1. All commits are applied by this actor. Between the basis computation
//     and the end of pinRead nothing commits, so the database state equals
//     the state at basis.
//  2. A deferred BEGIN takes no snapshot. The probe SELECT reads the schema
//     b-tree and opens the WAL read transaction here, on the actor. From
//     now on SQLite serves this transaction the frames up to basis only,
//     however many commits the actor applies later. The worker therefore
//     reads the basis state even if it starts much later.
//  3. The outcome goes to the submitter only once basis <= durableOff in the
//     same gen, or once a later checkpoint folded it into a durable base
//     (onReadDone, advanceWatermark, checkpointLocked). A crash before that
//     fails the read. Errors are gated the same way: they also reflect the
//     snapshot.
//
// A naive worker that opens its transaction after the actor moved on reads
// the newest applied state, which can include commits that never become
// durable. TestReadSnapshotPinnedAtBasis catches that.
//
// Lease: leaseExpiring is checked on the actor before the pin, as before.
// A pinned read that outlives the lease still completes. Its snapshot is
// the basis state, which this owner committed while it held the lease, and
// the result is released only after that state is durable (the watermark
// and checkpoint paths check the lease before they advance). It is a
// correct read of committed state that linearizes at the pin. No write of
// a later owner can precede the pin: a new owner waits out the lease.
func (t *Tenant) pinRead(w *readWork, basis int64) {
	ctx, cancel := context.WithCancelCause(w.ctx)
	ar := t.readers.add(cancel)
	if ar == nil {
		cancel(nil)
		t.replyRead(w, readOutcome{err: ErrClosed})
		return
	}
	abort := func(err error) {
		if c := context.Cause(ctx); c != nil {
			err = c
		}
		cancel(nil)
		t.readers.remove(ar)
		t.replyRead(w, readOutcome{err: err})
	}
	// The reader slot of w guarantees a free pool connection, so this does
	// not wait. The timeout only bounds the actor if that accounting is
	// ever wrong.
	cctx, ccancel := context.WithTimeout(ctx, time.Second)
	defer ccancel()
	if t.probe == nil {
		// prepared once; database/sql then keeps it per connection, so a
		// pin does not parse SQL
		p, err := t.roDB.PrepareContext(cctx, `SELECT 1 FROM sqlite_master LIMIT 1`)
		if err != nil {
			abort(err)
			return
		}
		t.probe = p
	}
	conn, err := t.roDB.Conn(cctx)
	if err != nil {
		abort(err)
		return
	}
	// for the read deadline (guards.go); without a handle only the
	// driver's context checks stop the query
	h := t.connHandle(conn)
	// Background, not ctx: database/sql rolls a transaction back on its own
	// goroutine when the BeginTx context ends, and the connection would
	// then return to the pool after the worker released its slot. The
	// worker rolls back itself; ctx cancels the statements.
	tx, err := conn.BeginTx(context.Background(), nil)
	if err != nil {
		closeReadConn(conn, h)
		abort(memErr(err, t.metrics))
		return
	}
	// The probe also runs with Background: it reads one schema page and
	// cannot wait for a lock (only this actor writes or checkpoints), and
	// a cancelable context costs two goroutines per statement.
	var one int
	err = tx.StmtContext(context.Background(), t.probe).QueryRowContext(context.Background()).Scan(&one)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		tx.Rollback()
		closeReadConn(conn, h)
		abort(memErr(err, t.metrics))
		return
	}
	pr := &pinnedRead{w: w, ctx: ctx, cancel: cancel, conn: conn, db: h, tx: tx, ar: ar,
		basis: basis, gen: t.mf.Gen, durable: basis <= t.durableOff}
	// A free worker takes the read; start one only when every free worker
	// is already spoken for. The send never blocks: each queued pin holds
	// a reader slot, and pinCh has room for MaxReaders.
	t.pool.mu.Lock()
	t.pool.queued++
	spawn := t.pool.queued > t.pool.free
	if spawn {
		t.pool.free++ // the new worker
	}
	t.pool.mu.Unlock()
	t.pinCh <- pr
	if spawn {
		go t.readWorkerLoop()
	}
}

// readWorkerIdle is how long an idle reader worker waits for the next
// pin before it exits. Reuse keeps the grown goroutine stack: SQLite runs
// deep, and a new goroutine pays the stack growth again on each read.
const readWorkerIdle = 2 * time.Second

// readPool counts the reader workers of a tenant for the pin hand-off.
//
// A worker is free from the end of its read until it takes the next pin.
// It becomes free before it returns its tenant reader slot (readWorker),
// so a read admitted with that slot finds it free. pinRead starts a new
// worker only when the queued pins outnumber the free workers. At that
// moment every worker either runs a read or is counted for a queued pin,
// and each of these holds a reader slot. So the workers never outnumber
// MaxReaders, and serial reads use one worker.
type readPool struct {
	mu     sync.Mutex
	queued int // pins in pinCh that no worker took yet
	free   int // workers that run no read
}

// take records that a free worker took a queued pin.
func (p *readPool) take() {
	p.mu.Lock()
	p.queued--
	p.free--
	p.mu.Unlock()
}

// finish records that a worker ended its read.
func (p *readPool) finish() {
	p.mu.Lock()
	p.free++
	p.mu.Unlock()
}

// leave lets a free worker exit unless a queued pin counts on it.
func (p *readPool) leave() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.queued >= p.free {
		return false
	}
	p.free--
	return true
}

// readWorkerLoop serves pins until it is idle for readWorkerIdle or the
// tenant closes, and no queued pin counts on it.
func (t *Tenant) readWorkerLoop() {
	idle := time.NewTimer(readWorkerIdle)
	defer idle.Stop()
	for {
		select {
		case pr := <-t.pinCh:
			t.pool.take()
			t.readWorker(pr)
			idle.Reset(readWorkerIdle)
		case <-idle.C:
			if t.pool.leave() {
				return
			}
			idle.Reset(readWorkerIdle)
		case <-t.done:
			if t.pool.leave() {
				return
			}
			// a pin queued before the close counts on this worker; its
			// context is canceled, so it ends at once
			pr := <-t.pinCh
			t.pool.take()
			t.readWorker(pr)
		}
	}
}

// readWorker runs one pinned read off the actor.
func (t *Tenant) readWorker(pr *pinnedRead) {
	if h := t.readHook; h != nil {
		h(pr.ctx)
	}
	ctx, cancel := withDeadline(pr.ctx, t.opts.QueryTimeout, "read")
	it := watchInterrupt(ctx, pr.db)
	out := t.execRead(ctx, pr.tx, pr.w)
	// no interrupt may reach the ROLLBACK or the next user of conn
	it.stop()
	if out.err != nil {
		// report why the context ended (deadline, checkpoint preemption,
		// tenant close) instead of the driver's interrupt error
		if c := context.Cause(ctx); c != nil {
			out = readOutcome{err: c}
		}
		switch {
		case errors.Is(out.err, ErrQueryTimeout):
			t.metrics.ReadTimeouts.Add(1)
		case errors.Is(out.err, ErrResultTooLarge):
			t.metrics.ResultTooLarge.Add(1)
		}
		out.err = memErr(out.err, t.metrics)
	}
	cancel()
	pr.tx.Rollback()
	closeReadConn(pr.conn, pr.db)
	pr.cancel(nil)
	// the snapshot is closed: a waiting checkpoint or close may proceed
	t.readers.remove(pr.ar)
	t.lastUsedMs.Store(time.Now().UnixMilli())
	if pr.durable {
		t.pool.finish() // before the slot returns, see readPool
		t.releaseSlots(pr.w)
		t.deliverRead(pr.w, out)
		return
	}
	// The tenant slot returns when the actor takes the outcome (onReadDone).
	// So readDone never holds more than MaxReaders outcomes, and this send
	// never waits for the actor.
	t.releasePodSlot(pr.w)
	t.pool.finish()
	select {
	case t.readDone <- readResult{w: pr.w, basis: pr.basis, gen: pr.gen, out: out}:
		// The send can win against a closed done. The actor no longer
		// reads readDone then, and its final drain (finishClose) may have
		// run already, so drain here too.
		select {
		case <-t.done:
			t.drainReadDone()
		default:
		}
	case <-t.done:
		// the basis may never become durable now
		t.releaseSlots(pr.w)
		pr.w.reply <- readOutcome{err: t.deadError()}
	}
}

// drainReadDone fails the outcomes left in readDone after the tenant
// closed and returns their tenant slots. The receive makes exactly one
// party (finishClose or a late worker) the owner of each outcome.
// Only after done is closed.
func (t *Tenant) drainReadDone() {
	for {
		select {
		case rr := <-t.readDone:
			t.releaseSlots(rr.w)
			rr.w.reply <- readOutcome{err: t.deadError()}
		default:
			return
		}
	}
}

// execRead executes the statements of w inside the pinned transaction tx.
// ctx ends when the client goes away, the read deadline passes, a
// checkpoint preempts the read or the tenant closes. The result limit
// covers the whole response.
func (t *Tenant) execRead(ctx context.Context, tx *sql.Tx, w *readWork) readOutcome {
	lim := t.newResultLimit(false)
	if w.tx {
		res, err := execTxStatements(ctx, tx, w.stmts, lim)
		return readOutcome{results: res, err: err}
	}
	res, err := runQuery(ctx, tx, w.stmts[0], lim)
	if err != nil {
		return readOutcome{err: err}
	}
	return readOutcome{results: []QueryResult{res}}
}

// onReadDone releases or parks an outcome whose basis was not durable at
// pin time. Actor-loop only.
func (t *Tenant) onReadDone(rr readResult) {
	t.releaseSlots(rr.w) // the tenant slot; the worker returned the pod slot
	// a gen change means a checkpoint folded the basis into a durable base
	if rr.gen != t.mf.Gen || rr.basis <= t.durableOff {
		t.deliverRead(rr.w, rr.out)
		return
	}
	t.readWaits = append(t.readWaits, readWait{end: rr.basis, w: rr.w, out: rr.out})
	t.cutSegment()
}

// releaseReadWait hands a parked outcome to its submitter. Callers have
// established that its basis is durable.
func (t *Tenant) releaseReadWait(rw readWait) {
	if rw.txReply != nil {
		rw.txReply <- rw.txOut // parked /tx outcome
		return
	}
	t.deliverRead(rw.w, rw.out)
}

func (t *Tenant) bumpProgress() { t.progressMs.Store(time.Now().UnixMilli()) }

// drainReaders waits until no read snapshot is open. After ReadDrainWait
// it cancels the remaining reads with ErrReadPreempted and waits once
// more. False means a read did not stop in time. Actor-loop only.
func (t *Tenant) drainReaders() bool {
	if t.readers.waitIdle(t.opts.ReadDrainWait, t.bumpProgress) {
		return true
	}
	t.readers.cancelAll(ErrReadPreempted)
	return t.readers.waitIdle(t.opts.ReadDrainWait, t.bumpProgress)
}

// closeReaders cancels in-flight reads and closes roDB once none uses it.
// A read that ignores cancellation for longer than ReadDrainWait keeps its
// connection; the last such read closes roDB when it ends.
func (t *Tenant) closeReaders() {
	cause := t.deadErr
	if cause == nil {
		cause = ErrClosed
	}
	t.readers.cancelAll(cause)
	if !t.readers.waitIdle(t.opts.ReadDrainWait, t.bumpProgress) && t.metrics.Logger != nil {
		t.metrics.Logger.Printf("tenant %s closed with %d reads still running", t.ID, t.readers.count())
	}
	ro := t.roDB
	t.readers.closeWith(func() {
		if ro != nil {
			ro.Close()
		}
	})
}

// readerSet tracks the open read snapshots of one tenant.
type readerSet struct {
	mu     sync.Mutex
	active map[*activeRead]struct{}
	idle   chan struct{} // closed when active drops to zero; nil while idle
	closed bool          // no new snapshots
	onIdle func()        // set by closeWith while reads still run
}

type activeRead struct{ cancel context.CancelCauseFunc }

// add registers a snapshot. It returns nil once the set is closed.
func (s *readerSet) add(cancel context.CancelCauseFunc) *activeRead {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	if s.active == nil {
		s.active = map[*activeRead]struct{}{}
	}
	if len(s.active) == 0 {
		s.idle = make(chan struct{})
	}
	ar := &activeRead{cancel: cancel}
	s.active[ar] = struct{}{}
	return ar
}

func (s *readerSet) remove(ar *activeRead) {
	var fn func()
	s.mu.Lock()
	delete(s.active, ar)
	if len(s.active) == 0 && s.idle != nil {
		close(s.idle)
		s.idle = nil
		fn, s.onIdle = s.onIdle, nil
	}
	s.mu.Unlock()
	if fn != nil {
		fn()
	}
}

func (s *readerSet) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.active)
}

func (s *readerSet) cancelAll(cause error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for ar := range s.active {
		ar.cancel(cause)
	}
}

// waitIdle waits up to d for zero open snapshots. tick runs about once a
// second so a waiting actor keeps feeding the watchdog.
func (s *readerSet) waitIdle(d time.Duration, tick func()) bool {
	deadline := time.Now().Add(d)
	for {
		s.mu.Lock()
		ch := s.idle
		s.mu.Unlock()
		if ch == nil {
			return true
		}
		left := time.Until(deadline)
		if left <= 0 {
			return false
		}
		if left > time.Second {
			left = time.Second
		}
		tm := time.NewTimer(left)
		select {
		case <-ch:
		case <-tm.C:
		}
		tm.Stop()
		if tick != nil {
			tick()
		}
	}
}

// closeWith closes the set and runs fn once no snapshot is open: now, or
// when the last running read ends.
func (s *readerSet) closeWith(fn func()) {
	s.mu.Lock()
	s.closed = true
	if len(s.active) > 0 {
		s.onIdle = fn
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()
	fn()
}

// queryLimiter bounds the queries admitted at once across all tenants of
// a pod. A nil limiter admits everything.
type queryLimiter struct {
	slots   chan struct{}
	metrics *Metrics
}

func newQueryLimiter(n int, m *Metrics) *queryLimiter {
	if n <= 0 {
		n = defaultPodMaxQueries()
	}
	return &queryLimiter{slots: make(chan struct{}, n), metrics: m}
}

// acquire waits for a slot until ctx ends or done closes. A saturated pod
// queues the query instead of failing it; the client decides how long.
func (l *queryLimiter) acquire(ctx context.Context, done <-chan struct{}) error {
	if l == nil {
		return nil
	}
	select {
	case l.slots <- struct{}{}:
		l.metrics.QueriesInflight.Add(1)
		return nil
	default:
	}
	l.metrics.QueriesWaiting.Add(1)
	defer l.metrics.QueriesWaiting.Add(-1)
	select {
	case l.slots <- struct{}{}:
		l.metrics.QueriesInflight.Add(1)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return ErrClosed
	}
}

func (l *queryLimiter) release() {
	if l == nil {
		return
	}
	l.metrics.QueriesInflight.Add(-1)
	<-l.slots
}

// closeReadConn returns a read connection to the pool. A connection that
// still has an open transaction is closed instead: SQLITE_NOMEM can also
// fail the ROLLBACK, and the next pin would fail its BEGIN and hold a
// snapshot that blocks the checkpoint. Closing a read connection is safe
// (PERSIST_WAL, see registerAuthorizerOnce); SQLite rolls back on close.
func closeReadConn(c *sql.Conn, h uintptr) {
	if err := endOpenTx(c, h); err != nil {
		c.Raw(func(any) error { return driver.ErrBadConn })
	}
	c.Close()
}
