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
// the request context. The pod slot returns when the query has finished.
//
// A result whose basis is not durable yet parks until the durable gate
// releases it. Before Q1 a read parked before it executed, so it held no
// result. Now a parked read holds its full result (up to MaxResultBytes)
// in the Go heap, so two bounds apply until it is delivered or dropped:
//
//   - It keeps its tenant reader slot. So at most MaxReaders results of a
//     tenant are parked or executing, and a new read of that tenant waits
//     for admission with its request context.
//   - It holds a share of the pod park budget (PodMaxParkedBytes) of the
//     size of its result estimate. The worker takes the share before it
//     returns the pod slot. When the budget is full, the worker waits with
//     the pod slot, so the waiting results stay inside the bound of the
//     executing ones (PodMaxQueries x MaxResultBytes).
//
// A parked read holds no snapshot and no connection: it is not in the
// reader set, so the checkpoint drain and close never wait for it. Its
// release needs only durability progress, which needs no read slot: the
// actor cuts a segment when it pins and when it parks a read (pinRead,
// onReadDone), and segment PUTs and acks run on the actor and the upload
// goroutines. A caller that gives up drops its parked result: an
// AfterFunc on the request context tells the actor (readAbandon).

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"slices"
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
	durable bool          // basis <= durableOff at pin time
	pinTime time.Duration // actor time of the pin, charged with the read
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
		// the actor or the worker owns it: the canceled ctx stops the
		// query or the budget wait, or drops a parked result (onReadDone),
		// and the owner returns the slots.
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

// releaseSlots returns the admission slots of w and its park budget
// share. Idempotent.
func (t *Tenant) releaseSlots(w *readWork) {
	t.releasePodSlot(w)
	if n := w.parkBytes.Swap(0); n > 0 {
		t.qlim.releasePark(n)
	}
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

// deliverRead hands a computed outcome to the submitter and returns the
// slots and the park budget share of w.
func (t *Tenant) deliverRead(w *readWork, o readOutcome) {
	t.releaseSlots(w)
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
	pinStart := time.Now()
	ctx, cancel := context.WithCancelCause(w.ctx)
	ar := t.readers.add(cancel)
	if ar == nil {
		cancel(nil)
		t.replyRead(w, readOutcome{err: ErrClosed})
		return
	}
	// abort is the one exit for a failed pin. It maps SQLITE_NOMEM from
	// any step (prepare, connection open, BEGIN, probe) to
	// ErrOutOfMemory, after the context cause, which wins.
	abort := func(err error) {
		if c := context.Cause(ctx); c != nil {
			err = c
		}
		err = memErr(err, t.metrics)
		cancel(nil)
		// charge before remove: the last remove can retire the record
		t.usage.addTime(kindRead, time.Since(pinStart))
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
		abort(err)
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
		abort(err)
		return
	}
	pr := &pinnedRead{w: w, ctx: ctx, cancel: cancel, conn: conn, db: h, tx: tx, ar: ar,
		basis: basis, gen: t.mf.Gen, durable: basis <= t.durableOff, pinTime: time.Since(pinStart)}
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
	start := time.Now()
	lim := t.newResultLimit(false)
	out := t.execRead(ctx, pr.tx, pr.w, lim)
	t.usage.charge(kindRead, pr.pinTime+time.Since(start), lim)
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
		t.deliverRead(pr.w, out)
		return
	}
	// The result waits for durability. It keeps the tenant slot until it
	// is delivered or dropped, so readDone never holds more than MaxReaders
	// outcomes, and the send below never waits for the actor. The park
	// budget share comes before the pod slot returns: while the budget is
	// full, this worker waits with its pod slot (see the top of the file).
	n := int64(resultCost) // an error outcome holds no rows (execRead)
	if out.err == nil {
		n = max(n, lim.bytes)
	}
	n, err := t.qlim.reservePark(pr.w.ctx, t.done, n)
	t.pool.finish() // before the slot returns, see readPool
	if err != nil {
		// the caller gave up or the tenant closed: drop the result
		if errors.Is(err, ErrClosed) {
			err = t.deadError()
		}
		t.releaseSlots(pr.w)
		pr.w.reply <- readOutcome{err: err}
		return
	}
	pr.w.parkBytes.Store(n)
	t.releasePodSlot(pr.w)
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
// closed and returns their tenant slots and park budget shares. The
// receive makes exactly one party (finishClose or a late worker) the
// owner of each outcome. Only after done is closed.
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
func (t *Tenant) execRead(ctx context.Context, tx *sql.Tx, w *readWork, lim *resultLimit) readOutcome {
	if w.tx {
		res, err := execTxStatements(ctx, tx, w.stmts, lim)
		if err != nil {
			// execTxStatements returns the results of the statements before
			// the failed one. No caller uses them (the /tx handler answers
			// with the error only), and a parked outcome must not keep them:
			// its park budget share counts no rows for an error. The error
			// is typed like on the write path (classifyStmtErr), so a
			// statement the engine refuses stays final.
			return readOutcome{err: classifyStmtErr(err)}
		}
		return readOutcome{results: res}
	}
	res, err := runQuery(ctx, tx, w.stmts[0], lim)
	if err != nil {
		return readOutcome{err: err}
	}
	return readOutcome{results: []QueryResult{res}}
}

// onReadDone releases or parks an outcome whose basis was not durable at
// pin time. A parked outcome keeps its tenant slot and its park budget
// share until releaseReadWait, abandonRead or failQueued. Actor-loop only.
func (t *Tenant) onReadDone(rr readResult) {
	// a gen change means a checkpoint folded the basis into a durable base
	if rr.gen != t.mf.Gen || rr.basis <= t.durableOff {
		t.deliverRead(rr.w, rr.out)
		return
	}
	w := rr.w
	if err := w.ctx.Err(); err != nil {
		t.deliverRead(w, readOutcome{err: err}) // nobody waits for the result
		return
	}
	// The caller can give up while the result is parked. The callback
	// tells the actor, which drops the result and returns the slots. It
	// runs at most once, after stop it never runs, and a message for a
	// result that is gone already is ignored (abandonRead).
	stop := context.AfterFunc(w.ctx, func() {
		select {
		case t.readAbandon <- w:
		case <-t.done:
		}
	})
	t.readWaits = append(t.readWaits, readWait{end: rr.basis, w: w, out: rr.out, stop: stop})
	t.metrics.ParkedReads.Add(1)
	t.cutSegment()
}

// releaseReadWait hands a parked outcome to its submitter. Callers have
// established that its basis is durable, and remove rw from readWaits.
func (t *Tenant) releaseReadWait(rw readWait) {
	t.unparkRead(rw)
	t.deliverRead(rw.w, rw.out)
}

// unparkRead ends the parked state of rw. Its caller removes rw from
// readWaits and replies, which returns the slots and the budget share.
func (t *Tenant) unparkRead(rw readWait) {
	rw.stop()
	t.metrics.ParkedReads.Add(-1)
}

// abandonRead drops the parked result of w after its caller gave up. The
// reply only fills the buffer: the caller returned already. Nothing is
// done when the result was delivered before the message arrived.
// Actor-loop only.
func (t *Tenant) abandonRead(w *readWork) {
	i := slices.IndexFunc(t.readWaits, func(rw readWait) bool { return rw.w == w })
	if i < 0 {
		return
	}
	rw := t.readWaits[i]
	t.readWaits = slices.Delete(t.readWaits, i, i+1) // clears the tail: no result stays referenced
	t.unparkRead(rw)
	t.deliverRead(w, readOutcome{err: context.Cause(w.ctx)})
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
	// fn runs after the last read removed its snapshot, and a read charges
	// its usage before that (readWorker), so the readers' hold on the usage
	// record ends after their last charge.
	t.readers.closeWith(func() {
		if ro != nil {
			ro.Close()
		}
		t.metrics.usage.release(&t.usage)
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
// a pod, and the bytes of their results that wait for durability. A nil
// limiter admits everything.
type queryLimiter struct {
	slots   chan struct{}
	metrics *Metrics

	// park budget (reservePark): estimated result bytes of reads that
	// wait for durability, across all tenants
	mu       sync.Mutex
	parkMax  int64 // 0 = no bound
	parkUsed int64
	parkQ    []*parkWaiter // FIFO: a large result is not passed by small ones
}

type parkWaiter struct {
	n     int64
	ready chan struct{} // closed when the share is granted
}

func newQueryLimiter(o TenantOpts, m *Metrics) *queryLimiter {
	n := o.PodMaxQueries
	if n <= 0 {
		n = defaultPodMaxQueries()
	}
	return &queryLimiter{slots: make(chan struct{}, n), metrics: m, parkMax: podParkBudget(o, n)}
}

// podParkBudget resolves PodMaxParkedBytes for a pod with n query slots.
// The default is the bound of the executing results, n x MaxResultBytes:
// parked results then add at most as much again.
func podParkBudget(o TenantOpts, n int) int64 {
	if o.PodMaxParkedBytes < 0 {
		return 0
	}
	if o.PodMaxParkedBytes > 0 {
		return o.PodMaxParkedBytes
	}
	per := guardOpt(o.MaxResultBytes, defaultMaxResultBytes)
	if per == 0 { // result limit off: count each result at the default
		per = defaultMaxResultBytes
	}
	return int64(n) * per
}

// reservePark takes a share of n bytes of the park budget. It waits in
// FIFO order until the share fits, ctx ends or done closes. A share above
// the budget is cut to the budget, so it fits once nothing else is
// parked: that one result then passes the budget (see PodMaxParkedBytes).
// It returns the size of the share to pass to releasePark.
func (l *queryLimiter) reservePark(ctx context.Context, done <-chan struct{}, n int64) (int64, error) {
	if l == nil {
		return 0, nil
	}
	l.mu.Lock()
	if l.parkMax > 0 && n > l.parkMax {
		n = l.parkMax
	}
	if len(l.parkQ) == 0 && (l.parkMax == 0 || l.parkUsed+n <= l.parkMax) {
		l.parkUsed += n
		l.metrics.ParkedBytes.Add(n)
		l.mu.Unlock()
		return n, nil
	}
	pw := &parkWaiter{n: n, ready: make(chan struct{})}
	l.parkQ = append(l.parkQ, pw)
	l.metrics.ParkWaiting.Add(1)
	l.mu.Unlock()
	defer l.metrics.ParkWaiting.Add(-1)
	var err error
	select {
	case <-pw.ready:
		return n, nil
	case <-ctx.Done():
		err = ctx.Err()
	case <-done:
		err = ErrClosed
	}
	l.mu.Lock()
	select {
	case <-pw.ready:
		// granted while it gave up: return the share
		l.mu.Unlock()
		l.releasePark(n)
		return 0, err
	default:
	}
	if i := slices.Index(l.parkQ, pw); i >= 0 {
		l.parkQ = slices.Delete(l.parkQ, i, i+1)
	}
	l.grantPark() // the next waiter may fit now
	l.mu.Unlock()
	return 0, err
}

// releasePark returns a share taken by reservePark.
func (l *queryLimiter) releasePark(n int64) {
	if l == nil || n <= 0 {
		return
	}
	l.mu.Lock()
	l.parkUsed -= n
	l.metrics.ParkedBytes.Add(-n)
	l.grantPark()
	l.mu.Unlock()
}

// grantPark grants waiting shares in order while they fit. Needs l.mu.
func (l *queryLimiter) grantPark() {
	for len(l.parkQ) > 0 {
		pw := l.parkQ[0]
		if l.parkMax > 0 && l.parkUsed+pw.n > l.parkMax {
			return
		}
		l.parkUsed += pw.n
		l.metrics.ParkedBytes.Add(pw.n)
		close(pw.ready)
		l.parkQ = slices.Delete(l.parkQ, 0, 1)
	}
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
