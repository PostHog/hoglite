package hoglite

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "modernc.org/sqlite"
)

// TenantOpts tunes durability/lease behavior. Defaults target real S3
// (~70 ms PUT RTT). Tests shrink them.
type TenantOpts struct {
	Lease          time.Duration // pod lease length (crash failover time)
	HeartbeatEvery time.Duration // pod lease heartbeat interval (<< Lease)
	BatchWindow    time.Duration // group-commit window
	MaxSegBytes    int64         // cut a segment at this size
	CheckpointByte int64         // checkpoint when WAL flushed past this
	MaxSegments    int           // or when this many segments since checkpoint
	Inflight       int           // max concurrent segment PUTs
	PutTimeout     time.Duration // per-attempt S3 PUT timeout
	SkewMargin     time.Duration // acquirer waits lease + this; owner stops PUTs lease/3 early
	IdleRelease    time.Duration // release ownership after this much write-idle time (0 = never)
	// Compress stores new segments and base images as zstd frames. Readers
	// accept both forms regardless of this flag. Turn it off for the first
	// rollout from a build without compression support, so pods of the old
	// build can still take over tenants during the rolling deploy.
	Compress bool
}

func DefaultOpts() TenantOpts {
	return TenantOpts{
		Lease:          12 * time.Second, // crash failover: wait lease+skew
		HeartbeatEvery: 1 * time.Second,  // one PUT/s per pod; 5 missed beats before the gate closes
		BatchWindow:    5 * time.Millisecond,
		MaxSegBytes:    256 << 10,
		CheckpointByte: 8 << 20,
		MaxSegments:    128,
		Inflight:       32,
		PutTimeout:     5 * time.Second,
		SkewMargin:     3500 * time.Millisecond, // must exceed max PUT duration
		IdleRelease:    60 * time.Second,
		Compress:       true,
	}
}

type Statement struct {
	SQL  string `json:"sql"`
	Args []any  `json:"args"`
	// Transaction guards: if set and the outcome mismatches, the whole
	// transaction rolls back with ExpectFailedError naming the index.
	ExpectAffected  *int64 `json:"expect_affected,omitempty"`   // RowsAffected must equal
	ExpectScalarInt *int64 `json:"expect_scalar_int,omitempty"` // first col of first row must equal
}

type QueryResult struct {
	Cols     []string `json:"cols"`
	Rows     [][]any  `json:"rows"`
	Affected int64    `json:"affected"`
}

type TxRequest struct {
	Client string      `json:"client"`
	TxID   string      `json:"txid"`
	Stmts  []Statement `json:"statements"`
	// FP is a request fingerprint (hash of the logical payload). On a
	// dedup hit a mismatched FP is a 422-class error, not a replayed result:
	// idempotency keys may never be reused for a different request.
	FP string `json:"fp,omitempty"`
}

type txWork struct {
	req   TxRequest
	reply chan txOutcome
}

type txOutcome struct {
	results  []QueryResult
	dup      bool
	readOnly bool
	err      error
}

// allReadOnly is the STRICT classifier for the no-durability fast path:
// only statements that cannot write. isQuerySQL is about "returns rows"
// (INSERT..RETURNING, writable WITH) and must not be reused here.
func allReadOnly(stmts []Statement) bool {
	if len(stmts) == 0 {
		return false
	}
	for _, st := range stmts {
		ls := strings.ToLower(stripSQLComments(st.SQL))
		ls = strings.TrimLeft(ls, "(")
		readPrefix := strings.HasPrefix(ls, "select") || strings.HasPrefix(ls, "values") ||
			strings.HasPrefix(ls, "explain")
		if strings.HasPrefix(ls, "with") {
			// a CTE can wrap INSERT/UPDATE/DELETE .. RETURNING
			readPrefix = !sqlContainsDML(ls)
		}
		if !readPrefix || st.ExpectAffected != nil {
			return false
		}
	}
	return true
}

var dmlWords = []string{"insert", "update", "delete", "replace", "create", "drop",
	"alter", "pragma", "attach", "detach", "vacuum", "reindex", "returning"}

func sqlContainsDML(ls string) bool {
	for _, w := range dmlWords {
		for i := 0; i+len(w) <= len(ls); {
			j := strings.Index(ls[i:], w)
			if j < 0 {
				break
			}
			i += j
			left := i == 0 || !isSQLWordByte(ls[i-1])
			right := i+len(w) >= len(ls) || !isSQLWordByte(ls[i+len(w)])
			if left && right {
				return true
			}
			i += len(w)
		}
	}
	return false
}

func isSQLWordByte(c byte) bool {
	return c == '_' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z'
}

type readWork struct {
	stmt  Statement
	reply chan readOutcome
}

type readOutcome struct {
	res QueryResult
	err error
}

var ErrClosed = errors.New("tenant closed")

// ExpectFailedError aborts a transaction when a statement expectation fails.
type ExpectFailedError struct {
	Index int
	What  string
	// Guard carries the failing expectation for typed client handling
	// (the REST adapter maps it to typed refusals).
	Guard string
}

func (e *ExpectFailedError) Error() string {
	return fmt.Sprintf("expectation failed at statement %d: %s", e.Index, e.What)
}

var ErrNotOwner = errors.New("lease lost or expiring")

// pendingAck ties one applied transaction to the WAL offset that makes it
// durable. Acked only when the durable watermark passes end.
type pendingAck struct {
	end     int64
	reply   chan txOutcome
	outcome txOutcome
}

// segment is one flushed batch of WAL bytes plus everyone it can ack.
type segment struct {
	key     string
	data    []byte
	start   int64
	end     int64
	done    bool // PUT completed
	waiters []chan txOutcome
	results []txOutcome
}

// leaseClock is the pod-wide write gate (one heartbeated lease object per
// pod; the Manager implements it).
type leaseClock interface {
	leaseExpiring() bool
	leaseWriteDeadline() time.Time
}

type Tenant struct {
	ID      string
	store   Backend
	prefix  string
	opts    TenantOpts
	self    string // per-boot fencing identity
	routeID string // stable routable pod name
	clock   leaseClock

	dir     string
	dbPath  string
	walPath string

	mf   *Manifest
	etag string

	db       *sql.DB
	roDB     *sql.DB // query_only read path; WAL allows concurrent readers
	wal      *os.File
	pageSize int64
	parser   walParser

	submittedOff int64        // WAL bytes cut into segments this gen
	durableOff   int64        // contiguous durable watermark
	hasWrites    bool         // any real (non-read-only, non-dup) commit this gen
	pending      []pendingAck // ordered by end
	segs         []*segment   // in-flight/undrained, offset order
	segsSinceC   int

	batchOpen bool // a flush is pending

	segDone chan segDone
	sem     chan struct{}

	readWaits []readWait // reads parked until their basis is durable

	writeCh chan *txWork
	readCh  chan *readWork
	ctlCh   chan func()
	done    chan struct{}

	dead    bool
	deadErr error
	claimed bool // no heartbeats before the ownership claim lands

	hbTicks    int          // actor-loop ticks since the last ownership re-verify
	progressMs atomic.Int64 // bumped every actor iteration; watchdog liveness
	zombie     atomic.Bool  // watchdog cleared our ownership; refuse everything
	cleaned    atomic.Bool  // cleanup goroutine scheduled (watchdog + failAll)
	claimGen   int64        // manager leaseGen at claim time; mismatch = fenced
	claimEpoch int64        // manifest epoch of THIS incarnation (immutable after open)
	claimInst  string       // manifest instance of THIS incarnation

	debugUnfreeze chan struct{} // test hook

	ckptSem  chan struct{} // manager-bounded: max concurrent checkpoint drains
	claimSem chan struct{} // manager-bounded: claims never share the release lane

	claimAttempt string // per-acquisition nonce: the claim's base key suffix

	onClose  func(*Tenant) // manager removes us from its registry
	onPoison func(*Tenant) // manager publishes the re-acquire cooldown; runs BEFORE done closes

	heatCommits atomic.Int64 // commits since last controller scan
	classHint   atomic.Value // string: manifest class, actor-owned write
	lastUsedMs  atomic.Int64 // both actor and manager read/write

	metrics *Metrics
}

type segDone struct {
	seg *segment
	err error
}

// readWait parks a read until durableOff >= end (the read's basis: the
// commit end of the last applied transaction at arrival). P0-5: a read
// must never return applied-but-not-yet-durable state; after a crash such
// state vanishes and snapshot ids would be reused under an OCC basis that
// already saw them.
type readWait struct {
	end int64
	w   *readWork
	// when non-nil, this wait carries a parked /tx outcome to release
	// instead of running a query
	txReply chan txOutcome
	txOut   txOutcome
}

func openTenant(id string, dir string, mf *Manifest, etag string, store Backend, prefix, self, routeID string, opts TenantOpts, metrics *Metrics, clock leaseClock) (*Tenant, error) {
	t := &Tenant{
		ID: id, store: store, prefix: prefix, opts: opts, self: self, routeID: routeID, dir: dir, clock: clock,
		dbPath: filepath.Join(dir, "db.sqlite"), walPath: filepath.Join(dir, "db.sqlite-wal"),
		mf: mf, etag: etag, claimEpoch: mf.Epoch, claimInst: mf.Instance,
		segDone: make(chan segDone, 256),
		sem:     make(chan struct{}, opts.Inflight),
		writeCh: make(chan *txWork, 4096),
		readCh:  make(chan *readWork, 4096),
		ctlCh:   make(chan func(), 64),
		done:    make(chan struct{}),
		metrics: metrics,
	}
	registerAuthorizerOnce()
	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=wal_autocheckpoint(0)"+
		"&_pragma=synchronous(OFF)&_pragma=busy_timeout(10000)&_pragma=foreign_keys(1)", t.dbPath)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	t.db = db
	ro, err := sql.Open("sqlite", dsn+"&_pragma=query_only(1)")
	if err != nil {
		db.Close()
		return nil, err
	}
	ro.SetMaxOpenConns(1)
	t.roDB = ro
	// note: rowid-ordered on purpose — the retention trim uses it.
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS _hw_dedup(
		client TEXT NOT NULL, txid TEXT NOT NULL, result TEXT NOT NULL,
		reqfp TEXT NOT NULL DEFAULT '',
		PRIMARY KEY(client, txid))`); err != nil {
		db.Close()
		return nil, fmt.Errorf("open tenant %s: %w", id, err)
	}
	// upgrade path for tenants created before reqfp existed
	var nfp int64
	if err := db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('_hw_dedup') WHERE name='reqfp'`).Scan(&nfp); err == nil && nfp == 0 {
		if _, err := db.Exec(`ALTER TABLE _hw_dedup ADD COLUMN reqfp TEXT NOT NULL DEFAULT ''`); err != nil {
			db.Close()
			return nil, fmt.Errorf("dedup upgrade: %w", err)
		}
	}
	var ps int64
	if err := db.QueryRow(`PRAGMA page_size`).Scan(&ps); err != nil {
		db.Close()
		return nil, err
	}
	t.pageSize = ps
	if f, err := os.OpenFile(t.walPath, os.O_CREATE|os.O_RDONLY, 0o644); err == nil {
		t.wal = f
	} else {
		db.Close()
		return nil, err
	}
	t.lastUsedMs.Store(time.Now().UnixMilli())
	t.progressMs.Store(time.Now().UnixMilli()) // watchdog baseline: wedged-before-first-tick counts
	t.classHint.Store(mf.Class)
	go t.run()
	return t, nil
}

var errClosedStatic = ErrClosed

func (t *Tenant) deadError() error {
	if t.deadErr != nil {
		return t.deadErr
	}
	return errClosedStatic
}

func (t *Tenant) SubmitTx(req TxRequest) ([]QueryResult, bool, error) {
	t0 := time.Now()
	w := &txWork{req: req, reply: make(chan txOutcome, 1)}
	select {
	case t.writeCh <- w:
	case <-t.done:
		return nil, false, t.deadError()
	}
	defer func() {
		ewmaUpdate(&t.metrics.CommitEwmaUs, time.Since(t0).Microseconds())
	}()
	select {
	case o := <-w.reply:
		return o.results, o.dup, o.err
	case <-t.done:
		select {
		case o := <-w.reply:
			return o.results, o.dup, o.err
		default:
		}
		return nil, false, t.deadError()
	}
}

func (t *Tenant) SubmitQuery(st Statement) (QueryResult, error) {
	w := &readWork{stmt: st, reply: make(chan readOutcome, 1)}
	select {
	case t.readCh <- w:
	case <-t.done:
		return QueryResult{}, t.deadError()
	}
	select {
	case o := <-w.reply:
		return o.res, o.err
	case <-t.done:
		select {
		case o := <-w.reply:
			return o.res, o.err
		default:
		}
		return QueryResult{}, t.deadError()
	}
}

// Stop gracefully flushes, checkpoints and optionally releases ownership.
func (t *Tenant) Stop(release bool, newClass string) {
	if !t.Alive() {
		return
	}
	doneCh := make(chan struct{})
	select {
	case t.ctlCh <- func() { t.shutdown(release, newClass); close(doneCh) }:
		select {
		case <-doneCh:
		case <-t.done: // actor died before running our closure
		}
	case <-t.done:
	}
}

// Crash simulates kill -9: fail everything, no flush, no release.
func (t *Tenant) Crash() {
	if !t.Alive() {
		return
	}
	doneCh := make(chan struct{})
	select {
	case t.ctlCh <- func() { t.failLocked(errors.New("crash")); close(doneCh) }:
		select {
		case <-doneCh:
		case <-t.done:
		}
	case <-t.done:
	}
}

func (t *Tenant) Alive() bool {
	select {
	case <-t.done:
		return false
	default:
		return true
	}
}

// Poison kills the tenant with err (manager fail-stop path).
func (t *Tenant) Poison(err error) {
	if !t.Alive() {
		return
	}
	doneCh := make(chan struct{})
	select {
	case t.ctlCh <- func() { t.failLocked(err); close(doneCh) }:
		select {
		case <-doneCh:
		case <-t.done:
		}
	case <-t.done:
	}
}

// Freeze/Unfreeze simulate a partitioned owner (actor stalls, heartbeats
// stop) without dying.
func (t *Tenant) Freeze() {
	entered := make(chan struct{})
	t.debugUnfreeze = make(chan struct{})
	t.ctlCh <- func() { close(entered); <-t.debugUnfreeze }
	<-entered
}

func (t *Tenant) Unfreeze() { close(t.debugUnfreeze) }

// ---------------- actor loop ----------------

func (t *Tenant) run() {
	hb := time.NewTicker(t.opts.HeartbeatEvery)
	defer hb.Stop()
	var flushTimer *time.Timer
	var flushC <-chan time.Time
	defer func() {
		if flushTimer != nil {
			flushTimer.Stop()
		}
	}()

	armFlush := func() {
		if flushTimer == nil {
			flushTimer = time.NewTimer(t.opts.BatchWindow)
			flushC = flushTimer.C
		}
	}
	disarmFlush := func() {
		if flushTimer != nil {
			if !flushTimer.Stop() {
				select {
				case <-flushTimer.C:
				default:
				}
			}
			flushTimer = nil
			flushC = nil
		}
	}

	for {
		if t.dead {
			return
		}
		select {
		case w := <-t.writeCh:
			t.lastUsedMs.Store(time.Now().UnixMilli())
			o := t.applyTx(w.req)
			if o.err != nil {
				w.reply <- o
				break
			}
			if o.readOnly {
				// durable-read gate: a read-only /tx must not observe
				// applied-but-unshipped writes. Park until the watermark covers
				// the basis, exactly like /query.
				basis, err := t.currentCommitEnd()
				if err != nil {
					w.reply <- txOutcome{err: err}
					t.failLocked(err)
					return
				}
				if basis <= t.durableOff {
					w.reply <- o
					break
				}
				t.readWaits = append(t.readWaits, readWait{end: basis, txReply: w.reply, txOut: o})
				t.cutSegment()
				break
			}
			// a real commit (not a dedup replay) writes WAL bytes this gen
			if !o.dup {
				t.hasWrites = true
			}
			// end=-1: "covered by the next segment cut". Dedup hits too: if
			// a cut then finds no new bytes, every pending entry is provably
			// a dup (a real commit always advances the commit end) and is
			// re-anchored to submittedOff.
			t.pending = append(t.pending, pendingAck{end: -1, reply: w.reply, outcome: o})
			t.batchOpen = true
			armFlush()
			t.metrics.Commits.Add(1)
			t.heatCommits.Add(1)
			if t.walFileSize()-t.submittedOff >= t.opts.MaxSegBytes {
				t.cutSegment()
				disarmFlush()
			}
			t.maybeCheckpoint()
			if t.dead {
				return
			}
		case r := <-t.readCh:
			t.lastUsedMs.Store(time.Now().UnixMilli())
			if t.leaseExpiring() {
				r.reply <- readOutcome{err: ErrNotOwner}
				break
			}
			// durable-reads: the read's basis is the current applied commit
			// end. Serve at once when that is already durable; otherwise
			// force a cut and park until the watermark passes it.
			basis, err := t.currentCommitEnd()
			if err != nil {
				r.reply <- readOutcome{err: err}
				t.failLocked(err)
				return
			}
			if basis <= t.durableOff {
				res, err := t.runQuery(r.stmt)
				r.reply <- readOutcome{res, err}
				if err == nil {
					t.metrics.Queries.Add(1)
				}
				break
			}
			t.readWaits = append(t.readWaits, readWait{end: basis, w: r})
			t.cutSegment() // ship pending bytes so the watermark can pass
		case <-flushC:
			flushTimer = nil
			flushC = nil
			t.cutSegment()
		case sd := <-t.segDone:
			t.onSegDone(sd)
			if t.dead {
				return
			}
		case f := <-t.ctlCh:
			f()
			if t.dead {
				return
			}
		case <-hb.C:
			t.progressMs.Store(time.Now().UnixMilli())
			if !t.claimed {
				continue
			}
			if t.opts.IdleRelease > 0 &&
				time.Now().UnixMilli()-t.lastUsedMs.Load() > t.idleReleaseMs() {
				t.shutdown(true, "") // release so the next acquire is instant
				return
			}
			// slow ownership re-verify (defense in depth): a manifest that no
			// longer names us means someone took over without waiting for the
			// pod lease. Poison rather than keep writing into a dead gen.
			t.hbTicks++
			// ownership re-verify every ~30s per tenant (330 GET/s at 10k
			// tenants, spread across 256 shards). Defense in depth; the lease
			// gates are the primary fence.
			if t.hbTicks >= 30 {
				t.hbTicks = 0
				t.reverifyOwnership()
				if t.dead {
					return
				}
			}
		}
	}
}

// idleReleaseMs adds up to 50% hash jitter to IdleRelease so a fleet of
// tenants created in one burst does not release in one burst.
func (t *Tenant) idleReleaseMs() int64 {
	base := t.opts.IdleRelease.Milliseconds()
	h := fnv.New32a()
	h.Write([]byte(t.ID))
	return base + int64(h.Sum32()%256)*base/512
}

// reverifyOwnership reads the manifest and poisons the tenant when it no
// longer names this process as owner. Runs on the actor loop, rarely.
func (t *Tenant) reverifyOwnership() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	mf, _, err := t.store.GetManifest(ctx, t.ID)
	if err != nil {
		return // transient; the next tick re-checks
	}
	if mf == nil || mf.Owner != t.self || mf.Epoch != t.claimEpoch || mf.Instance != t.claimInst {
		t.failLocked(fmt.Errorf("%w: manifest no longer names this incarnation", ErrNotOwner))
	}
}

// currentCommitEnd is the byte offset of the end of the last committed
// transaction, per the incremental WAL parser.
func (t *Tenant) currentCommitEnd() (int64, error) {
	wal, err := t.readWALTail()
	if err != nil {
		return 0, err
	}
	return t.parser.CommitEnd(wal), nil
}

func (t *Tenant) walFileSize() int64 {
	if fi, err := os.Stat(t.walPath); err == nil {
		return fi.Size()
	}
	return 0
}

// readWALTail returns the WAL bytes the parser still needs: the whole file
// for a cold parser, else only the tail from the last verified commit frame.
func (t *Tenant) readWALTail() ([]byte, error) {
	size := t.walFileSize()
	if size == 0 {
		return nil, nil
	}
	off := int64(0)
	if t.parser.valid {
		off = t.parser.commitOff
	}
	if off >= size {
		return nil, nil
	}
	buf := make([]byte, size-off)
	if _, err := t.wal.ReadAt(buf, off); err != nil {
		return nil, err
	}
	return buf, nil
}

func (t *Tenant) leaseExpiring() bool {
	if t.zombie.Load() {
		return true
	}
	if t.clock != nil {
		if m, ok := t.clock.(*Manager); ok && t.claimed && t.claimGen != m.leaseGen.Load() {
			return true // born before a lapse sweep: fenced regardless of registry
		}
		return t.clock.leaseExpiring()
	}
	return time.Now().UnixMilli() > t.mf.OwnerUntilMs-int64(t.opts.Lease/time.Millisecond)/3
}

func (t *Tenant) applyTx(req TxRequest) (o txOutcome) {
	if t.leaseExpiring() {
		o.err = ErrNotOwner
		return o
	}
	// read-only: nothing to persist, nothing to dedup; ack straight away.
	// A retry simply re-executes. Durability is vacuous for pure reads.
	if allReadOnly(req.Stmts) {
		o.readOnly = true
	}
	ctx := context.Background()
	tx, err := t.db.BeginTx(ctx, nil)
	if err != nil {
		o.err = err
		return o
	}
	defer tx.Rollback()
	if req.Client != "" && req.TxID != "" && !o.readOnly {
		var raw, fp string
		err := tx.QueryRowContext(ctx, `SELECT result, reqfp FROM _hw_dedup WHERE client=? AND txid=?`,
			req.Client, req.TxID).Scan(&raw, &fp)
		if err == nil {
			if fp != "" && req.FP != "" && fp != req.FP {
				o.err = errors.New("idempotency key reused with a different request")
				return o
			}
			dec := json.NewDecoder(strings.NewReader(raw))
			dec.UseNumber()
			if dec.Decode(&o.results) != nil {
				o.err = errors.New("corrupt dedup entry")
				return o
			}
			o.dup = true
			t.metrics.DedupHits.Add(1)
			return o
		}
		if !errors.Is(err, sql.ErrNoRows) {
			o.err = err
			return o
		}
	}
	for i, st := range req.Stmts {
		res, err := execStatement(tx, st)
		if err != nil {
			o.err = err
			return o
		}
		if st.ExpectAffected != nil && res.Affected != *st.ExpectAffected {
			o.err = &ExpectFailedError{Index: i, What: fmt.Sprintf("affected %d != %d", res.Affected, *st.ExpectAffected)}
			return o
		}
		if st.ExpectScalarInt != nil {
			var got int64
			ok := len(res.Rows) > 0 && len(res.Rows[0]) > 0
			if ok {
				switch v := res.Rows[0][0].(type) {
				case int64:
					got = v
				case json.Number:
					got, _ = v.Int64()
				default:
					ok = false
				}
			}
			if !ok || got != *st.ExpectScalarInt {
				o.err = &ExpectFailedError{Index: i, What: fmt.Sprintf("scalar %d != %d", got, *st.ExpectScalarInt)}
				return o
			}
		}
		o.results = append(o.results, res)
	}
	if req.Client != "" && req.TxID != "" && !o.readOnly {
		raw, _ := json.Marshal(o.results)
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO _hw_dedup(client, txid, result, reqfp) VALUES(?,?,?,?)`,
			req.Client, req.TxID, string(raw), req.FP); err != nil {
			o.err = err
			return o
		}
	}
	if err := tx.Commit(); err != nil {
		o.err = err
		return o
	}
	return o
}

var errForbiddenSQL = errors.New("statement class not allowed through /tx")

// stripSQLComments removes leading /* */ and -- comments so classifiers
// see the first real keyword (a comment prefix must not smuggle PRAGMA or
// COMMIT past the guards).
func stripSQLComments(s string) string {
	for {
		t := strings.TrimSpace(s)
		switch {
		case strings.HasPrefix(t, "--"):
			i := strings.IndexByte(t, '\n')
			if i < 0 {
				return ""
			}
			s = t[i+1:]
		case strings.HasPrefix(t, "/*"):
			i := strings.Index(t, "*/")
			if i < 0 {
				return ""
			}
			s = t[i+2:]
		default:
			return t
		}
	}
}

// forbiddenTXSQL rejects statement classes that would break the WAL-shipping
// or dedup invariants: transaction control (an explicit COMMIT escapes the
// wrapper's atomicity), session state (journal/WAL pragmas change the byte
// stream the parser reasons about), ATTACH (cross-database access), and the
// internal dedup table.
func forbiddenTXSQL(sqlText string) bool {
	ls := strings.ToLower(stripSQLComments(sqlText))
	for _, p := range []string{"pragma", "attach", "detach", "begin", "commit", "rollback",
		"savepoint", "release", "vacuum", "reindex", "analyze"} {
		if strings.HasPrefix(ls, p) {
			return true
		}
	}
	return strings.Contains(ls, "_hw_dedup")
}

func execStatement(tx *sql.Tx, st Statement) (QueryResult, error) {
	ctx := context.Background()
	if !singleStatement(st.SQL) {
		return QueryResult{}, errors.New("one statement per Statement")
	}
	if forbiddenTXSQL(st.SQL) {
		return QueryResult{}, errForbiddenSQL
	}
	args := normalizeArgs(st.Args)
	if isQuerySQL(st.SQL) {
		rows, err := tx.QueryContext(ctx, st.SQL, args...)
		if err != nil {
			return QueryResult{}, err
		}
		defer rows.Close()
		qr, err := drainRows(rows)
		if err != nil {
			return qr, err
		}
		if qr.Affected == 0 && len(qr.Rows) > 0 {
			qr.Affected = int64(len(qr.Rows)) // DML ... RETURNING
		}
		return qr, nil
	}
	res, err := tx.ExecContext(ctx, st.SQL, args...)
	if err != nil {
		return QueryResult{}, err
	}
	n, _ := res.RowsAffected()
	return QueryResult{Affected: n}, nil
}

func (t *Tenant) runQuery(st Statement) (QueryResult, error) {
	// read-only is enforced by SQLite (query_only connection), not by
	// textual classification: INSERT..RETURNING and writable WITH slip
	// past prefix checks, and this path never ships their bytes.
	if !singleStatement(st.SQL) {
		return QueryResult{}, errors.New("one statement per Statement")
	}
	ls := strings.ToLower(stripSQLComments(st.SQL))
	if !strings.HasPrefix(ls, "select") && !strings.HasPrefix(ls, "with") &&
		!strings.HasPrefix(ls, "values") && !strings.HasPrefix(ls, "explain") {
		return QueryResult{}, errors.New("statement class not allowed through /query")
	}
	if sqlContainsDML(ls) || strings.Contains(ls, "_hw_dedup") {
		return QueryResult{}, errors.New("statement class not allowed through /query")
	}
	rows, err := t.roDB.QueryContext(context.Background(), st.SQL, normalizeArgs(st.Args)...)
	if err != nil {
		return QueryResult{}, err
	}
	defer rows.Close()
	return drainRows(rows)
}

func singleStatement(s string) bool {
	// reject statement smuggling: at most a trailing semicolon
	if i := strings.IndexByte(s, ';'); i >= 0 && strings.TrimSpace(s[i+1:]) != "" {
		return false
	}
	return true
}

func isQuerySQL(s string) bool {
	ls := strings.ToLower(stripSQLComments(s))
	ls = strings.TrimLeft(ls, "(")
	if strings.Contains(ls, " returning ") || strings.HasSuffix(ls, " returning") {
		return true // RETURNING yields rows even for INSERT/UPDATE/DELETE
	}
	return strings.HasPrefix(ls, "select") || strings.HasPrefix(ls, "with") ||
		strings.HasPrefix(ls, "explain") || strings.HasPrefix(ls, "values")
}

func drainRows(rows *sql.Rows) (QueryResult, error) {
	cols, err := rows.Columns()
	if err != nil {
		return QueryResult{}, err
	}
	out := QueryResult{Cols: cols}
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return out, err
		}
		for i, v := range vals {
			vals[i] = jsonValue(v)
		}
		out.Rows = append(out.Rows, vals)
	}
	return out, rows.Err()
}

func jsonValue(v any) any {
	switch x := v.(type) {
	case []byte:
		return string(x)
	case time.Time:
		return x.UTC().Format(time.RFC3339Nano)
	default:
		return v
	}
}

func normalizeArgs(args []any) []any {
	out := make([]any, len(args))
	for i, a := range args {
		switch v := a.(type) {
		case json.Number:
			if n, err := v.Int64(); err == nil {
				out[i] = n
			} else if f, err := v.Float64(); err == nil {
				out[i] = f
			} else {
				out[i] = v.String()
			}
		case map[string]any:
			if s, ok := v["$b64"].(string); ok {
				b, _ := base64.StdEncoding.DecodeString(s)
				out[i] = b
			} else {
				raw, _ := json.Marshal(v)
				out[i] = string(raw)
			}
		default:
			out[i] = a
		}
	}
	return out
}

// cutSegment parses the WAL, cuts the committed byte range since the last
// cut into one segment, and submits its PUT. Actor-loop only.
//
// The segment bytes come from the FILE at [submittedOff, end): the
// incremental parser cache (shared with the durable-read path) may run
// ahead of submittedOff, so the parser's tail slice is never the source.
func (t *Tenant) cutSegment() {
	if !t.batchOpen {
		return
	}
	t.batchOpen = false
	end, err := t.currentCommitEnd()
	if err != nil {
		t.failLocked(fmt.Errorf("wal parse: %w", err))
		return
	}
	if end <= t.submittedOff {
		// No new committed bytes: every pending entry is provably a dedup hit
		// (a real commit always advances the commit end). Their originals are
		// already inside cut segments; ack them once the watermark reaches
		// submittedOff. A real commit in this state means the WAL parser
		// disagrees with SQLite — never ack that.
		for _, pa := range t.pending {
			if !pa.outcome.dup {
				t.failLocked(fmt.Errorf("wal parser: committed tx produced no new bytes"))
				return
			}
		}
		for i := range t.pending {
			t.pending[i].end = t.submittedOff
		}
		t.advanceWatermark()
		return
	}
	if t.leaseExpiring() {
		t.failLocked(fmt.Errorf("%w: cannot flush inside lease margin", ErrNotOwner))
		return
	}
	select {
	case t.sem <- struct{}{}:
	default:
		// inflight cap reached: never block the actor (it is the one who
		// releases sem). Keep the batch open; the cut retries on the next
		// segDone or the next flush tick.
		t.batchOpen = true
		return
	}
	data := make([]byte, end-t.submittedOff)
	if _, err := t.wal.ReadAt(data, t.submittedOff); err != nil {
		t.failLocked(fmt.Errorf("wal read: %w", err))
		return
	}
	s := &segment{
		key:  segKey(t.prefix, t.ID, t.mf.Instance, t.mf.Gen, t.mf.Epoch, t.submittedOff),
		data: data, start: t.submittedOff, end: end,
	}
	for _, pa := range t.pending {
		s.waiters = append(s.waiters, pa.reply)
		s.results = append(s.results, pa.outcome)
	}
	t.pending = nil
	t.submittedOff = end
	t.segsSinceC++
	t.segs = append(t.segs, s)
	t.metrics.SegPuts.Add(1)
	t.metrics.SegBytes.Add(int64(len(data)))
	t.metrics.InflightSegs.Add(1)
	compress := t.opts.Compress
	go func() {
		// compress off the actor, once per segment (not per attempt). The
		// key keeps the RAW WAL offset; recovery uses decoded lengths.
		stored := segmentObject(s.data, compress)
		t.metrics.SegStoredBytes.Add(int64(len(stored)))
		var err error
		for attempt := 0; ; attempt++ {
			// The write fence is re-read PER ATTEMPT, not captured at cut
			// time: while the pod lease keeps heartbeating, the deadline
			// moves forward. During an S3 throttle storm a segment that
			// cannot land inside the margin captured at cut time would
			// poison the tenant even though we still hold the lease — and
			// the takeover storm makes the throttle worse (death spiral
			// seen in the 10k ramp). Retrying while the lease is alive is
			// safe: the ack gate (leaseExpiring in advanceWatermark) still
			// blocks every ack the moment the lease actually lapses, and a
			// late-landing segment is just unreplayed garbage under the old
			// generation (the sweeper reaps it).
			fence := t.writeDeadline()
			attemptTimeout := t.opts.PutTimeout
			if d := time.Until(fence); d < attemptTimeout {
				attemptTimeout = d
			}
			if attemptTimeout <= 0 {
				// fence in the past: the lease is gone or expiring. Retry only
				// if the lease is still alive (heartbeats renewing); otherwise
				// give up — the watchdog/lapse path will fail-stop us.
				if t.leaseExpiring() {
					if err == nil {
						err = fmt.Errorf("segment put: lease expired")
					}
					break
				}
				attemptTimeout = t.opts.PutTimeout
			}
			// stop retrying once the tenant is dead: the outcome is moot
			select {
			case <-t.done:
				if err == nil {
					err = ErrClosed
				}
				goto done
			default:
			}
			ctx, cancel := context.WithTimeout(context.Background(), attemptTimeout)
			err = t.store.PutSegment(ctx, s.key, stored)
			cancel()
			if err == nil {
				break
			}
			// back off hard on throttle: SlowDown means the bucket is
			// saturated and hammering it prolongs the storm
			if isThrottle(err) {
				time.Sleep(time.Duration(500*(1<<min(attempt, 4))) * time.Millisecond)
				continue
			}
			backoff := 50 * (1 << min(attempt, 4)) // 50ms..800ms cap
			time.Sleep(time.Duration(backoff) * time.Millisecond)
		}
	done:
		select {
		case t.segDone <- segDone{seg: s, err: err}:
		case <-t.done:
		}
	}()
}

// isThrottle reports whether an S3 error is a throttle (503 SlowDown or
// context-deadline churn from SDK-internal throttle retries).
func isThrottle(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "SlowDown") || strings.Contains(s, "StatusCode: 503")
}

// progressReader wraps a file and reports every successful Read.
type progressReader struct {
	f      *os.File
	lastNs *atomic.Int64
	onRead func()
}

func (p *progressReader) Read(b []byte) (int, error) {
	n, err := p.f.Read(b)
	if n > 0 {
		p.lastNs.Store(time.Now().UnixNano())
		if p.onRead != nil {
			p.onRead()
		}
	}
	return n, err
}

// Seek delegates to the underlying file: the AWS SDK type-asserts the body
// to io.Seeker for signing and retry rewinds. Hiding it would break
// SDK-internal retries.
func (p *progressReader) Seek(offset int64, whence int) (int64, error) {
	return p.f.Seek(offset, whence)
}

// putBaseWithStallCancel uploads f via PutBase with two bounds: a stall
// cancel (no bytes read for stallTimeout → dead connection, retry on a
// fresh one) and a hard per-attempt budget scaled to the object size
// (30s floor + size at 1MB/s floor throughput). The hard cap exists so a
// trickling upload cannot hold the checkpoint semaphore for hours
// (measured: 64B/s "progress" ran 4.5min; a 2GB base at 3KB/s would
// take days). A base that cannot sustain 1MB/s is retried on a fresh
// connection; after 4 capped attempts the tenant poisons and a peer with
// a healthier path takes over.
func (t *Tenant) putBaseWithStallCancel(key string, f *os.File, size int64, stallTimeout time.Duration) error {
	var last atomic.Int64
	last.Store(time.Now().UnixNano())
	budget := 30*time.Second + time.Duration(size/(1<<20))*time.Second
	pctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(budget))
	defer cancel()
	done := make(chan struct{})
	defer close(done)
	go func() {
		tick := time.NewTicker(stallTimeout / 2)
		defer tick.Stop()
		for {
			select {
			case <-done:
				return
			case <-pctx.Done():
				return
			case <-tick.C:
				if time.Since(time.Unix(0, last.Load())) > stallTimeout {
					cancel()
					return
				}
			}
		}
	}()
	pr := &progressReader{f: f, lastNs: &last, onRead: func() {
		t.progressMs.Store(time.Now().UnixMilli())
	}}
	return t.store.PutBase(pctx, key, pr, size)
}

// prepareBaseUpload returns the file and size to upload as the base image.
// With compression on it writes one zstd frame to a temp file next to the
// database. The reads bump progressMs, so a long compression of a large
// base does not look like a wedged actor. Any compression failure (for
// example a full disk) falls back to the raw file: raw is always a valid
// base, and a checkpoint must not fail for an optimization.
func (t *Tenant) prepareBaseUpload(f *os.File, size int64) (*os.File, int64, func()) {
	noop := func() {}
	if !t.opts.Compress {
		return f, size, noop
	}
	logf := func(format string, args ...any) {
		if t.metrics.Logger != nil {
			t.metrics.Logger.Printf(format, args...)
		}
	}
	tmp, err := os.CreateTemp(t.dir, "base-*.zst")
	if err != nil {
		logf("tenant %s: base compress: %v (uploading raw)", t.ID, err)
		return f, size, noop
	}
	var once sync.Once
	cleanup := func() {
		once.Do(func() {
			tmp.Close()
			os.Remove(tmp.Name())
		})
	}
	var last atomic.Int64
	src := &progressReader{f: f, lastNs: &last, onRead: func() {
		t.progressMs.Store(time.Now().UnixMilli())
	}}
	stored, err := compressStream(tmp, src, size)
	if err != nil {
		cleanup()
		logf("tenant %s: base compress: %v (uploading raw)", t.ID, err)
		return f, size, noop
	}
	return tmp, stored, cleanup
}

// writeDeadline is the absolute fence for data-plane PUTs: the pod lease
// clock when attached to a manager, the manifest clock otherwise (tests).
func (t *Tenant) writeDeadline() time.Time {
	if t.clock != nil {
		return t.clock.leaseWriteDeadline()
	}
	return time.UnixMilli(t.mf.OwnerUntilMs).Add(-t.opts.Lease / 3)
}

// onSegDone marks a segment durable and advances the contiguous watermark.
// Acks are always in offset order: a later segment never acknowledges
// before an earlier one. Actor-loop only.
func (t *Tenant) onSegDone(sd segDone) {
	<-t.sem
	t.metrics.InflightSegs.Add(-1)
	if sd.err != nil {
		t.failLocked(fmt.Errorf("segment put %s: %w", sd.seg.key, sd.err))
		return
	}
	sd.seg.done = true
	t.advanceWatermark()
	if t.batchOpen {
		t.cutSegment() // retry a cut deferred by the inflight cap
	}
}

func (t *Tenant) advanceWatermark() {
	for len(t.segs) > 0 && t.segs[0].done {
		// P0: never acknowledge a durable PUT after the lease lapsed. The
		// bytes are in S3 (a later acquirer may replay them), but the
		// client must NOT hear success from a fenced-off owner: it retries
		// and gets exactly-once via dedup on the new owner.
		if t.leaseExpiring() {
			t.failLocked(fmt.Errorf("%w: durable segment arrived after lease margin", ErrNotOwner))
			return
		}
		s := t.segs[0]
		t.segs = t.segs[1:]
		t.durableOff = s.end
		for i, ch := range s.waiters {
			ch <- s.results[i]
		}
	}
	rest := t.pending[:0]
	for _, pa := range t.pending {
		if pa.end >= 0 && pa.end <= t.durableOff {
			// same gate as the segment-waiter path above: a fenced-off owner
			// must never ack, even for watermarked-but-uncut commits
			if t.leaseExpiring() {
				t.failLocked(fmt.Errorf("%w: pending ack after lease margin", ErrNotOwner))
				return
			}
			pa.reply <- pa.outcome
		} else {
			rest = append(rest, pa)
		}
	}
	t.pending = rest
	if len(t.readWaits) > 0 {
		var waiting []readWait
		for _, rw := range t.readWaits {
			// release only when the basis is durable AND no newer commit is
			// still in flight: executing now must not expose applied-but-
			// unshipped rows (a newer commit that never lands is a phantom)
			curEnd, err := t.currentCommitEnd()
			if err != nil {
				t.failLocked(err)
				return
			}
			fullyDurable := curEnd <= t.durableOff && !t.batchOpen && len(t.segs) == 0
			if rw.end <= t.durableOff && fullyDurable {
				if rw.txReply != nil {
					rw.txReply <- rw.txOut // parked /tx outcome
					continue
				}
				res, err := t.runQuery(rw.w.stmt)
				rw.w.reply <- readOutcome{res, err}
				if err == nil {
					t.metrics.Queries.Add(1)
				}
			} else {
				waiting = append(waiting, rw)
			}
		}
		t.readWaits = waiting
	}
}

func (t *Tenant) maybeCheckpoint() {
	if t.submittedOff < t.opts.CheckpointByte && t.segsSinceC < t.opts.MaxSegments {
		return
	}
	t.checkpointLocked("")
}

// checkpointLocked: flush everything, drain in order, fold WAL into a new
// base image, CAS the manifest. The pod lease keeps heartbeating in the
// manager while we drain, so a slow drain cannot lapse the lease.
// Actor-loop only.
func (t *Tenant) checkpointLocked(newClass string) bool {
	t.progressMs.Store(time.Now().UnixMilli())
	t.cutSegment()
	// bound concurrent checkpoints pod-wide: a mass idle release must not
	// fire thousands of drains+uploads at S3 at once. Blocking is safe:
	// the pod lease heartbeats independently of this actor.
	if t.ckptSem != nil {
		if !t.claimed {
			// claim path: the CAS below IS the acquisition. Never skip it,
			// and never queue behind slow RELEASE checkpoints — claims use
			// their own semaphore (8 slow uploads froze takeovers).
			sem := t.claimSem
			if sem == nil {
				sem = t.ckptSem
			}
			tick := time.NewTicker(5 * time.Second)
			for {
				select {
				case sem <- struct{}{}:
					tick.Stop()
					goto acquiredClaim
				case <-tick.C:
					t.progressMs.Store(time.Now().UnixMilli())
				}
			}
		acquiredClaim:
			defer func() { <-sem }()
		} else {
			timer := time.NewTimer(t.opts.Lease) // never wait past the gate margin
			select {
			case t.ckptSem <- struct{}{}:
				timer.Stop()
				defer func() { <-t.ckptSem }()
			case <-timer.C:
				// contention storm (mass shutdown/release): SKIP the checkpoint.
				// Durability is unaffected — the WAL segments are already in S3;
				// the next acquirer replays them. The base fold is an
				// optimization, not a correctness requirement.
				t.metrics.Logger.Printf("checkpoint %s skipped: drain semaphore saturated", t.ID)
				return true
			}
		}
	}
	t.progressMs.Store(time.Now().UnixMilli()) // sem wait can be long
	// drain in-flight segments
	for len(t.segs) > 0 {
		sd := <-t.segDone
		<-t.sem
		t.progressMs.Store(time.Now().UnixMilli())
		if sd.err != nil {
			t.failLocked(fmt.Errorf("drain: %w", sd.err))
			return false
		}
		sd.seg.done = true
		t.advanceWatermark()
	}
	if t.leaseExpiring() {
		t.failLocked(fmt.Errorf("%w: lease expired during checkpoint drain", ErrNotOwner))
		return false
	}
	// the checkpoint is on the actor loop: every S3 call here must have a
	// per-attempt bound with retries, or one stalled connection freezes the
	// tenant for the full timeout and trips the watchdog
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	t.progressMs.Store(time.Now().UnixMilli()) // upload block can be long
	if _, err := t.db.ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		t.failLocked(fmt.Errorf("wal checkpoint: %w", err))
		return false
	}
	newGen := t.mf.Gen + 1
	newBaseKey := baseKey(t.prefix, t.ID, t.mf.Instance, newGen, t.mf.Epoch)
	if t.claimAttempt != "" {
		// acquisition claims upload to a per-attempt key: a racing acquirer
		// with the same gen+epoch can never overwrite our base, and vice
		// versa. The winner's CAS names its exact object.
		newBaseKey = baseKeyAttempt(t.prefix, t.ID, t.mf.Instance, newGen, t.mf.Epoch, t.claimAttempt)
	}
	f, err := os.Open(t.dbPath)
	if err != nil {
		t.failLocked(err)
		return false
	}
	fi, _ := f.Stat()
	t.progressMs.Store(time.Now().UnixMilli())
	// compress once into a temp file, then upload that file: retries and
	// SDK rewinds need a seekable body, and the base can be GBs, so it
	// never sits in memory
	upload, uploadSize, cleanupUpload := t.prepareBaseUpload(f, fi.Size())
	defer cleanupUpload()
	// Progress-bounded upload: an attempt is canceled only when no bytes
	// move for stallTimeout, never for being merely slow — a multi-GB base
	// on a degraded link must still finish (a fixed 20s cap
	// made large tenants permanently uncheckpointable).
	var putErr error
	for attempt := 0; attempt < 4; attempt++ {
		if _, serr := upload.Seek(0, 0); serr != nil {
			f.Close()
			t.failLocked(fmt.Errorf("base seek: %w", serr))
			return false
		}
		putErr = t.putBaseWithStallCancel(newBaseKey, upload, uploadSize, 10*time.Second)
		t.progressMs.Store(time.Now().UnixMilli())
		if putErr == nil {
			break
		}
	}
	cleanupUpload() // free the temp file before the CAS phase
	if putErr != nil {
		f.Close()
		t.failLocked(fmt.Errorf("base put: %w", putErr))
		return false
	}
	f.Close()
	t.metrics.BaseBytes.Add(fi.Size())
	t.metrics.BaseStoredBytes.Add(uploadSize)
	oldGen := t.mf.Gen
	oldBase := t.mf.BaseKey
	oldIID := t.mf.Instance
	nm := *t.mf
	nm.Gen = newGen
	nm.BaseKey = newBaseKey
	// the new generation holds only objects this incarnation writes, so
	// the format follows our own write setting
	nm.Format = manifestFormatRaw
	if t.opts.Compress {
		nm.Format = manifestFormatZstd
	}
	if newClass != "" {
		nm.Class = newClass
		t.classHint.Store(newClass)
	}
	// OwnerUntilMs is informational only now: liveness comes from the pod
	// lease object. Set it so debug readers see a plausible expiry.
	nm.OwnerUntilMs = t.writeDeadline().Add(t.opts.Lease / 3).UnixMilli()
	nm.UpdatedMs = time.Now().UnixMilli()
	// the CAS gets its own budget, independent of the 90s checkpoint ctx:
	// a legitimately long base upload (multi-GB, slow link) must not starve
	// the publish step. Fencing still comes from the write-deadline check
	// below and the ack-time lease gate, not from this context.
	casCtx, casCancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer casCancel()
	var etag string
	for attempt := 0; ; attempt++ {
		t.progressMs.Store(time.Now().UnixMilli()) // CAS retries can take seconds
		cctx, ccancel := context.WithTimeout(casCtx, 15*time.Second)
		etag, err = t.store.PutManifestCAS(cctx, t.ID, &nm, t.etag)
		ccancel()
		if err == nil {
			break
		}
		if errors.Is(err, ErrCAS) {
			// ErrCAS is not a definite loss when a retry can follow an
			// ambiguous attempt that actually landed. Re-read the manifest
			// before touching the base we uploaded: only delete it when the
			// manifest does NOT name it (we genuinely lost).
			if t.claimAttempt != "" {
				if cur, _, rerr := t.store.GetManifest(casCtx, t.ID); rerr == nil && cur != nil && cur.BaseKey != newBaseKey {
					ctx2, cancel2 := context.WithTimeout(context.Background(), 15*time.Second)
					_ = t.store.DeleteObjects(ctx2, newBaseKey)
					cancel2()
				}
				t.claimAttempt = ""
			}
			t.failLocked(fmt.Errorf("checkpoint cas: %w", err))
			return false
		}
		// transient error: the CAS outcome is AMBIGUOUS (it may have landed).
		// NEVER delete the base: if the CAS actually landed, the manifest names
		// it and deleting it corrupts the tenant. Leave it; the sweeper or the
		// loser-cleanup path reaps it if we truly lost.
		if attempt >= 5 || time.Now().After(t.writeDeadline()) {
			t.claimAttempt = ""
			t.failLocked(fmt.Errorf("checkpoint cas: %w", err))
			return false
		}
		time.Sleep(time.Duration(200*(1<<attempt)) * time.Millisecond)
	}
	t.claimAttempt = ""
	t.etag = etag
	t.mf = &nm
	t.submittedOff = 0
	t.durableOff = 0
	t.segsSinceC = 0
	t.hasWrites = false
	t.parser.Reset()
	// the new base image contains everything the local DB has committed,
	// including any pending entries whose segments were deferred (sem cap)
	// or never cut: the base IS their durability. Gate first: the CAS
	// above proved ownership, but a zombie flag set mid-checkpoint must
	// still stop the acks.
	if t.leaseExpiring() {
		t.failLocked(fmt.Errorf("%w: checkpoint ack after lease margin", ErrNotOwner))
		return false
	}
	for _, pa := range t.pending {
		pa.reply <- pa.outcome
	}
	t.pending = nil
	for _, rw := range t.readWaits {
		if rw.txReply != nil {
			rw.txReply <- rw.txOut
			continue
		}
		res, err := t.runQuery(rw.w.stmt)
		rw.w.reply <- readOutcome{res, err}
		if err == nil {
			t.metrics.Queries.Add(1)
		}
	}
	t.readWaits = nil
	// re-open wal handle: TRUNCATE may have replaced the file
	t.wal.Close()
	if f2, err := os.OpenFile(t.walPath, os.O_CREATE|os.O_RDONLY, 0o644); err == nil {
		t.wal = f2
	}
	t.metrics.Checkpoints.Add(1)
	go func(oldBase, oldIID string, oldGen int64) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		keys := []string{oldBase}
		if segs, err := t.store.ListSegments(ctx, t.ID, oldIID, oldGen); err == nil {
			for _, sg := range segs {
				keys = append(keys, sg.Key)
			}
		}
		_ = t.store.DeleteObjects(ctx, keys...)
	}(oldBase, oldIID, oldGen)
	return true
}

func (t *Tenant) shutdown(release bool, newClass string) {
	if t.dead {
		return
	}
	// checkpoint only when there is real work to flush. A tenant that only
	// bootstrapped (schema, no user commits) has nothing to ship; the idle
	// release of 10k such tenants must not storm S3 with 10k checkpoint CASs.
	// checkpoint only when there is real work to flush. hasWrites is set by
	// any real (non-read-only, non-dup) commit this gen. The other three
	// conditions are the in-flight tail of the same thing (a submitted-but-
	// unacked segment is still a write).
	if t.hasWrites || t.submittedOff > 0 || t.batchOpen || len(t.segs) > 0 {
		if !t.checkpointLocked(newClass) {
			return
		}
	}
	if release {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		nm := *t.mf
		nm.Owner = ""
		nm.OwnerUntilMs = 0
		if newClass != "" {
			nm.Class = newClass
		}
		nm.UpdatedMs = time.Now().UnixMilli()
		_, _ = t.store.PutManifestCAS(ctx, t.ID, &nm, t.etag) // best effort
	}
	t.failQueued(ErrClosed)
	t.finishClose()
}

// failLocked poisons the tenant: fail everything pending, close.
func (t *Tenant) failLocked(err error) {
	if t.dead {
		return
	}
	if t.metrics.Logger != nil {
		t.metrics.Logger.Printf("tenant %s poisoned: %v", t.ID, err)
	}
	t.deadErr = err
	t.failQueued(err)
	t.finishClose()
}

func (t *Tenant) failQueued(err error) {
	for _, pa := range t.pending {
		o := pa.outcome
		o.err = err
		pa.reply <- o
	}
	t.pending = nil
	for _, rw := range t.readWaits {
		if rw.txReply != nil {
			rw.txReply <- txOutcome{err: err}
			continue
		}
		rw.w.reply <- readOutcome{err: err}
	}
	t.readWaits = nil
	for _, s := range t.segs {
		for i, ch := range s.waiters {
			o := s.results[i]
			o.err = err
			ch <- o
		}
	}
	t.segs = nil
	for {
		select {
		case w := <-t.writeCh:
			w.reply <- txOutcome{err: err}
		default:
			goto reads
		}
	}
reads:
	for {
		select {
		case r := <-t.readCh:
			r.reply <- readOutcome{err: err}
		default:
			return
		}
	}
}

func (t *Tenant) finishClose() {
	if t.dead {
		return
	}
	t.dead = true
	t.db.Close()
	if t.roDB != nil {
		t.roDB.Close()
	}
	t.wal.Close()
	// publish the poison cooldown BEFORE done closes: once done is closed,
	// LocalTenant can drop us from the registry and an Acquire can start
	// a takeover. The cooldown must already be visible then.
	if t.onPoison != nil {
		t.onPoison(t)
	}
	close(t.done)
	if t.onClose != nil {
		t.onClose(t)
	}
}

// HeatCommits returns and resets the per-tenant commit counter.
func (t *Tenant) HeatCommits() int64 { return t.heatCommits.Swap(0) }

// quietSinceMs returns when the tenant last saw write activity.
func (t *Tenant) lastQuietSince() time.Duration {
	return time.Duration(time.Now().UnixMilli()-t.lastUsedMs.Load()) * time.Millisecond
}

// Class returns the manifest class, safe for non-actor readers.
func (t *Tenant) Class() string {
	if v := t.classHint.Load(); v != nil {
		return v.(string)
	}
	return ""
}

// DebugStatus returns actor internals for /status.
func (t *Tenant) DebugStatus() map[string]any {
	return map[string]any{
		"tenant": t.ID, "gen": t.mf.Gen, "epoch": t.mf.Epoch,
		"wal_submitted_bytes": t.submittedOff, "durable_watermark": t.durableOff,
		"pending_acks": len(t.pending), "inflight_segments": len(t.segs),
		"segments_since_checkpoint": t.segsSinceC, "alive": t.Alive(),
	}
}
