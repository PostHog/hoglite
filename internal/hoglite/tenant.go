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
	"runtime"
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
	// Compress makes the next generation a zstd generation (base and
	// every segment are zstd frames). A change applies at the next
	// checkpoint; the current generation keeps its format. Readers accept
	// both formats regardless of this flag. Turn it off for the first
	// rollout from a build without compression support, so pods of the old
	// build can still take over tenants during the rolling deploy.
	Compress bool
	// DeltaMaxChain turns on page-delta checkpoints (needs Compress): a
	// checkpoint uploads only the pages changed since the previous one,
	// and recovery applies the deltas to the base in order. After this
	// many deltas the next checkpoint uploads a full base and starts a new
	// chain. 0 turns deltas off: every checkpoint uploads a full base, in
	// format 0 or 1, which builds without delta support can read. Values
	// above the reader limit (1024) are clamped to it.
	DeltaMaxChain int
	// DeltaMaxRatio also starts a new chain when the stored bytes of all
	// deltas would exceed this fraction of the stored base. Recovery then
	// downloads at most (1 + DeltaMaxRatio) times the base.
	DeltaMaxRatio float64
	MaxReaders    int           // concurrent read snapshots per tenant (read-only pool size)
	ReadDrainWait time.Duration // checkpoint/close grace for in-flight reads before they are canceled
	PodMaxQueries int           // pod-wide cap on admitted queries; read by the Manager, not per tenant
	// PodMaxParkedBytes caps the estimated result bytes of reads that
	// wait for durability, across all tenants of a pod (reads.go); read by
	// the Manager. Zero selects PodMaxQueries x MaxResultBytes, a negative
	// value turns the cap off. A result larger than the cap is charged the
	// whole cap, so it parks only when nothing else is parked, and it
	// alone can pass the cap. A cap of at least MaxResultBytes is strict.
	PodMaxParkedBytes int64
	// Query guards (guards.go). Zero selects the default, a negative value
	// turns the guard off.
	QueryTimeout   time.Duration // execution deadline of a read (/query, read-only /tx)
	WriteTimeout   time.Duration // execution deadline of a write transaction (runs on the actor)
	MaxResultRows  int           // rows per read response (not for write transactions)
	MaxResultBytes int64         // estimated bytes of a response (reads and writes)
	// CacheKiB bounds the page cache of each SQLite connection of the
	// tenant (the write connection and each read connection), in KiB.
	// Zero selects the default, a negative value keeps the SQLite
	// default (2000 KiB). See memory.go.
	CacheKiB int
	// Usage accounting (usage.go); read by the Manager, not per tenant.
	// UsageTopN tenants appear in /metrics (zero selects the default, a
	// negative value lists none). UsageInterval is the sample period,
	// UsageHalfLife the decay of the rates; zero selects the default.
	UsageTopN     int
	UsageInterval time.Duration
	UsageHalfLife time.Duration
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
		// Chain cap: recovery fetches the stored deltas 8 at a time, so 32
		// deltas cost 4 GET round trips. With small deltas the chain then
		// saves about 32/(1+ratio) full uploads per rebase.
		DeltaMaxChain: 32,
		// Ratio 0.5: recovery downloads at most 1.5x the base. A higher
		// ratio saves fewer uploads than it adds to every cold acquire:
		// with deltas of size d, a rebase cycle uploads base*(1+r) over
		// r*base/d+1 checkpoints.
		DeltaMaxRatio:  0.5,
		MaxReaders:     4,
		ReadDrainWait:  2 * time.Second,
		PodMaxQueries:  defaultPodMaxQueries(),
		QueryTimeout:   defaultQueryTimeout,
		WriteTimeout:   defaultWriteTimeout,
		MaxResultRows:  defaultMaxResultRows,
		MaxResultBytes: defaultMaxResultBytes,
		CacheKiB:       defaultCacheKiB,
		UsageTopN:      defaultUsageTopN,
		UsageInterval:  defaultUsageInterval,
		UsageHalfLife:  defaultUsageHalfLife,
	}
}

// defaultPodMaxQueries allows two executing queries per usable CPU. Reads
// are mostly CPU-bound in the pure-Go SQLite. More concurrency only adds
// queueing inside the scheduler and takes CPU from the tenant actors.
func defaultPodMaxQueries() int {
	return max(4, 2*runtime.GOMAXPROCS(0))
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
	// poison fails the tenant after the reply: the write connection or
	// the WAL is in a state that the actor cannot trust (applyTx).
	poison error
}

// allReadOnly is the STRICT classifier for the no-durability fast path:
// only statements that cannot write. isQuerySQL is about "returns rows"
// (INSERT..RETURNING, writable WITH) and must not be reused here.
//
// Guards do not change the class. A guard does not write, and every prefix
// accepted here is also isQuerySQL, so ExpectAffected on a read compares
// the row count of the pinned snapshot. Such a transaction gets no dedup
// receipt: a retry executes the read again, as for every read-only /tx.
// A misclassified write cannot slip through: the reader path runs on a
// query_only connection, and SQLite refuses the write.
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
		if !readPrefix {
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

// readWork is one admitted read (/query or read-only /tx). It holds one
// per-tenant reader slot until its outcome is delivered or dropped, and
// one pod-wide query slot until its query finished. A result that waits
// for durability also holds a share of the pod park budget (reads.go).
type readWork struct {
	ctx   context.Context
	stmts []Statement
	tx    bool // read-only /tx: run every statement with its guards
	reply chan readOutcome
	// taken decides who returns the admission slots: the first party to
	// set it (the actor on receipt, or the submitter that gives up) owns
	// the release.
	taken       atomic.Bool
	released    atomic.Bool  // tenant reader slot returned (releaseSlots)
	podReleased atomic.Bool  // pod query slot returned (releasePodSlot)
	parkBytes   atomic.Int64 // park budget share not yet returned (releaseSlots)
}

type readOutcome struct {
	results []QueryResult
	err     error
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

	readWaits []readWait // read results parked until their basis is durable

	readSlots chan struct{}         // per-tenant read admission: one token per roDB connection
	qlim      *queryLimiter         // pod-wide query admission (manager-owned; nil = unbounded)
	readers   readerSet             // pinned read snapshots (open read transactions on roDB)
	pinCh     chan *pinnedRead      // queued pins for the reader workers; room for MaxReaders
	pool      readPool              // reader worker accounting for pinCh
	probe     *sql.Stmt             // pin probe prepared on roDB; actor-loop only
	readDone  chan readResult       // worker outcomes whose basis was not yet durable at pin time
	readHook  func(context.Context) // test hook: runs in the worker after the pin
	txHook    func()                // test hook: runs on the actor after the statements of a write
	foldHook  func()                // test hook: runs in foldWAL after a PASSIVE fold, before TRUNCATE
	// readAbandon carries parked reads whose caller gave up (the AfterFunc
	// of onReadDone). The actor drops their results.
	readAbandon chan *readWork
	// ckptDeferUntil delays size-triggered checkpoints after one was
	// skipped because reads did not drain. Actor-owned.
	ckptDeferUntil time.Time
	ckptWantSince  time.Time // first size-triggered checkpoint deferred for open reads

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

	// dbBytes is the size of the local database file after the last
	// checkpoint (or materialize). Between checkpoints SQLite writes only
	// the WAL, so a different size means the delta assumption broke; the
	// next checkpoint then uploads a full base.
	dbBytes int64

	onClose  func(*Tenant) // manager removes us from its registry
	onPoison func(*Tenant) // manager publishes the re-acquire cooldown; runs BEFORE done closes

	heatCommits atomic.Int64 // commits since last controller scan
	classHint   atomic.Value // string: manifest class, actor-owned write
	lastUsedMs  atomic.Int64 // both actor and manager read/write

	usage tenantUsage // execution accounting (usage.go)

	metrics *Metrics
}

type segDone struct {
	seg *segment
	err error
}

// readWait parks a computed read result until durableOff >= end (the
// read's basis: the commit end of the last applied transaction when its
// snapshot was pinned). P0-5: a read must never return applied-but-not-
// yet-durable state; after a crash such state vanishes and snapshot ids
// would be reused under an OCC basis that already saw them.
//
// The entry holds the tenant reader slot and the park budget share of w,
// so parked results stay bounded (reads.go). Read-only /tx outcomes park
// here the same way: they run on the reader path.
type readWait struct {
	end  int64
	w    *readWork
	out  readOutcome
	stop func() bool // stops the abandon callback of onReadDone
}

func openTenant(id string, dir string, mf *Manifest, etag string, store Backend, prefix, self, routeID string, opts TenantOpts, metrics *Metrics, clock leaseClock) (*Tenant, error) {
	if opts.MaxReaders <= 0 {
		opts.MaxReaders = 4
	}
	if opts.ReadDrainWait <= 0 {
		opts.ReadDrainWait = 2 * time.Second
	}
	opts.QueryTimeout = guardOpt(opts.QueryTimeout, defaultQueryTimeout)
	opts.WriteTimeout = guardOpt(opts.WriteTimeout, defaultWriteTimeout)
	opts.MaxResultRows = guardOpt(opts.MaxResultRows, defaultMaxResultRows)
	opts.MaxResultBytes = guardOpt(opts.MaxResultBytes, defaultMaxResultBytes)
	opts.CacheKiB = guardOpt(opts.CacheKiB, defaultCacheKiB)
	t := &Tenant{
		ID: id, store: store, prefix: prefix, opts: opts, self: self, routeID: routeID, dir: dir, clock: clock,
		dbPath: filepath.Join(dir, "db.sqlite"), walPath: filepath.Join(dir, "db.sqlite-wal"),
		mf: mf, etag: etag, claimEpoch: mf.Epoch, claimInst: mf.Instance,
		segDone: make(chan segDone, 256),
		sem:     make(chan struct{}, opts.Inflight),
		writeCh: make(chan *txWork, 4096),
		readCh:  make(chan *readWork, 4096),
		// Each outcome here still holds its tenant reader slot (until it
		// is delivered or dropped), so the buffer never fills and a worker
		// never waits for the actor.
		readDone:  make(chan readResult, opts.MaxReaders),
		readSlots: make(chan struct{}, opts.MaxReaders),
		pinCh:     make(chan *pinnedRead, opts.MaxReaders),
		ctlCh:     make(chan func(), 64),
		done:      make(chan struct{}),
		metrics:   metrics,

		// one message per parked read, and each parked read holds a slot
		readAbandon: make(chan *readWork, opts.MaxReaders),
	}
	if fi, err := os.Stat(t.dbPath); err == nil {
		t.dbBytes = fi.Size() // before SQLite opens it: base + chain as materialized
	}
	registerAuthorizerOnce()
	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=wal_autocheckpoint(0)"+
		"&_pragma=synchronous(OFF)&_pragma=busy_timeout(10000)&_pragma=foreign_keys(1)", t.dbPath)
	if opts.CacheKiB > 0 {
		// negative cache_size means KiB; the pragma runs before the
		// authorizer is installed, and clients cannot send PRAGMA
		dsn += fmt.Sprintf("&_pragma=cache_size(-%d)", opts.CacheKiB)
	}
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
	// one connection per reader slot: a pin on the actor never waits for
	// the pool (see pinRead)
	ro.SetMaxOpenConns(opts.MaxReaders)
	ro.SetMaxIdleConns(opts.MaxReaders)
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
	metrics.usage.register(&t.usage, id, time.Now())
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
	return t.SubmitTxCtx(context.Background(), req)
}

// SubmitTxCtx is SubmitTx with a request context. A read-only transaction
// runs on the reader path (off the actor) and ctx bounds its admission
// wait and its execution. Writes ignore ctx: once queued, a write runs to
// its durable outcome so a retry can never race it. Only the server-side
// WriteTimeout stops a write, and it rolls the write back (applyTx).
func (t *Tenant) SubmitTxCtx(ctx context.Context, req TxRequest) ([]QueryResult, bool, error) {
	if allReadOnly(req.Stmts) {
		o := t.submitRead(ctx, &readWork{stmts: req.Stmts, tx: true})
		return o.results, false, o.err
	}
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
	return t.SubmitQueryCtx(context.Background(), st)
}

// SubmitQueryCtx runs one read-only statement. ctx bounds the wait for a
// reader slot and a pod query slot, and cancels the running query.
func (t *Tenant) SubmitQueryCtx(ctx context.Context, st Statement) (QueryResult, error) {
	if err := checkQuerySQL(st.SQL); err != nil {
		return QueryResult{}, err
	}
	o := t.submitRead(ctx, &readWork{stmts: []Statement{st}})
	if o.err != nil {
		return QueryResult{}, o.err
	}
	return o.results[0], nil
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
	// The actor's hold on the usage record ends after its last charge:
	// a checkpoint or write that fails the tenant charges in a defer that
	// runs after finishClose (usage.go).
	defer t.metrics.usage.release(&t.usage)
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
			if o.poison != nil {
				w.reply <- o
				t.failLocked(o.poison)
				return
			}
			if o.err != nil {
				w.reply <- o
				break
			}
			if o.readOnly {
				// Not reachable: SubmitTxCtx sends every transaction that
				// allReadOnly accepts to the reader path, and applyTx uses the
				// same test. Such a result must not park here, outside the
				// slot and budget bounds of the reader path, and it may show
				// state that is not durable yet. So it fails.
				w.reply <- txOutcome{err: errors.New("read-only transaction on the write path")}
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
			if !r.taken.CompareAndSwap(false, true) {
				break // the submitter gave up and returned the slots
			}
			if err := r.ctx.Err(); err != nil {
				t.replyRead(r, readOutcome{err: err})
				break
			}
			if t.leaseExpiring() {
				t.replyRead(r, readOutcome{err: ErrNotOwner})
				break
			}
			// durable-reads: the read's basis is the current applied commit
			// end. pinRead fixes the snapshot at exactly that point and hands
			// execution to a worker; the result waits for the watermark when
			// the basis is not durable yet (see pinRead).
			basis, err := t.currentCommitEnd()
			if err != nil {
				t.replyRead(r, readOutcome{err: err})
				t.failLocked(err)
				return
			}
			t.pinRead(r, basis)
			if basis > t.durableOff {
				t.cutSegment() // ship pending bytes so the watermark can pass
			}
		case rr := <-t.readDone:
			t.onReadDone(rr)
		case w := <-t.readAbandon:
			t.abandonRead(w)
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
			if t.opts.IdleRelease > 0 && t.readers.count() == 0 &&
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
	// usage accounting: the whole transaction, including the rollback in
	// the deferred cleanup below (this defer runs last)
	var lim *resultLimit
	defer func(start time.Time) { t.usage.charge(kindWrite, time.Since(start), lim) }(time.Now())
	if t.leaseExpiring() {
		o.err = ErrNotOwner
		return o
	}
	// read-only: nothing to persist, nothing to dedup; ack straight away.
	// A retry simply re-executes. Durability is vacuous for pure reads.
	if allReadOnly(req.Stmts) {
		o.readOnly = true
	}
	// The deadline bounds how long the statements of one transaction hold
	// the actor. The decision point is just before COMMIT: a transaction
	// whose deadline has passed by then rolls back, also when every
	// statement finished. No dedup receipt, no pending ack, no new
	// committed WAL bytes for cutSegment to ship. COMMIT itself is not
	// bounded (no interrupt, no context): it writes the frames of the
	// transaction and is cut short by nothing, so a deadline that passes
	// during COMMIT does not turn a committed transaction into an error.
	// A dedup hit replays its stored result as it is (I2), without the
	// deadline or the result limit.
	ctx, cancel := withDeadline(context.Background(), t.opts.WriteTimeout, "write transaction")
	defer cancel()
	// The commit end before this transaction. A failed COMMIT compares
	// against it (see below). The parse also covers the commits since the
	// last cut, which the next cut then does not parse again.
	preEnd, err := t.currentCommitEnd()
	if err != nil {
		o.err = fmt.Errorf("wal parse: %w", err)
		o.poison = o.err
		return o
	}
	// A dedicated sql.Conn gives the sqlite handle of this transaction's
	// connection. The pool may replace the connection between
	// transactions, so the handle is looked up each time. Without a
	// handle only the driver's own context checks stop the work.
	conn, err := t.db.Conn(context.Background())
	if err != nil {
		o.err = memErr(err, t.metrics)
		return o
	}
	defer conn.Close()
	h := t.connHandle(conn)
	// Background: the deadline must not end the transaction on its own
	// (database/sql would roll back on another goroutine). The statements
	// carry the deadline.
	tx, err := conn.BeginTx(context.Background(), nil)
	if err != nil {
		o.err = memErr(err, t.metrics)
		return o
	}
	committed := false
	defer func() {
		if committed {
			return
		}
		tx.Rollback()
		// SQLITE_NOMEM can also fail the ROLLBACK (its statement needs
		// memory too). A transaction left open would make every later
		// BEGIN fail, and it holds uncommitted pages. The write
		// connection cannot be replaced safely: closing the last
		// connection checkpoints and deletes the WAL under the shipper.
		if err := endOpenTx(conn, h); err != nil && o.poison == nil {
			o.poison = fmt.Errorf("write connection left inside a transaction: %w", err)
		}
	}()
	it := watchInterrupt(ctx, h)
	lim = t.newResultLimit(!o.readOnly)
	o = t.runTx(ctx, tx, req, lim, o)
	if h := t.txHook; h != nil {
		h()
	}
	// no interrupt may reach the COMMIT or the ROLLBACK below
	it.stop()
	if o.dup {
		return o // the receipt answers; this transaction changed nothing
	}
	if ctx.Err() != nil {
		// Also when every statement finished: past the deadline at the
		// commit decision means rollback, so the client sees one rule.
		if c := context.Cause(ctx); errors.Is(c, ErrQueryTimeout) {
			t.metrics.WriteTimeouts.Add(1)
			o.err = c
		} else if o.err == nil {
			o.err = ctx.Err()
		}
		o.results = nil
		return o
	}
	if o.err != nil {
		if errors.Is(o.err, ErrResultTooLarge) {
			t.metrics.ResultTooLarge.Add(1)
		}
		// SQLITE_NOMEM rolled the transaction back: nothing to ship
		o.err = memErr(o.err, t.metrics)
		return o
	}
	if err := tx.Commit(); err != nil {
		// tx.Commit ends the database/sql transaction also on failure, and
		// the driver rolls back what is still open. Whether SQLite wrote
		// a commit frame is not visible from the error: a failure after
		// the frames reached the WAL file (for example SQLITE_NOMEM in
		// the wal-index update) reports an error and rolls back in memory
		// only. The parser and the shipper read the file, so they would
		// ship a transaction that SQLite does not have. The commit end
		// tells: unchanged means nothing committed, a deferred foreign
		// key failure for example. Changed means poison before the next
		// cut can ship it; recovery then rebuilds from S3.
		committed = true // tx is done; the deferred check still runs below
		o.err = memErr(err, t.metrics)
		o.results = nil
		if err := endOpenTx(conn, h); err != nil {
			o.poison = fmt.Errorf("write connection left inside a transaction after a failed COMMIT: %w", err)
			return o
		}
		end, perr := t.currentCommitEnd()
		switch {
		case perr != nil:
			o.poison = fmt.Errorf("wal parse after a failed COMMIT: %w", perr)
		case end != preEnd:
			o.poison = fmt.Errorf("COMMIT failed but the WAL holds a new commit frame (%d -> %d): %w", preEnd, end, err)
		}
		if o.poison != nil {
			// The commit end moved, or could not be read: the frames of
			// this transaction may be in the WAL file. Never a refusal:
			// the caller retries with the same txid, which deduplicates
			// against whatever recovery finds.
			o.err = outcomeUnknown(o.err)
		} else {
			// The commit end is unchanged: nothing committed (a deferred
			// foreign key failure, for example), a definite rollback.
			o.err = classifyStmtErr(o.err)
		}
		return o
	}
	committed = true
	return o
}

// endOpenTx makes sure that the connection behind c has no open
// transaction after a rollback. It retries ROLLBACK a few times, because
// SQLITE_NOMEM can fail one attempt and the next can find memory. Without
// a handle (h == 0) the state is not visible, and endOpenTx trusts the
// rollback. A non-nil result means a transaction is still open.
func endOpenTx(c *sql.Conn, h uintptr) error {
	if h == 0 || sqliteAutocommit(h) {
		return nil
	}
	var err error
	for i := 0; i < 3; i++ {
		_, err = c.ExecContext(context.Background(), "ROLLBACK")
		if sqliteAutocommit(h) {
			return nil
		}
		time.Sleep(time.Duration(i+1) * 10 * time.Millisecond)
	}
	if err == nil {
		err = errors.New("transaction still open")
	}
	return err
}

// runTx executes the dedup lookup, the statements and the dedup receipt
// of req inside tx. ctx carries the write deadline, lim the result budget.
func (t *Tenant) runTx(ctx context.Context, tx *sql.Tx, req TxRequest, lim *resultLimit, o txOutcome) txOutcome {
	if req.Client != "" && req.TxID != "" && !o.readOnly {
		var raw, fp string
		err := tx.QueryRowContext(ctx, `SELECT result, reqfp FROM _hw_dedup WHERE client=? AND txid=?`,
			req.Client, req.TxID).Scan(&raw, &fp)
		if err == nil {
			if fp != "" && req.FP != "" && fp != req.FP {
				o.err = &RefusedError{Err: errors.New("idempotency key reused with a different request")}
				return o
			}
			dec := json.NewDecoder(strings.NewReader(raw))
			dec.UseNumber()
			if dec.Decode(&o.results) != nil {
				// the key's commit IS in this database; only its answer is
				// unreadable, so the caller cannot be told it failed
				o.err = &OutcomeUnknownError{Err: errors.New("corrupt dedup entry")}
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
	if o.results, o.err = execTxStatements(ctx, tx, req.Stmts, lim); o.err != nil {
		// a statement-level failure refuses the set (422); anything else
		// rolled back on a transient error (503). Guard misses keep their
		// own type (409).
		o.err = classifyStmtErr(o.err)
		return o
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
	return o
}

// execTxStatements runs the statements of one /tx in order and enforces
// their guards. Shared by the write path (actor, write connection) and the
// read-only path (reader worker, pinned read-only snapshot).
//
// lim is the result budget of the whole transaction.
func execTxStatements(ctx context.Context, tx *sql.Tx, stmts []Statement, lim *resultLimit) ([]QueryResult, error) {
	var results []QueryResult
	for i, st := range stmts {
		res, err := execStatement(ctx, tx, st, lim)
		if err != nil {
			return results, err
		}
		if st.ExpectAffected != nil && res.Affected != *st.ExpectAffected {
			return results, &ExpectFailedError{Index: i, What: fmt.Sprintf("affected %d != %d", res.Affected, *st.ExpectAffected)}
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
				return results, &ExpectFailedError{Index: i, What: fmt.Sprintf("scalar %d != %d", got, *st.ExpectScalarInt)}
			}
		}
		results = append(results, res)
	}
	return results, nil
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

func execStatement(ctx context.Context, tx *sql.Tx, st Statement, lim *resultLimit) (QueryResult, error) {
	lim.startStmt()
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
		qr, err := drainRows(rows, lim)
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
	if err := lim.addResult(nil); err != nil {
		return QueryResult{}, err
	}
	n, _ := res.RowsAffected()
	return QueryResult{Affected: n}, nil
}

// checkQuerySQL is the textual gate for /query. It runs before admission,
// so a rejected statement never takes a reader slot.
func checkQuerySQL(sqlText string) error {
	// read-only is enforced by SQLite (query_only connection), not by
	// textual classification: INSERT..RETURNING and writable WITH slip
	// past prefix checks, and this path never ships their bytes.
	if !singleStatement(sqlText) {
		return errors.New("one statement per Statement")
	}
	ls := strings.ToLower(stripSQLComments(sqlText))
	if !strings.HasPrefix(ls, "select") && !strings.HasPrefix(ls, "with") &&
		!strings.HasPrefix(ls, "values") && !strings.HasPrefix(ls, "explain") {
		return errors.New("statement class not allowed through /query")
	}
	if sqlContainsDML(ls) || strings.Contains(ls, "_hw_dedup") {
		return errors.New("statement class not allowed through /query")
	}
	return nil
}

// runQuery executes one checked /query statement inside a pinned read
// transaction on the query_only connection.
func runQuery(ctx context.Context, tx *sql.Tx, st Statement, lim *resultLimit) (QueryResult, error) {
	lim.startStmt()
	rows, err := tx.QueryContext(ctx, st.SQL, normalizeArgs(st.Args)...)
	if err != nil {
		return QueryResult{}, err
	}
	defer rows.Close()
	return drainRows(rows, lim)
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

// drainRows reads all rows into memory. lim fails the query once the
// response passes its row or byte budget; it never truncates.
func drainRows(rows *sql.Rows, lim *resultLimit) (QueryResult, error) {
	cols, err := rows.Columns()
	if err != nil {
		return QueryResult{}, err
	}
	if err := lim.addResult(cols); err != nil {
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
		if err := lim.addRow(vals); err != nil {
			return QueryResult{}, err
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
	// the generation format, fixed when the generation was published,
	// decides the segment form; the Compress flag only affects the next
	// generation. A zstd generation never takes a raw segment, and no
	// reader accepts a zstd segment above maxSegmentRawBytes: fail before
	// any ack of this batch rather than publish an unreadable object.
	format := t.mf.Format
	if format != manifestFormatRaw && end-t.submittedOff > maxSegmentRawBytes {
		t.failLocked(fmt.Errorf("%w: batch of %d bytes, limit %d", errSegmentTooLarge, end-t.submittedOff, maxSegmentRawBytes))
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
	go func() {
		// compress off the actor, once per segment (not per attempt). The
		// key keeps the RAW WAL offset; recovery uses decoded lengths.
		stored, err := segmentObject(s.data, format)
		if err != nil {
			goto done // checked on the actor already; never PUT it
		}
		t.metrics.SegStoredBytes.Add(int64(len(stored)))
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

// prepareBaseUpload returns the file and size to upload as the base image
// and the format of the new generation. The format covers the base and
// every segment of the generation, so it is decided here, once.
//
// With compression on it writes one zstd frame to a temp file next to the
// database and returns manifestFormatZstd. The reads bump progressMs, so
// a long compression of a large base does not look like a wedged actor.
// Any compression failure (for example a full disk), or a base above the
// reader limit, gives the raw file and manifestFormatRaw: a raw
// generation is always valid, and a checkpoint must not fail for an
// optimization. Never a raw base in a zstd generation.
func (t *Tenant) prepareBaseUpload(f *os.File, size int64) (*os.File, int64, int, func()) {
	noop := func() {}
	if !t.opts.Compress {
		return f, size, manifestFormatRaw, noop
	}
	logf := func(format string, args ...any) {
		if t.metrics.Logger != nil {
			t.metrics.Logger.Printf(format, args...)
		}
	}
	if size > maxBaseRawBytes {
		logf("tenant %s: base of %d bytes exceeds the zstd limit (raw generation)", t.ID, size)
		return f, size, manifestFormatRaw, noop
	}
	tmp, err := os.CreateTemp(t.dir, "base-*.zst")
	if err != nil {
		logf("tenant %s: base compress: %v (raw generation)", t.ID, err)
		return f, size, manifestFormatRaw, noop
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
	stored, err := compressStream(tmp, src, size, maxBaseRawBytes)
	if err != nil {
		cleanup()
		logf("tenant %s: base compress: %v (raw generation)", t.ID, err)
		return f, size, manifestFormatRaw, noop
	}
	return tmp, stored, manifestFormatZstd, cleanup
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
		// Clear the slot: the backing array outlives the reslice, and
		// an idle tenant would pin its last segment (data and results)
		// in the Go heap until its next cut.
		t.segs[0] = nil
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
	clear(t.pending[len(rest):]) // acked entries hold results; see t.segs
	t.pending = rest
	if len(t.readWaits) > 0 {
		// every parked entry is a result computed on the snapshot at its
		// basis, so basis <= durableOff is the whole condition. Newer
		// in-flight commits cannot leak: nothing executes here.
		var waiting []readWait
		for _, rw := range t.readWaits {
			if rw.end <= t.durableOff {
				t.releaseReadWait(rw)
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
	now := time.Now()
	if now.Before(t.ckptDeferUntil) {
		return // a recent attempt found reads that would not drain
	}
	// This fold is optional, and draining open snapshots stalls the actor.
	// Wait for a moment without readers; force the drain only after
	// 10 x ReadDrainWait of continuous overlap.
	if t.readers.count() > 0 {
		if t.ckptWantSince.IsZero() {
			t.ckptWantSince = now
		}
		if now.Sub(t.ckptWantSince) < 10*t.opts.ReadDrainWait {
			return
		}
	}
	t.ckptWantSince = time.Time{}
	t.checkpointLocked("")
}

// checkpointLocked: flush everything, drain in order, fold WAL into a new
// base image, CAS the manifest. The pod lease keeps heartbeating in the
// manager while we drain, so a slow drain cannot lapse the lease.
// Actor-loop only.
func (t *Tenant) checkpointLocked(newClass string) bool {
	// usage accounting: actor time of the checkpoint, uploads included
	defer func(start time.Time) { t.usage.charge(kindCheckpoint, time.Since(start), nil) }(time.Now())
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
	// The fold (foldWAL: PASSIVE for a delta, then TRUNCATE) needs a WAL
	// with no open read snapshot. Only the actor pins snapshots (pinRead),
	// and the actor stays in this function until the fold is done, so no
	// new snapshot can start. Wait for the
	// pinned ones to end: bounded, then canceled. This runs after the
	// segment drain, so a skip still leaves every commit durable and acked.
	if !t.drainReaders() {
		if !t.claimed {
			// a claim has no published readers; never skip the acquisition
			t.failLocked(errors.New("checkpoint: reads did not drain during claim"))
			return false
		}
		// Durability is unaffected: every segment is in S3. The fold into
		// a new base retries on the next trigger.
		t.metrics.Logger.Printf("checkpoint %s skipped: %d reads did not drain", t.ID, t.readers.count())
		// back off: without it every later write would wait the full
		// drain again behind the same stuck read
		t.ckptDeferUntil = time.Now().Add(5 * t.opts.ReadDrainWait)
		t.cutSegment() // a cut deferred by the inflight cap must not wait for the next write
		return true
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
	ckptStart := time.Now()
	// a delta lists the pages of the committed WAL: plan it before the
	// WAL is truncated (nil: this checkpoint uploads a full base)
	plan := t.planDelta()
	dbSize, plan, err := t.foldWAL(ctx, plan)
	if err != nil {
		if t.skipNoMemCheckpoint(err, newClass) {
			return true
		}
		t.failLocked(fmt.Errorf("wal checkpoint: %w", memErr(err, t.metrics)))
		return false
	}
	newGen := t.mf.Gen + 1
	f, err := os.Open(t.dbPath)
	if err != nil {
		t.failLocked(err)
		return false
	}
	t.progressMs.Store(time.Now().UnixMilli())
	// compress once into a temp file, then upload that file: retries and
	// SDK rewinds need a seekable body, and the base can be GBs, so it
	// never sits in memory
	up := t.prepareCheckpointUpload(f, dbSize, plan, newGen)
	defer up.cleanup()
	// Progress-bounded upload: an attempt is canceled only when no bytes
	// move for stallTimeout, never for being merely slow — a multi-GB base
	// on a degraded link must still finish (a fixed 20s cap
	// made large tenants permanently uncheckpointable).
	var putErr error
	for attempt := 0; up.key != "" && attempt < 4; attempt++ {
		if _, serr := up.file.Seek(0, 0); serr != nil {
			f.Close()
			t.failLocked(fmt.Errorf("checkpoint object seek: %w", serr))
			return false
		}
		putErr = t.putBaseWithStallCancel(up.key, up.file, up.size, 10*time.Second)
		t.progressMs.Store(time.Now().UnixMilli())
		if putErr == nil {
			break
		}
	}
	up.cleanup() // free the temp file before the CAS phase
	if putErr != nil {
		f.Close()
		t.failLocked(fmt.Errorf("checkpoint put %s: %w", up.key, putErr))
		return false
	}
	f.Close()
	if up.delta {
		t.metrics.DeltaCheckpoints.Add(1)
		t.metrics.DeltaBytes.Add(up.raw)
		t.metrics.DeltaStoredBytes.Add(up.size)
	} else {
		t.metrics.BaseBytes.Add(up.raw)
		t.metrics.BaseStoredBytes.Add(up.size)
		if up.format == manifestFormatChain {
			t.metrics.Rebases.Add(1)
		}
	}
	oldMf := t.mf
	oldGen := t.mf.Gen
	oldIID := t.mf.Instance
	nm := *t.mf
	nm.Gen = newGen
	nm.BaseKey = up.baseKey
	nm.BaseSize = up.baseSize
	nm.Deltas = up.deltas
	// the new generation holds only objects this incarnation writes (the
	// base or delta above and the segments cut after this CAS; cutSegment
	// reads t.mf.Format) plus, in a chain gen, the chain it extends. All
	// of them are zstd in a chain gen. So the format follows the upload.
	nm.Format = up.format
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
			// The claim uploaded only up.key: the base and deltas of the
			// chain it extends belong to the winner too, never delete them.
			if t.claimAttempt != "" {
				if cur, _, rerr := t.store.GetManifest(casCtx, t.ID); rerr == nil && cur != nil && up.key != "" && !manifestNames(cur, up.key) {
					ctx2, cancel2 := context.WithTimeout(context.Background(), 15*time.Second)
					_ = t.store.DeleteObjects(ctx2, up.key)
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
	t.dbBytes = dbSize
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
		t.releaseReadWait(rw)
	}
	t.readWaits = nil
	// re-open wal handle: TRUNCATE may have replaced the file
	t.wal.Close()
	if f2, err := os.OpenFile(t.walPath, os.O_CREATE|os.O_RDONLY, 0o644); err == nil {
		t.wal = f2
	}
	t.metrics.Checkpoints.Add(1)
	t.metrics.CheckpointMs.Add(time.Since(ckptStart).Milliseconds())
	// after the CAS only: the segments of the old gen, and the old base and
	// deltas that the new manifest no longer names (all of them after a
	// rebase, none after a delta)
	obsolete := obsoleteObjects(oldMf, &nm)
	go func(obsolete []string, oldIID string, oldGen int64) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		keys := obsolete
		if segs, err := t.store.ListSegments(ctx, t.ID, oldIID, oldGen); err == nil {
			for _, sg := range segs {
				keys = append(keys, sg.Key)
			}
		}
		if len(keys) > 0 {
			_ = t.store.DeleteObjects(ctx, keys...)
		}
	}(obsolete, oldIID, oldGen)
	return true
}

// skipNoMemCheckpoint decides whether a checkpoint that failed with
// SQLITE_NOMEM can be skipped instead of failing the tenant. The heap
// limit is process wide, so the tenant that hits it in a checkpoint is
// often not the one that uses the memory. Poisoning it would turn memory
// pressure into failovers. A checkpoint allocates its frame index before
// it copies a page, and it truncates the WAL only after a full copy. So
// after SQLITE_NOMEM the WAL still holds every committed frame, and a
// copied page equals the committed state in the WAL. The check below
// confirms that the WAL still reaches the shipped offset; a WAL that
// shrank fails the tenant. Durability is unaffected: every segment is in
// S3. A claim never skips (the checkpoint is the acquisition), and a
// class change keeps the poison path.
//
// A delta checkpoint folds in two steps (foldWAL). SQLITE_NOMEM in the
// PASSIVE step is the case above. SQLITE_NOMEM in the TRUNCATE step after
// PASSIVE folded frames is not: SQLite counts the WAL as folded, and the
// next write can restart the WAL at its start, under the shipped offsets.
// That case fails the tenant (walFoldedError).
func (t *Tenant) skipNoMemCheckpoint(err error, newClass string) bool {
	var folded *walFoldedError
	if !isNoMem(err) || errors.As(err, &folded) || !t.claimed || newClass != "" {
		return false
	}
	end, perr := t.currentCommitEnd()
	if perr != nil || end < t.submittedOff || t.walFileSize() < t.submittedOff {
		return false
	}
	t.metrics.OutOfMemory.Add(1)
	t.metrics.Logger.Printf("checkpoint %s skipped: %v", t.ID, err)
	t.ckptDeferUntil = time.Now().Add(5 * t.opts.ReadDrainWait)
	t.cutSegment() // as in the read-drain skip above
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
	// graceful close: let in-flight reads finish inside the grace before
	// finishClose cancels the rest
	t.readers.waitIdle(t.opts.ReadDrainWait, t.bumpProgress)
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
	// pending and segment waiters were APPLIED (committed to the local
	// database, WAL bytes possibly shipped or folded into an uploaded
	// base): their outcome is unknown, never a plain failure. Queued
	// writes and parked read-only transactions never applied anything.
	applied := outcomeUnknown(err)
	for _, pa := range t.pending {
		o := pa.outcome
		o.err = applied
		pa.reply <- o
	}
	t.pending = nil
	for _, rw := range t.readWaits {
		t.unparkRead(rw)
		t.deliverRead(rw.w, readOutcome{err: err}) // returns the slot and the budget share
	}
	t.readWaits = nil
	for _, s := range t.segs {
		for i, ch := range s.waiters {
			o := s.results[i]
			o.err = applied
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
			if r.taken.CompareAndSwap(false, true) {
				t.replyRead(r, readOutcome{err: err})
			}
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
	t.closeReaders()
	t.wal.Close()
	// publish the poison cooldown BEFORE done closes: once done is closed,
	// LocalTenant can drop us from the registry and an Acquire can start
	// a takeover. The cooldown must already be visible then.
	if t.onPoison != nil {
		t.onPoison(t)
	}
	close(t.done)
	// a worker that sent before the close owns no reply path any more
	t.drainReadDone()
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
		"format": t.mf.Format, "delta_chain": len(t.mf.Deltas),
		"active_reads": t.readers.count(),
		"usage":        t.usageStatus(),
	}
}
