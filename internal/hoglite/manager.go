package hoglite

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	mrand "math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func newInstanceID() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

type Metrics struct {
	Logger       interface{ Printf(string, ...any) }
	Commits      atomic.Int64
	Queries      atomic.Int64
	DedupHits    atomic.Int64
	SegPuts      atomic.Int64
	SegBytes     atomic.Int64
	Acquires     atomic.Int64
	Checkpoints  atomic.Int64
	Proxied      atomic.Int64
	ProxyFails   atomic.Int64
	Evictions    atomic.Int64
	Sweeps       atomic.Int64
	SweptObjects atomic.Int64
	AcquireWait  atomic.Int64 // ms spent waiting for leases
	InflightSegs atomic.Int64 // gauge: segment PUTs currently in flight
	// CommitEwmaUs is the EWMA of full commit latency (submit to durable
	// ack) across the pod. It is the primary autoscaling signal: it rises
	// as soon as S3 queueing starts, well before client-visible timeouts.
	CommitEwmaUs atomic.Int64

	// Stored (possibly compressed) bytes PUT for segments and base images.
	// SegBytes is the raw WAL byte count; raw vs stored gives the ratio.
	SegStoredBytes  atomic.Int64
	BaseBytes       atomic.Int64 // raw base image bytes uploaded
	BaseStoredBytes atomic.Int64

	// Page-delta checkpoints (format 2). DeltaCheckpoints counts
	// checkpoints that uploaded a delta or no object at all; Rebases counts
	// full bases that start a new chain. CheckpointMs is the total time of
	// the fold, upload and CAS of every checkpoint (writes wait meanwhile).
	DeltaCheckpoints atomic.Int64
	DeltaBytes       atomic.Int64 // raw delta bytes uploaded
	DeltaStoredBytes atomic.Int64
	Rebases          atomic.Int64
	CheckpointMs     atomic.Int64
	// pod-wide query admission (TenantOpts.PodMaxQueries): queries that
	// hold a slot, and queries waiting for one
	QueriesInflight atomic.Int64
	QueriesWaiting  atomic.Int64
}

// ewmaUpdate folds a sample into an atomic EWMA with alpha=1/8.
func ewmaUpdate(a *atomic.Int64, sample int64) {
	for {
		cur := a.Load()
		var next int64
		if cur == 0 {
			next = sample
		} else {
			next = cur + (sample-cur)/8
		}
		if a.CompareAndSwap(cur, next) {
			return
		}
	}
}

var (
	ErrNoTenant   = errors.New("tenant does not exist")
	ErrWrongClass = errors.New("tenant belongs to a different replica class")
	ErrBusy       = errors.New("tenant owned by a live peer")
)

// OwnerView is what the server needs for routing decisions.
type OwnerView struct {
	Manifest *Manifest
	Local    *Tenant // non-nil when this replica owns and has it open
}

// ControllerOpts tunes the heat-driven reclass loop.
type ControllerOpts struct {
	Enabled        bool
	ScanEvery      time.Duration
	WhaleThreshold float64       // commits/s sustained
	SustainedScans int           // consecutive hot scans before reclass
	Dwell          time.Duration // min time between reclasses of one tenant
}

type Manager struct {
	store   Backend
	prefix  string
	id      string
	routeID string // stable, shared with peers for proxy routing
	class   string
	baseDir string
	opts    TenantOpts
	metrics *Metrics

	// pod lease: one heartbeated object for the whole process. Tenants
	// gate writes on this clock instead of CASing their own manifest.
	// until=0 means the lease is not established (or was lost) and every
	// tenant must refuse writes.
	leaseUntil  atomic.Int64 // ms; expiry of this pod's lease
	leaseLost   atomic.Bool  // heartbeat failed past margin: fail-stop
	partitioned atomic.Bool  // test hook: heartbeats silently do not land
	// gating, when true, forces every tenant write gate closed regardless
	// of the lease clock. Set while a lapse sweep is in flight so no
	// tenant regains a valid gate from the fresh leaseUntil mid-sweep.
	gating atomic.Bool
	// leaseGen increments on every failAll sweep. A tenant captures it at
	// claim; a mismatch closes its write gate. This fences actors that are
	// mid-claim (not yet in m.tenants) or mid-shutdown (already removed)
	// when a lapse hits — both escape the zombie-flag sweep.
	leaseGen      atomic.Int64
	leaseStop     chan struct{}
	leaseStopOnce sync.Once
	ckptSem       chan struct{} // bounds concurrent checkpoint drains pod-wide
	claimSem      chan struct{} // claims get their own lane: slow release
	// checkpoints must never starve a takeover
	decodeSem chan struct{} // bounds concurrent decoding materializations pod-wide
	// legacyReadForTest makes this manager read like a build from before
	// compression: every object raw, no format gate, no delta chain.
	// s1ReadForTest makes it read like the build with compression but
	// without deltas: the format gate stops at format 1. Tests only.
	legacyReadForTest bool
	s1ReadForTest     bool
	qlim              *queryLimiter // bounds admitted queries across all tenants

	maxOpen int // soft cap on simultaneously open tenants

	// autoscaling knobs: Saturation reports max(commitEWMA/satCommitBudget,
	// inflightSegs/satInflightCap). HPA targets a value below 1.0.
	satCommitBudgetUs float64
	satInflightCap    float64

	ctrl     ControllerOpts
	hot      map[string]int
	lastFlip map[string]time.Time
	draining atomic.Bool

	mu      sync.Mutex
	tenants map[string]*Tenant
	closing map[string]chan struct{} // tenants mid-Stop; Acquire must wait
	opening map[string]*sync.Mutex   // singleflight per tenant
	// poisonedAt records ungraceful tenant deaths. A mass-poison event
	// (S3 stall) otherwise turns into a takeover storm: hundreds of
	// replays+checkpoints hitting the already-degraded store at once.
	// Acquire waits a short jittered cooldown before re-opening one.
	poisonedAt  map[string]time.Time
	poisonSweep time.Time // last amortized sweep of poisonedAt
}

func NewManager(store Backend, prefix, id, class, baseDir string, opts TenantOpts, maxOpen int, metrics *Metrics, ctrl ControllerOpts) *Manager {
	return NewManagerSat(store, prefix, id, class, baseDir, opts, maxOpen, metrics, ctrl, 500000, 128)
}

// NewManagerSat additionally sets the saturation-metric budgets.
func NewManagerSat(store Backend, prefix, id, class, baseDir string, opts TenantOpts, maxOpen int, metrics *Metrics, ctrl ControllerOpts, satCommitBudgetUs, satInflightCap float64) *Manager {
	normalizeDeltaOpts(&opts)
	// fencing identity is unique per boot, even when the routing id is a
	// stable pod name that a replacement pod will reuse. The self-lease
	// fast path only ever matches THIS process's own manifest writes.
	routeID := id
	id = id + "." + newInstanceID()[:8]
	m := &Manager{
		store: store, prefix: strings.Trim(prefix, "/"), id: id, routeID: routeID, class: class, baseDir: baseDir, opts: opts,
		metrics: metrics, maxOpen: maxOpen,
		satCommitBudgetUs: satCommitBudgetUs, satInflightCap: satInflightCap,
		tenants: map[string]*Tenant{}, opening: map[string]*sync.Mutex{}, closing: map[string]chan struct{}{},
		poisonedAt: map[string]time.Time{},
		hot:        map[string]int{}, lastFlip: map[string]time.Time{},
	}
	if metrics.Logger == nil {
		m.metrics.Logger = log.New(io.Discard, "", 0)
	}
	m.leaseStop = make(chan struct{})
	// 8 concurrent drains per pod: enough to absorb release bursts without
	// turning them into S3 storms
	m.ckptSem = make(chan struct{}, 8)
	m.claimSem = make(chan struct{}, 4)
	// an active decoder holds about 3.2 MiB for our frames (measured; up
	// to about 17 MiB for a hostile header): 32 slots cap decoder memory
	// near 100 MiB (550 MiB worst case) in a failover burst. Claims (4 lanes) are the slower stage, so
	// this does not limit takeover throughput.
	m.decodeSem = make(chan struct{}, 32)
	m.qlim = newQueryLimiter(opts.PodMaxQueries, m.metrics)
	// the first heartbeat is synchronous: no tenant may start before the
	// write gate has a clock
	m.heartbeatLease()
	go m.leaseLoop()
	go m.evictLoop()
	go m.watchdogLoop()
	m.ctrl = ctrl
	if ctrl.Enabled {
		go m.reclassLoop() // standard: promote hot; whale: demote cold
	}
	return m
}

// ID is the fencing identity (unique per boot).
func (m *Manager) ID() string { return m.id }

// RouteID is the stable peer-table identity (pod name).
func (m *Manager) RouteID() string { return m.routeID }

// LocalTenant returns the open tenant or nil.
func (m *Manager) LocalTenant(tid string) *Tenant {
	m.mu.Lock()
	defer m.mu.Unlock()
	t := m.tenants[tid]
	if t != nil && !t.Alive() {
		delete(m.tenants, tid)
		return nil
	}
	return t
}

// OwnerOf resolves current ownership from S3 (no cache; callers may cache).
func (m *Manager) OwnerOf(ctx context.Context, tid string) (*Manifest, error) {
	mf, _, err := m.store.GetManifest(ctx, tid)
	return mf, err
}

// ---------------- pod lease ----------------

// leaseLoop heartbeats this pod's lease object every Lease/3. One PUT for
// the whole process replaces per-tenant manifest heartbeats. A heartbeat
// that cannot land before the safety margin expires fail-stops the pod:
// every owned tenant is poisoned, because its write gate ran on this clock.
func (m *Manager) leaseLoop() {
	tick := time.NewTicker(m.opts.HeartbeatEvery)
	defer tick.Stop()
	m.heartbeatLease()
	for {
		select {
		case <-m.leaseStop:
			return
		case <-tick.C:
		}
		m.heartbeatLease()
	}
}

func (m *Manager) heartbeatLease() {
	if m.partitioned.Load() {
		return // heartbeats do not land; leaseUntil stops advancing
	}
	until := time.Now().Add(m.opts.Lease).UnixMilli()
	// the timeout is NOT the interval: a 1s heartbeat with a 1s deadline
	// fails on any jitter. Allow Lease/4 (3s at the 12s default) per beat.
	hbTimeout := m.opts.Lease / 4
	if hbTimeout < 2*time.Second {
		hbTimeout = 2 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), hbTimeout)
	err := m.store.PutPodLease(ctx, m.id, until)
	cancel()
	if err != nil {
		// no permanent brick: the write gate closes by itself when
		// leaseUntil passes its margin. When a heartbeat lands again the
		// lapse rule below decides whether our actors may resume.
		m.metrics.Logger.Printf("pod lease heartbeat failed (%v); gate closes at the write deadline", err)
		return
	}
	prev := m.leaseUntil.Load()
	// lapse rule: the previous WRITE DEADLINE passed before this heartbeat
	// landed. Our actors were gated during the gap and a skewed peer may
	// have claimed already, so no actor born under the old clock may
	// resume. Fail-stop everything; the leaseGen bump fences mid-claim and
	// mid-shutdown actors too. No skew slack here: a heal after the
	// deadline but before expiry+skew can still lose to a fast-clock peer.
	writeDeadline := prev - int64(m.opts.Lease/time.Millisecond)/3
	if prev > 0 && writeDeadline < time.Now().UnixMilli() {
		m.gating.Store(true)
		m.leaseUntil.Store(until)
		m.metrics.Logger.Printf("pod lease write deadline passed between heartbeats: fail-stopping all tenants")
		m.failAll(errors.New("pod lease write deadline passed"))
		m.gating.Store(false)
		return
	}
	m.leaseUntil.Store(until)
}

// leaseExpiring reports whether the pod lease is inside its safety margin
// or not established. All tenant write gates call this.
func (m *Manager) leaseExpiring() bool {
	if m.gating.Load() {
		return true
	}
	until := m.leaseUntil.Load()
	if until == 0 || m.leaseLost.Load() {
		return true
	}
	return time.Now().UnixMilli() > until-int64(m.opts.Lease/time.Millisecond)/3
}

// leaseWriteDeadline is the absolute deadline for any data-plane PUT.
func (m *Manager) leaseWriteDeadline() time.Time {
	return time.UnixMilli(m.leaseUntil.Load()).Add(-m.opts.Lease / 3)
}

// failAll poisons every owned tenant (lease lost, lapse, or watchdog).
// Safety comes first: every tenant's zombie flag closes its write gates
// instantly (atomic, no actor involvement). The actual poisons run as
// background cleanup — a wedged actor cannot stall the lease loop, and
// its gates are already closed anyway.
// failAll poisons every owned tenant (lease lost, lapse, or watchdog).
// The generation bump is the fence: any actor born before this sweep —
// mid-claim, mid-shutdown, or wedged — fails its write gate forever via
// leaseGen mismatch, whether or not it is in m.tenants right now.
// Zombie flags close gates synchronously for registered tenants; the
// poisons run as background cleanup so a wedged actor cannot stall the
// lease loop.
func (m *Manager) failAll(err error) {
	m.leaseGen.Add(1)
	m.mu.Lock()
	ts := make([]*Tenant, 0, len(m.tenants))
	for _, t := range m.tenants {
		ts = append(ts, t)
	}
	m.mu.Unlock()
	for _, t := range ts {
		t.zombie.Store(true)
	}
	for _, t := range ts {
		go t.Poison(err)
	}
}

// watchdogLoop releases tenants whose actor stopped making progress while
// the pod itself is healthy. Per-tenant leases used to catch this case;
// the pod lease cannot. No progress for 2 lease periods = wedged.
func (m *Manager) watchdogLoop() {
	tick := time.NewTicker(m.opts.Lease)
	defer tick.Stop()
	for {
		select {
		case <-m.leaseStop:
			return
		case <-tick.C:
		}
		m.mu.Lock()
		ts := make([]*Tenant, 0, len(m.tenants))
		for _, t := range m.tenants {
			ts = append(ts, t)
		}
		m.mu.Unlock()
		now := time.Now().UnixMilli()
		// the wedge threshold must exceed the checkpoint timeout (60s): a
		// healthy actor can block that long in one upload
		threshold := 2 * m.opts.Lease.Milliseconds()
		if threshold < 70000 {
			threshold = 70000
		}
		for _, t := range ts {
			last := t.progressMs.Load()
			if last > 0 && now-last > threshold && t.Alive() {
				// wedged actor: it cannot process Stop (its ctlCh is the thing
				// that is stuck). The zombie flag closes every write gate when
				// it wakes. Cleanup is scheduled ONCE per incarnation via the
				// cleaned flag — independent of zombie, which failAll also sets.
				zombie := t.zombie.CompareAndSwap(false, true)
				if !t.cleaned.CompareAndSwap(false, true) {
					continue
				}
				if zombie {
					m.metrics.Logger.Printf("watchdog: tenant %s wedged (no progress %dms), releasing", t.ID, now-last)
				}
				go m.clearDeadOwner(t.ID, t.claimEpoch, t.claimInst)
			}
		}
	}
}

// stopLease halts the lease loop (shutdown).
func (m *Manager) stopLease() {
	m.leaseStopOnce.Do(func() { close(m.leaseStop) })
}

// ownerLeaseLive resolves a foreign owner's liveness via its pod lease.
// Returns (live, error). A missing lease object means the owner never
// heartbeated: expired.
func (m *Manager) ownerLeaseLive(ctx context.Context, owner string) (bool, int64, error) {
	until, found, err := m.store.GetPodLease(ctx, owner)
	if err != nil {
		return false, 0, err
	}
	if !found {
		return false, 0, nil
	}
	now := time.Now().UnixMilli()
	return until+m.opts.SkewMargin.Milliseconds() > now, until, nil
}

// Acquire returns the open tenant, acquiring or re-acquiring ownership as
// needed. create=true creates the tenant when missing.
func (m *Manager) Acquire(ctx context.Context, tid string, create bool) (*Tenant, error) {
	if m.draining.Load() {
		return nil, ErrClosed
	}
	// wait for a closing tenant to finish (never two live copies)
	for {
		m.mu.Lock()
		ch, isClosing := m.closing[tid]
		m.mu.Unlock()
		if !isClosing {
			break
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ch:
		}
	}
	if t := m.LocalTenant(tid); t != nil {
		return t, nil
	}
	// jittered cooldown after an ungraceful death: damps takeover storms.
	// The wait loop re-reads the entry: a newer death during our sleep
	// replaces the timestamp and we honor the newer cooldown instead of
	// deleting it (stale waiters must not erase fresh deaths).
	for {
		m.mu.Lock()
		poisoned, wasPoisoned := m.poisonedAt[tid]
		m.mu.Unlock()
		if !wasPoisoned {
			break
		}
		cooldown := 500*time.Millisecond + time.Duration(mrand.Int63n(1500))*time.Millisecond
		if d := poisoned.Add(cooldown).Sub(time.Now()); d > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(d):
			}
		}
		m.mu.Lock()
		if cur, ok := m.poisonedAt[tid]; ok && cur.Equal(poisoned) {
			delete(m.poisonedAt, tid)
		}
		m.mu.Unlock()
		// a caller with shorter jitter may have re-opened the tenant while
		// we slept. Re-check before becoming a leader: taking over a live
		// local tenant wipes its directory.
		if t := m.LocalTenant(tid); t != nil {
			return t, nil
		}
	}
	// singleflight
	m.mu.Lock()
	sf, ok := m.opening[tid]
	if !ok {
		sf = &sync.Mutex{}
		sf.Lock()
		m.opening[tid] = sf
		m.mu.Unlock()
	} else {
		m.mu.Unlock()
		sf.Lock() // wait for the in-flight open
		sf.Unlock()
		// leader finished; loop normally (tenant now exists or create failed)
		return m.Acquire(ctx, tid, create)
	}
	defer func() {
		m.mu.Lock()
		delete(m.opening, tid)
		m.mu.Unlock()
		sf.Unlock()
	}()

	deadline := time.Now().Add(m.opts.Lease + 15*time.Second)
	attempts := 0
	for {
		attempts++
		if m.draining.Load() {
			// shutdown started while we were opening: never register or ack
			return nil, ErrClosed
		}
		if attempts > 20 {
			return nil, errors.New("acquire loop exceeded 20 attempts")
		}
		mf, etag, err := m.store.GetManifest(ctx, tid)
		if err != nil {
			return nil, err
		}
		if mf == nil {
			if !create {
				return nil, ErrNoTenant
			}
			return m.createTenant(ctx, tid, "")
		}
		// a create on an existing tenant with a dead lease is a no-op:
		// the tenant exists, the retry path for catalog creation is safe
		if create {
			if t := m.LocalTenant(tid); t != nil && t.Alive() {
				return t, nil
			}
		}
		if mf.Class != "" && mf.Class != m.class {
			return nil, fmt.Errorf("%w: %s needs class %q, we are %q", ErrWrongClass, tid, mf.Class, m.class)
		}
		now := time.Now().UnixMilli()
		if mf.Owner != "" && mf.Owner != m.id {
			// liveness comes from the owner's pod lease, not the manifest
			live, until, lerr := m.ownerLeaseLive(ctx, mf.Owner)
			if lerr != nil {
				return nil, lerr
			}
			if live {
				wait := time.Duration(until-now)*time.Millisecond + m.opts.SkewMargin
				if time.Now().Add(wait).After(deadline) {
					return nil, fmt.Errorf("%w: %s held by %s", ErrBusy, tid, mf.Owner)
				}
				m.metrics.AcquireWait.Add(int64(wait / time.Millisecond))
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(wait + 50*time.Millisecond):
				}
				continue
			}
		}
		// the manifest names us: a live local incarnation wins. This guards
		// the cooldown race (a peer caller claimed while we slept) and any
		// future path where the registry and the manifest disagree.
		if mf.Owner == m.id {
			if lt := m.LocalTenant(tid); lt != nil && lt.Alive() {
				return lt, nil
			}
		}
		// a tenant written by a newer build: refuse before touching any of
		// its objects. The check sits after the liveness wait, so a tenant
		// that a live newer peer owns still answers ErrBusy (re-route).
		if err := m.formatGate(tid, mf); err != nil {
			return nil, err
		}
		// lease expired or free: take over
		t, err := m.takeOver(ctx, tid, mf, etag)
		if errors.Is(err, ErrCAS) || errors.Is(err, errStaleGen) {
			continue // someone else won; re-read
		}
		if err != nil {
			return nil, err
		}
		// never register a fresh tenant after shutdown began: it would serve
		// writes on a drained pod. Check under the lock: ShutdownAll swaps
		// draining and then snapshots m.tenants under the same lock, so
		// either we see draining, or the snapshot saw us.
		m.mu.Lock()
		if m.draining.Load() {
			m.mu.Unlock()
			t.Stop(true, "")
			return nil, ErrClosed
		}
		m.tenants[tid] = t
		m.mu.Unlock()
		m.metrics.Acquires.Add(1)
		return t, nil
	}
}

// createTenant makes a brand-new tenant.
func (m *Manager) createTenant(ctx context.Context, tid, class string) (*Tenant, error) {
	dir := filepath.Join(m.baseDir, "t", tid)
	os.RemoveAll(dir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	dbPath := filepath.Join(dir, "db.sqlite")
	if err := os.WriteFile(dbPath, nil, 0o644); err != nil {
		return nil, err
	}
	iid := newInstanceID()
	mf := &Manifest{
		Epoch: 1, Gen: 1, Instance: iid, Owner: m.id, OwnerAddr: m.routeID,
		OwnerUntilMs: time.Now().Add(m.opts.Lease).UnixMilli(),
		BaseKey:      baseKey(m.prefix, tid, iid, 1, 1), Class: class,
		UpdatedMs: time.Now().UnixMilli(),
	}
	t, err := openTenant(tid, dir, mf, "", m.store, m.prefix, m.id, m.routeID, m.opts, m.metrics, m)
	if err == nil {
		t.ckptSem = m.ckptSem
		t.claimSem = m.claimSem
		t.qlim = m.qlim
	}
	if err != nil {
		return nil, err
	}
	t.onClose = m.noteClosed
	t.onPoison = m.notePoisoned
	// Write a base image so readers never see a manifest without a base.
	// checkpointLocked CASes with etag "" = create-only.
	okCh := make(chan bool, 1)
	t.ctlCh <- func() { okCh <- t.claimLocked("") }
	if !(<-okCh) {
		return nil, fmt.Errorf("create checkpoint failed: %v", t.deadErr)
	}
	// same TOCTOU guard as the takeover path: check draining under the lock
	m.mu.Lock()
	if m.draining.Load() {
		m.mu.Unlock()
		t.Stop(true, "")
		return nil, ErrClosed
	}
	m.tenants[tid] = t
	m.mu.Unlock()
	m.metrics.Acquires.Add(1)
	return t, nil
}

// takeOver rebuilds local state from S3 and CASes ownership.
func (m *Manager) takeOver(ctx context.Context, tid string, mf *Manifest, etag string) (*Tenant, error) {
	// coolFailedClaim damps transient store failures on any takeover step:
	// without it, a request retry loop re-downloads and re-uploads the
	// tenant against a degraded store at full rate.
	coolFailedClaim := func() { m.recordPoison(tid) }
	dir := filepath.Join(m.baseDir, "t", tid)
	os.RemoveAll(dir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	if err := m.materializeLocked(ctx, tid, mf, dir); err != nil {
		if !errors.Is(err, errStaleGen) && !errors.Is(err, errDecodeSlotWait) {
			coolFailedClaim()
		}
		return nil, err
	}

	nm := *mf
	nm.Epoch = mf.Epoch + 1
	nm.Owner = m.id
	nm.OwnerAddr = m.routeID
	nm.OwnerUntilMs = time.Now().Add(m.opts.Lease).UnixMilli()
	nm.UpdatedMs = time.Now().UnixMilli()
	attemptID := newInstanceID()[:8]

	t, err := openTenant(tid, dir, &nm, etag, m.store, m.prefix, m.id, m.routeID, m.opts, m.metrics, m)
	if err == nil {
		t.ckptSem = m.ckptSem
		t.claimSem = m.claimSem
		t.qlim = m.qlim
	}
	if err != nil {
		// local cache may be corrupt (disk, partial write): wipe and
		// rebuild from S3 once before giving up
		os.RemoveAll(dir)
		if err2 := os.MkdirAll(dir, 0o755); err2 != nil {
			return nil, err2
		}
		if err := m.materialize(ctx, tid, mf, dir); err != nil {
			if !errors.Is(err, errStaleGen) && !errors.Is(err, errDecodeSlotWait) {
				coolFailedClaim()
			}
			return nil, err
		}
		t, err = openTenant(tid, dir, &nm, etag, m.store, m.prefix, m.id, m.routeID, m.opts, m.metrics, m)
		if err == nil {
			t.ckptSem = m.ckptSem
			t.claimSem = m.claimSem
			t.qlim = m.qlim
		}
		if err != nil {
			coolFailedClaim()
			return nil, err
		}
	}
	t.onClose = m.noteClosed
	t.onPoison = m.notePoisoned
	t.claimAttempt = attemptID
	// Fold replayed WAL into a fresh base and claim ownership in one CAS.
	okCh := make(chan bool, 1)
	t.ctlCh <- func() { okCh <- t.claimLocked(etag) }
	if !(<-okCh) {
		err := t.deadErr
		if err == nil {
			err = ErrCAS
		}
		// a failed claim is not channelled through noteClosed (the tenant
		// never registered), so record the cooldown here: retries of a
		// failed claim must not storm the store either
		if !errors.Is(err, ErrCAS) && !errors.Is(err, errStaleGen) {
			coolFailedClaim()
		}
		return nil, err
	}
	return t, nil
}

// materialize downloads base + segments into dir (extracted from takeOver
// so the corrupt-cache retry can redo it).
func (m *Manager) materialize(ctx context.Context, tid string, mf *Manifest, dir string) error {
	return m.materializeLocked(ctx, tid, mf, dir)
}

var errStaleGen = errors.New("generation objects changed during recovery")

// errDecodeSlotWait: the caller's context ended while all decode slots
// were busy. Nothing touched the store, so no claim cooldown follows.
var errDecodeSlotWait = errors.New("waiting for a decode slot")

// formatGate refuses a manifest whose format this build cannot read.
func (m *Manager) formatGate(tid string, mf *Manifest) error {
	switch {
	case m.legacyReadForTest:
		return nil // the build before compression has no gate
	case m.s1ReadForTest:
		return checkManifestFormatMax(tid, mf, manifestFormatZstd)
	}
	return checkManifestFormat(tid, mf)
}

// materializeLocked downloads the base image, applies the delta chain and
// replays the contiguous segment run into dir.
func (m *Manager) materializeLocked(ctx context.Context, tid string, mf *Manifest, dir string) error {
	format := mf.Format
	chain := format == manifestFormatChain
	if m.legacyReadForTest {
		format = manifestFormatRaw // emulate a build from before compression
		chain = false              // ... which does not know deltas
	} else if err := m.formatGate(tid, mf); err != nil {
		return err
	} else if m.s1ReadForTest {
		chain = false
	}
	// a delta list outside a chain generation (or a chain without a size)
	// is a manifest no writer of this build produces: refuse it
	if !m.legacyReadForTest && !m.s1ReadForTest {
		if (format != manifestFormatChain && len(mf.Deltas) > 0) || (chain && mf.BaseSize <= 0) {
			return fmt.Errorf("inconsistent manifest for %s: format %d, %d deltas, base size %d", tid, format, len(mf.Deltas), mf.BaseSize)
		}
	}
	if format != manifestFormatRaw {
		// decoders hold memory while they run: bound how many
		// materializations decode at once, pod-wide. This slot is held only
		// here, never together with the claim or checkpoint semaphores.
		select {
		case m.decodeSem <- struct{}{}:
			defer func() { <-m.decodeSem }()
		case <-ctx.Done():
			return fmt.Errorf("%w: %w", errDecodeSlotWait, ctx.Err())
		}
	}
	dbPath := filepath.Join(dir, "db.sqlite")
	walPath := filepath.Join(dir, "db.sqlite-wal")

	base, err := os.OpenFile(dbPath, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0o644) // read too: the chain checks the header
	if err != nil {
		return err
	}
	// the generation format says how every object of the generation is
	// stored; there is no content sniffing. A zstd base streams through
	// the decoder to disk, never whole into memory.
	_, stored, err := fetchObjectTo(ctx, m.store, mf.BaseKey, base, format, maxBaseRawBytes)
	if err != nil {
		base.Close()
		if errors.Is(err, ErrNotFound) {
			return errStaleGen
		}
		return fmt.Errorf("fetch base: %w", err)
	}
	if chain {
		if stored != mf.BaseSize {
			base.Close()
			return fmt.Errorf("fetch base: %w: stored size %d, manifest says %d", ErrCorruptObject, stored, mf.BaseSize)
		}
		// base + deltas in order = the state at the start of this gen
		if err := applyChain(ctx, m.store, tid, mf, dir, base); err != nil {
			base.Close()
			return err
		}
	}
	if err := base.Close(); err != nil {
		return err
	}
	segs, err := m.store.ListSegments(ctx, tid, mf.Instance, mf.Gen)
	if err != nil {
		return fmt.Errorf("list segments: %w", err)
	}
	wal, err := os.OpenFile(walPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	var expect int64
	for _, s := range segs {
		if s.Offset > expect {
			break // gap: durable tail ends here
		}
		if s.Offset < expect {
			continue // duplicate offset from an older epoch; newer sorted first
		}
		var n int64
		if format == manifestFormatRaw {
			data, err := m.store.GetObject(ctx, s.Key)
			if err == nil {
				_, err = wal.Write(data)
			}
			n = int64(len(data))
			if err != nil {
				wal.Close()
				if errors.Is(err, ErrNotFound) {
					// the winner of a concurrent claim already GC'd this gen:
					// our manifest read is stale; restart from a fresh read
					return errStaleGen
				}
				return fmt.Errorf("fetch segment %s: %w", s.Key, err)
			}
		} else {
			// streams to the WAL file: a segment of any size up to the
			// writer's limit decodes without holding it in memory
			n, _, err = fetchDecodedTo(ctx, m.store, s.Key, wal, maxSegmentRawBytes)
			if err != nil {
				wal.Close()
				if errors.Is(err, ErrNotFound) {
					return errStaleGen
				}
				if errors.Is(err, ErrCorruptObject) {
					// corruption in an immutable object, exactly like a
					// broken checksum chain below: same error class
					return fmt.Errorf("wal replay incomplete: segment %s: %w (corruption in stored segments)", s.Key, err)
				}
				return fmt.Errorf("fetch segment %s: %w", s.Key, err)
			}
		}
		// contiguity runs on RAW lengths: keys carry raw WAL offsets, and
		// the stored size of a compressed segment is smaller
		expect += n
	}
	wal.Close()
	if expect == 0 {
		os.Remove(walPath) // no WAL: clean open
		return nil
	}
	// completeness check: the concatenated segments are all at
	// commit boundaries, so the checksum chain must end exactly at EOF.
	// A short parse means corruption in supposedly immutable objects —
	// refuse rather than publish a truncated replacement over history.
	// The check streams: one frame in memory, whatever the WAL size.
	wf, err := os.Open(walPath)
	if err != nil {
		return err
	}
	defer wf.Close()
	end, err := walCommitEndReader(wf)
	if err != nil {
		return err
	}
	if fi, err := wf.Stat(); err != nil || fi.Size() != expect {
		return fmt.Errorf("wal replay: file size does not match %d segment bytes (%v)", expect, err)
	}
	if end != expect {
		return fmt.Errorf("wal replay incomplete: parsed %d of %d bytes (corruption in stored segments)", end, expect)
	}
	return nil
}

// claimLocked checkpoints and CASes the manifest with epoch already bumped.
// Runs inside the actor loop. etag is the pre-acquisition manifest etag.
func (t *Tenant) claimLocked(oldEtag string) bool {
	t.etag = oldEtag
	// capture the manager's lease generation BEFORE claiming; a lapse sweep
	// landing between the CAS and this check must fence us
	if m, ok := t.clock.(*Manager); ok {
		t.claimGen = m.leaseGen.Load()
	}
	if !t.checkpointLocked("") {
		return false
	}
	if m, ok := t.clock.(*Manager); ok && t.claimGen != m.leaseGen.Load() {
		// the CAS landed but a lapse sweep raced us: we own the manifest
		// yet must not serve. Mark claimed so noteClosed clears ownership.
		t.claimed = true
		t.failLocked(errors.New("lease generation moved during claim"))
		return false
	}
	t.claimed = true
	return true
}

// reclassLoop promotes hot tenants to the whale class: release with a
// class flip; a whale-pool pod acquires on the next touch.
func (m *Manager) reclassLoop() {
	tick := time.NewTicker(m.ctrl.ScanEvery)
	defer tick.Stop()
	for range tick.C {
		m.mu.Lock()
		tenants := make([]*Tenant, 0, len(m.tenants))
		for _, t := range m.tenants {
			tenants = append(tenants, t)
		}
		m.mu.Unlock()
		dt := m.ctrl.ScanEvery.Seconds()
		for _, t := range tenants {
			rate := float64(t.HeatCommits()) / dt
			hot := rate >= m.ctrl.WhaleThreshold
			if m.class == "whale" {
				// reverse direction on the whale pool: a whale-class tenant
				// that has been quiet for a while demotes back to standard,
				// so the pool drains when the load leaves
				if !hot && t.Class() == "whale" && t.lastQuietSince() > m.ctrl.Dwell {
					m.Release(t.ID, "standard")
				}
				continue
			}
			if !hot {
				m.mu.Lock()
				delete(m.hot, t.ID)
				m.mu.Unlock()
				continue
			}
			m.mu.Lock()
			m.hot[t.ID]++
			n := m.hot[t.ID]
			lastFlip := m.lastFlip[t.ID]
			m.mu.Unlock()
			if n >= m.ctrl.SustainedScans && time.Since(lastFlip) > m.ctrl.Dwell && t.Class() != "whale" {
				if m.metrics.Logger != nil {
					m.metrics.Logger.Printf("reclass %s -> whale (%.0f commits/s sustained)", t.ID, rate)
				}
				m.mu.Lock()
				m.lastFlip[t.ID] = time.Now()
				delete(m.hot, t.ID)
				m.mu.Unlock()
				m.Release(t.ID, "whale")
			}
		}
	}
}

// noteClosed removes a self-closed tenant from the registry (actor-driven
// idle release). Safe against races with eviction: only removes the exact
// instance that closed.
// noteClosed deregisters a closed tenant. A tenant that died ungracefully
// (poisoned, crashed) on a LIVE pod leaves the manifest naming us owner;
// clear it so peers can take over instantly instead of waiting out the
// pod lease. Best-effort CAS: if the etag moved, someone already took it.
// notePoisoned publishes the re-acquire cooldown. It runs from finishClose
// BEFORE t.done closes, so no acquirer can observe the tenant gone without
// also seeing the cooldown (publication race).
func (m *Manager) notePoisoned(t *Tenant) {
	if t.deadErr == nil || m.leaseLost.Load() {
		return // graceful release, or the pod itself is dead
	}
	m.recordPoison(t.ID)
}

// recordPoison writes the cooldown entry with sweep+cap maintenance. All
// poisonedAt writes must go through here.
func (m *Manager) recordPoison(tid string) {
	m.mu.Lock()
	now := time.Now()
	m.poisonedAt[tid] = now
	// amortized sweep: at most one pass per 30s. Cooldowns are <= 2s,
	// so anything older than 30s is dead weight. A hard cardinality
	// cap protects the lock if a burst arrives faster than the sweep.
	if len(m.poisonedAt) > 10000 && now.Sub(m.poisonSweep) > 30*time.Second {
		m.poisonSweep = now
		cutoff := now.Add(-30 * time.Second)
		for k, v := range m.poisonedAt {
			if v.Before(cutoff) {
				delete(m.poisonedAt, k)
			}
		}
	}
	if len(m.poisonedAt) > 50000 {
		// emergency: drop arbitrary entries to get under the cap
		for k := range m.poisonedAt {
			delete(m.poisonedAt, k)
			if len(m.poisonedAt) <= 25000 {
				break
			}
		}
	}
	m.mu.Unlock()
}

func (m *Manager) noteClosed(t *Tenant) {
	m.mu.Lock()
	if cur, ok := m.tenants[t.ID]; ok && cur == t {
		delete(m.tenants, t.ID)
	}
	m.mu.Unlock()
	if t.deadErr == nil || !t.claimed {
		return // graceful path released already, or never claimed
	}
	if m.leaseLost.Load() {
		return // the pod itself is dead: only lease expiry may free us
	}
	go m.clearDeadOwner(t.ID, t.claimEpoch, t.claimInst)
}

// clearDeadOwner removes our ownership of a dead/wedged tenant. Fence:
// the manifest must still name this pod AND carry the incarnation's
// claim epoch AND instance (epoch resets to 1 on delete/recreate, so
// epoch alone is not unique). A fresh claim changes epoch+instance and
// is never cleared. The write-fence wait ensures the dead tenant's last
// PUTs have landed or died before a peer can replay.
func (m *Manager) clearDeadOwner(tid string, claimEpoch int64, claimInst string) {
	fence := m.leaseUntil.Load() + m.opts.SkewMargin.Milliseconds()
	if d := time.Until(time.UnixMilli(fence)); d > 0 {
		time.Sleep(d)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	mf, cur, err := m.store.GetManifest(ctx, tid)
	if err != nil || mf == nil || mf.Owner != m.id {
		return // taken over or gone
	}
	if mf.Epoch != claimEpoch || mf.Instance != claimInst {
		return // a new incarnation claimed it; never touch it
	}
	nm := *mf
	nm.Owner = ""
	nm.OwnerUntilMs = 0
	nm.UpdatedMs = time.Now().UnixMilli()
	if _, err := m.store.PutManifestCAS(ctx, tid, &nm, cur); err != nil {
		m.metrics.Logger.Printf("clearDeadOwner %s: %v (takeover waits for pod lease instead)", tid, err)
	}
}

// KillLease simulates kill -9 of the whole pod (tests): heartbeats stop,
// the lease lapses on its own, all tenants die without flush or release.
func (m *Manager) KillLease() {
	m.stopLease()
	m.leaseLost.Store(true)
	m.failAll(errors.New("pod killed"))
}

// partitioned simulates a network partition (tests): heartbeats silently
// stop landing. The lease lapses; tenants fail-stop at the margin. When
// the partition heals, the gap is detected and all owned tenants are
// poisoned, because ownership may have moved while we were away.
func (m *Manager) SetPartitioned(p bool) { m.partitioned.Store(p) }

// CrashTenant poisons one tenant without release (debug/failover tests).
func (m *Manager) CrashTenant(tid string) error {
	t := m.LocalTenant(tid)
	if t == nil {
		return errors.New("not open locally")
	}
	t.Crash()
	return nil
}

// Forget drops a tenant from the local registry without S3 cleanup.
func (m *Manager) Forget(tid string) {
	m.mu.Lock()
	delete(m.tenants, tid)
	m.mu.Unlock()
}

// Release checkpoints and gives up ownership (handoff / class change).
func (m *Manager) Release(tid string, newClass string) error {
	t := m.LocalTenant(tid)
	if t == nil {
		return errors.New("not owned locally")
	}
	m.mu.Lock()
	ch := make(chan struct{})
	m.closing[tid] = ch
	m.mu.Unlock()

	t.Stop(true, newClass)

	m.mu.Lock()
	if cur, ok := m.tenants[tid]; ok && cur == t {
		delete(m.tenants, tid)
	}
	delete(m.closing, tid)
	close(ch)
	m.mu.Unlock()
	return nil
}

// ShutdownAll checkpoints and releases every owned tenant (preStop path).
func (m *Manager) ShutdownAll() {
	// idempotent: the preStop hook and the SIGTERM handler both call this;
	// a second run must not clobber the first run's closing bookkeeping
	if !m.draining.CompareAndSwap(false, true) {
		return
	}
	defer m.stopLease() // heartbeat stops after every tenant released
	m.mu.Lock()
	ts := make([]*Tenant, 0, len(m.tenants))
	for id, t := range m.tenants {
		ts = append(ts, t)
		m.closing[id] = make(chan struct{})
	}
	m.tenants = map[string]*Tenant{}
	m.mu.Unlock()
	var wg sync.WaitGroup
	for _, t := range ts {
		wg.Add(1)
		go func(t *Tenant) { defer wg.Done(); t.Stop(true, "") }(t)
	}
	wg.Wait()
	m.mu.Lock()
	for id := range m.closing {
		close(m.closing[id])
		delete(m.closing, id)
	}
	m.mu.Unlock()
}

func (m *Manager) evictLoop() {
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()
	for range tick.C {
		for {
			m.mu.Lock()
			var victim *Tenant
			var victimID string
			if len(m.tenants) > m.maxOpen {
				var oldest int64 = 1 << 62
				for id, t := range m.tenants {
					if lu := t.lastUsedMs.Load(); lu < oldest && t.Alive() {
						oldest = lu
						victim = t
						victimID = id
					}
				}
			}
			if victim == nil {
				m.mu.Unlock()
				break
			}
			ch := make(chan struct{})
			m.closing[victimID] = ch
			m.mu.Unlock()

			victim.Stop(true, "") // checkpoint + release so re-acquire is instant

			m.mu.Lock()
			delete(m.tenants, victimID)
			delete(m.closing, victimID)
			close(ch)
			m.mu.Unlock()
			m.metrics.Evictions.Add(1)
		}
	}
}

// OpenCount is the number of locally open tenants.
func (m *Manager) OpenCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.tenants)
}

// Saturation returns the pod's load as a ratio where 1.0 means "at the
// operating limit". It is the maximum of the commit-latency ratio and the
// in-flight-segment ratio — the two signals that preceded degradation in
// the 10k ramp (queueing behind S3, not CPU). HPA should target < 1.0.
func (m *Manager) Saturation() float64 {
	lat := float64(m.metrics.CommitEwmaUs.Load()) / m.satCommitBudgetUs
	inf := float64(m.metrics.InflightSegs.Load()) / m.satInflightCap
	if inf > lat {
		return inf
	}
	return lat
}

func (m *Manager) Stats() map[string]any {
	m.mu.Lock()
	n := len(m.tenants)
	ids := make([]string, 0, n)
	for id := range m.tenants {
		ids = append(ids, id)
	}
	m.mu.Unlock()
	return map[string]any{
		"replica": m.id, "class": m.class, "prefix": m.prefix, "open_tenants": n, "tenant_ids": ids,
		"commits": m.metrics.Commits.Load(), "queries": m.metrics.Queries.Load(),
		"seg_puts": m.metrics.SegPuts.Load(), "seg_bytes": m.metrics.SegBytes.Load(),
		"seg_stored_bytes": m.metrics.SegStoredBytes.Load(),
		"base_bytes":       m.metrics.BaseBytes.Load(), "base_stored_bytes": m.metrics.BaseStoredBytes.Load(),
		"delta_checkpoints": m.metrics.DeltaCheckpoints.Load(), "delta_bytes": m.metrics.DeltaBytes.Load(),
		"delta_stored_bytes": m.metrics.DeltaStoredBytes.Load(), "rebases": m.metrics.Rebases.Load(),
		"checkpoint_ms": m.metrics.CheckpointMs.Load(),
		"acquires":      m.metrics.Acquires.Load(), "checkpoints": m.metrics.Checkpoints.Load(),
		"proxied": m.metrics.Proxied.Load(), "evictions": m.metrics.Evictions.Load(),
		"dedup_hits": m.metrics.DedupHits.Load(),
	}
}
