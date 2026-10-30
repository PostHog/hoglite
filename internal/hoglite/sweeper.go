package hoglite

import (
	"context"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// Orphan sweeper. Writers that crash can leave objects no manifest
// references: dead instance directories, base uploads from lost claim
// attempts, old WAL generations whose cleanup died with the writer.
// The sweeper deletes anything the manifests do not name.
//
// Leadership is dogfooded: one replica acquires the __hw_sweeper_lock
// tenant through the normal path and sweeps while it holds it. Lock loss
// or tenant poison just moves the job.

const sweeperLockTenant = "__hw_sweeper_lock"

type Sweeper struct {
	mgr     *Manager
	store   Backend
	prefix  string
	opts    TenantOpts // lease + skew for the claim-in-flight fence
	metrics *Metrics

	sweeping atomic.Bool // one sweepWhileHeld at a time
}

func NewSweeper(m *Manager, store Backend, prefix string, opts TenantOpts, metrics *Metrics) *Sweeper {
	return &Sweeper{mgr: m, store: store, prefix: prefix, opts: opts, metrics: metrics}
}

// Run sweeps until ctx is cancelled. Call as a goroutine.
func (s *Sweeper) Run(ctx context.Context) {
	tick := time.NewTicker(60 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		if s.sweeping.Load() {
			continue // previous sweep still running
		}
		lock, err := s.mgr.Acquire(ctx, sweeperLockTenant, true)
		if err != nil {
			continue // someone else sweeps
		}
		s.sweeping.Store(true)
		go func() {
			defer s.sweeping.Store(false)
			s.sweepWhileHeld(ctx, lock)
		}()
	}
}

func (s *Sweeper) sweepWhileHeld(ctx context.Context, lock *Tenant) {
	defer lock.Stop(true, "")
	for lock.Alive() {
		select {
		case <-ctx.Done():
			return
		default:
		}
		s.sweepOnce(ctx)
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Minute):
		}
	}
}

// sweepOnce removes every object that no live manifest references.
func (s *Sweeper) sweepOnce(ctx context.Context) {
	tids, err := s.store.ListTenantIDs(ctx)
	if err != nil {
		return
	}
	for _, tid := range tids {
		if tid == sweeperLockTenant {
			continue
		}
		s.sweepTenant(ctx, tid)
	}
}

func (s *Sweeper) sweepTenant(ctx context.Context, tid string) {
	mf, etag1, err := s.store.GetManifest(ctx, tid)
	if err != nil || mf == nil {
		return
	}
	// a newer format may add object kinds this build does not know, and
	// the gen parse below would misjudge them: leave such tenants alone
	if checkManifestFormat(tid, mf) != nil {
		return
	}
	// never sweep a tenant whose owner's pod lease is live: the owner may
	// be mid-claim (base uploaded before CAS) or mid-checkpoint. Orphans
	// of live tenants are collected after they go idle (release) or dead
	// (pod lease expiry).
	if mf.Owner != "" {
		live, _, err := s.mgr.ownerLeaseLive(ctx, mf.Owner)
		if err != nil {
			return // unknown: never sweep on a read error
		}
		if live {
			return
		}
		// the lease object is stale or gone; fall through to the UpdatedMs
		// guard below, which still bounds claim-in-flight races.
	}
	// an expired lease does not mean "no claim in flight": an acquirer
	// uploads its candidate base before the CAS. Only sweep tenants whose
	// lease expired more than lease+skew ago, so no claim can be mid-flight.
	if mf.OwnerUntilMs > 0 &&
		time.Since(time.UnixMilli(mf.OwnerUntilMs)) < s.opts.Lease+s.opts.SkewMargin {
		return
	}
	// also skip tenants with a live lease whose manifest moved recently:
	// a claim writes UpdatedMs on acquire. Released tenants (owner="") are
	// safe to sweep regardless — a claim on a released tenant must still
	// pass the CAS fence below before anything is deleted.
	if mf.Owner != "" &&
		time.Since(time.UnixMilli(mf.UpdatedMs)) < s.opts.Lease+s.opts.SkewMargin {
		return
	}
	liveInstance := tenantPrefix(s.prefix, tid) + "/i/" + mf.Instance + "/"
	allPrefix := tenantPrefix(s.prefix, tid) + "/i/"
	keys, err := s.store.ListPrefix(ctx, allPrefix)
	if err != nil {
		return
	}
	var garbage []string
	for _, k := range keys {
		if !strings.HasPrefix(k, liveInstance) {
			garbage = append(garbage, k) // dead instance dir
			continue
		}
		// only strictly-older generations are safe to delete. A claim
		// candidate is always gen > current (acquisition bumps gen), and a
		// current-gen object may be mid-checkpoint. Parse the gen from the
		// key and delete only gen < manifest gen.
		g := keyGen(k, liveInstance)
		if g >= 0 && g < mf.Gen {
			garbage = append(garbage, k)
		}
	}
	if len(garbage) == 0 {
		return
	}
	// fence: the manifest must not have moved since we computed the set
	// (a fresh claim names a new gen/base; deleting from a stale read could
	// remove live objects)
	mf2, etag2, err := s.store.GetManifest(ctx, tid)
	if err != nil || mf2 == nil {
		return
	}
	if etag2 != etag1 || !manifestEqual(mf, mf2) {
		return // changed under us: recompute next cycle
	}
	_ = s.store.DeleteObjects(ctx, garbage...)
	s.metrics.Sweeps.Add(1)
	s.metrics.SweptObjects.Add(int64(len(garbage)))
}

func manifestEqual(a, b *Manifest) bool {
	return a.Epoch == b.Epoch && a.Gen == b.Gen && a.Instance == b.Instance &&
		a.Owner == b.Owner && a.OwnerUntilMs == b.OwnerUntilMs && a.BaseKey == b.BaseKey
}

// keyGen parses the generation from a base or wal key, anchored to the
// instance prefix so a bucket prefix containing "base"/"wal" components
// cannot mislead the parse.
func keyGen(key, instancePrefix string) int64 {
	if !strings.HasPrefix(key, instancePrefix) {
		return -1
	}
	rel := strings.TrimPrefix(key, instancePrefix)
	// layout: base/<gen>-<epoch>[-<attempt>].db  or  wal/<gen>/<epoch>/<off>.seg
	parts := strings.Split(rel, "/")
	if len(parts) < 2 {
		return -1
	}
	var g string
	switch parts[0] {
	case "base":
		g, _, _ = strings.Cut(parts[1], "-")
	case "wal":
		g = parts[1]
	default:
		return -1
	}
	n, err := strconv.ParseInt(g, 10, 64)
	if err != nil {
		return -1
	}
	return n
}
