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
		s.tryLead(ctx)
	}
}

// tryLead is one tick of Run: take the lock and start sweeping, or step
// aside. It reports whether this replica now sweeps.
//
// On a cold start every replica takes the lock at the same tick, so the
// lock tenant sees the same create race as any tenant all replicas create
// at once. Acquire handles it: the create CAS fences it, the losers delete
// their base, are not poisoned, and get ErrBusy at once instead of
// waiting out the winner's lease.
//
// On later ticks a follower steps aside as soon as it sees a live holder.
// Acquire would wait out the holder's lease, and with the holder
// heartbeating, block until its deadline (lease+15 s) every tick. Stepping
// aside on a lease that is still valid for a holder that just died only
// costs one tick: the next one finds the lease expired and takes over.
// No answer leaves this replica on the strength of that lease.
func (s *Sweeper) tryLead(ctx context.Context) bool {
	if s.sweeping.Load() {
		return false // previous sweep still running
	}
	if mf, _, err := s.store.GetManifest(ctx, sweeperLockTenant); err == nil && mf != nil &&
		mf.Owner != "" && mf.Owner != s.mgr.id {
		if live, _, err := s.mgr.ownerLeaseLive(ctx, mf.Owner); err == nil && live {
			return false // a live peer sweeps
		}
	}
	lock, err := s.mgr.Acquire(ctx, sweeperLockTenant, true)
	if err != nil {
		return false // someone else sweeps
	}
	s.sweeping.Store(true)
	go func() {
		defer s.sweeping.Store(false)
		s.sweepWhileHeld(ctx, lock)
	}()
	return true
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
		s.sweepOwnLock(ctx, lock)
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
			// held by this replica for as long as it sweeps, so the
			// live-owner guard of sweepTenant would always skip it:
			// sweepOwnLock collects it under the lock instead
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
	if s.mgr.formatGate(tid, mf) != nil {
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
	garbage := staleKeys(mf, keys, liveInstance)
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

// sweepOwnLock collects the stale objects of the lock tenant itself.
//
// sweepTenant can never do it: it skips tenants with a live owner, and
// whenever a sweep runs the lock tenant's owner is this replica. Without
// this pass nothing ever collects them. The lock gets no writes, so a
// handover uploads nothing and the stale objects are few: the base of a
// cold-start create-race loser whose delete failed (one per losing
// replica each time the lock tenant is created), and any old-gen object
// whose post-claim delete failed, should a claim ever rewrite the lock's
// base. Few is not none, and nothing else would ever remove them, so the
// holder sweeps its own lock.
//
// The owner is us, so the hazards the generic guards stand in for are
// known here. It is safe to delete exactly what staleKeys returns:
//   - Our own incarnation: the manifest must name this pod, the lock's
//     claim epoch and its instance, and must not move between the two
//     reads. Otherwise do nothing; the next sweep looks again.
//   - Our actor writes nothing past the claim. The lock gets no commits
//     (its idle release only CASes owner=""), and the claim checkpoint
//     finished before Acquire returned. staleKeys never returns a
//     current-gen object in the live instance anyway.
//   - Peers: a takeover writes its candidate into the live instance at
//     gen+1, which staleKeys keeps. A dead instance dir can only receive
//     a create upload, and a create CAS cannot succeed while a manifest
//     exists, so that upload is a loser's and deleting it loses nothing.
//   - Defense in depth: we delete only while our pod lease is outside its
//     margin and under the same deadline as any data-plane PUT, so no peer
//     can treat the lock as free while our deletes are in flight.
//
// The live lock's base (named by the manifest) is never in the set.
func (s *Sweeper) sweepOwnLock(ctx context.Context, lock *Tenant) {
	if !lock.Alive() || lock.ID != sweeperLockTenant {
		return
	}
	tid := lock.ID
	mf, etag1, err := s.store.GetManifest(ctx, tid)
	if err != nil || mf == nil || s.mgr.formatGate(tid, mf) != nil {
		return
	}
	ours := func(mf *Manifest) bool {
		return mf.Owner == s.mgr.id && mf.Epoch == lock.claimEpoch && mf.Instance == lock.claimInst
	}
	if !ours(mf) {
		return // released or taken over since we acquired it
	}
	liveInstance := tenantPrefix(s.prefix, tid) + "/i/" + mf.Instance + "/"
	keys, err := s.store.ListPrefix(ctx, tenantPrefix(s.prefix, tid)+"/i/")
	if err != nil {
		return
	}
	garbage := staleKeys(mf, keys, liveInstance)
	if len(garbage) == 0 {
		return
	}
	mf2, etag2, err := s.store.GetManifest(ctx, tid)
	if err != nil || mf2 == nil || etag2 != etag1 || !manifestEqual(mf, mf2) || !ours(mf2) {
		return
	}
	if !lock.Alive() || s.mgr.leaseExpiring() {
		return
	}
	dctx, cancel := context.WithDeadline(ctx, s.mgr.leaseWriteDeadline())
	defer cancel()
	if err := s.store.DeleteObjects(dctx, garbage...); err != nil {
		return
	}
	s.metrics.Sweeps.Add(1)
	s.metrics.SweptObjects.Add(int64(len(garbage)))
}

// staleKeys returns the keys under a tenant's instance dirs that mf does
// not name and can never name again: everything in a dead instance dir,
// and the unnamed objects of strictly older generations in the live one.
func staleKeys(mf *Manifest, keys []string, liveInstance string) []string {
	// the base and every delta the manifest names are live, whatever gen
	// their keys carry: a chain base and its deltas stay in use while
	// later gens append to the chain
	live := map[string]bool{}
	for _, k := range manifestObjects(mf) {
		live[k] = true
	}
	var garbage []string
	for _, k := range keys {
		if !strings.HasPrefix(k, liveInstance) {
			garbage = append(garbage, k) // dead instance dir
			continue
		}
		if live[k] {
			continue
		}
		// only strictly-older generations are safe to delete. A claim
		// candidate is always gen > current (acquisition bumps gen), and a
		// current-gen object may be mid-checkpoint. Parse the gen from the
		// key and delete only gen < manifest gen. An unnamed base or delta
		// of an older gen can never become live again: a chain only gains
		// objects of newer gens.
		g := keyGen(k, liveInstance)
		if g >= 0 && g < mf.Gen {
			garbage = append(garbage, k)
		}
	}
	return garbage
}

func manifestEqual(a, b *Manifest) bool {
	if len(a.Deltas) != len(b.Deltas) {
		return false
	}
	for i := range a.Deltas {
		if a.Deltas[i] != b.Deltas[i] {
			return false
		}
	}
	return a.Epoch == b.Epoch && a.Gen == b.Gen && a.Instance == b.Instance &&
		a.Owner == b.Owner && a.OwnerUntilMs == b.OwnerUntilMs && a.BaseKey == b.BaseKey &&
		a.Format == b.Format
}

// keyGen parses the generation from a base or wal key, anchored to the
// instance prefix so a bucket prefix containing "base"/"wal" components
// cannot mislead the parse.
func keyGen(key, instancePrefix string) int64 {
	if !strings.HasPrefix(key, instancePrefix) {
		return -1
	}
	rel := strings.TrimPrefix(key, instancePrefix)
	// layout: base/<gen>-<epoch>[-<attempt>].db, wal/<gen>/<epoch>/<off>.seg
	// or ckpt/<gen>-<epoch>[-<attempt>].{base,delta}
	parts := strings.Split(rel, "/")
	if len(parts) < 2 {
		return -1
	}
	var g string
	switch parts[0] {
	case "base", "ckpt":
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
