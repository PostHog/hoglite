package hoglite

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// unnamedObjects lists tid's stored objects that mf does not name.
func unnamedObjects(fs *FakeStore, tid string, mf *Manifest) []string {
	named := map[string]bool{}
	for _, k := range manifestObjects(mf) {
		named[k] = true
	}
	var out []string
	for _, k := range tenantObjects(fs, tid) {
		if !named[k] {
			out = append(out, k)
		}
	}
	return out
}

// lockReplica is one replica of a cold pool, as far as the sweeper goes.
type lockReplica struct {
	mgr *Manager
	sw  *Sweeper
	log *logCapture
	met *Metrics
}

func newLockReplicas(t *testing.T, store Backend, n int) []*lockReplica {
	t.Helper()
	var rs []*lockReplica
	for i := 0; i < n; i++ {
		r := &lockReplica{log: &logCapture{}}
		r.met = &Metrics{Logger: r.log}
		r.mgr = NewManager(store, "fake", "hl-"+string(rune('0'+i)), "standard", t.TempDir(), testOpts(), 1000, r.met, ControllerOpts{})
		r.sw = NewSweeper(r.mgr, store, "fake", testOpts(), r.met)
		rs = append(rs, r)
	}
	return rs
}

// Cold start: every replica's sweeper takes __hw_sweeper_lock on the same
// tick. Exactly one leads. The others must see the designed fence outcome
// of a create race (no poison, no cooldown, no orphaned base) and step
// aside at once. Later ticks of the followers also step aside at once:
// the lock has a live holder, so there is nothing to wait for.
func TestSweeperLockColdStartRace(t *testing.T) {
	fs := NewFakeStore()
	const n = 3
	rs := newLockReplicas(t, newCreateBarrierStore(fs, sweeperLockTenant, n), n)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	led := make([]bool, n)
	took := make([]time.Duration, n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i, r := range rs {
		wg.Add(1)
		go func(i int, r *lockReplica) {
			defer wg.Done()
			<-start
			t0 := time.Now()
			led[i] = r.sw.tryLead(ctx)
			took[i] = time.Since(t0)
		}(i, r)
	}
	close(start)
	wg.Wait()

	leaders := 0
	var leader *lockReplica
	for i, r := range rs {
		t.Logf("%s: led=%v took=%v", r.mgr.ID(), led[i], took[i])
		if led[i] {
			leaders++
			leader = r
		}
		if took[i] > testOpts().Lease/2 {
			t.Errorf("%s: tick took %v; a lost lock race must step aside at once", r.mgr.ID(), took[i])
		}
	}
	if leaders != 1 {
		t.Fatalf("want exactly one sweeper leader, got %d", leaders)
	}
	mf, _, err := fs.GetManifest(context.Background(), sweeperLockTenant)
	if err != nil || mf == nil {
		t.Fatalf("no lock manifest: %v", err)
	}
	if mf.Owner != leader.mgr.ID() {
		t.Fatalf("lock manifest names %s, leader is %s", mf.Owner, leader.mgr.ID())
	}
	lost := int64(0)
	for _, r := range rs {
		if p := r.log.matching("poisoned"); len(p) > 0 {
			t.Errorf("%s logged a poison for a lost lock race: %q", r.mgr.ID(), p)
		}
		r.mgr.mu.Lock()
		_, cooled := r.mgr.poisonedAt[sweeperLockTenant]
		r.mgr.mu.Unlock()
		if cooled {
			t.Errorf("%s: a lost lock race started a poison cooldown", r.mgr.ID())
		}
		lost += r.met.CreateRacesLost.Load()
	}
	if lost != n-1 {
		t.Errorf("CreateRacesLost = %d across the pool, want %d", lost, n-1)
	}
	if o := unnamedObjects(fs, sweeperLockTenant, mf); len(o) > 0 {
		t.Errorf("orphaned objects under the lock tenant after the race: %v", o)
	}

	// later ticks: a follower answers at once while the leader holds the
	// lock, and the leader still holds it afterwards
	for _, r := range rs {
		if r == leader {
			continue
		}
		t0 := time.Now()
		if r.sw.tryLead(ctx) {
			t.Fatalf("%s took the lock from a live leader", r.mgr.ID())
		}
		if d := time.Since(t0); d > testOpts().Lease/2 {
			t.Errorf("%s: follower tick took %v with a live leader, want an immediate step-aside", r.mgr.ID(), d)
		}
	}
	if mf2, _, _ := fs.GetManifest(context.Background(), sweeperLockTenant); mf2.Owner != mf.Owner || mf2.Epoch != mf.Epoch {
		t.Fatalf("lock moved: %s/%d -> %s/%d", mf.Owner, mf.Epoch, mf2.Owner, mf2.Epoch)
	}
}

// The residual orphans of the lock tenant: a create-race loser that could
// not delete its base, and old-gen objects a failed post-claim delete
// leaves behind. The generic pass never collects them (the lock always has a
// live owner while a sweep runs, and sweepOnce skips it by name). The
// holder's own pass does, and the live lock survives: its base is intact
// and a peer can still take it over after a release.
func TestSweeperCollectsOwnLockOrphans(t *testing.T) {
	fs := NewFakeStore()
	bs := newCreateBarrierStore(fs, sweeperLockTenant, 2)
	bs.failDeletes.Store(true)
	rs := newLockReplicas(t, bs, 2)
	ctx := context.Background()

	winner, lost := raceCreate(t, sweeperLockTenant, rs[0].mgr, rs[1].mgr)
	if len(lost) != 1 || !errors.Is(lost[0], ErrBusy) {
		t.Fatalf("loser: want ErrBusy, got %v", lost)
	}
	bs.failDeletes.Store(false)
	holder, other := rs[0], rs[1]
	if winner != holder.mgr {
		holder, other = other, holder
	}
	mf, _, _ := fs.GetManifest(ctx, sweeperLockTenant)
	orphans := unnamedObjects(fs, sweeperLockTenant, mf)
	if len(orphans) != 1 || strings.Contains(orphans[0], "/i/"+mf.Instance+"/") {
		t.Fatalf("want the loser's base in a dead instance dir, got %v", orphans)
	}

	// the generic pass cannot help: it skips the lock tenant
	holder.sw.sweepOnce(ctx)
	if _, err := fs.GetObject(ctx, orphans[0]); err != nil {
		t.Fatalf("precondition: the generic pass is not expected to touch the lock tenant: %v", err)
	}
	lock := holder.mgr.LocalTenant(sweeperLockTenant)
	if lock == nil {
		t.Fatal("holder has no local lock tenant")
	}
	holder.sw.sweepOwnLock(ctx, lock)
	if _, err := fs.GetObject(ctx, orphans[0]); err == nil {
		t.Fatalf("lock-tenant orphan %s survived the holder's sweep", orphans[0])
	}
	if _, err := fs.GetObject(ctx, mf.BaseKey); err != nil {
		t.Fatalf("the sweep deleted the live lock's base: %v", err)
	}
	if !lock.Alive() {
		t.Fatal("the sweep killed the live lock")
	}

	// handover: a takeover of the unwritten lock uploads nothing and keeps
	// naming the first base, whose gen is now older than the manifest's.
	// Plant the objects a failed post-claim delete of a rebase would leave
	// (an old-gen chain base, an old-format base) and sweep again.
	if err := holder.mgr.Release(sweeperLockTenant, ""); err != nil {
		t.Fatal(err)
	}
	lock2, err := other.mgr.Acquire(ctx, sweeperLockTenant, true)
	if err != nil {
		t.Fatalf("peer takeover of the released lock: %v", err)
	}
	mf2, _, _ := fs.GetManifest(ctx, sweeperLockTenant)
	if mf2.Owner != other.mgr.ID() || mf2.Gen <= mf.Gen || mf2.BaseKey != mf.BaseKey {
		t.Fatalf("want a takeover that keeps naming the first base at a newer gen, got %+v", mf2)
	}
	inst := tenantPrefix("fake", sweeperLockTenant) + "/i/" + mf2.Instance + "/"
	stale := []string{
		fmt.Sprintf("%sckpt/%06d-%06d-cccccccc.base", inst, mf2.Gen-1, mf2.Epoch-1),
		inst + "base/000001-000001.db",
	}
	for _, k := range stale {
		if err := fs.PutSegment(ctx, k, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	other.sw.sweepOwnLock(ctx, lock2)
	for _, k := range stale {
		if _, err := fs.GetObject(ctx, k); err == nil {
			t.Fatalf("stale lock object %s survived the holder's sweep", k)
		}
	}
	if _, err := fs.GetObject(ctx, mf2.BaseKey); err != nil {
		t.Fatalf("the sweep deleted the live lock's base (an older gen than the manifest, but named): %v", err)
	}
	if o := unnamedObjects(fs, sweeperLockTenant, mf2); len(o) > 0 {
		t.Fatalf("lock tenant still holds unnamed objects: %v", o)
	}
	// the lock is still a working tenant: release it and take it back
	if err := other.mgr.Release(sweeperLockTenant, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := holder.mgr.Acquire(ctx, sweeperLockTenant, true); err != nil {
		t.Fatalf("lock unusable after the sweep: %v", err)
	}
}

// The holder's pass deletes only what can never be live: never the base
// the manifest names, never a current-gen or newer object in the live
// instance (a checkpoint or a peer's claim candidate in flight), nothing
// when the manifest names another incarnation, and nothing while the pod
// lease is inside its margin.
func TestSweeperOwnLockSparesLiveObjects(t *testing.T) {
	fs := NewFakeStore()
	ctx := context.Background()
	rs := newLockReplicas(t, fs, 1)
	r := rs[0]
	lock, err := r.mgr.Acquire(ctx, sweeperLockTenant, true)
	if err != nil {
		t.Fatal(err)
	}
	mf, _, _ := fs.GetManifest(ctx, sweeperLockTenant)
	inst := tenantPrefix("fake", sweeperLockTenant) + "/i/" + mf.Instance + "/"
	put := func(k string) {
		if err := fs.PutSegment(ctx, k, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	exists := func(k string) bool { _, err := fs.GetObject(ctx, k); return err == nil }
	sameGen := fmt.Sprintf("%sckpt/%06d-%06d-aaaaaaaa.base", inst, mf.Gen, mf.Epoch)     // current gen, unnamed: mid-checkpoint
	nextGen := fmt.Sprintf("%sckpt/%06d-%06d-bbbbbbbb.base", inst, mf.Gen+1, mf.Epoch+1) // a peer's claim candidate
	deadDir := tenantPrefix("fake", sweeperLockTenant) + "/i/deadbeefdeadbeef/ckpt/000001-000001.base"

	// another incarnation named by the manifest: hands off
	put(deadDir)
	foreign := *mf
	foreign.Owner = "someone-else"
	foreign.Epoch = mf.Epoch + 1
	_, etag, _ := fs.GetManifest(ctx, sweeperLockTenant)
	etag2, err := fs.PutManifestCAS(ctx, sweeperLockTenant, &foreign, etag)
	if err != nil {
		t.Fatal(err)
	}
	r.sw.sweepOwnLock(ctx, lock)
	if !exists(deadDir) {
		t.Fatal("swept a lock tenant whose manifest names another incarnation")
	}
	if _, err := fs.PutManifestCAS(ctx, sweeperLockTenant, mf, etag2); err != nil {
		t.Fatal(err)
	}

	// pod lease inside its margin: hands off
	r.mgr.gating.Store(true)
	r.sw.sweepOwnLock(ctx, lock)
	r.mgr.gating.Store(false)
	if !exists(deadDir) {
		t.Fatal("swept the lock tenant while the pod lease was inside its margin")
	}

	// our incarnation, lease fine: only the dead instance dir goes
	put(sameGen)
	put(nextGen)
	r.sw.sweepOwnLock(ctx, lock)
	if exists(deadDir) {
		t.Fatal("dead instance dir survived")
	}
	for _, k := range []string{mf.BaseKey, sameGen, nextGen} {
		if !exists(k) {
			t.Fatalf("swept a live or possibly-live object %s", k)
		}
	}
	if !lock.Alive() {
		t.Fatal("lock died")
	}
}

// End to end through the leader loop: a replica that takes the lock on a
// tick collects the lock tenant's orphan in its first pass.
func TestSweeperLeaderCollectsOwnLockOrphan(t *testing.T) {
	fs := NewFakeStore()
	bs := newCreateBarrierStore(fs, sweeperLockTenant, 2)
	bs.failDeletes.Store(true)
	rs := newLockReplicas(t, bs, 2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	winner, _ := raceCreate(t, sweeperLockTenant, rs[0].mgr, rs[1].mgr)
	bs.failDeletes.Store(false)
	mf, _, _ := fs.GetManifest(ctx, sweeperLockTenant)
	orphans := unnamedObjects(fs, sweeperLockTenant, mf)
	if len(orphans) != 1 {
		t.Fatalf("want one lock-tenant orphan, got %v", orphans)
	}
	// the winner lets go; the other replica's next tick takes the lock
	if err := winner.Release(sweeperLockTenant, ""); err != nil {
		t.Fatal(err)
	}
	next := rs[0]
	if next.mgr == winner {
		next = rs[1]
	}
	if !next.sw.tryLead(ctx) {
		t.Fatal("the free lock was not taken")
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := fs.GetObject(ctx, orphans[0]); err != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the sweeper leader never collected the lock-tenant orphan %s", orphans[0])
		}
		time.Sleep(20 * time.Millisecond)
	}
	mf2, _, _ := fs.GetManifest(ctx, sweeperLockTenant)
	if _, err := fs.GetObject(ctx, mf2.BaseKey); err != nil {
		t.Fatalf("live lock base gone: %v", err)
	}
	if lock := next.mgr.LocalTenant(sweeperLockTenant); lock == nil || !lock.Alive() {
		t.Fatal("the leader lost its lock")
	}
}
