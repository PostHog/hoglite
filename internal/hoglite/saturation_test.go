package hoglite

import (
	"context"
	"fmt"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// hoglite_saturation is max(commit-latency term, in-flight term), and the
// autoscaler sums it. The latency term is the commit EWMA, which only
// moves when a commit ends: without decay, one slow burst pins an idle
// pod above 1 and the fleet never scales in. These tests pin both sides:
// the term decays once the pod is idle, and it stays at full value while
// the pod serves (or is stuck on) slow commits.

const scaleThreshold = 0.7 // KEDA ScaledObject threshold on sum(hoglite_saturation)

func TestSatLatencyFactor(t *testing.T) {
	grace, hl := 30*time.Second, 30*time.Second
	end := time.UnixMilli(1_700_000_000_000)
	last := end.UnixMilli()
	for _, tc := range []struct {
		name     string
		lastMs   int64
		inflight int64
		age      time.Duration
		want     float64
	}{
		{"never committed", 0, 0, time.Hour, 1},
		{"just ended", last, 0, 0, 1},
		{"inside grace", last, 0, grace, 1},
		{"one half-life past grace", last, 0, grace + hl, 0.5},
		{"two half-lives past grace", last, 0, grace + 2*hl, 0.25},
		{"commit in flight, long quiet", last, 1, time.Hour, 1},
	} {
		got := satLatencyFactor(end.Add(tc.age), tc.lastMs, tc.inflight, grace, hl)
		if math.Abs(got-tc.want) > 1e-9 {
			t.Errorf("%s: factor %v, want %v", tc.name, got, tc.want)
		}
	}
	// monotone over an idle stretch: no oscillation without a commit
	prev := 2.0
	for age := time.Duration(0); age < 10*time.Minute; age += 250 * time.Millisecond {
		f := satLatencyFactor(end.Add(age), last, 0, grace, hl)
		if f > prev || f < 0 {
			t.Fatalf("factor %v at %v after %v: not monotone", f, age, prev)
		}
		prev = f
	}
	// the defaults: 2.0 falls below the threshold 75.5s after the last commit
	g, h := (&Metrics{}).satDecay()
	if g != defaultSatIdleGrace || h != defaultSatHalfLife {
		t.Fatalf("zero Metrics decay %v/%v, want the defaults", g, h)
	}
	if s := 2.0 * satLatencyFactor(end.Add(76*time.Second), last, 0, g, h); s >= scaleThreshold {
		t.Fatalf("2.0 after 76s idle = %v, want < %v", s, scaleThreshold)
	}
	if s := 2.0 * satLatencyFactor(end.Add(30*time.Second), last, 0, g, h); s != 2.0 {
		t.Fatalf("2.0 at the end of the grace = %v, want unchanged", s)
	}
}

// satClock is a clock the saturation tests drive by hand. The decay
// depends only on the clock and the commit state, so these tests run on
// it rather than on sleeps: what they assert is then the same on a busy
// machine as on an idle one, and a commit stuck for "minutes" costs no
// wall time. Each slowed segment PUT advances it (segGateStore.clock),
// so every commit's latency sample is at least the PUT delay.
type satClock struct{ ns atomic.Int64 }

func newSatClock() *satClock {
	c := &satClock{}
	c.ns.Store(time.UnixMilli(1_700_000_000_000).UnixNano())
	return c
}

func (c *satClock) Now() time.Time          { return time.Unix(0, c.ns.Load()) }
func (c *satClock) Advance(d time.Duration) { c.ns.Add(int64(d)) }

// satKeys hands out row keys. Keys must be unique across concurrent
// writers: a wall-clock key (time.Now().UnixNano()) repeats when two
// goroutines read the clock in the same tick, which they do when one
// group commit releases both, and the insert then fails on the primary
// key (CI run 41295621383).
var satKeys atomic.Int64

// satTestManager builds a pod whose commit-latency budget is budget, on
// the clock clk. The tests slow segment PUTs to 2.5 x budget, so a slow
// commit reads as 2.5. The decay is shortened to grace 300ms, half-life
// 150ms (of clk).
func satTestManager(t *testing.T, fs Backend, met *Metrics, clk *satClock, budget time.Duration) *Manager {
	t.Helper()
	met.satIdleGraceNs.Store(int64(300 * time.Millisecond))
	met.satHalfLifeNs.Store(int64(150 * time.Millisecond))
	met.satNow = clk.Now
	if gs, ok := fs.(*segGateStore); ok {
		gs.clock = clk
	}
	m := NewManagerSat(fs, "fake", "a", "standard", t.TempDir(), deltaTestOpts(), 1000, met, ControllerOpts{},
		float64(budget.Microseconds()), 128)
	t.Cleanup(m.ShutdownAll)
	return m
}

func commitN(t *testing.T, ten *Tenant, prefix string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if _, _, err := ten.SubmitTx(TxRequest{Client: "c", TxID: fmt.Sprint(prefix, i),
			Stmts: []Statement{insertStmt(satKeys.Add(1), "v")}}); err != nil {
			t.Fatalf("commit %s%d: %v", prefix, i, err)
		}
	}
}

// (a) A burst of slow commits, then idle: saturation holds through the
// grace, then falls monotonically and crosses the scaling threshold at
// grace + halfLife*log2(S/threshold). A commit after the idle stretch
// moves it one EWMA step from the decayed value.
func TestSaturationDecaysAfterSlowBurst(t *testing.T) {
	const lat = 50 * time.Millisecond
	fs := newSegGateStore()
	met := &Metrics{}
	clk := newSatClock()
	const budget = lat * 2 / 5
	m := satTestManager(t, fs, met, clk, budget)
	mustCreate(t, m, "t1")
	makeSchema(t, m, "t1")
	ten := m.LocalTenant("t1")

	fs.delay.Store(int64(lat))
	commitN(t, ten, "slow", 24) // alpha 1/8: the EWMA is within 5% of lat
	fs.delay.Store(0)
	idleFrom := clk.Now() // the last commit ended now, on a whole ms
	s0 := m.Saturation()
	if s0 < 2 {
		t.Fatalf("saturation after the slow burst = %.3f, want >= 2", s0)
	}
	grace, hl := met.satDecay()
	below := grace + time.Duration(float64(hl)*math.Log2(s0/scaleThreshold))
	t.Logf("slow burst: saturation %.3f, expected below %.1f at %v idle", s0, scaleThreshold, below)

	const tick = 5 * time.Millisecond
	prev, crossed := s0, time.Duration(0)
	for age := time.Duration(0); age < below+300*time.Millisecond; age = clk.Now().Sub(idleFrom) {
		s := m.Saturation()
		if s > prev+1e-9 {
			t.Fatalf("saturation rose while idle: %.4f -> %.4f at %v", prev, s, age)
		}
		if age <= grace && s != s0 {
			t.Fatalf("saturation decayed inside the grace: %.4f at %v, want %.4f", s, age, s0)
		}
		if age > grace && s >= s0 {
			t.Fatalf("saturation %.4f not decaying %v past the grace", s, age-grace)
		}
		if n := met.InflightSegs.Load(); n != 0 {
			t.Fatalf("%d segments in flight on an idle pod", n)
		}
		if crossed == 0 && s < scaleThreshold {
			crossed = age
		}
		prev = s
		clk.Advance(tick)
	}
	if crossed == 0 {
		t.Fatalf("saturation %.4f still at or above %.1f after %v idle", prev, scaleThreshold, clk.Now().Sub(idleFrom))
	}
	if crossed < below || crossed > below+tick {
		t.Fatalf("crossed %.1f after %v idle, want within one %v tick of %v", scaleThreshold, crossed, tick, below)
	}
	t.Logf("crossed %.1f after %v idle", scaleThreshold, crossed)

	// A commit after the idle stretch moves the signal one EWMA step from
	// what the pod showed: not back up to the stale burst (fast commit),
	// and not to the whole of one sample (slow commit after a long idle).
	step := func(what string, slow bool) {
		t.Helper()
		before := m.Saturation()
		if slow {
			fs.delay.Store(int64(lat))
		}
		c0 := clk.Now()
		commitN(t, ten, "after-idle-"+what, 1) // fresh TxIDs: a dedup hit never reaches S3
		took := clk.Now().Sub(c0)
		fs.delay.Store(0)
		after := m.Saturation()
		bound := before + float64(took.Microseconds())/8/float64(budget.Microseconds())
		t.Logf("%s commit after idle: saturation %.3f -> %.3f (bound %.3f)", what, before, after, bound)
		if after > bound+1e-3 {
			t.Fatalf("%s commit after idle: saturation %.3f -> %.3f, above one EWMA step (%.3f)", what, before, after, bound)
		}
		if after >= scaleThreshold {
			t.Fatalf("one %s commit after idle lifted saturation to %.3f", what, after)
		}
	}
	step("fast", false)
	// long enough that the stored EWMA decays below 1us: it must not reach
	// 0, or ewmaUpdate would seed it with the whole next sample
	clk.Advance(grace + 18*hl)
	step("slow", true)
}

// (b) Sustained slow commits keep saturation at the full EWMA for many
// grace periods, and a commit stuck in flight with nothing ending keeps it
// too: the autoscaler must not go blind while the pod is slow.
func TestSaturationHoldsUnderSlowCommits(t *testing.T) {
	const lat = 50 * time.Millisecond
	t.Run("sustained", func(t *testing.T) {
		fs := newSegGateStore()
		met := &Metrics{}
		clk := newSatClock()
		m := satTestManager(t, fs, met, clk, lat*2/5)
		mustCreate(t, m, "t1")
		makeSchema(t, m, "t1")
		ten := m.LocalTenant("t1")
		fs.delay.Store(int64(lat))
		commitN(t, ten, "warm", 24)

		grace, _ := met.satDecay()
		stop := make(chan struct{})
		var wg sync.WaitGroup
		var errs atomic.Value
		var commits atomic.Int64
		for w := 0; w < 2; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				for i := 0; ; i++ {
					select {
					case <-stop:
						return
					default:
					}
					if _, _, err := ten.SubmitTx(TxRequest{Client: fmt.Sprint("w", w), TxID: fmt.Sprint(i),
						Stmts: []Statement{insertStmt(satKeys.Add(1), "v")}}); err != nil {
						errs.Store(fmt.Errorf("worker %d tx %d: %w", w, i, err))
						return
					}
					commits.Add(1)
				}
			}(w)
		}
		// every slowed PUT advances clk by lat: run until 7 graces have
		// passed on it, reading as often as the scheduler lets us
		low, reads := math.Inf(1), 0
		for start := clk.Now(); clk.Now().Sub(start) < 7*grace; runtime.Gosched() {
			low = math.Min(low, m.Saturation())
			reads++
			if errs.Load() != nil {
				break
			}
		}
		close(stop)
		wg.Wait()
		if err, _ := errs.Load().(error); err != nil {
			t.Fatal(err)
		}
		// every commit takes at least lat = 2.5 x budget, and the warm-up
		// brought the EWMA within 5% of it
		if low < 2 {
			t.Fatalf("saturation fell to %.3f under sustained slow commits, want >= 2", low)
		}
		t.Logf("sustained slow commits for %v: %d commits, %d reads, saturation min %.3f", 7*grace, commits.Load(), reads, low)
	})
	t.Run("stalled commit", func(t *testing.T) {
		fs := newSegGateStore()
		met := &Metrics{}
		clk := newSatClock()
		m := satTestManager(t, fs, met, clk, lat*2/5)
		// runs before the shutdown: a failed check must not leave its drain
		// behind the held PUT
		t.Cleanup(func() { fs.armed.Store(false) })
		mustCreate(t, m, "t1")
		makeSchema(t, m, "t1")
		ten := m.LocalTenant("t1")
		fs.delay.Store(int64(lat))
		commitN(t, ten, "slow", 4)
		s0 := m.Saturation()

		fs.armed.Store(true)
		txDone := make(chan error, 1)
		go func() {
			_, _, err := ten.SubmitTx(TxRequest{Client: "c", TxID: "stuck", Stmts: []Statement{insertStmt(-1, "v")}})
			txDone <- err
		}()
		<-fs.entered
		grace, hl := met.satDecay()
		stuck := grace + 6*hl
		for start := clk.Now(); clk.Now().Sub(start) < stuck; clk.Advance(5 * time.Millisecond) {
			if s := m.Saturation(); s < s0 {
				t.Fatalf("saturation decayed to %.3f (from %.3f) while a commit was stuck for %v", s, s0, clk.Now().Sub(start))
			}
		}
		fs.armed.Store(false)
		fs.gate <- struct{}{}
		if err := <-txDone; err != nil {
			t.Fatal(err)
		}
		if s := m.Saturation(); s <= s0 {
			t.Fatalf("saturation %.3f after a commit stuck for %v, want above %.3f", s, stuck, s0)
		}
	})
}

// (c) A fresh pod that never commits reports 0, read after read, past
// many half-lives: no drift from the decay or from reads.
func TestSaturationBaselineWithoutCommits(t *testing.T) {
	fs := NewFakeStore()
	met := &Metrics{}
	clk := newSatClock()
	m := satTestManager(t, fs, met, clk, 20*time.Millisecond)
	mustCreate(t, m, "t1")
	if _, err := m.Acquire(context.Background(), "t2", true); err != nil {
		t.Fatal(err)
	}
	grace, hl := met.satDecay()
	for start := clk.Now(); clk.Now().Sub(start) < grace+6*hl; clk.Advance(2 * time.Millisecond) {
		if s := m.Saturation(); s != 0 {
			t.Fatalf("saturation %.4f on a pod with no commits, want 0", s)
		}
	}
	if n := met.CommitEwmaUs.Load(); n != 0 {
		t.Fatalf("commit EWMA %d without commits", n)
	}
}

// (d) A read is one moment of the commit state. The two edges of an idle
// stretch race a concurrent read: a commit stuck past the grace ending
// (it must not read as an idle pod, the latency term decayed to ~0), and
// the first commit after a long idle starting (it must not read as the
// stale burst before commitStart folds the decay in). Before satMu both
// showed up in a few hundred trials under -race.
func TestSaturationReadIsOneMoment(t *testing.T) {
	const trials, reads = 4000, 200
	race := func(arm func(*Metrics, *satClock), act func(*Metrics, *satClock), bad func(s, s0 float64) bool) int {
		hits := 0
		for i := 0; i < trials; i++ {
			met, clk := &Metrics{}, newSatClock()
			met.satNow = clk.Now
			mgr := &Manager{metrics: met, satCommitBudgetUs: 1000, satInflightCap: 128}
			met.CommitEwmaUs.Store(2000) // 2.0 at full value
			met.CommitLastMs.Store(clk.Now().Add(-time.Hour).UnixMilli())
			arm(met, clk)
			s0 := mgr.Saturation()
			done := make(chan struct{})
			go func() { act(met, clk); close(done) }()
			for r := 0; r < reads; r++ {
				if bad(mgr.Saturation(), s0) {
					hits++
					break
				}
			}
			<-done
		}
		return hits
	}
	// a commit in flight for an hour: full value, and more once it ends
	dips := race(
		func(met *Metrics, _ *satClock) { met.CommitsInflight.Store(1) },
		func(met *Metrics, clk *satClock) { met.commitDone(clk.Now().Add(-time.Hour)) },
		func(s, s0 float64) bool { return s < s0 })
	// idle for an hour: ~0, and the first commit starts from there
	spikes := race(
		func(*Metrics, *satClock) {},
		func(met *Metrics, _ *satClock) { met.commitStart() },
		func(s, s0 float64) bool { return s > s0+0.01 })
	if dips != 0 || spikes != 0 {
		t.Fatalf("in %d trials: %d reads saw a stuck commit's end as an idle pod, %d saw the stale burst at a commit's start", trials, dips, spikes)
	}
}
