package hoglite

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// registryTID is the tenant hoglake creates on first start from every
// replica at once.
const registryTID = "hoglake-__hoglake_registry"

// createBarrierStore holds the create-only manifest CAS (etag "") of one
// tenant until n callers have reached it. Every racer has then already
// uploaded its base, which is the interleaving seen in production: all
// replicas read "no manifest", all upload, all CAS, one wins.
type createBarrierStore struct {
	*FakeStore
	tid     string
	n       int
	mu      sync.Mutex
	arrived int
	release chan struct{}
	// failDeletes makes every DELETE fail: the residual interleaving in
	// which a loser cannot remove its base (S3 error, or a crash between
	// the CAS and the delete)
	failDeletes atomic.Bool
}

func (s *createBarrierStore) DeleteObjects(ctx context.Context, keys ...string) error {
	if s.failDeletes.Load() {
		return errors.New("injected delete failure")
	}
	return s.FakeStore.DeleteObjects(ctx, keys...)
}

func newCreateBarrierStore(fs *FakeStore, tid string, n int) *createBarrierStore {
	return &createBarrierStore{FakeStore: fs, tid: tid, n: n, release: make(chan struct{})}
}

func (s *createBarrierStore) PutManifestCAS(ctx context.Context, tid string, m *Manifest, etag string) (string, error) {
	if tid == s.tid && etag == "" {
		s.mu.Lock()
		s.arrived++
		if s.arrived == s.n {
			close(s.release)
		}
		s.mu.Unlock()
		select {
		case <-s.release:
		case <-time.After(5 * time.Second): // a racer never arrived: let the test fail on its own checks
		}
	}
	return s.FakeStore.PutManifestCAS(ctx, tid, m, etag)
}

// logCapture records every log line of a manager.
type logCapture struct {
	mu    sync.Mutex
	lines []string
}

func (l *logCapture) Printf(f string, a ...any) {
	l.mu.Lock()
	l.lines = append(l.lines, fmt.Sprintf(f, a...))
	l.mu.Unlock()
}

func (l *logCapture) matching(sub string) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []string
	for _, s := range l.lines {
		if strings.Contains(s, sub) {
			out = append(out, s)
		}
	}
	return out
}

// raceCluster is a set of replicas on one store and one prefix, each with
// its own log and metrics, wired to proxy to each other.
type raceCluster struct {
	*testCluster
	logs map[string]*logCapture
	mets map[string]*Metrics
}

func newRaceCluster(t *testing.T, store Backend, fs *FakeStore, ids ...string) *raceCluster {
	t.Helper()
	c := &raceCluster{
		testCluster: &testCluster{fs: fs, servers: map[string]*httptest.Server{}, mgrs: map[string]*Manager{}},
		logs:        map[string]*logCapture{}, mets: map[string]*Metrics{},
	}
	for _, id := range ids {
		c.logs[id] = &logCapture{}
		c.mets[id] = &Metrics{Logger: c.logs[id]}
		c.mgrs[id] = NewManager(store, "fake", id, "standard", t.TempDir(), testOpts(), 1000, c.mets[id], ControllerOpts{})
	}
	urls := map[string]string{}
	for _, id := range ids {
		c.servers[id] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			NewServer(c.mgrs[id], urls).Handler().ServeHTTP(w, r)
		}))
		t.Cleanup(c.servers[id].Close)
	}
	for id, srv := range c.servers {
		urls[id] = srv.URL
	}
	return c
}

// tenantObjects lists the stored objects of tid's instance dirs.
func tenantObjects(fs *FakeStore, tid string) []string {
	p := tenantPrefix("fake", tid) + "/i/"
	var out []string
	for _, k := range fs.ObjectKeys() {
		if strings.HasPrefix(k, p) {
			out = append(out, k)
		}
	}
	return out
}

// Cold start of a pool: every replica creates the registry tenant at the
// same moment. Exactly one create CAS wins. The losers must treat that as
// the designed fence outcome: no poison, no orphaned base, a prompt
// "exists" answer, and every replica serves the tenant through the owner.
func TestCreateRaceColdStart(t *testing.T) {
	ids := []string{"hl-0", "hl-1", "hl-2"}
	fs := NewFakeStore()
	c := newRaceCluster(t, newCreateBarrierStore(fs, registryTID, len(ids)), fs, ids...)

	type res struct {
		code int
		out  map[string]any
		took time.Duration
	}
	results := map[string]res{}
	var mu sync.Mutex
	var wg sync.WaitGroup
	start := make(chan struct{})
	for _, id := range ids {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			<-start
			t0 := time.Now()
			code, out, _ := c.do(t, id, "PUT", "/v1/tenants/"+registryTID, map[string]any{})
			mu.Lock()
			results[id] = res{code, out, time.Since(t0)}
			mu.Unlock()
		}(id)
	}
	close(start)
	wg.Wait()

	mf, _, err := fs.GetManifest(context.Background(), registryTID)
	if err != nil || mf == nil {
		t.Fatalf("no manifest after the race: %v", err)
	}
	winner := mf.OwnerAddr
	winners := 0
	for _, id := range ids {
		r := results[id]
		t.Logf("%s: %d %v (%v)", id, r.code, r.out, r.took)
		if r.code != 200 {
			t.Errorf("%s: create answered %d %v, want 200", id, r.code, r.out)
		}
		if r.code == 200 && r.out["existing"] == nil {
			winners++
			if id != winner {
				t.Errorf("%s claims the create but the manifest names %s", id, winner)
			}
		} else if r.out["owner"] != winner {
			t.Errorf("%s: existing answer names owner %v, manifest names %s", id, r.out["owner"], winner)
		}
		// a loser answers as soon as it knows it lost: it never waits out
		// the winner's lease
		if r.took > testOpts().Lease {
			t.Errorf("%s: create took %v, longer than a lease", id, r.took)
		}
	}
	if winners != 1 {
		t.Fatalf("want exactly one winner, got %d", winners)
	}
	lost := int64(0)
	for _, id := range ids {
		if p := c.logs[id].matching("poisoned"); len(p) > 0 {
			t.Errorf("%s logged a poison for a lost create race: %q", id, p)
		}
		lost += c.mets[id].CreateRacesLost.Load()
		c.mgrs[id].mu.Lock()
		_, cooled := c.mgrs[id].poisonedAt[registryTID]
		c.mgrs[id].mu.Unlock()
		if cooled {
			t.Errorf("%s: a lost create race started a poison cooldown", id)
		}
	}
	if lost != int64(len(ids)-1) {
		t.Errorf("CreateRacesLost = %d across the pool, want %d", lost, len(ids)-1)
	}
	// no orphan: every object under the tenant is one the manifest names
	named := map[string]bool{}
	for _, k := range manifestObjects(mf) {
		named[k] = true
	}
	// the loser deletes its base before its create returns: check at once
	var orphans []string
	for _, k := range tenantObjects(fs, registryTID) {
		if !named[k] {
			orphans = append(orphans, k)
		}
	}
	if len(orphans) > 0 {
		t.Fatalf("orphaned objects after the race: %v", orphans)
	}

	// every replica serves the tenant right away, the losers by proxy
	tx := func(id, txid, sql string) {
		t.Helper()
		body := map[string]any{"client": "c-" + id, "txid": txid, "statements": []map[string]any{{"sql": sql}}}
		if code, out, _ := c.do(t, id, "POST", "/v1/t/"+registryTID+"/tx", body); code != 200 {
			t.Fatalf("tx via %s: %d %v", id, code, out)
		}
	}
	tx(winner, "schema", `CREATE TABLE IF NOT EXISTS reg(k TEXT PRIMARY KEY)`)
	for _, id := range ids {
		tx(id, "ins", fmt.Sprintf(`INSERT INTO reg VALUES('%s')`, id))
	}
	for _, id := range ids {
		code, out, _ := c.do(t, id, "POST", "/v1/t/"+registryTID+"/query", map[string]any{"sql": `SELECT COUNT(*) FROM reg`})
		if code != 200 {
			t.Fatalf("query via %s: %d %v", id, code, out)
		}
		if n := out["rows"].([]any)[0].([]any)[0].(float64); n != float64(len(ids)) {
			t.Fatalf("query via %s: %v rows, want %d", id, n, len(ids))
		}
	}
	// the owner never moved: the losers proxied, they did not take over
	if mf2, _, _ := fs.GetManifest(context.Background(), registryTID); mf2.Owner != mf.Owner || mf2.Epoch != mf.Epoch {
		t.Fatalf("ownership moved after the race: %s/%d -> %s/%d", mf.Owner, mf.Epoch, mf2.Owner, mf2.Epoch)
	}
}

// raceCreate runs Acquire(create) for tid on every manager at once and
// returns the winner and the losers' errors.
func raceCreate(t *testing.T, tid string, mgrs ...*Manager) (*Manager, []error) {
	t.Helper()
	var wg sync.WaitGroup
	errs := make([]error, len(mgrs))
	for i, m := range mgrs {
		wg.Add(1)
		go func(i int, m *Manager) {
			defer wg.Done()
			_, errs[i] = m.Acquire(context.Background(), tid, true)
		}(i, m)
	}
	wg.Wait()
	var winner *Manager
	var lost []error
	for i, err := range errs {
		if err == nil {
			if winner != nil {
				t.Fatalf("two creates won: %s and %s", winner.ID(), mgrs[i].ID())
			}
			winner = mgrs[i]
			continue
		}
		lost = append(lost, err)
	}
	if winner == nil {
		t.Fatalf("no create won: %v", lost)
	}
	return winner, lost
}

// A loser that cannot delete its base (S3 error, or a crash between the
// CAS and the delete) leaves one object in its own dead instance dir. It
// is still a lost race, not a poison. The sweeper collects the object once
// the tenant is no longer held by a live owner (here: released), and the
// live data is untouched.
func TestCreateRaceResidualOrphanIsSwept(t *testing.T) {
	fs := NewFakeStore()
	bs := newCreateBarrierStore(fs, "t1", 2)
	bs.failDeletes.Store(true)
	log := &logCapture{}
	met := &Metrics{Logger: log}
	a := NewManager(bs, "fake", "a", "standard", t.TempDir(), testOpts(), 1000, met, ControllerOpts{})
	b := NewManager(bs, "fake", "b", "standard", t.TempDir(), testOpts(), 1000, met, ControllerOpts{})
	winner, lost := raceCreate(t, "t1", a, b)
	if len(lost) != 1 || !errors.Is(lost[0], ErrBusy) {
		t.Fatalf("loser: want ErrBusy (exists, owned by the winner), got %v", lost)
	}
	if p := log.matching("poisoned"); len(p) > 0 {
		t.Fatalf("a lost create logged a poison: %q", p)
	}
	bs.failDeletes.Store(false)

	ctx := context.Background()
	mf, _, _ := fs.GetManifest(ctx, "t1")
	named := map[string]bool{}
	for _, k := range manifestObjects(mf) {
		named[k] = true
	}
	var orphans []string
	for _, k := range tenantObjects(fs, "t1") {
		if !named[k] {
			orphans = append(orphans, k)
		}
	}
	if len(orphans) != 1 || strings.Contains(orphans[0], "/i/"+mf.Instance+"/") {
		t.Fatalf("want exactly the loser's base in a foreign instance dir, got %v", orphans)
	}

	makeSchema(t, winner, "t1")
	mustTx(t, winner, "t1", "c", "s1", insertStmt(1, "v"))
	sw := NewSweeper(winner, fs, "fake", testOpts(), &Metrics{})
	// held by a live owner: never swept (the owner may be mid-claim)
	sw.sweepOnce(ctx)
	if _, err := fs.GetObject(ctx, orphans[0]); err != nil {
		t.Fatalf("swept under a live owner: %v", err)
	}
	if err := winner.Release("t1", ""); err != nil {
		t.Fatal(err)
	}
	sw.sweepOnce(ctx)
	if _, err := fs.GetObject(ctx, orphans[0]); err == nil {
		t.Fatalf("orphan %s survived the sweep of a released tenant", orphans[0])
	}
	if n := countRows(t, winner, "t1"); n != 1 {
		t.Fatalf("sweep ate live data: %d rows", n)
	}
}

// lateCreateStore lands the first create-only manifest CAS of tid but
// reports a transient error, as an S3 PUT that commits after the client
// timed out. The retry then sees ErrCAS against its own manifest.
type lateCreateStore struct {
	*FakeStore
	tid  string
	once atomic.Bool
}

func (s *lateCreateStore) PutManifestCAS(ctx context.Context, tid string, m *Manifest, etag string) (string, error) {
	etag2, err := s.FakeStore.PutManifestCAS(ctx, tid, m, etag)
	if err == nil && tid == s.tid && etag == "" && s.once.CompareAndSwap(false, true) {
		return "", errors.New("injected timeout after commit")
	}
	return etag2, err
}

// An ErrCAS on the retry of an ambiguous create is not a lost race: the
// manifest names our own base. The base must survive (deleting it would
// destroy the tenant), the outcome is not counted as a lost race, and the
// tenant is usable on the next acquire.
func TestCreateAmbiguousCASIsNotALostRace(t *testing.T) {
	fs := NewFakeStore()
	met := &Metrics{}
	m := NewManager(&lateCreateStore{FakeStore: fs, tid: "t1"}, "fake", "a", "standard", t.TempDir(), testOpts(), 1000, met, ControllerOpts{})
	ctx := context.Background()
	if _, err := m.Acquire(ctx, "t1", true); err == nil {
		t.Fatal("want the create to fail: its own CAS landed under an error")
	}
	mf, _, _ := fs.GetManifest(ctx, "t1")
	if mf == nil {
		t.Fatal("no manifest")
	}
	if _, err := fs.GetObject(ctx, mf.BaseKey); err != nil {
		t.Fatalf("the base the manifest names was deleted: %v", err)
	}
	if n := met.CreateRacesLost.Load(); n != 0 {
		t.Fatalf("an ambiguous create of our own counted as %d lost races", n)
	}
	// the manifest names this pod: the next acquire (after the claim
	// cooldown, which Acquire waits out) takes it over
	makeSchema(t, m, "t1")
	mustTx(t, m, "t1", "c", "s1", insertStmt(1, "v"))
	if n := countRows(t, m, "t1"); n != 1 {
		t.Fatalf("rows: %d", n)
	}
}
