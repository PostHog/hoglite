package hoglite

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// casHoldStore parks the first manifest CAS of tid that matches (create:
// etag "", or a checkpoint of an existing tenant: etag set) once armed,
// until the test releases it. The rest pass through.
type casHoldStore struct {
	*FakeStore
	tid     string
	create  bool
	armed   atomic.Bool
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func newCASHoldStore(fs *FakeStore, tid string, create bool) *casHoldStore {
	return &casHoldStore{FakeStore: fs, tid: tid, create: create, entered: make(chan struct{}), release: make(chan struct{})}
}

func (s *casHoldStore) PutManifestCAS(ctx context.Context, tid string, m *Manifest, etag string) (string, error) {
	if tid == s.tid && (etag == "") == s.create && s.armed.Load() {
		hold := false
		s.once.Do(func() { hold = true })
		if hold {
			close(s.entered)
			select {
			case <-s.release:
			case <-time.After(30 * time.Second):
			}
		}
	}
	return s.FakeStore.PutManifestCAS(ctx, tid, m, etag)
}

// promptCreate is the bound for a create on a tenant that already exists:
// well under one lease. Before the fast path such a create waited out the
// owner's lease, and kept waiting while the owner heartbeat, until the
// acquire deadline (lease+15 s).
const promptCreate = time.Second

// The production report: a 3-replica pool behind one Service, production
// lease settings. The registry tenant is owned by one pod; hoglake's
// ensureRegistry PUT lands on any pod. On a non-owner it slept out the
// acquire deadline (lease+15 s, 15.5 s measured) and hoglake's 8 s budget
// ran out. Every pod must answer in milliseconds with the owner's address.
func TestCreateOnNonOwnerAnswersAtOnceProductionOpts(t *testing.T) {
	fs := NewFakeStore()
	c := newRaceClusterOpts(t, DefaultOpts(), fs, fs, "hl-0", "hl-1", "hl-2")
	code, out, took := createVia(t, c, "hl-0", registryTID)
	if code != 200 || out["existing"] != nil {
		t.Fatalf("first create: %d %v", code, out)
	}
	t.Logf("hl-0 creates: %v", took)
	for _, id := range []string{"hl-0", "hl-1", "hl-2", "hl-1", "hl-2"} {
		// hoglake sends no body: exercise that exact request
		req, _ := http.NewRequest("PUT", c.servers[id].URL+"/v1/tenants/"+registryTID, nil)
		t0 := time.Now()
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		var out map[string]any
		json.NewDecoder(resp.Body).Decode(&out)
		resp.Body.Close()
		took := time.Since(t0)
		t.Logf("create on %s (owner hl-0 live): %d %v in %v", id, resp.StatusCode, out, took)
		if resp.StatusCode != 200 {
			t.Errorf("%s: %d %v", id, resp.StatusCode, out)
		}
		if id != "hl-0" && (out["existing"] != true || out["owner"] != "hl-0") {
			t.Errorf("%s: want existing with owner hl-0, got %v", id, out)
		}
		if took > promptCreate {
			t.Errorf("%s: create of an existing tenant took %v, want < %v", id, took, promptCreate)
		}
	}
	if mf := mustManifest(t, fs, registryTID); mf.OwnerAddr != "hl-0" {
		t.Fatalf("ownership moved to %s", mf.OwnerAddr)
	}
}

// newRaceClusterOpts is newRaceCluster with explicit tenant options.
func newRaceClusterOpts(t *testing.T, opts TenantOpts, store Backend, fs *FakeStore, ids ...string) *raceCluster {
	t.Helper()
	c := &raceCluster{
		testCluster: &testCluster{fs: fs, servers: map[string]*httptest.Server{}, mgrs: map[string]*Manager{}},
		logs:        map[string]*logCapture{}, mets: map[string]*Metrics{},
	}
	urls := map[string]string{}
	for _, id := range ids {
		c.logs[id] = &logCapture{}
		c.mets[id] = &Metrics{Logger: c.logs[id]}
		c.mgrs[id] = NewManager(store, "fake", id, "standard", t.TempDir(), opts, 1000, c.mets[id], ControllerOpts{})
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

func createVia(t *testing.T, c *raceCluster, id, tid string) (int, map[string]any, time.Duration) {
	t.Helper()
	t0 := time.Now()
	code, out, _ := c.do(t, id, "PUT", "/v1/tenants/"+tid, map[string]any{})
	return code, out, time.Since(t0)
}

func txVia(t *testing.T, c *raceCluster, id, tid, txid, sql string) {
	t.Helper()
	body := map[string]any{"client": "c-" + id, "txid": txid, "statements": []map[string]any{{"sql": sql}}}
	if code, out, _ := c.do(t, id, "POST", "/v1/t/"+tid+"/tx", body); code != 200 {
		t.Fatalf("tx %s via %s: %d %v", txid, id, code, out)
	}
}

func rowsVia(t *testing.T, c *raceCluster, id, tid string) float64 {
	t.Helper()
	code, out, _ := c.do(t, id, "POST", "/v1/t/"+tid+"/query", map[string]any{"sql": `SELECT COUNT(*) FROM kv`})
	if code != 200 {
		t.Fatalf("query via %s: %d %v", id, code, out)
	}
	return out["rows"].([]any)[0].([]any)[0].(float64)
}

func seedTenant(t *testing.T, c *raceCluster, id, tid string, rows int) {
	t.Helper()
	if code, out, _ := createVia(t, c, id, tid); code != 200 || out["existing"] != nil {
		t.Fatalf("seed create via %s: %d %v", id, code, out)
	}
	txVia(t, c, id, tid, "schema", `CREATE TABLE IF NOT EXISTS kv(k INTEGER PRIMARY KEY, v TEXT)`)
	for i := 0; i < rows; i++ {
		txVia(t, c, id, tid, fmt.Sprintf("seed-%d", i), fmt.Sprintf(`INSERT INTO kv VALUES(%d, 'v')`, i))
	}
}

// A create for a tenant that exists with a live owner answers "existing"
// with the owner's address at once: from several replicas at the same
// time, and again on a retry. Nothing moves: no takeover, no local copy
// on the callers, no lease wait.
func TestCreateExistingTenantAnswersAtOnce(t *testing.T) {
	fs := NewFakeStore()
	c := newRaceCluster(t, fs, fs, "a", "b", "c")
	seedTenant(t, c, "a", "t1", 3)
	mf, _, _ := fs.GetManifest(context.Background(), "t1")

	var wg sync.WaitGroup
	for _, id := range []string{"b", "c", "b", "c"} { // two each: the second is a retry
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			code, out, took := createVia(t, c, id, "t1")
			t.Logf("%s: %d %v (%v)", id, code, out, took)
			if code != 200 || out["existing"] != true || out["owner"] != "a" {
				t.Errorf("%s: create of an existing tenant answered %d %v, want 200 existing owner a", id, code, out)
			}
			if took > promptCreate {
				t.Errorf("%s: create of an existing tenant took %v, want < %v", id, took, promptCreate)
			}
		}(id)
	}
	wg.Wait()
	for _, id := range []string{"b", "c"} {
		if w := c.mets[id].AcquireWait.Load(); w != 0 {
			t.Errorf("%s waited %d ms for a lease on a create of an existing tenant", id, w)
		}
		if c.mgrs[id].LocalTenant("t1") != nil {
			t.Errorf("%s opened a local copy of a tenant a live peer owns", id)
		}
	}
	if mf2, _, _ := fs.GetManifest(context.Background(), "t1"); mf2.Owner != mf.Owner || mf2.Epoch != mf.Epoch {
		t.Fatalf("ownership moved: %s/%d -> %s/%d", mf.Owner, mf.Epoch, mf2.Owner, mf2.Epoch)
	}
	if n := rowsVia(t, c, "b", "t1"); n != 3 {
		t.Fatalf("rows via b: %v", n)
	}
}

// The fast path covers only a live owner. A brand-new tenant is created; a
// released tenant and one whose owner's lease ran out are acquired by the
// create, exactly as before.
func TestCreateFreeTenantStillAcquires(t *testing.T) {
	fs := NewFakeStore()
	c := newRaceCluster(t, fs, fs, "a", "b", "c")
	ctx := context.Background()
	seedTenant(t, c, "a", "t1", 2) // new: created, not "existing"

	// released: the create takes it
	if err := c.mgrs["a"].Release("t1", ""); err != nil {
		t.Fatal(err)
	}
	code, out, _ := createVia(t, c, "b", "t1")
	if code != 200 || out["existing"] != nil || out["owner"] != c.mgrs["b"].ID() {
		t.Fatalf("create of a released tenant: %d %v, want b to acquire it", code, out)
	}
	if mf, _, _ := fs.GetManifest(ctx, "t1"); mf.Owner != c.mgrs["b"].ID() {
		t.Fatalf("released tenant not acquired by the create: owner %s", mf.Owner)
	}

	// owner dead and its lease expired past the skew margin: the create
	// takes it over
	c.mgrs["b"].KillLease()
	time.Sleep(testOpts().Lease + testOpts().SkewMargin + 200*time.Millisecond)
	code, out, _ = createVia(t, c, "c", "t1")
	if code != 200 || out["existing"] != nil || out["owner"] != c.mgrs["c"].ID() {
		t.Fatalf("create of a tenant with an expired owner: %d %v, want c to acquire it", code, out)
	}
	if n := rowsVia(t, c, "c", "t1"); n != 2 {
		t.Fatalf("rows after takeover by create: %v", n)
	}
}

// A create where the manifest names an owner that has just died, while its
// pod lease still looks valid, must acquire the tenant, never answer
// "existing" with the dead pod as the owner. Three ways to be dead: the
// process is gone; the process is up but lost its pod lease; the pod was
// restarted under the same name (a StatefulSet pod reuses its address,
// but it is a new incarnation that does not hold the tenant).
func TestCreateJustAfterOwnerDeathAcquires(t *testing.T) {
	for _, how := range []string{"process-gone", "lease-lost", "restarted-same-name"} {
		t.Run(how, func(t *testing.T) {
			fs := NewFakeStore()
			c := newRaceCluster(t, fs, fs, "a", "b")
			seedTenant(t, c, "a", "t1", 2)
			c.mgrs["a"].KillLease()
			switch how {
			case "process-gone":
				c.servers["a"].Close()
			case "restarted-same-name":
				// the handler reads c.mgrs at request time: "a" now
				// answers as a fresh process
				c.mgrs["a"] = NewManager(fs, "fake", "a", "standard", t.TempDir(), testOpts(), 1000, &Metrics{}, ControllerOpts{})
			}
			if live, _, _ := c.mgrs["b"].ownerLeaseLive(context.Background(), mustManifest(t, fs, "t1").Owner); !live {
				t.Fatal("precondition: the dead owner's lease must still look live")
			}
			code, out, took := createVia(t, c, "b", "t1")
			t.Logf("create right after the owner died (%s): %d %v (%v)", how, code, out, took)
			if out["existing"] != nil {
				t.Fatalf("answered existing for a tenant whose owner is dead: %d %v", code, out)
			}
			if code != 200 || out["owner"] != c.mgrs["b"].ID() {
				t.Fatalf("want b to acquire the tenant, got %d %v", code, out)
			}
			if mf := mustManifest(t, fs, "t1"); mf.Owner != c.mgrs["b"].ID() {
				t.Fatalf("manifest owner %s, want b", mf.Owner)
			}
			if n := rowsVia(t, c, "b", "t1"); n != 2 {
				t.Fatalf("rows: %v, want 2", n)
			}
		})
	}
}

func mustManifest(t *testing.T, fs *FakeStore, tid string) *Manifest {
	t.Helper()
	mf, _, err := fs.GetManifest(context.Background(), tid)
	if err != nil || mf == nil {
		t.Fatalf("manifest %s: %v", tid, err)
	}
	return mf
}

// A create arrives while the live owner is mid-checkpoint (its manifest
// CAS in flight). It answers "existing" at once and does not disturb the
// checkpoint, which lands afterwards under the same owner and epoch.
func TestCreateArrivesMidCheckpoint(t *testing.T) {
	fs := NewFakeStore()
	hs := newCASHoldStore(fs, "t1", false)
	c := newRaceCluster(t, hs, fs, "a", "b")
	seedTenant(t, c, "a", "t1", 3)
	ctx := context.Background()
	mf, _, _ := fs.GetManifest(ctx, "t1")
	ten := c.mgrs["a"].LocalTenant("t1")

	hs.armed.Store(true)
	done := make(chan bool, 1)
	ten.ctlCh <- func() { done <- ten.checkpointLocked("") }
	select {
	case <-hs.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("checkpoint never reached its CAS")
	}

	code, out, took := createVia(t, c, "b", "t1")
	t.Logf("create mid-checkpoint: %d %v (%v)", code, out, took)
	if code != 200 || out["existing"] != true || out["owner"] != "a" {
		t.Errorf("create mid-checkpoint: %d %v, want 200 existing owner a", code, out)
	}
	if took > promptCreate {
		t.Errorf("create mid-checkpoint took %v, want < %v", took, promptCreate)
	}
	close(hs.release)
	if ok := <-done; !ok {
		t.Fatalf("checkpoint failed: %v", ten.deadErr)
	}
	mf2, _, _ := fs.GetManifest(ctx, "t1")
	if mf2.Owner != mf.Owner || mf2.Epoch != mf.Epoch || mf2.Gen <= mf.Gen {
		t.Fatalf("checkpoint did not land under the same owner: %s/%d/g%d -> %s/%d/g%d", mf.Owner, mf.Epoch, mf.Gen, mf2.Owner, mf2.Epoch, mf2.Gen)
	}
	if !ten.Alive() {
		t.Fatal("owner's tenant died")
	}
	if n := rowsVia(t, c, "b", "t1"); n != 3 {
		t.Fatalf("rows via b: %v", n)
	}
}

// Two creates of a new tenant, the second arriving while the first is
// mid-create-checkpoint (base uploaded, create CAS in flight). The late
// one finds no manifest and creates too; whichever CAS lands first wins
// (here the late one), and the other answers "existing" with the winner's
// address at once. One owner, no poison, no orphan.
func TestCreateArrivesMidCreateCheckpoint(t *testing.T) {
	fs := NewFakeStore()
	hs := newCASHoldStore(fs, "t2", true)
	hs.armed.Store(true)
	c := newRaceCluster(t, hs, fs, "a", "b")
	type res struct {
		code int
		out  map[string]any
		took time.Duration
	}
	aDone := make(chan res, 1)
	go func() {
		code, out, took := createVia(t, c, "a", "t2")
		aDone <- res{code, out, took}
	}()
	select {
	case <-hs.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("a's create never reached its CAS")
	}
	bCode, bOut, bTook := createVia(t, c, "b", "t2")
	close(hs.release)
	a := <-aDone
	t.Logf("a: %d %v (%v); b: %d %v (%v)", a.code, a.out, a.took, bCode, bOut, bTook)
	if bCode != 200 || bOut["existing"] != nil || bOut["owner"] != c.mgrs["b"].ID() {
		t.Fatalf("late create: %d %v, want b to win", bCode, bOut)
	}
	if a.code != 200 || a.out["existing"] != true || a.out["owner"] != "b" {
		t.Fatalf("held create: %d %v, want 200 existing owner b", a.code, a.out)
	}
	// a's time includes the hold; once released it must not wait a lease
	if a.took-bTook > promptCreate {
		t.Errorf("held create answered %v after the release", a.took-bTook)
	}
	mf, _, _ := fs.GetManifest(context.Background(), "t2")
	if p := c.logs["a"].matching("poisoned"); len(p) > 0 {
		t.Errorf("a logged a poison: %q", p)
	}
	if o := unnamedObjects(fs, "t2", mf); len(o) > 0 {
		t.Errorf("orphans: %v", o)
	}
	txVia(t, c, "a", "t2", "schema", `CREATE TABLE IF NOT EXISTS kv(k INTEGER PRIMARY KEY, v TEXT)`)
	txVia(t, c, "a", "t2", "r1", `INSERT INTO kv VALUES(1, 'v')`)
	if n := rowsVia(t, c, "b", "t2"); n != 1 {
		t.Fatalf("rows: %v", n)
	}
}

// The lost-race path asks the winner too. A create that loses its CAS to a
// peer which is gone by the time we ask must not answer "existing" on the
// strength of the winner's fresh lease: it answers a retryable 503, and
// the retry acquires once the lease has run out.
func TestCreateLostRaceToDeadWinnerIsNotExisting(t *testing.T) {
	fs := NewFakeStore()
	hs := newCASHoldStore(fs, "t3", true)
	hs.armed.Store(true)
	c := newRaceCluster(t, hs, fs, "a", "b")
	aDone := make(chan [2]any, 1)
	go func() {
		code, out, _ := createVia(t, c, "a", "t3")
		aDone <- [2]any{code, out}
	}()
	<-hs.entered
	if code, out, _ := createVia(t, c, "b", "t3"); code != 200 || out["existing"] != nil {
		t.Fatalf("b's create: %d %v, want b to win", code, out)
	}
	c.mgrs["b"].KillLease()
	c.servers["b"].Close()
	close(hs.release)
	got := <-aDone
	t.Logf("a after losing to a dead winner: %v %v", got[0], got[1])
	if out := got[1].(map[string]any); out["existing"] != nil || got[0].(int) != 503 {
		t.Fatalf("want a retryable 503, got %v %v", got[0], out)
	}
	if p := c.logs["a"].matching("poisoned"); len(p) > 0 {
		t.Errorf("a logged a poison: %q", p)
	}
	// the client's retry: acquires once b's lease has run out
	code, out, took := createVia(t, c, "a", "t3")
	t.Logf("retry: %d %v (%v)", code, out, took)
	if code != 200 || out["existing"] != nil || out["owner"] != c.mgrs["a"].ID() {
		t.Fatalf("retry: want a to acquire, got %d %v", code, out)
	}
}

// The confirmation request itself (x-hw-noproxy) sent to a pod that does
// not hold the tenant, while another live pod does: it answers 421 with
// the owner at once. It neither forwards again nor waits out the owner's
// lease, so a stale route costs the asking pod milliseconds.
func TestCreateConfirmationAtNonOwnerAnswers421(t *testing.T) {
	fs := NewFakeStore()
	c := newRaceCluster(t, fs, fs, "a", "b", "c")
	seedTenant(t, c, "a", "t1", 1)
	req, _ := http.NewRequest("PUT", c.servers["c"].URL+"/v1/tenants/t1", nil)
	req.Header.Set("x-hw-noproxy", "1")
	t0 := time.Now()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	json.NewDecoder(resp.Body).Decode(&out)
	resp.Body.Close()
	took := time.Since(t0)
	t.Logf("confirmation at non-owner c: %d %v (%v)", resp.StatusCode, out, took)
	if resp.StatusCode != 421 || out["owner"] != "a" {
		t.Fatalf("want 421 naming owner a, got %d %v", resp.StatusCode, out)
	}
	if took > promptCreate {
		t.Fatalf("took %v", took)
	}
	if c.mets["c"].Proxied.Load() != 0 {
		t.Fatal("the confirmation was forwarded again")
	}
}

// An owner whose pod lease has expired, but whose process still answers
// HTTP (partitioned from the store, its heartbeats not landing), is not
// asked: the create acquires the tenant. Asking it could get a 200 from a
// pod about to lose the tenant.
func TestCreateExpiredOwnerStillServingHTTPIsNotAsked(t *testing.T) {
	fs := NewFakeStore()
	c := newRaceCluster(t, fs, fs, "a", "b")
	seedTenant(t, c, "a", "t1", 2)
	c.mgrs["a"].SetPartitioned(true)
	time.Sleep(testOpts().Lease + testOpts().SkewMargin + 200*time.Millisecond)
	code, out, took := createVia(t, c, "b", "t1")
	t.Logf("create with a partitioned owner: %d %v (%v)", code, out, took)
	if out["existing"] != nil || code != 200 || out["owner"] != c.mgrs["b"].ID() {
		t.Fatalf("want b to acquire, got %d %v", code, out)
	}
	if n := c.mets["b"].Proxied.Load(); n != 0 {
		t.Fatalf("asked an owner whose lease had expired (%d forwards)", n)
	}
}

// A confirmation request that reaches a pod which then loses a create
// race (no manifest when it looked, a peer's CAS landed first) answers
// 421 as well: the asked pod never forwards a confirmation on.
func TestCreateConfirmationLosingRaceAnswers421(t *testing.T) {
	fs := NewFakeStore()
	hs := newCASHoldStore(fs, "t4", true)
	hs.armed.Store(true)
	c := newRaceCluster(t, hs, fs, "a", "c")
	type res struct {
		code int
		out  map[string]any
	}
	cDone := make(chan res, 1)
	go func() {
		req, _ := http.NewRequest("PUT", c.servers["c"].URL+"/v1/tenants/t4", nil)
		req.Header.Set("x-hw-noproxy", "1")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			cDone <- res{0, nil}
			return
		}
		var out map[string]any
		json.NewDecoder(resp.Body).Decode(&out)
		resp.Body.Close()
		cDone <- res{resp.StatusCode, out}
	}()
	<-hs.entered // c's create CAS is parked
	if code, out, _ := createVia(t, c, "a", "t4"); code != 200 || out["existing"] != nil {
		t.Fatalf("a's create: %d %v, want a to win", code, out)
	}
	close(hs.release)
	got := <-cDone
	t.Logf("confirmation at c after it lost the race: %d %v", got.code, got.out)
	if got.code != 421 || got.out["owner"] != "a" {
		t.Fatalf("want 421 naming owner a, got %d %v", got.code, got.out)
	}
	if c.mets["c"].Proxied.Load() != 0 {
		t.Fatal("the confirmation was forwarded on")
	}
}

// The window inside TestCreateJustAfterOwnerDeathAcquires/lease-lost, held
// open: the owner has lost its pod lease but failAll's poison has not yet
// removed the tenant from its registry (it runs on goroutines). Asked to
// confirm, the owner must deny; the create must not answer "existing".
// CI run 41303215740 (attempt 2) caught the old code confirming here.
func TestCreateConfirmDeniedByFencedOwner(t *testing.T) {
	fs := NewFakeStore()
	c := newRaceCluster(t, fs, fs, "a", "b")
	seedTenant(t, c, "a", "t1", 2)
	a := c.mgrs["a"]
	a.stopLease()
	a.leaseLost.Store(true) // KillLease without failAll: the tenant stays open
	if a.LocalTenant("t1") == nil {
		t.Fatal("precondition: t1 still open on a")
	}

	req, _ := http.NewRequest(http.MethodPut, c.servers["a"].URL+"/v1/tenants/t1", strings.NewReader(`{}`))
	req.Header.Set("x-hw-noproxy", "1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 421 {
		t.Fatalf("confirm from an owner without its lease: %d, want 421", resp.StatusCode)
	}

	code, out, took := createVia(t, c, "b", "t1")
	t.Logf("create via b: %d %v (%v)", code, out, took)
	if out["existing"] != nil {
		t.Fatalf("answered existing for a tenant whose owner lost its lease: %d %v", code, out)
	}
	if code != 200 || out["owner"] != c.mgrs["b"].ID() {
		t.Fatalf("want b to acquire the tenant, got %d %v", code, out)
	}
}
