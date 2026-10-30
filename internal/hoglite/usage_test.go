package hoglite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

// usageOpts turns off the size-triggered checkpoints and the periodic
// sampler, so a test sees only its own work and samples with its own
// clock.
func usageOpts() TenantOpts {
	o := testOpts()
	o.CheckpointByte = 64 << 20
	o.MaxSegments = 1 << 20
	o.UsageInterval = time.Hour
	o.UsageHalfLife = 10 * time.Second
	return o
}

func usageDelta(after, before usageCounts) usageCounts {
	var d usageCounts
	d.add(after, before)
	return d
}

func acquire(t *testing.T, m *Manager, tid string) *Tenant {
	t.Helper()
	ten, err := m.Acquire(context.Background(), tid, false)
	if err != nil {
		t.Fatalf("acquire %s: %v", tid, err)
	}
	return ten
}

// Work is charged to the tenant that ran it, under the right kind, with
// its statements, rows and bytes. Pod totals equal the sum of the open
// tenants and do not change when a tenant closes.
func TestUsageAttribution(t *testing.T) {
	met := &Metrics{}
	m := newReadTestManager(t, NewFakeStore(), usageOpts(), met)
	defer m.ShutdownAll()
	for _, tid := range []string{"a", "b"} {
		mustCreate(t, m, tid)
		makeSchema(t, m, tid)
	}
	mustTx(t, m, "b", "c", "fill", insertStmt(1, "x"), insertStmt(2, "y"), insertStmt(3, "z"))
	a, b := acquire(t, m, "a"), acquire(t, m, "b")
	a0, b0 := a.usage.load(), b.usage.load()

	// a: 5 write transactions with 2 statements each, one returns a row
	for i := 0; i < 5; i++ {
		mustTx(t, m, "a", "c", fmt.Sprint("w", i), insertStmt(int64(1000+10*i), "v"),
			Statement{SQL: `INSERT INTO kv(v) VALUES('r') RETURNING k`})
	}
	// a dedup replay is a write operation that runs no statement
	if _, dup, err := a.SubmitTx(TxRequest{Client: "c", TxID: "w0", Stmts: []Statement{insertStmt(1000, "v")}}); err != nil || !dup {
		t.Fatalf("replay: dup=%v err=%v", dup, err)
	}
	// b: 7 queries with 3 rows each, one read-only /tx with 2 statements
	for i := 0; i < 7; i++ {
		if res := mustQuery(t, m, "b", `SELECT k, v FROM kv ORDER BY k`); len(res.Rows) != 3 {
			t.Fatalf("rows %d", len(res.Rows))
		}
	}
	if _, _, err := b.SubmitTx(TxRequest{Stmts: []Statement{{SQL: `SELECT count(*) FROM kv`}, {SQL: `SELECT k FROM kv WHERE k < 3`}}}); err != nil {
		t.Fatal(err)
	}

	da, db := usageDelta(a.usage.load(), a0), usageDelta(b.usage.load(), b0)
	if da.Ops[kindWrite] != 6 || da.Stmts[kindWrite] != 10 || da.Rows[kindWrite] != 5 ||
		da.Bytes[kindWrite] <= 0 || da.ExecNs[kindWrite] <= 0 {
		t.Fatalf("a write usage: %+v", da)
	}
	if da.Ops[kindRead] != 0 || da.ExecNs[kindRead] != 0 || da.Stmts[kindRead] != 0 {
		t.Fatalf("a got read usage: %+v", da)
	}
	if db.Ops[kindRead] != 8 || db.Stmts[kindRead] != 9 || db.Rows[kindRead] != 7*3+1+2 ||
		db.Bytes[kindRead] <= 0 || db.ExecNs[kindRead] <= 0 {
		t.Fatalf("b read usage: %+v", db)
	}
	if db.Ops[kindWrite] != 0 || db.ExecNs[kindWrite] != 0 {
		t.Fatalf("b got write usage: %+v", db)
	}

	// a checkpoint is charged to its tenant only
	if !checkpointNow(a) {
		t.Fatal("checkpoint failed")
	}
	da2, db2 := usageDelta(a.usage.load(), a0), usageDelta(b.usage.load(), b0)
	if da2.Ops[kindCheckpoint] != 1 || da2.ExecNs[kindCheckpoint] <= 0 || db2.Ops[kindCheckpoint] != 0 {
		t.Fatalf("checkpoint usage: a %+v b %+v", da2, db2)
	}

	// pod totals: the sum of both tenants (no other tenant ran)
	var sum usageCounts
	sum.add(a.usage.load(), usageCounts{})
	sum.add(b.usage.load(), usageCounts{})
	if tot := met.usage.totals(); tot != sum {
		t.Fatalf("totals %+v, want %+v", tot, sum)
	}
	// closing a keeps its counts in the totals (the release may add a
	// checkpoint, here it has nothing to fold)
	if err := m.Release("a", ""); err != nil {
		t.Fatal(err)
	}
	tot := met.usage.totals()
	for _, k := range []usageKind{kindRead, kindWrite} {
		if tot.ExecNs[k] != sum.ExecNs[k] || tot.Ops[k] != sum.Ops[k] || tot.Rows[k] != sum.Rows[k] ||
			tot.Stmts[k] != sum.Stmts[k] || tot.Bytes[k] != sum.Bytes[k] {
			t.Fatalf("%s totals changed on close: %+v, want %+v", usageKindNames[k], tot, sum)
		}
	}
	if tot.Ops[kindCheckpoint] < sum.Ops[kindCheckpoint] || tot.ExecNs[kindCheckpoint] < sum.ExecNs[kindCheckpoint] {
		t.Fatalf("checkpoint totals went down on close: %+v, want at least %+v", tot, sum)
	}
}

// The time of a read stopped at its deadline counts: it held a slot and
// the CPU until then.
func TestUsageChargesStoppedRead(t *testing.T) {
	o := usageOpts()
	o.QueryTimeout = 150 * time.Millisecond
	m := newReadTestManager(t, NewFakeStore(), o, &Metrics{})
	defer m.ShutdownAll()
	mustCreate(t, m, "a")
	ten := acquire(t, m, "a")
	u0 := ten.usage.load()
	start := time.Now()
	_, err := ten.SubmitQuery(Statement{SQL: slowSQL})
	wall := time.Since(start)
	if !errors.Is(err, ErrQueryTimeout) {
		t.Fatalf("err %v, want a timeout", err)
	}
	d := usageDelta(ten.usage.load(), u0)
	if d.Ops[kindRead] != 1 || d.Stmts[kindRead] != 1 {
		t.Fatalf("usage %+v", d)
	}
	if got := time.Duration(d.ExecNs[kindRead]); got < 140*time.Millisecond || got > wall {
		t.Fatalf("charged %v for a read stopped at 150ms (call took %v)", got, wall)
	}
}

// A deliberately slow query and a slow write put their tenants at the top
// of the list, in /metrics, /v1/info and the tenant status, and the
// charged time never exceeds what the caller waited.
func TestUsageSlowWorkTops(t *testing.T) {
	met := &Metrics{}
	m := newReadTestManager(t, NewFakeStore(), usageOpts(), met)
	defer m.ShutdownAll()
	for i := 0; i < 5; i++ {
		tid := fmt.Sprint("q", i)
		mustCreate(t, m, tid)
		makeSchema(t, m, tid)
		for j := 0; j < 3; j++ {
			mustQuery(t, m, tid, `SELECT count(*) FROM kv`)
			mustTx(t, m, tid, "c", fmt.Sprint(j), insertStmt(int64(j), "v"))
		}
	}
	mustCreate(t, m, "slowread")
	mustCreate(t, m, "slowwrite")
	makeSchema(t, m, "slowwrite")
	sr, sw := acquire(t, m, "slowread"), acquire(t, m, "slowwrite")

	start := time.Now()
	if _, err := sr.SubmitQuery(Statement{SQL: `WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c LIMIT 60000) SELECT count(*) FROM c`}); err != nil {
		t.Fatal(err)
	}
	readWall := time.Since(start)
	start = time.Now()
	if _, _, err := sw.SubmitTx(TxRequest{Client: "c", TxID: "big", Stmts: []Statement{{SQL: `INSERT INTO kv(v) SELECT hex(x) FROM
		(WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c LIMIT 20000) SELECT x FROM c)`}}}); err != nil {
		t.Fatal(err)
	}
	writeWall := time.Since(start)
	if r := time.Duration(sr.usage.load().ExecNs[kindRead]); r <= 0 || r > readWall {
		t.Fatalf("slow read charged %v, call took %v", r, readWall)
	}
	if w := time.Duration(sw.usage.load().ExecNs[kindWrite]); w <= 0 || w > writeWall+100*time.Millisecond {
		// the schema tx of slowwrite is in the counter too
		t.Fatalf("slow write charged %v, call took %v", w, writeWall)
	}

	met.usage.sample(time.Now().Add(10 * time.Second))
	top, pod := met.usage.snapshot()
	if len(top) < 2 {
		t.Fatalf("top list too short: %+v", top)
	}
	names := []string{top[0].Tenant, top[1].Tenant}
	if !(names[0] == "slowread" || names[0] == "slowwrite") || !(names[1] == "slowread" || names[1] == "slowwrite") {
		t.Fatalf("slow tenants not at the top: %+v", top)
	}
	for _, e := range top[:2] {
		k := kindRead
		if e.Tenant == "slowwrite" {
			k = kindWrite
		}
		if e.Rates.Exec[k] < e.Rates.score()*0.9 {
			t.Fatalf("%s: rate not under kind %s: %+v", e.Tenant, usageKindNames[k], e.Rates)
		}
	}
	if pod.score() < (top[0].Rates.score()+top[1].Rates.score())*(1-1e-9) {
		t.Fatalf("pod rate %v below the top two", pod)
	}

	srv := httptest.NewServer(NewServer(m, nil).Handler())
	defer srv.Close()
	body := httpGet(t, srv.URL+"/metrics")
	first := regexp.MustCompile(`(?m)^hoglite_tenant_exec_seconds_rate\{tenant="([^"]+)"`).FindStringSubmatch(body)
	if first == nil || first[1] != top[0].Tenant {
		t.Fatalf("first tenant series %v, want %s", first, top[0].Tenant)
	}
	var info struct {
		Usage struct {
			TopN int `json:"top_n"`
			Top  []struct {
				Tenant string  `json:"tenant"`
				Score  float64 `json:"score"`
			} `json:"top"`
			Totals map[string]map[string]float64 `json:"totals"`
		} `json:"usage"`
	}
	if err := json.Unmarshal([]byte(httpGet(t, srv.URL+"/v1/info")), &info); err != nil {
		t.Fatal(err)
	}
	if info.Usage.TopN != defaultUsageTopN || len(info.Usage.Top) < 2 || info.Usage.Top[0].Tenant != top[0].Tenant ||
		info.Usage.Totals["read"]["ops"] < 16 {
		t.Fatalf("/v1/info usage: %+v", info.Usage)
	}
	var st struct {
		Local struct {
			Usage struct {
				Totals map[string]map[string]float64 `json:"totals"`
				Score  float64                       `json:"score"`
			} `json:"usage"`
		} `json:"local"`
	}
	if err := json.Unmarshal([]byte(httpGet(t, srv.URL+"/v1/t/slowread/status")), &st); err != nil {
		t.Fatal(err)
	}
	if st.Local.Usage.Totals["read"]["ops"] != 1 || st.Local.Usage.Totals["read"]["exec_seconds"] <= 0 || st.Local.Usage.Score <= 0 {
		t.Fatalf("status usage: %+v", st.Local.Usage)
	}
}

func httpGet(t *testing.T, url string) string {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("GET %s: %d %s", url, resp.StatusCode, b)
	}
	return string(b)
}

// fakeUsage registers a record that is not backed by a tenant.
func fakeUsage(tr *usageTracker, id string, now time.Time) *tenantUsage {
	u := &tenantUsage{}
	tr.register(u, id, now)
	return u
}

func near(a, b float64) bool { return math.Abs(a-b) <= 1e-9+1e-6*math.Abs(b) }

// Top-N selection, tie order, the minimum rate, decay, overtaking and
// retirement, on a tracker with a synthetic clock.
func TestUsageTopNAndDecay(t *testing.T) {
	tr := &usageTracker{}
	tr.configure(2, time.Hour, 10*time.Second)
	t0 := time.Unix(1_000_000, 0)
	a, b, c := fakeUsage(tr, "a", t0), fakeUsage(tr, "b", t0), fakeUsage(tr, "c", t0)
	tiny := fakeUsage(tr, "tiny", t0)
	a.charge(kindRead, 3*time.Second, nil)
	b.charge(kindWrite, 2*time.Second, nil)
	c.charge(kindRead, time.Second, nil)
	tiny.charge(kindRead, 100*time.Microsecond, nil) // 10 us/s: below usageMinRate

	// one interval of one half-life: the rate moves half way
	tr.sample(t0.Add(10 * time.Second))
	top, pod := tr.snapshot()
	if len(top) != 2 || top[0].Tenant != "a" || top[1].Tenant != "b" {
		t.Fatalf("top %+v", top)
	}
	if !near(top[0].Rates.Exec[kindRead], 0.15) || !near(top[0].Rates.Ops[kindRead], 0.05) || !near(top[1].Rates.Exec[kindWrite], 0.1) {
		t.Fatalf("rates %+v", top)
	}
	if !near(pod.Exec[kindRead], 0.5*(4.0001/10)) || !near(pod.Exec[kindWrite], 0.1) {
		t.Fatalf("pod rate %+v", pod)
	}

	// no work for one half-life: every rate halves, the order stays
	tr.sample(t0.Add(20 * time.Second))
	top, _ = tr.snapshot()
	if len(top) != 2 || top[0].Tenant != "a" || !near(top[0].Rates.Exec[kindRead], 0.075) {
		t.Fatalf("after decay: %+v", top)
	}

	// c overtakes with a burst: 30 s of work in 10 s (three busy cores)
	c.charge(kindRead, 30*time.Second, nil)
	tr.sample(t0.Add(30 * time.Second))
	top, _ = tr.snapshot()
	if top[0].Tenant != "c" || top[1].Tenant != "a" {
		t.Fatalf("after burst: %+v", top)
	}

	// a retires: its last delta reaches the totals, it leaves the list,
	// and a late charge after the retire is not counted
	a.charge(kindRead, time.Second, nil)
	before := tr.totals()
	tr.retire(a)
	if after := tr.totals(); after != before {
		t.Fatalf("totals changed on retire: %+v -> %+v", before, after)
	}
	a.charge(kindRead, time.Hour, nil)
	tr.sample(t0.Add(40 * time.Second))
	top, _ = tr.snapshot()
	for _, e := range top {
		if e.Tenant == "a" || e.Tenant == "tiny" {
			t.Fatalf("%s listed: %+v", e.Tenant, top)
		}
	}
	if got := tr.totals().ExecNs[kindRead]; got != int64(35*time.Second+100*time.Microsecond) {
		t.Fatalf("read total %v", time.Duration(got))
	}

	// equal scores: the tenant id decides, on every sample
	tr2 := &usageTracker{}
	tr2.configure(3, time.Hour, 10*time.Second)
	for _, id := range []string{"z", "m", "x", "b"} {
		fakeUsage(tr2, id, t0).charge(kindRead, time.Second, nil)
	}
	tr2.sample(t0.Add(10 * time.Second))
	top, _ = tr2.snapshot()
	if len(top) != 3 || top[0].Tenant != "b" || top[1].Tenant != "m" || top[2].Tenant != "x" {
		t.Fatalf("tie order %+v", top)
	}

	// after a long quiet time every rate decays below the minimum: an idle
	// pod lists nobody
	tr.sample(t0.Add(40*time.Second + 10*time.Minute))
	if top, _ = tr.snapshot(); len(top) != 0 {
		t.Fatalf("idle pod lists %+v", top)
	}

	// a negative top N lists nobody; zero keeps the default
	tr3 := &usageTracker{}
	tr3.configure(-1, 0, 0)
	fakeUsage(tr3, "a", t0).charge(kindRead, time.Second, nil)
	tr3.sample(t0.Add(10 * time.Second))
	if top, _ = tr3.snapshot(); len(top) != 0 {
		t.Fatalf("top N off lists %+v", top)
	}
	var tr4 usageTracker // never configured
	for i := 0; i < defaultUsageTopN+5; i++ {
		fakeUsage(&tr4, fmt.Sprint(i), t0).charge(kindWrite, time.Second, nil)
	}
	tr4.sample(t0.Add(10 * time.Second))
	if top, _ = tr4.snapshot(); len(top) != defaultUsageTopN {
		t.Fatalf("default top N: %d entries", len(top))
	}

	// two records of one id give one entry, the higher one
	tr5 := &usageTracker{}
	tr5.configure(2, time.Hour, 10*time.Second)
	fakeUsage(tr5, "dup", t0).charge(kindRead, 2500*time.Millisecond, nil)
	fakeUsage(tr5, "dup", t0).charge(kindRead, 3*time.Second, nil)
	fakeUsage(tr5, "other", t0).charge(kindRead, 2*time.Second, nil)
	tr5.sample(t0.Add(10 * time.Second))
	if top, _ = tr5.snapshot(); len(top) != 2 || top[0].Tenant != "dup" || !near(top[0].Rates.Exec[kindRead], 0.15) || top[1].Tenant != "other" {
		t.Fatalf("duplicate id: %+v", top)
	}

	// a sample at the same instant, or a clock step back, changes nothing
	tr4.sample(t0.Add(10 * time.Second))
	tr4.sample(t0)
	if top, _ = tr4.snapshot(); len(top) != defaultUsageTopN {
		t.Fatalf("repeat sample: %d entries", len(top))
	}
}

var (
	promSample = regexp.MustCompile(`^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[a-z_]+="(?:[^"\\\n]|\\.)*"(?:,[a-z_]+="(?:[^"\\\n]|\\.)*")*\})? (\S+)$`)
	promMeta   = regexp.MustCompile(`^# (HELP|TYPE) ([a-zA-Z_:][a-zA-Z0-9_:]*) (.+)$`)
)

// The /metrics output parses as the Prometheus text format: TYPE lines
// come once per family before its samples, every family is contiguous,
// values parse, the tenant label has only top-N values and is escaped.
func TestUsageMetricsFormat(t *testing.T) {
	if got := promLabel("a\"b\\c\nd"); got != `a\"b\\c\nd` {
		t.Fatalf("promLabel: %q", got)
	}
	o := usageOpts()
	o.UsageTopN = 3
	met := &Metrics{}
	m := newReadTestManager(t, NewFakeStore(), o, met)
	defer m.ShutdownAll()
	for i := 0; i < 6; i++ {
		tid := fmt.Sprint("t", i)
		mustCreate(t, m, tid)
		// t4 and t5 run far more work than the others
		n, rows := 1, 100
		if i >= 4 {
			n, rows = 5*(i-2), 5000
		}
		for j := 0; j < n; j++ {
			mustQuery(t, m, tid, fmt.Sprintf(`WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c LIMIT %d) SELECT count(*) FROM c`, rows))
		}
	}
	// a record with an odd id (not reachable through the API)
	fakeUsage(&met.usage, "odd\"id", time.Now()).charge(kindRead, time.Hour, nil)
	met.usage.sample(time.Now().Add(10 * time.Second))

	srv := httptest.NewServer(NewServer(m, nil).Handler())
	defer srv.Close()
	body := httpGet(t, srv.URL+"/metrics")
	typed := map[string]string{}
	done := map[string]bool{} // families whose samples ended
	cur := ""
	tenants := map[string]bool{}
	for _, line := range strings.Split(strings.TrimSuffix(body, "\n"), "\n") {
		if mm := promMeta.FindStringSubmatch(line); mm != nil {
			if mm[1] == "TYPE" {
				if _, dup := typed[mm[2]]; dup {
					t.Fatalf("second TYPE for %s", mm[2])
				}
				typed[mm[2]] = mm[3]
			}
			continue
		}
		mm := promSample.FindStringSubmatch(line)
		if mm == nil {
			t.Fatalf("bad line %q", line)
		}
		name := mm[1]
		if name != cur {
			if done[name] {
				t.Fatalf("family %s is not contiguous", name)
			}
			done[cur] = true
			cur = name
		}
		if _, err := strconv.ParseFloat(mm[3], 64); err != nil {
			t.Fatalf("bad value in %q", line)
		}
		if strings.HasPrefix(name, "hoglite_exec_") || strings.HasPrefix(name, "hoglite_tenant_") {
			if _, ok := typed[name]; !ok {
				t.Fatalf("%s has no TYPE before its samples", name)
			}
		}
		if tm := regexp.MustCompile(`tenant="((?:[^"\\]|\\.)*)"`).FindStringSubmatch(line); tm != nil {
			tenants[tm[1]] = true
		}
	}
	if typed["hoglite_exec_seconds_total"] != "counter" || typed["hoglite_tenant_exec_seconds_rate"] != "gauge" {
		t.Fatalf("types %v", typed)
	}
	// the top 3: the odd record, then t5 and t4 (most queries)
	if len(tenants) != 3 || !tenants[`odd\"id`] || !tenants["t5"] || !tenants["t4"] {
		t.Fatalf("tenant label values %v", tenants)
	}
	for _, k := range usageKindNames {
		if !strings.Contains(body, fmt.Sprintf("hoglite_exec_seconds_total{kind=%q} ", k)) {
			t.Fatalf("no total for %s", k)
		}
	}
	if n := strings.Count(body, "\nhoglite_tenant_exec_seconds_rate{"); n != 3*int(numUsageKinds) {
		t.Fatalf("%d tenant rate series, want %d", n, 3*numUsageKinds)
	}
}

// Concurrent reads, writes, closes, reopens, samples and scrapes: no data
// race (go test -race), totals never go down, and nothing is lost.
func TestUsageConcurrent(t *testing.T) {
	met := &Metrics{}
	m := newReadTestManager(t, NewFakeStore(), usageOpts(), met)
	srv := httptest.NewServer(NewServer(m, nil).Handler())
	defer srv.Close()
	const nt = 4
	for i := 0; i < nt; i++ {
		mustCreate(t, m, fmt.Sprint("c", i))
		makeSchema(t, m, fmt.Sprint("c", i))
	}
	base := met.usage.totals()
	var reads, writes atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < nt; i++ {
		tid := fmt.Sprint("c", i)
		for g := 0; g < 3; g++ {
			wg.Add(1)
			go func(g int) {
				defer wg.Done()
				for n := 0; ; n++ {
					select {
					case <-stop:
						return
					default:
					}
					ten, err := m.Acquire(context.Background(), tid, false)
					if err != nil {
						continue // mid release
					}
					if g == 0 {
						if _, _, err := ten.SubmitTx(TxRequest{Client: fmt.Sprint(g), TxID: fmt.Sprint(time.Now().UnixNano(), n),
							Stmts: []Statement{{SQL: `INSERT INTO kv(v) VALUES('x')`}}}); err == nil {
							writes.Add(1)
						}
						continue
					}
					if _, err := ten.SubmitQuery(Statement{SQL: `SELECT count(*) FROM kv`}); err == nil {
						reads.Add(1)
					}
				}
			}(g)
		}
	}
	// observer: samples, totals, scrapes, tenant status
	errc := make(chan error, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		prev := met.usage.totals()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			met.usage.sample(time.Now())
			cur := met.usage.totals()
			for k := 0; k < int(numUsageKinds); k++ {
				if cur.ExecNs[k] < prev.ExecNs[k] || cur.Ops[k] < prev.Ops[k] || cur.Stmts[k] < prev.Stmts[k] ||
					cur.Rows[k] < prev.Rows[k] || cur.Bytes[k] < prev.Bytes[k] {
					select {
					case errc <- fmt.Errorf("totals went down: %+v -> %+v", prev, cur):
					default:
					}
				}
			}
			prev = cur
			if i%10 == 0 {
				httpGet(t, srv.URL+"/metrics")
				httpGet(t, srv.URL+"/v1/info")
				if ten := m.LocalTenant("c0"); ten != nil {
					ten.usageStatus()
				}
			}
			time.Sleep(time.Millisecond)
		}
	}()
	// closes and reopens while the load runs
	for i := 0; i < 6; i++ {
		time.Sleep(100 * time.Millisecond)
		m.Release(fmt.Sprint("c", i%nt), "")
	}
	time.Sleep(100 * time.Millisecond)
	close(stop)
	wg.Wait()
	select {
	case err := <-errc:
		t.Fatal(err)
	default:
	}
	m.ShutdownAll()
	tot := usageDelta(met.usage.totals(), base)
	// every successful read and write ran and was charged before its reply;
	// failed ones (tenant closing) may add a few more
	if tot.Ops[kindRead] < reads.Load() || tot.Ops[kindWrite] < writes.Load() || reads.Load() == 0 || writes.Load() == 0 {
		t.Fatalf("ops read %d write %d, want at least %d and %d", tot.Ops[kindRead], tot.Ops[kindWrite], reads.Load(), writes.Load())
	}
	// one statement per executed read; one row each, except reads that a
	// close canceled before their row
	if tot.Stmts[kindRead] != tot.Ops[kindRead] || tot.Rows[kindRead] > tot.Ops[kindRead] || tot.Rows[kindRead] < reads.Load() {
		t.Fatalf("read statements %d rows %d, ops %d, successful %d", tot.Stmts[kindRead], tot.Rows[kindRead], tot.Ops[kindRead], reads.Load())
	}
	met.usage.mu.Lock()
	live := len(met.usage.live)
	met.usage.mu.Unlock()
	if live != 0 {
		t.Fatalf("%d records still live after shutdown", live)
	}
}

// failBase fails PutBase while fail is set, after a short delay.
type failBase struct {
	Backend
	fail atomic.Bool
}

func (b *failBase) PutBase(ctx context.Context, key string, r io.Reader, n int64) error {
	if b.fail.Load() {
		time.Sleep(10 * time.Millisecond)
		return errors.New("test: upload failed")
	}
	return b.Backend.PutBase(ctx, key, r, n)
}

// A checkpoint that fails the tenant charges after finishClose. The pod
// totals must still include it, with its time (from the review of
// b744c7d).
func TestUsageFailedCheckpointCharged(t *testing.T) {
	met := &Metrics{}
	backend := &failBase{Backend: NewFakeStore()}
	m := NewManager(backend, "fake", "a", "standard", t.TempDir(), usageOpts(), 1000, met, ControllerOpts{})
	defer m.ShutdownAll()
	ten, err := m.Acquire(context.Background(), "a", true)
	if err != nil {
		t.Fatal(err)
	}
	before := met.usage.totals()
	local := ten.usage.load()
	backend.fail.Store(true)
	if checkpointNow(ten) {
		t.Fatal("checkpoint did not fail")
	}
	<-ten.done
	waitFor(t, "record retired", func() bool {
		met.usage.mu.Lock()
		defer met.usage.mu.Unlock()
		return len(met.usage.live) == 0
	})
	pod := usageDelta(met.usage.totals(), before)
	own := usageDelta(ten.usage.load(), local)
	if own.Ops[kindCheckpoint] != 1 || own.ExecNs[kindCheckpoint] < int64(10*time.Millisecond) {
		t.Fatalf("tenant counters %+v", own)
	}
	if pod != own {
		t.Fatalf("pod delta %+v, tenant delta %+v", pod, own)
	}
}

// A read that ignores its cancel past ReadDrainWait ends after the tenant
// closed. Its charge still reaches the pod totals: the record retires
// only when that read ends.
func TestUsageLateReadCharged(t *testing.T) {
	met := &Metrics{}
	o := usageOpts()
	o.ReadDrainWait = 50 * time.Millisecond
	m := newReadTestManager(t, NewFakeStore(), o, met)
	defer m.ShutdownAll()
	mustCreate(t, m, "a")
	ten := acquire(t, m, "a")
	hook, pinned, gate := blockingHook()
	ten.readHook = hook
	before := met.usage.totals()
	read := goQuery(context.Background(), ten, "SELECT 1")
	waitPinned(t, pinned)
	ten.Stop(true, "")
	<-ten.done
	met.usage.mu.Lock()
	live := len(met.usage.live)
	met.usage.mu.Unlock()
	if live != 1 {
		t.Fatalf("%d live records while a read runs, want 1", live)
	}
	close(gate)
	recvRead(t, read, 3*time.Second)
	waitFor(t, "record retired", func() bool {
		met.usage.mu.Lock()
		defer met.usage.mu.Unlock()
		return len(met.usage.live) == 0
	})
	d := usageDelta(met.usage.totals(), before)
	if d.Ops[kindRead] != 1 || d.ExecNs[kindRead] <= 0 {
		t.Fatalf("late read not in the pod totals: %+v", d)
	}
}

// A successful read charges the time of its pin on the actor too. The
// test holds the only pool connection, so the pin waits for it.
func TestUsagePinTimeCharged(t *testing.T) {
	m, ten, met := guardTenant(t, usageOpts())
	mustQuery(t, m, "t1", "SELECT 1")
	ten.roDB.SetMaxOpenConns(1)
	held, err := ten.roDB.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	before := met.usage.totals()
	out := goQuery(context.Background(), ten, "SELECT 1")
	waitFor(t, "actor pin", func() bool { return ten.readers.count() == 1 })
	time.Sleep(40 * time.Millisecond)
	held.Close()
	if res := recvRead(t, out, time.Second); res.err != nil {
		t.Fatal(res.err)
	}
	d := usageDelta(met.usage.totals(), before)
	if d.Ops[kindRead] != 1 || d.ExecNs[kindRead] < int64(40*time.Millisecond) {
		t.Fatalf("pin time not charged: ops %d, %v", d.Ops[kindRead], time.Duration(d.ExecNs[kindRead]))
	}
}

// A pin that fails charges its time but no read operation: once when the
// connection wait ends at the caller deadline, once on SQLITE_NOMEM.
func TestUsageFailedPinCharged(t *testing.T) {
	m, ten, met := guardTenant(t, usageOpts())
	mustQuery(t, m, "t1", "SELECT 1")
	ten.roDB.SetMaxOpenConns(1)
	held, err := ten.roDB.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	before := met.usage.totals()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	res := recvRead(t, goQuery(ctx, ten, "SELECT 1"), 3*time.Second)
	held.Close()
	if res.err == nil {
		t.Fatal("read without a connection succeeded")
	}
	waitFor(t, "pin ended", func() bool { return ten.readers.count() == 0 })
	d := usageDelta(met.usage.totals(), before)
	if d.Ops[kindRead] != 0 || d.ExecNs[kindRead] < int64(50*time.Millisecond) {
		t.Fatalf("failed pin: ops %d, %v", d.Ops[kindRead], time.Duration(d.ExecNs[kindRead]))
	}

	noLookasideForTest(t)
	mustQuery(t, m, "t1", "SELECT 1")
	before = met.usage.totals()
	restore := withHeapLimitAt(t, 1)
	_, err = ten.SubmitQuery(Statement{SQL: "SELECT 1"})
	restore()
	expectNoMem(t, err)
	d = usageDelta(met.usage.totals(), before)
	if d.Ops[kindRead] != 0 || d.ExecNs[kindRead] <= 0 {
		t.Fatalf("NOMEM pin: %+v", d)
	}
}

// Each kind's counters are on cache lines that no other kind and no other
// field uses, in real heap allocations and at every 8-byte start offset.
func TestUsageCountersOwnCacheLines(t *testing.T) {
	const line = 64
	check := func(base uintptr) string {
		u := unsafe.Offsetof(Tenant{}.usage)
		first := base + u                                       // first byte of tenantUsage
		tail := base + u + unsafe.Offsetof(tenantUsage{}.holds) // first field after the counters
		var lo, hi [numUsageKinds]uintptr
		for k := range lo {
			off := base + u + unsafe.Offsetof(tenantUsage{}.k) + uintptr(k)*unsafe.Sizeof(kindCounters{})
			lo[k] = (off + unsafe.Offsetof(kindCounters{}.execNs)) / line
			hi[k] = (off + unsafe.Offsetof(kindCounters{}.bytes) + 7) / line
			if lo[k] <= (first-1)/line || hi[k] >= tail/line {
				return fmt.Sprintf("kind %d shares a line with another field", k)
			}
			for j := 0; j < k; j++ {
				if lo[k] <= hi[j] {
					return fmt.Sprintf("kinds %d and %d share a line", j, k)
				}
			}
		}
		return ""
	}
	for off := uintptr(0); off < line; off += 8 {
		if msg := check(off); msg != "" {
			t.Fatalf("start offset %d: %s", off, msg)
		}
	}
	ts := make([]*Tenant, 200)
	offs := map[uintptr]int{}
	for i := range ts {
		ts[i] = new(Tenant)
		base := uintptr(unsafe.Pointer(ts[i]))
		offs[base%line]++
		if msg := check(base); msg != "" {
			t.Fatalf("allocation at %#x: %s", base, msg)
		}
		// the addresses that check computes are the real field addresses
		if got := uintptr(unsafe.Pointer(&ts[i].usage.k[1].execNs)); got != base+unsafe.Offsetof(Tenant{}.usage)+unsafe.Offsetof(tenantUsage{}.k)+unsafe.Sizeof(kindCounters{}) {
			t.Fatalf("layout mismatch at %#x", got)
		}
	}
	t.Logf("Tenant start offsets mod %d: %v", line, offs)
}
