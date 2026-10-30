package hoglite

// Per-tenant execution accounting. Operators use it to find the tenants
// that use the pod (noisy neighbors), and later quotas will build on it.
//
// What is measured. The wall time while the work of a tenant executes:
//   - read: the pin on the actor (BEGIN and probe) plus execRead on the
//     worker, which includes the statement steps and the row drain.
//   - write: applyTx on the actor: WAL parse, BEGIN, dedup lookup,
//     statements, dedup receipt, COMMIT or ROLLBACK.
//   - checkpoint: checkpointLocked on the actor. This includes the wait
//     for a pod checkpoint slot and the base upload, so it is mostly I/O.
//     It is shown, but it is not part of the ranking.
//
// Not measured: segment cuts and segment PUTs (small, and the PUT is
// network wait on its own goroutine), admission waits, the HTTP layer and
// JSON encoding.
//
// Why wall time and not CPU time. Go has no CPU clock per goroutine. A
// thread CPU clock (CLOCK_THREAD_CPUTIME_ID) is correct only while the
// goroutine stays on one OS thread, so it needs runtime.LockOSThread for
// the whole read. A locked goroutine that blocks (a contended SQLite
// mutex, a channel) forces a thread hand-off. Measured on 8 vCPUs:
// parallel point reads got 20-100% slower and parallel scans 25% slower.
// The clock call itself costs about 170 ns, a wall clock pair 75 ns.
// Wall time while executing is a good proxy here: the pod query semaphore
// (2 x GOMAXPROCS) bounds the running reads. On a saturated pod a read
// then waits for a CPU about as long as it runs, more when actors and the
// HTTP layer also need CPU. All reads that run at the same time wait the
// same way, so the shares between tenants stay correct.
// In the same measurement, 8 parallel scans on 8 vCPUs charged 1.66 times
// their thread CPU time.
// The time a read holds a pod query slot is also the resource that a
// quota must ration. Disk waits count too; they also hold the slot.
//
// Cost on the hot paths: two clock reads and five atomic adds on a cache
// line of the tenant, per read and per write transaction. No lock, no
// clock call per row.
//
// Exposure without a label per tenant. A sampler (usageTracker.sample,
// every UsageInterval) folds the counters into decayed rates per tenant.
// /metrics shows pod totals and the rates of the top UsageTopN tenants by
// read plus write time. A tenant series exists only while the tenant is
// in the top N. The decay (half-life UsageHalfLife) keeps the ranking
// stable: a tenant does not drop out after one quiet interval. A tenant
// below usageMinRate is never listed, so an idle pod lists nobody. The
// tenant label therefore has at most UsageTopN values at a time, and it
// changes only when the heavy users change. Equal scores sort by tenant
// id, so ties do not swap places between samples. A series that leaves
// the list ends; Prometheus marks it stale at the next scrape. Dashboards
// should use topk or max_over_time on the gauges, and the pod counters
// for totals. Per-tenant counters (no limit on tenants) are in the
// tenant status endpoint, the top list is also in /v1/info.
//
// Lifetime. The counters of a tenant live in its Tenant and start at zero
// when the tenant opens on this pod. When it closes, the tracker folds its
// last delta into the pod totals. Work that a read finishes after the
// close (a read that ignored its cancel for longer than ReadDrainWait) is
// not counted, nor is a checkpoint that fails the tenant (it is charged
// after the close). The pod totals never go down.
//
// Not wired into the heat/reclass loop (HeatCommits). A CPU-based whale
// decision needs its own thresholds and tests, and a wrong threshold moves
// tenants between pools. The tracker gives the input for that later.

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type usageKind int

const (
	kindRead usageKind = iota
	kindWrite
	kindCheckpoint
	numUsageKinds
)

var usageKindNames = [numUsageKinds]string{"read", "write", "checkpoint"}

const (
	defaultUsageTopN     = 20
	defaultUsageInterval = 10 * time.Second
	defaultUsageHalfLife = time.Minute
	// usageMinRate is the smallest rate (seconds of read plus write time
	// per second) that lists a tenant in the top N: 0.1 ms per second.
	usageMinRate = 1e-4
)

// kindCounters are the counters of one kind. The padding keeps each kind
// on its own cache line: writes count on the actor, reads on workers.
type kindCounters struct {
	execNs atomic.Int64
	ops    atomic.Int64 // reads executed, write transactions, checkpoints
	stmts  atomic.Int64 // statements started
	rows   atomic.Int64 // result rows
	bytes  atomic.Int64 // estimated result bytes (resultLimit)
	_      [24]byte
}

// tenantUsage is the accounting of one open tenant. The hot paths only
// add to the atomic counters. The other fields belong to the tracker and
// are guarded by usageTracker.mu.
type tenantUsage struct {
	k [numUsageKinds]kindCounters

	id       string
	openedAt time.Time
	last     usageCounts // counters at the last sample or the close
	rate     usageRates
	retired  bool
}

// usageCounts is a plain copy of the counters.
type usageCounts struct {
	ExecNs, Ops, Stmts, Rows, Bytes [numUsageKinds]int64
}

// usageRates are decayed per-second rates: seconds of execution per
// second, and operations per second.
type usageRates struct {
	Exec, Ops [numUsageKinds]float64
}

// charge records one finished operation of kind k. lim gives the
// statements, rows and bytes of its response; nil adds only time.
func (u *tenantUsage) charge(k usageKind, d time.Duration, lim *resultLimit) {
	c := &u.k[k]
	c.execNs.Add(int64(d))
	c.ops.Add(1)
	if lim == nil {
		return
	}
	if lim.stmts > 0 {
		c.stmts.Add(int64(lim.stmts))
	}
	if lim.rows > 0 {
		c.rows.Add(int64(lim.rows))
	}
	if lim.bytes > 0 {
		c.bytes.Add(lim.bytes)
	}
}

// addTime records execution time without an operation (a failed pin).
func (u *tenantUsage) addTime(k usageKind, d time.Duration) {
	u.k[k].execNs.Add(int64(d))
}

func (u *tenantUsage) load() usageCounts {
	var c usageCounts
	for k := range u.k {
		c.ExecNs[k] = u.k[k].execNs.Load()
		c.Ops[k] = u.k[k].ops.Load()
		c.Stmts[k] = u.k[k].stmts.Load()
		c.Rows[k] = u.k[k].rows.Load()
		c.Bytes[k] = u.k[k].bytes.Load()
	}
	return c
}

func (c *usageCounts) add(o, minus usageCounts) {
	for k := 0; k < int(numUsageKinds); k++ {
		c.ExecNs[k] += o.ExecNs[k] - minus.ExecNs[k]
		c.Ops[k] += o.Ops[k] - minus.Ops[k]
		c.Stmts[k] += o.Stmts[k] - minus.Stmts[k]
		c.Rows[k] += o.Rows[k] - minus.Rows[k]
		c.Bytes[k] += o.Bytes[k] - minus.Bytes[k]
	}
}

// score ranks tenants: read plus write time per second. Checkpoints are
// mostly upload wait and stay out of the ranking.
func (r *usageRates) score() float64 { return r.Exec[kindRead] + r.Exec[kindWrite] }

// decay moves r toward the rates of delta d over dt seconds with weight a.
func (r *usageRates) decay(d usageCounts, dt, a float64) {
	for k := 0; k < int(numUsageKinds); k++ {
		r.Exec[k] = ewmaStep(r.Exec[k], float64(d.ExecNs[k])/1e9/dt, a)
		r.Ops[k] = ewmaStep(r.Ops[k], float64(d.Ops[k])/dt, a)
	}
}

func ewmaStep(cur, sample, a float64) float64 {
	v := cur + a*(sample-cur)
	if v < 1e-12 {
		return 0 // no endless tail of tiny values
	}
	return v
}

// usageTop is one entry of the top-N list.
type usageTop struct {
	Tenant string
	Rates  usageRates
}

// usageTracker samples the usage of all open tenants of a pod. It lives
// in Metrics, so every tenant that shares the Metrics registers with it.
// The zero value is ready to use, with the default options.
type usageTracker struct {
	mu       sync.Mutex
	live     map[*tenantUsage]struct{}
	topN     int
	halfLife time.Duration
	interval time.Duration

	base     usageCounts // pod totals up to the last sample, plus closed tenants
	retiring usageCounts // deltas of tenants closed since the last sample
	lastTick time.Time
	podRate  usageRates
	top      []usageTop
}

// configure sets the options; zero selects the default, a negative top
// N lists no tenant.
func (tr *usageTracker) configure(topN int, interval, halfLife time.Duration) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	tr.topN = guardOpt(topN, defaultUsageTopN)
	tr.interval = interval
	if tr.interval <= 0 {
		tr.interval = defaultUsageInterval
	}
	tr.halfLife = halfLife
	if tr.halfLife <= 0 {
		tr.halfLife = defaultUsageHalfLife
	}
}

// optsLocked returns the options in force, with defaults applied.
func (tr *usageTracker) optsLocked() (topN int, interval, halfLife time.Duration) {
	topN, interval, halfLife = tr.topN, tr.interval, tr.halfLife
	if topN == 0 && interval == 0 {
		topN = defaultUsageTopN // never configured
	}
	if interval <= 0 {
		interval = defaultUsageInterval
	}
	if halfLife <= 0 {
		halfLife = defaultUsageHalfLife
	}
	return
}

// register adds the usage of a tenant that just opened.
func (tr *usageTracker) register(u *tenantUsage, id string, now time.Time) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if tr.live == nil {
		tr.live = map[*tenantUsage]struct{}{}
	}
	if tr.lastTick.IsZero() {
		tr.lastTick = now
	}
	u.id = id
	u.openedAt = now
	tr.live[u] = struct{}{}
}

// retire folds the last delta of a closed tenant into the pod totals and
// stops sampling it. Later additions to its counters are not counted.
func (tr *usageTracker) retire(u *tenantUsage) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if _, ok := tr.live[u]; !ok {
		return
	}
	cur := u.load()
	tr.base.add(cur, u.last)
	tr.retiring.add(cur, u.last)
	u.last = cur
	u.retired = true
	delete(tr.live, u)
}

// sample folds the counters since the last sample into the decayed rates
// and selects the top N. The Manager calls it every interval; tests call
// it with their own clock.
func (tr *usageTracker) sample(now time.Time) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	topN, _, halfLife := tr.optsLocked()
	if tr.lastTick.IsZero() {
		tr.lastTick = now
	}
	dt := now.Sub(tr.lastTick).Seconds()
	if dt <= 0 {
		return
	}
	tr.lastTick = now
	a := 1 - math.Exp(-dt*math.Ln2/halfLife.Seconds())
	pod := tr.retiring
	tr.retiring = usageCounts{}
	cands := make([]*tenantUsage, 0, 64)
	for u := range tr.live {
		cur := u.load()
		var d usageCounts
		d.add(cur, u.last)
		u.last = cur
		tr.base.add(d, usageCounts{})
		pod.add(d, usageCounts{})
		u.rate.decay(d, dt, a)
		if u.rate.score() >= usageMinRate {
			cands = append(cands, u)
		}
	}
	tr.podRate.decay(pod, dt, a)
	// highest score first; the tenant id breaks ties, so equal tenants do
	// not swap places between samples
	sort.Slice(cands, func(i, j int) bool {
		si, sj := cands[i].rate.score(), cands[j].rate.score()
		if si != sj {
			return si > sj
		}
		return cands[i].id < cands[j].id
	})
	// One series per tenant id: two records of one id (two managers on
	// one Metrics, or an overlap of a close and a reopen) would give a
	// duplicate series. The higher score wins.
	tr.top = tr.top[:0]
	seen := make(map[string]bool, topN)
	for _, u := range cands {
		if len(tr.top) >= topN {
			break
		}
		if seen[u.id] {
			continue
		}
		seen[u.id] = true
		tr.top = append(tr.top, usageTop{Tenant: u.id, Rates: u.rate})
	}
}

// totals returns the pod totals: closed tenants, plus every open tenant up
// to now. The values never go down: retire moves a delta from the open
// sum into base, and sample moves it from the open sum into base.
func (tr *usageTracker) totals() usageCounts {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	c := tr.base
	for u := range tr.live {
		c.add(u.load(), u.last)
	}
	return c
}

// snapshot returns the top-N list and the pod rates of the last sample.
func (tr *usageTracker) snapshot() ([]usageTop, usageRates) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return append([]usageTop(nil), tr.top...), tr.podRate
}

// tenantView returns the decayed rates of one tenant and when it opened.
func (tr *usageTracker) tenantView(u *tenantUsage) (usageRates, time.Time) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return u.rate, u.openedAt
}

// loop samples every interval until stop closes.
func (tr *usageTracker) loop(stop <-chan struct{}) {
	tr.mu.Lock()
	_, interval, _ := tr.optsLocked()
	tr.mu.Unlock()
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case now := <-tick.C:
			tr.sample(now)
		case <-stop:
			return
		}
	}
}

// kindMap renders one value per kind for JSON.
func kindMap[T any](f func(k usageKind) T) map[string]T {
	m := make(map[string]T, numUsageKinds)
	for k := usageKind(0); k < numUsageKinds; k++ {
		m[usageKindNames[k]] = f(k)
	}
	return m
}

func countsJSON(c usageCounts) map[string]any {
	return kindMap(func(k usageKind) any {
		return map[string]any{
			"exec_seconds": float64(c.ExecNs[k]) / 1e9, "ops": c.Ops[k],
			"statements": c.Stmts[k], "rows": c.Rows[k], "result_bytes": c.Bytes[k],
		}
	})
}

func ratesJSON(r usageRates) map[string]any {
	return kindMap(func(k usageKind) any {
		return map[string]float64{"exec_seconds_per_s": r.Exec[k], "ops_per_s": r.Ops[k]}
	})
}

// info is the usage section of /v1/info: pod totals, pod rates and the
// top-N list. Its size does not grow with the tenant count.
func (tr *usageTracker) info() map[string]any {
	tot := tr.totals()
	top, pod := tr.snapshot()
	tr.mu.Lock()
	topN, interval, halfLife := tr.optsLocked()
	tr.mu.Unlock()
	list := make([]map[string]any, 0, len(top))
	for _, e := range top {
		list = append(list, map[string]any{"tenant": e.Tenant, "score": e.Rates.score(), "rates": ratesJSON(e.Rates)})
	}
	return map[string]any{
		"measure":    "wall time while executing",
		"top_n":      topN,
		"interval_s": interval.Seconds(), "half_life_s": halfLife.Seconds(),
		"totals": countsJSON(tot), "rates": ratesJSON(pod), "top": list,
	}
}

// status is the usage section of the per-tenant status: the counters
// since the tenant opened on this pod, and its decayed rates.
func (t *Tenant) usageStatus() map[string]any {
	r, since := t.metrics.usage.tenantView(&t.usage)
	return map[string]any{
		"since":  since.UTC().Format(time.RFC3339),
		"totals": countsJSON(t.usage.load()),
		"rates":  ratesJSON(r),
		"score":  r.score(),
	}
}

// writeUsageMetrics appends the usage families in the Prometheus text
// format. Per-tenant series only for the top N of the last sample.
func writeUsageMetrics(b *strings.Builder, tr *usageTracker) {
	tot := tr.totals()
	top, pod := tr.snapshot()
	family := func(name, typ, help string, f func(k usageKind) string) {
		fmt.Fprintf(b, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, typ)
		for k := usageKind(0); k < numUsageKinds; k++ {
			fmt.Fprintf(b, "%s{kind=%q} %s\n", name, usageKindNames[k], f(k))
		}
	}
	family("hoglite_exec_seconds_total", "counter", "Wall time spent executing tenant work on this pod, by kind.",
		func(k usageKind) string { return promFloat(float64(tot.ExecNs[k]) / 1e9) })
	family("hoglite_exec_ops_total", "counter", "Executed reads, write transactions and checkpoints.",
		func(k usageKind) string { return strconv.FormatInt(tot.Ops[k], 10) })
	family("hoglite_exec_statements_total", "counter", "Statements started by reads and write transactions.",
		func(k usageKind) string { return strconv.FormatInt(tot.Stmts[k], 10) })
	family("hoglite_exec_result_rows_total", "counter", "Result rows of reads and write transactions.",
		func(k usageKind) string { return strconv.FormatInt(tot.Rows[k], 10) })
	family("hoglite_exec_result_bytes_total", "counter", "Estimated result bytes of reads and write transactions.",
		func(k usageKind) string { return strconv.FormatInt(tot.Bytes[k], 10) })
	family("hoglite_exec_seconds_rate", "gauge", "Decayed rate of hoglite_exec_seconds_total over all tenants.",
		func(k usageKind) string { return promFloat(pod.Exec[k]) })
	tenantFamily := func(name, help string, f func(r *usageRates, k usageKind) float64) {
		fmt.Fprintf(b, "# HELP %s %s\n# TYPE %s gauge\n", name, help, name)
		for i := range top {
			for k := usageKind(0); k < numUsageKinds; k++ {
				fmt.Fprintf(b, "%s{tenant=\"%s\",kind=%q} %s\n", name, promLabel(top[i].Tenant),
					usageKindNames[k], promFloat(f(&top[i].Rates, k)))
			}
		}
	}
	tenantFamily("hoglite_tenant_exec_seconds_rate",
		"Decayed execution seconds per second of the top tenants by read plus write time.",
		func(r *usageRates, k usageKind) float64 { return r.Exec[k] })
	tenantFamily("hoglite_tenant_exec_ops_rate", "Decayed operations per second of the top tenants.",
		func(r *usageRates, k usageKind) float64 { return r.Ops[k] })
}

func promFloat(v float64) string { return strconv.FormatFloat(v, 'g', -1, 64) }

// promLabel escapes a label value for the text format. Tenant ids are
// restricted by validTID, so this only matters for ids from other paths.
func promLabel(s string) string {
	if !strings.ContainsAny(s, "\\\"\n") {
		return s
	}
	return strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(s)
}
