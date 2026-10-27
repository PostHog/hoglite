// hoglake-reimpl is a modified hoglake control plane for the proof-of-cost/speed
// comparison. It serves hoglake's REST contract (the subset the bench
// drives) with hoglite as the backing store instead of Postgres.
//
// Tenant mapping: one hoglake catalog = one hoglite tenant.
// One mutating request = one hoglite transaction. A returned
// snapshot_id is therefore durable in S3.
//
// Files:
//   - main.go: hoglite client, commit pipeline, routes.
//   - schema.go: per-catalog SQL schema and shared SQL fragments.
//   - txb.go: transaction builder with typed guard failures.
//   - catalog.go: catalogs, namespaces, snapshots.
//   - tables.go: table DDL and read planning (files, scan, changes).
//   - commit.go: the commit tail (appends and deletion vectors).
//   - offsets.go: consumer offsets.
package main

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var hlURL = flag.String("hoglite", "http://hoglite-front.hoglite.svc:9101", "hoglite base url (any replica)")
var whaleURL = flag.String("hoglite-whale", "http://hoglite-whale-front.hoglite.svc:9101", "whale pool base url")
var addr = flag.String("addr", ":8080", "listen")
var maxDepth = flag.Int("max-depth", 64, "max in-flight commits per catalog")
var adaptQueue = flag.Bool("adaptive", true, "adapt per-catalog pipeline depth to queue wait")

// globalQueueCap bounds queued+in-flight commits across ALL catalogs.
// Without it, S3 stall windows grow per-catalog queues until the pod OOMs
// (10k ramp, wave 9). Exceeding it returns 503: the client retries,
// the pod stays alive, and the queue drains.
var globalQueueCap = flag.Int("queue-cap", 20000, "max queued+in-flight commits across all catalogs (503 backpressure beyond this)")

var client = &http.Client{Timeout: 60 * time.Second, Transport: &http.Transport{
	MaxIdleConns: 512, MaxIdleConnsPerHost: 128, IdleConnTimeout: 90 * time.Second,
}}

var reqSeq atomic.Int64

// ---- hoglite client ----

type stmt struct {
	SQL             string `json:"sql"`
	Args            []any  `json:"args"`
	ExpectAffected  *int64 `json:"expect_affected,omitempty"`
	ExpectScalarInt *int64 `json:"expect_scalar_int,omitempty"`
}

type qres struct {
	Cols     []string `json:"cols"`
	Rows     [][]any  `json:"rows"`
	Affected int64    `json:"affected"`
}

// tenantRoutes caches the pool (standard/whale) per catalog after a 421 hint.
var tenantRoutes sync.Map

func routeFor(tenant string) string {
	if v, ok := tenantRoutes.Load(tenant); ok {
		sv := v.(string)
		if sv == "whale" {
			return *whaleURL
		}
		if strings.HasPrefix(sv, "owner:") {
			// route to the stable pod name; per-boot ids carry a .nonce suffix.
			// The tenant's class decides which peer service owns it.
			owner := strings.TrimPrefix(sv, "owner:")
			if i := strings.Index(owner, "."); i > 0 {
				owner = owner[:i]
			}
			svc := "hoglite-peer"
			if v, ok := tenantRoutes.Load(tenant); ok && v.(string) == "whale" {
				svc = "hoglite-whale-peer"
			}
			return "http://" + owner + "." + svc + ".hoglite.svc:9101"
		}
	}
	return *hlURL
}

func hlTx(tenant string, stmts []stmt) ([]qres, error) {
	id := fmt.Sprintf("%016x", reqSeq.Add(1)) + hex.EncodeToString(rand8())
	return hlTxID(tenant, id, stmts)
}

// hlTxID sends with a caller-fixed txid so retries stay exactly-once.
func hlTxID(tenant, id string, stmts []stmt) ([]qres, error) {
	return hlTxIDFP(tenant, id, "", stmts)
}

func hlTxIDFP(tenant, id, fp string, stmts []stmt) ([]qres, error) {
	body, _ := json.Marshal(map[string]any{
		"client": "hoglake-reimpl", "txid": id, "fp": fp, "statements": stmts,
	})
	deadline := time.Now().Add(120 * time.Second)
	for {
		res, retry, err := hlTxOnce(routeFor(tenant), tenant, body)
		if err == nil {
			return res, nil
		}
		if !retry || time.Now().After(deadline) {
			return nil, err
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func hlTxOnce(base, tenant string, body []byte) ([]qres, bool, error) {
	resp, err := client.Post(base+"/v1/t/"+tenant+"/tx", "application/json", bytes.NewReader(body))
	if err != nil {
		// a stale direct-owner pin (dead pod, moved tenant) must not pin
		// forever: clear it so the next attempt re-resolves
		if v, ok := tenantRoutes.Load(tenant); ok && strings.HasPrefix(v.(string), "owner:") {
			tenantRoutes.Delete(tenant)
		}
		return nil, true, err // transport: retryable
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == 503 {
		return nil, true, fmt.Errorf("transient: %s", b) // owner lost mid-flight
	}
	if resp.StatusCode == 421 {
		var ae struct {
			Class string `json:"class"`
			Owner string `json:"owner"`
		}
		json.Unmarshal(b, &ae)
		// class pin wins over owner pin: a whale tenant routes to the whale
		// pool, never to a standard pod's name. An owner hint without a class
		// hint routes direct (standard pool only).
		if ae.Class == "whale" {
			tenantRoutes.Store(tenant, "whale")
		} else if ae.Owner != "" {
			tenantRoutes.Store(tenant, "owner:"+ae.Owner)
		} else {
			tenantRoutes.Delete(tenant)
		}
		return nil, true, fmt.Errorf("misdirected: %s", b)
	}
	// 409/422 and everything else are deterministic refusals: surface
	// immediately instead of burning the retry budget on an impossible
	// request (P1-7). Only 421/503/transport retry.
	if resp.StatusCode != 200 {
		return nil, false, &hlError{Status: resp.StatusCode, Body: string(b)}
	}
	var out struct {
		Results []qres `json:"results"`
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	return out.Results, false, dec.Decode(&out)
}

// hlError is a deterministic refusal from hoglite.
// A 409 names the failed guard: "expectation failed at statement N".
type hlError struct {
	Status int
	Body   string
}

func (e *hlError) Error() string { return fmt.Sprintf("hoglite %d: %s", e.Status, e.Body) }

var stmtIdxRE = regexp.MustCompile(`expectation failed at statement (\d+)`)

// failedGuard returns the index of the statement whose guard failed.
// It returns -1 when err is not a guard failure. The structured body
// field (guard_index) wins; the message parse is the fallback for older
// hoglite builds.
func failedGuard(err error) int {
	var he *hlError
	if !errors.As(err, &he) || he.Status != 409 {
		return -1
	}
	var body struct {
		Error      string `json:"error"`
		GuardIndex int    `json:"guard_index"`
	}
	if json.Unmarshal([]byte(he.Body), &body) == nil && body.GuardIndex > 0 &&
		strings.Contains(body.Error, "expect_failed") {
		// hoglite's GuardIndex is 1-based; txb registers guards 0-based
		return body.GuardIndex - 1
	}
	m := stmtIdxRE.FindStringSubmatch(he.Body)
	if m == nil {
		return -1
	}
	n, _ := strconv.Atoi(m[1])
	return n
}

func hlQuery(tenant, sql string, args ...any) (qres, error) {
	body, _ := json.Marshal(map[string]any{"sql": sql, "args": args})
	deadline := time.Now().Add(120 * time.Second)
	for {
		out, retry, err := hlQueryOnce(routeFor(tenant), tenant, body)
		if err == nil {
			return out, nil
		}
		if !retry || time.Now().After(deadline) {
			return qres{}, err
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func hlQueryOnce(base, tenant string, body []byte) (qres, bool, error) {
	var out qres
	resp, err := client.Post(base+"/v1/t/"+tenant+"/query", "application/json", bytes.NewReader(body))
	if err != nil {
		if v, ok := tenantRoutes.Load(tenant); ok && strings.HasPrefix(v.(string), "owner:") {
			tenantRoutes.Delete(tenant)
		}
		return out, true, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == 503 {
		return out, true, fmt.Errorf("transient: %s", b)
	}
	if resp.StatusCode == 421 {
		var ae struct {
			Class string `json:"class"`
		}
		json.Unmarshal(b, &ae)
		if ae.Class == "whale" {
			tenantRoutes.Store(tenant, "whale")
		} else {
			tenantRoutes.Delete(tenant)
		}
		return out, true, fmt.Errorf("misdirected: %s", b)
	}
	if resp.StatusCode != 200 {
		return out, false, &hlError{Status: resp.StatusCode, Body: string(b)}
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	return out, false, dec.Decode(&out)
}

func hlCreateTenant(tenant string) error {
	req, _ := http.NewRequest("PUT", *hlURL+"/v1/tenants/"+tenant, nil)
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	// 200 = created; 409/422 from a retried create on an existing tenant is
	// also success for our purposes (idempotent): the tenant exists either
	// way. Transient errors (503) surface as errors and retry above.
	if resp.StatusCode == 200 || resp.StatusCode == 409 || resp.StatusCode == 422 {
		return nil
	}
	b, _ := io.ReadAll(resp.Body)
	return fmt.Errorf("create tenant %d: %s", resp.StatusCode, b)
}

func rand8() []byte { b := make([]byte, 8); rand.Read(b); return b }

// ---- per-catalog commit pipeline with adaptive depth ----
//
// This models the pipelined hoglake committer: the request handler enqueues
// (stage A would live before this point in hoglake proper), a per-catalog
// sender preserves submission order, and at most `depth` commits are in
// flight to hoglite at once (stage B). Depth adapts: queue waits double
// it, idle halves it. Durability semantics are unchanged: a commit answers
// when hoglite reports it durable.

// retire removes an idle pipeline. It drains under a per-pipeline mutex
// so no in-flight submit can send on a closed channel.
func (p *pipeline) retire() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.inflight.Load() > 0 || len(p.ch) > 0 {
		return false
	}
	if p.retired {
		return false
	}
	p.retired = true
	close(p.ch)
	return true
}

// submit is the only way to enqueue; it refuses a retired pipeline and
// sheds load past the global queue cap.
// drainRetire retires a pipeline that lost its map slot while busy.
// Unreachable pipelines receive no new work, so retire() eventually
// succeeds once in-flight commits finish.
func drainRetire(p *pipeline) {
	for i := 0; i < 600; i++ {
		if p.retire() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	log.Printf("pipeline for a retired tenant failed to drain in 60s; sender leaks")
}

func (p *pipeline) submit(req *pipeReq) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.retired {
		return false
	}
	if n := globalQueued.Add(1); n > int64(*globalQueueCap) {
		globalQueued.Add(-1)
		return false
	}
	p.ch <- req
	return true
}

var globalQueued atomic.Int64

type pipeReq struct {
	id       string
	fp       string
	stmts    []stmt
	enqueued time.Time
	reply    chan pipeRes
}

type pipeRes struct {
	res []qres
	err error
}

type pipeline struct {
	tenant   string
	ch       chan *pipeReq
	depth    atomic.Int64
	inflight atomic.Int64
	idleAt   atomic.Int64 // unix ms
	commitN  atomic.Int64

	mu      sync.Mutex // guards retired; submit and retire take it
	retired bool
}

var pipes sync.Map // catalog -> *pipeline
var maxInFlight int

func pipeFor(tenant string) *pipeline {
	if v, ok := pipes.Load(tenant); ok {
		return v.(*pipeline)
	}
	p := &pipeline{tenant: tenant, ch: make(chan *pipeReq, 4096)}
	p.depth.Store(1)
	p.idleAt.Store(time.Now().UnixMilli())
	actual, _ := pipes.LoadOrStore(tenant, p)
	if actual == p {
		go p.sender()
	}
	return actual.(*pipeline)
}

func (p *pipeline) sender() {
	for req := range p.ch {
		for p.inflight.Load() >= p.depth.Load() {
			time.Sleep(300 * time.Microsecond)
		}
		p.inflight.Add(1)
		p.commitN.Add(1)
		go func(req *pipeReq) {
			defer p.inflight.Add(-1)
			defer globalQueued.Add(-1)
			res, err := hlTxIDFP(p.tenant, req.id, req.fp, req.stmts)
			req.reply <- pipeRes{res, err}
			if len(p.ch) == 0 {
				p.idleAt.Store(time.Now().UnixMilli())
			}
		}(req)
	}
}

// adaptPipelines runs globally: queue pressure doubles depth, idleness
// halves it toward 1.
func adaptPipelines() {
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	for range tick.C {
		pipes.Range(func(key, v any) bool {
			p := v.(*pipeline)
			queued := len(p.ch)
			// retire long-idle pipelines: goroutine+channel per catalog is
			// not free at 100k catalogs (P2-4)
			if queued == 0 && p.inflight.Load() == 0 &&
				time.Now().UnixMilli()-p.idleAt.Load() > 10*60*1000 {
				// deregister first so no new work arrives, then close;
				// the sender drains any racing enqueue before exiting
				if pipes.CompareAndDelete(key, v) && p.retire() {
				return true
			}
			// retire() failed (busy): re-register so the tenant isn't
			// orphaned — but only if nobody created a fresh one meanwhile.
			// If a fresh pipeline won the slot, ours is unreachable: no new
			// work can arrive, so its in-flight work drains and a background
			// retry retires it. Without this the old sender leaks forever.
			if actual, loaded := pipes.LoadOrStore(key, v); loaded && actual != v {
				go drainRetire(v.(*pipeline))
			}
			return true
			}
			if queued > 0 && p.depth.Load() < int64(maxInFlight) {
				p.depth.Store(min64(p.depth.Load()*2, int64(maxInFlight)))
			} else if queued == 0 && p.inflight.Load() == 0 &&
				time.Now().UnixMilli()-p.idleAt.Load() > 5000 && p.depth.Load() > 1 {
				p.depth.Store(max64(p.depth.Load()/2, 1))
			}
			return true
		})
	}
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func pipelinesDebug(w http.ResponseWriter, r *http.Request) {
	out := map[string]any{}
	pipes.Range(func(k, v any) bool {
		p := v.(*pipeline)
		out[k.(string)] = map[string]any{
			"depth": p.depth.Load(), "queued": len(p.ch),
			"inflight": p.inflight.Load(), "commits": p.commitN.Load(),
			"route": routeFor(p.tenant),
		}
		return true
	})
	writeJSON(w, 200, out)
}

// metricsHandler exposes Prometheus text format: pipeline depth, queue
// and commit counters per catalog. Scraped via the prometheus.io
// annotations on the deployment.
func metricsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	var b strings.Builder
	b.WriteString("# TYPE hoglake_reimpl_pipeline_depth gauge\n")
	b.WriteString("# TYPE hoglake_reimpl_pipeline_inflight gauge\n")
	b.WriteString("# TYPE hoglake_reimpl_pipeline_commits_total counter\n")
	pipes.Range(func(k, v any) bool {
		p := v.(*pipeline)
		c := strings.ReplaceAll(k.(string), `"`, ``)
		fmt.Fprintf(&b, `hoglake_reimpl_pipeline_depth{catalog=%q} %d`+"\n", c, p.depth.Load())
		fmt.Fprintf(&b, `hoglake_reimpl_pipeline_inflight{catalog=%q} %d`+"\n", c, p.inflight.Load())
		fmt.Fprintf(&b, `hoglake_reimpl_pipeline_commits_total{catalog=%q} %d`+"\n", c, p.commitN.Load())
		return true
	})
	fmt.Fprintf(&b, "# TYPE hoglake_reimpl_queue_global gauge\n")
	fmt.Fprintf(&b, "hoglake_reimpl_queue_global %d\n", globalQueued.Load())
	fmt.Fprintf(&b, "hoglake_reimpl_queue_cap %d\n", *globalQueueCap)
	fmt.Fprint(w, b.String())
}

func i64(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case float64:
		return int64(x)
	case json.Number:
		n, _ := x.Int64()
		return n
	}
	return 0
}

// ---- routes ----

func newMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	mux.HandleFunc("POST /v1/catalogs", createCatalog)
	mux.HandleFunc("GET /v1/catalogs/{c}", getCatalog)
	mux.HandleFunc("POST /v1/catalogs/{c}/namespaces", createNamespace)
	mux.HandleFunc("GET /v1/catalogs/{c}/namespaces", listNamespaces)
	mux.HandleFunc("GET /v1/catalogs/{c}/namespaces/{ns}", getNamespace)
	mux.HandleFunc("POST /v1/catalogs/{c}/namespaces/{ns}/tables", createTable)
	mux.HandleFunc("GET /v1/catalogs/{c}/namespaces/{ns}/tables", listTables)
	mux.HandleFunc("GET /v1/catalogs/{c}/namespaces/{ns}/tables/{t}", getTable)
	mux.HandleFunc("DELETE /v1/catalogs/{c}/namespaces/{ns}/tables/{t}", dropTable)
	mux.HandleFunc("GET /v1/catalogs/{c}/namespaces/{ns}/tables/{t}/files", listFiles)
	mux.HandleFunc("GET /v1/catalogs/{c}/namespaces/{ns}/tables/{t}/scan", scanPlan)
	mux.HandleFunc("GET /v1/catalogs/{c}/namespaces/{ns}/tables/{t}/changes", changes)
	mux.HandleFunc("POST /v1/catalogs/{c}/commit", commit)
	// Prepared commits carry the same payload plus an idempotency_key.
	// The durable-retry contract lives in the dedup table either way.
	mux.HandleFunc("POST /v1/catalogs/{c}/commit/prepared", commit)
	mux.HandleFunc("GET /v1/catalogs/{c}/snapshots", listSnapshots)
	mux.HandleFunc("GET /v1/catalogs/{c}/consumers/{consumer}/offsets", listOffsets)
	mux.HandleFunc("GET /v1/catalogs/{c}/consumers/{consumer}/offsets/{uuid}", getOffset)
	mux.HandleFunc("PUT /v1/catalogs/{c}/consumers/{consumer}/offsets/{uuid}", putOffset)
	// Pipeline internals expose pod DNS names. Debug-gated like
	// hoglited's debug endpoints (HL_DEBUG_ENDPOINTS).
	if os.Getenv("HL_DEBUG_ENDPOINTS") != "" {
		mux.HandleFunc("GET /v1/pipelines", pipelinesDebug)
	}
	mux.HandleFunc("GET /metrics", metricsHandler)
	return mux
}

func main() {
	flag.Parse()
	maxInFlight = *maxDepth
	if *adaptQueue {
		go adaptPipelines()
	}
	log.Printf("hoglake-reimpl listening %s -> %s (adaptive=%v maxDepth=%d)", *addr, *hlURL, *adaptQueue, *maxDepth)
	log.Fatal(http.ListenAndServe(*addr, newMux()))
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func apiErr(w http.ResponseWriter, code int, err string) {
	writeJSON(w, code, map[string]any{"error": err})
}

// apiErrDetail writes an ApiError with a code, prose, and extra fields.
func apiErrDetail(w http.ResponseWriter, code int, err, detail string, extra map[string]any) {
	body := map[string]any{"error": err}
	if detail != "" {
		body["detail"] = detail
	}
	for k, v := range extra {
		body[k] = v
	}
	writeJSON(w, code, body)
}
