package hoglite

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

type Server struct {
	mgr    *Manager
	peers  map[string]string // replica id -> base URL
	client *http.Client

	mu          sync.Mutex
	ownerCache  map[string]ownerCacheEntry
	ownerCacheT time.Duration
}

type ownerCacheEntry struct {
	owner   string
	untilMs int64
	at      time.Time
}

// ownerLive reports whether the named owner pod's lease is fresh, with a
// short per-pod cache. Pod leases are shared across tenants, so this is
// one S3 GET per second per owner pod at most.
func (s *Server) ownerLive(ctx context.Context, owner string) bool {
	if owner == "" || owner == s.mgr.ID() {
		return true
	}
	s.mu.Lock()
	if e, ok := s.ownerCache["lease:"+owner]; ok && time.Since(e.at) < s.ownerCacheT {
		live := e.untilMs+500 > time.Now().UnixMilli() // small skew slack for readers
		s.mu.Unlock()
		return live
	}
	s.mu.Unlock()
	live, until, err := s.mgr.ownerLeaseLive(ctx, owner)
	if err != nil {
		return true // unknown: stay optimistic, the proxy path re-checks
	}
	s.mu.Lock()
	s.ownerCache["lease:"+owner] = ownerCacheEntry{owner: owner, untilMs: until, at: time.Now()}
	s.mu.Unlock()
	return live
}

func NewServer(mgr *Manager, peers map[string]string) *Server {
	return &Server{
		mgr: mgr, peers: peers,
		client:     &http.Client{Timeout: 120 * time.Second},
		ownerCache: map[string]ownerCacheEntry{}, ownerCacheT: time.Second,
	}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/healthz", func(w http.ResponseWriter, r *http.Request) {
		// unhealthy when the write gate is closed (lease lost or expiring):
		// k8s stops routing writes to a fenced-off pod
		if s.mgr.leaseLost.Load() || s.mgr.leaseExpiring() {
			w.WriteHeader(503)
			return
		}
		w.WriteHeader(200)
	})
	mux.HandleFunc("GET /metrics", s.wrap(s.handleMetrics))
	mux.HandleFunc("GET /v1/info", s.wrap(s.handleInfo))
	mux.HandleFunc("PUT /v1/tenants/{tid}", s.wrap(s.handleCreate))
	mux.HandleFunc("DELETE /v1/tenants/{tid}", s.wrap(s.handleDelete))
	mux.HandleFunc("POST /v1/t/{tid}/tx", s.wrap(s.handleTx))
	mux.HandleFunc("POST /v1/t/{tid}/query", s.wrap(s.handleQuery))
	mux.HandleFunc("GET /v1/t/{tid}/status", s.wrap(s.handleStatus))
	mux.HandleFunc("POST /v1/t/{tid}/release", s.wrap(s.handleRelease))
	mux.HandleFunc("POST /v1/shutdown", s.wrap(s.handleShutdown))
	mux.HandleFunc("GET /v1/shutdown", s.wrap(s.handleShutdown)) // k8s preStop hooks are GET-only
	if os.Getenv("HL_DEBUG_ENDPOINTS") != "" {
		// fault-injection endpoint for tests and drills; off by default
		mux.HandleFunc("POST /v1/debug/crash/{tid}", s.wrap(s.handleCrash))
	}
	return mux
}

type apiError struct {
	Error string `json:"error"`
	Owner string `json:"owner,omitempty"`
	Class string `json:"class,omitempty"`
	// structured guard failure detail (typed refusals downstream)
	GuardIndex int    `json:"guard_index,omitempty"`
	GuardKind  string `json:"guard_kind,omitempty"`
	// Code names a query guard refusal: query_timeout, result_too_large,
	// out_of_memory
	Code string `json:"code,omitempty"`
}

// guardStatus maps a query guard refusal to its HTTP status. Both are
// deterministic for the same request and data, so neither uses 503, the
// status that clients retry. 409 and 422 also keep their meaning (guard
// failure, SQL error): the REST adapter acts on both. A timed-out write
// rolled back and wrote no dedup receipt, so a later retry runs it again.
//
// SQLITE_NOMEM at the SQLite heap limit (memory.go) is 507 with code
// out_of_memory. It is not 503 either: a query that alone needs more than
// the limit fails the same way on every retry, and a retry loop keeps the
// pod at its memory limit for every tenant. Before the cap it was a plain
// SQL error (422 for /tx, 409 for /query), which the REST adapter reads as
// a constraint or guard failure. The statement rolled back; a write wrote
// no dedup receipt.
func guardStatus(err error) (int, apiError, bool) {
	switch {
	case errors.Is(err, ErrOutOfMemory):
		return 507, apiError{Error: err.Error(), Code: "out_of_memory"}, true
	case errors.Is(err, ErrQueryTimeout):
		return 504, apiError{Error: err.Error(), Code: "query_timeout"}, true
	case errors.Is(err, ErrResultTooLarge):
		return 413, apiError{Error: err.Error(), Code: "result_too_large"}, true
	}
	return 0, apiError{}, false
}

func (s *Server) wrap(h func(http.ResponseWriter, *http.Request) (int, any)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		code, body := h(w, r)
		if body == nil {
			if code > 0 {
				w.WriteHeader(code)
			}
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		enc := json.NewEncoder(w)
		enc.Encode(body)
	}
}

func decodeBody(r *http.Request, v any) error {
	dec := json.NewDecoder(http.MaxBytesReader(nil, r.Body, 64<<20))
	dec.UseNumber()
	return dec.Decode(v)
}

func errCode(err error) int {
	if code, _, ok := guardStatus(err); ok {
		return code
	}
	switch {
	case errors.Is(err, ErrNoTenant):
		return 404
	case errors.Is(err, ErrWrongClass), errors.Is(err, ErrBusy):
		return 421 // Misdirected Request: caller should re-route
	case errors.Is(err, ErrNotOwner), errors.Is(err, ErrClosed):
		return 503 // transient: retry
	default:
		// transient S3/SQL-infra errors are 503 so callers retry
		msg := err.Error()
		if strings.Contains(msg, "context deadline exceeded") ||
			strings.Contains(msg, "SlowDown") ||
			strings.Contains(msg, "retry Send token") ||
			strings.Contains(msg, "segment put") ||
			strings.Contains(msg, "base put") ||
			strings.Contains(msg, "checkpoint cas") ||
			strings.Contains(msg, "heartbeat cas") {
			return 503
		}
		return 500
	}
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) (int, any) {
	m := s.mgr.metrics
	var b strings.Builder
	fmt.Fprintf(&b, "hoglite_commits_total %d\n", m.Commits.Load())
	fmt.Fprintf(&b, "hoglite_queries_total %d\n", m.Queries.Load())
	fmt.Fprintf(&b, "hoglite_segment_puts_total %d\n", m.SegPuts.Load())
	fmt.Fprintf(&b, "hoglite_segment_bytes_total %d\n", m.SegBytes.Load())
	fmt.Fprintf(&b, "hoglite_segment_stored_bytes_total %d\n", m.SegStoredBytes.Load())
	fmt.Fprintf(&b, "hoglite_base_bytes_total %d\n", m.BaseBytes.Load())
	fmt.Fprintf(&b, "hoglite_base_stored_bytes_total %d\n", m.BaseStoredBytes.Load())
	fmt.Fprintf(&b, "hoglite_delta_checkpoints_total %d\n", m.DeltaCheckpoints.Load())
	fmt.Fprintf(&b, "hoglite_delta_bytes_total %d\n", m.DeltaBytes.Load())
	fmt.Fprintf(&b, "hoglite_delta_stored_bytes_total %d\n", m.DeltaStoredBytes.Load())
	fmt.Fprintf(&b, "hoglite_rebases_total %d\n", m.Rebases.Load())
	fmt.Fprintf(&b, "hoglite_checkpoint_ms_total %d\n", m.CheckpointMs.Load())
	fmt.Fprintf(&b, "hoglite_acquires_total %d\n", m.Acquires.Load())
	fmt.Fprintf(&b, "hoglite_checkpoints_total %d\n", m.Checkpoints.Load())
	fmt.Fprintf(&b, "hoglite_proxied_total %d\n", m.Proxied.Load())
	fmt.Fprintf(&b, "hoglite_evictions_total %d\n", m.Evictions.Load())
	fmt.Fprintf(&b, "hoglite_dedup_hits_total %d\n", m.DedupHits.Load())
	fmt.Fprintf(&b, "hoglite_sweeps_total %d\n", m.Sweeps.Load())
	fmt.Fprintf(&b, "hoglite_swept_objects_total %d\n", m.SweptObjects.Load())
	fmt.Fprintf(&b, "hoglite_open_tenants %d\n", s.mgr.OpenCount())
	fmt.Fprintf(&b, "hoglite_inflight_segments %d\n", m.InflightSegs.Load())
	fmt.Fprintf(&b, "hoglite_queries_inflight %d\n", m.QueriesInflight.Load())
	fmt.Fprintf(&b, "hoglite_queries_waiting %d\n", m.QueriesWaiting.Load())
	fmt.Fprintf(&b, "hoglite_commit_latency_ewma_us %d\n", m.CommitEwmaUs.Load())
	fmt.Fprintf(&b, "hoglite_read_timeouts_total %d\n", m.ReadTimeouts.Load())
	fmt.Fprintf(&b, "hoglite_write_timeouts_total %d\n", m.WriteTimeouts.Load())
	fmt.Fprintf(&b, "hoglite_result_too_large_total %d\n", m.ResultTooLarge.Load())
	interruptOK := 0
	if interruptSupport() == nil {
		interruptOK = 1
	}
	fmt.Fprintf(&b, "hoglite_sqlite_interrupt_available %d\n", interruptOK)
	fmt.Fprintf(&b, "hoglite_interrupt_handle_misses_total %d\n", m.InterruptMisses.Load())
	sqUsed, sqHW := SQLiteMemory()
	sqHard, sqSoft := SQLiteHeapLimits()
	fmt.Fprintf(&b, "hoglite_sqlite_memory_used_bytes %d\n", sqUsed)
	fmt.Fprintf(&b, "hoglite_sqlite_memory_highwater_bytes %d\n", sqHW)
	fmt.Fprintf(&b, "hoglite_sqlite_heap_limit_bytes %d\n", sqHard)
	fmt.Fprintf(&b, "hoglite_sqlite_soft_heap_limit_bytes %d\n", sqSoft)
	fmt.Fprintf(&b, "hoglite_go_memory_limit_bytes %d\n", goMemoryLimit())
	fmt.Fprintf(&b, "hoglite_out_of_memory_total %d\n", m.OutOfMemory.Load())
	// saturation: max(commit-latency ratio, inflight ratio); 1.0 = at
	// operating limit. This is the HPA target metric.
	fmt.Fprintf(&b, "hoglite_saturation %.4f\n", s.mgr.Saturation())
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	w.WriteHeader(200)
	w.Write([]byte(b.String()))
	return 0, nil
}

func (s *Server) handleInfo(w http.ResponseWriter, r *http.Request) (int, any) {
	return 200, s.mgr.Stats()
}

func (s *Server) handleCreate(w http.ResponseWriter, r *http.Request) (int, any) {
	tid := r.PathValue("tid")
	if !validTID(tid) {
		return 400, apiError{Error: "bad tenant id"}
	}
	var body struct {
		Class string `json:"class"`
	}
	decodeBody(r, &body)
	if body.Class != "" && body.Class != s.mgr.class {
		return s.proxyOr421(w, r, tid, "/v1/tenants/"+tid, nil)
	}
	_, err := s.mgr.Acquire(r.Context(), tid, true)
	if err != nil {
		// create on an existing tenant is a no-op success (idempotent):
		// if the manifest exists, the tenant exists, regardless of who
		// currently owns it
		if errors.Is(err, ErrBusy) || errors.Is(err, ErrWrongClass) {
			if mf, _, gerr := s.mgr.store.GetManifest(r.Context(), tid); gerr == nil && mf != nil {
				return 200, map[string]any{"tenant": tid, "owner": mf.OwnerAddr, "existing": true}
			}
		}
		return errCode(err), apiError{Error: err.Error()}
	}
	return 200, map[string]any{"tenant": tid, "owner": s.mgr.ID()}
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) (int, any) {
	// test/ops helper: drop everything under the tenant prefix
	tid := r.PathValue("tid")
	// never delete under a live remote owner: fence first or refuse
	if mf, err := s.mgr.OwnerOf(r.Context(), tid); err == nil && mf != nil &&
		mf.Owner != "" && s.ownerLive(r.Context(), mf.Owner) && mf.Owner != s.mgr.ID() {
		return 409, apiError{Error: "tenant has a live owner; release it first", Owner: mf.OwnerAddr}
	}
	if t := s.mgr.LocalTenant(tid); t != nil {
		t.Stop(false, "")
	}
	s.mgr.Forget(tid)
	if err := s.mgr.store.DeleteTenant(r.Context(), tid); err != nil {
		return 500, apiError{Error: err.Error()}
	}
	return 200, map[string]any{"deleted": tid}
}

// cachedOwner returns the last seen live owner's ROUTABLE address, if fresh.
func (s *Server) cachedOwner(tid string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.ownerCache[tid]
	if !ok || time.Since(e.at) > s.ownerCacheT {
		return "", false
	}
	// no untilMs check: manifest OwnerUntilMs is informational under pod
	// leases. The 1s TTL bounds staleness; a dead owner makes the proxy
	// fail, which triggers a manifest re-read.
	return e.owner, true
}

func (s *Server) cacheOwner(tid, owner string, untilMs int64) {
	s.mu.Lock()
	if len(s.ownerCache) > 100000 { // bound: drop all and rebuild
		s.ownerCache = map[string]ownerCacheEntry{}
	}
	s.ownerCache[tid] = ownerCacheEntry{owner: owner, untilMs: untilMs, at: time.Now()}
	s.mu.Unlock()
}

// routeToOwner resolves whether we own tid. If not, proxies to the owner.
// Returns the tenant when local, or nil after writing a proxied response.
func (s *Server) localOrProxy(w http.ResponseWriter, r *http.Request, tid string, path string, reqBody []byte) *Tenant {
	if t := s.mgr.LocalTenant(tid); t != nil {
		return t
	}
	ctx := r.Context()
	// fast path: cached owner address from a recent manifest read
	if owner, ok := s.cachedOwner(tid); ok && owner != s.mgr.RouteID() && r.Header.Get("x-hw-noproxy") == "" {
		if base, ok := s.peers[owner]; ok {
			s.mgr.metrics.Proxied.Add(1)
			if s.proxyTo(w, r, base+path, reqBody) {
				return nil
			}
			// transport error: fall through to manifest re-read
		}
	}
	mf, err := s.mgr.OwnerOf(ctx, tid)
	if err != nil {
		writeJSON(w, 500, apiError{Error: err.Error()})
		return nil
	}
	if mf == nil {
		writeJSON(w, 404, apiError{Error: ErrNoTenant.Error()})
		return nil
	}
	live := mf.Owner != "" && s.ownerLive(ctx, mf.Owner)
	if live {
		addr := mf.OwnerAddr
		if addr == "" {
			addr = mf.Owner
		}
		s.cacheOwner(tid, addr, mf.OwnerUntilMs)
	}
	if live && mf.Owner != s.mgr.ID() {
		ownerAddr := mf.OwnerAddr
		if ownerAddr == "" {
			ownerAddr = mf.Owner
		}
		if r.Header.Get("x-hw-noproxy") != "" {
			writeJSON(w, 421, apiError{Error: "not owner", Owner: ownerAddr, Class: mf.Class})
			return nil
		}
		base, ok := s.peers[ownerAddr]
		if ok {
			s.mgr.metrics.Proxied.Add(1)
			if s.proxyTo(w, r, base+path, reqBody) {
				return nil
			}
			// unreachable or stale route: re-read the manifest once
			s.mu.Lock()
			delete(s.ownerCache, tid)
			s.mu.Unlock()
			if mf2, _, err := s.mgr.store.GetManifest(ctx, tid); err == nil && mf2 != nil {
				prevOwner := mf.Owner
				mf = mf2
				live = mf.Owner != "" && s.ownerLive(ctx, mf.Owner)
				if live {
					addr2 := mf.OwnerAddr
					if addr2 == "" {
						addr2 = mf.Owner
					}
					s.cacheOwner(tid, addr2, mf.OwnerUntilMs)
					if mf.Owner != s.mgr.ID() && mf.Owner != prevOwner {
						// ownership genuinely moved: one re-proxy to the new owner
						if base2, ok2 := s.peers[addr2]; ok2 {
							s.mgr.metrics.Proxied.Add(1)
							if s.proxyTo(w, r, base2+path, reqBody) {
								return nil
							}
						}
					}
				}
			}
			// else: owner unreachable with a nominally live lease, or the
			// lease is dead: acquire locally (Acquire waits out the lease)
		} else {
			// owner not in our peer table: the manifest may name a pod we
			// can't route to (stale OwnerAddr, or a peer we don't know).
			// Fall through to local acquisition — the lease check in
			// Acquire is the fence; if the owner is alive we wait it out.
		}
	}
	// free or expired lease, or we are the manifest owner: acquire locally
	t, err := s.mgr.Acquire(ctx, tid, false)
	if err != nil {
		code := errCode(err)
		ae := apiError{Error: err.Error()}
		if mf2, _, e2 := s.mgr.store.GetManifest(ctx, tid); e2 == nil && mf2 != nil {
			// always the routable address, never the fencing identity
			if mf2.OwnerAddr != "" {
				ae.Owner = mf2.OwnerAddr
			} else if i := strings.Index(mf2.Owner, "."); i > 0 {
				ae.Owner = mf2.Owner[:i]
			} else {
				ae.Owner = mf2.Owner
			}
			ae.Class = mf2.Class
		}
		writeJSON(w, code, ae)
		return nil
	}
	return t
}

func (s *Server) proxyTo(w http.ResponseWriter, r *http.Request, url string, body []byte) bool {
	req, err := http.NewRequestWithContext(r.Context(), r.Method, url, bytes.NewReader(body))
	if err != nil {
		writeJSON(w, 500, apiError{Error: err.Error()})
		return false
	}
	req.Header = r.Header.Clone()
	req.Header.Set("x-hw-noproxy", "1")
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		s.mgr.metrics.ProxyFails.Add(1)
		return false // transport error: caller falls through to acquisition
	}
	defer resp.Body.Close()
	for k := range resp.Header {
		if strings.HasPrefix(strings.ToLower(k), "x-hw-") {
			w.Header().Set(k, resp.Header.Get(k))
		}
	}
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
	return true
}

func (s *Server) proxyOr421(w http.ResponseWriter, r *http.Request, tid, path string, body []byte) (int, any) {
	// class mismatch at create time: route by scanning peers is out of
	// prototype scope; tell the caller which class to find.
	return 421, apiError{Error: "wrong class", Class: s.mgr.class}
}

func (s *Server) handleTx(w http.ResponseWriter, r *http.Request) (int, any) {
	tid := r.PathValue("tid")
	if !validTID(tid) {
		return 400, apiError{Error: "bad tenant id"}
	}
	body, err := io.ReadAll(http.MaxBytesReader(nil, r.Body, 64<<20))
	if err != nil {
		return 400, apiError{Error: err.Error()}
	}
	t := s.localOrProxy(w, r, tid, r.URL.Path, body)
	if t == nil {
		return 0, nil // response already written
	}
	var req TxRequest
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	if err := dec.Decode(&req); err != nil {
		return 400, apiError{Error: err.Error()}
	}
	if len(req.Stmts) == 0 {
		return 400, apiError{Error: "no statements"}
	}
	if req.Client == "" || req.TxID == "" {
		// exactly-once under retries requires an idempotency key
		return 400, apiError{Error: "client and txid required"}
	}
	w.Header().Set("x-hw-owner", s.mgr.ID())
	results, dup, err := t.SubmitTxCtx(r.Context(), req)
	if err != nil {
		if code, ae, ok := guardStatus(err); ok {
			ae.Owner = s.mgr.ID()
			return code, ae
		}
		var ef *ExpectFailedError
		switch {
		case errors.As(err, &ef):
			// deterministic refusal (conflict guard, missing table): the
			// caller re-prepares; retrying the same payload is pointless
			return 409, apiError{Error: "expect_failed: " + err.Error(), Owner: s.mgr.ID(),
				GuardIndex: ef.Index + 1, GuardKind: ef.Guard}
		case errors.Is(err, ErrNotOwner), errors.Is(err, ErrClosed), errors.Is(err, ErrReadPreempted),
			errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			return 503, apiError{Error: err.Error(), Owner: s.mgr.ID()} // transient: retry
		default:
			// transient infra errors (segment PUT timeouts, S3 throttling,
			// checkpoint/heartbeat CAS throttle) must NOT read as a permanent
			// SQL refusal: 503, caller retries
			msg := err.Error()
			if strings.Contains(msg, "context deadline exceeded") ||
				strings.Contains(msg, "SlowDown") ||
				strings.Contains(msg, "retry Send token") ||
				strings.Contains(msg, "segment put") ||
				strings.Contains(msg, "base put") ||
				strings.Contains(msg, "checkpoint cas") ||
				strings.Contains(msg, "heartbeat cas") {
				return 503, apiError{Error: msg, Owner: s.mgr.ID()}
			}
			return 422, apiError{Error: err.Error()} // SQL errors are permanent
		}
	}
	return 200, map[string]any{"results": results, "dup": dup}
}

func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) (int, any) {
	tid := r.PathValue("tid")
	if !validTID(tid) {
		return 400, apiError{Error: "bad tenant id"}
	}
	body, err := io.ReadAll(http.MaxBytesReader(nil, r.Body, 16<<20))
	if err != nil {
		return 400, apiError{Error: err.Error()}
	}
	t := s.localOrProxy(w, r, tid, r.URL.Path, body)
	if t == nil {
		return 0, nil
	}
	var st Statement
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	if err := dec.Decode(&st); err != nil {
		return 400, apiError{Error: err.Error()}
	}
	w.Header().Set("x-hw-owner", s.mgr.ID())
	res, err := t.SubmitQueryCtx(r.Context(), st)
	if err != nil {
		if code, ae, ok := guardStatus(err); ok {
			ae.Owner = s.mgr.ID()
			return code, ae
		}
		if errors.Is(err, ErrNotOwner) || errors.Is(err, ErrClosed) || errors.Is(err, ErrReadPreempted) ||
			errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return 503, apiError{Error: err.Error(), Owner: s.mgr.ID()} // transient: retry
		}
		return 409, apiError{Error: err.Error()}
	}
	return 200, res
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) (int, any) {
	tid := r.PathValue("tid")
	out := map[string]any{}
	if t := s.mgr.LocalTenant(tid); t != nil {
		out["local"] = t.DebugStatus()
	}
	mf, _, err := s.mgr.store.GetManifest(r.Context(), tid)
	if err != nil {
		return 500, apiError{Error: err.Error()}
	}
	out["manifest"] = mf
	return 200, out
}

func (s *Server) handleRelease(w http.ResponseWriter, r *http.Request) (int, any) {
	tid := r.PathValue("tid")
	newClass := r.URL.Query().Get("class")
	if err := s.mgr.Release(tid, newClass); err != nil {
		return 409, apiError{Error: err.Error()}
	}
	return 200, map[string]any{"released": tid, "class": newClass}
}

// handleCrash poisons one tenant actor without release: simulates the loss
// of local state while the replica process keeps running.
func (s *Server) handleCrash(w http.ResponseWriter, r *http.Request) (int, any) {
	tid := r.PathValue("tid")
	if err := s.mgr.CrashTenant(tid); err != nil {
		return 404, apiError{Error: err.Error()}
	}
	return 200, map[string]any{"crashed": tid}
}

func (s *Server) handleShutdown(w http.ResponseWriter, r *http.Request) (int, any) {
	go s.mgr.ShutdownAll()
	return 202, map[string]any{"draining": true}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

var tidOK = func() func(string) bool {
	return func(tid string) bool {
		if len(tid) == 0 || len(tid) > 128 {
			return false
		}
		for _, c := range tid {
			ok := c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.' || c >= 'A' && c <= 'Z'
			if !ok {
				return false
			}
		}
		return true
	}
}()

func validTID(tid string) bool {
	// "." and ".." escape the per-tenant cache directory; dots are legal
	// in S3 keys but never needed here
	if tid == "" || tid == "." || tid == ".." || strings.Contains(tid, "..") || strings.HasPrefix(tid, ".") {
		return false
	}
	return tidOK(tid)
}
