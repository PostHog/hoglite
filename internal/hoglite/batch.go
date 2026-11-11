package hoglite

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
)

// POST /v1/query-batch: one read-only query per item, each item against its
// own tenant, in one request.
//
// A client that needs one small read from many tenants (a catalog list: one
// meta row per catalog tenant) otherwise sends one /query per tenant. Each of
// those is a round trip and, on a pod that does not own the tenant, a proxy
// hop, so the cost is N sequential round trips. This endpoint takes the N
// reads in one request and returns N answers in the same order.
//
// Every item gets the answer /query would give: the same routing, the same
// read-only rule, the same status and body. An item's failure is its own
// and does not fail the batch. The request is 200 whenever the batch could
// be read. Each entry carries the status and body that /query would have
// sent, so a client parses an item the way it parses a /query response.
//
// Routing, per item, on the pod that receives the batch:
//
//   - a tenant open here is answered here;
//   - a tenant with a live owner in the peer table goes to that owner in ONE
//     sub-batch per peer, sent with x-hw-noproxy. The peer answers the items
//     it owns and 421s the rest. A 421 item, or every item of a sub-batch
//     whose request failed, then takes the single-item path below;
//   - every other tenant (unknown, unowned, expired lease, unroutable owner)
//     takes the single-item path: the same code /v1/t/{tid}/query runs, so a
//     free tenant is acquired here and a missing one answers 404.
//
// So a batch costs the client one round trip and costs the cluster at most
// one hop per peer for the tenants with a known live owner. Items run with
// at most batchParallel at a time, per phase. Interior reads and writes are
// not offered: an item is a /query statement and nothing else.

const (
	// maxBatchQueries bounds one request. A client with more items splits
	// them; the bound keeps one request's work inside a client's attempt
	// timeout when many of its tenants must be acquired cold.
	maxBatchQueries = 1000
	// batchParallel bounds the items one batch runs at a time, per phase.
	batchParallel = 32
)

// BatchQuery is one item of a /v1/query-batch request.
type BatchQuery struct {
	Tenant string `json:"tenant"`
	SQL    string `json:"sql"`
	Args   []any  `json:"args,omitempty"`
}

type batchRequest struct {
	Queries []BatchQuery `json:"queries"`
}

// BatchAnswer is one item's answer: the status and body /query would send.
type BatchAnswer struct {
	Status int             `json:"status"`
	Body   json.RawMessage `json:"body"`
}

type batchResponse struct {
	Results []BatchAnswer `json:"results"`
}

func (s *Server) handleQueryBatch(w http.ResponseWriter, r *http.Request) (int, any) {
	raw, err := io.ReadAll(http.MaxBytesReader(nil, r.Body, 16<<20))
	if err != nil {
		return 400, apiError{Error: err.Error()}
	}
	var req batchRequest
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&req); err != nil {
		return 400, apiError{Error: err.Error()}
	}
	if len(req.Queries) == 0 {
		return 400, apiError{Error: "no queries"}
	}
	if len(req.Queries) > maxBatchQueries {
		return 400, apiError{Error: fmt.Sprintf("%d queries in one batch; at most %d", len(req.Queries), maxBatchQueries)}
	}
	// Validate the complete batch before ownership lookups or execution.
	if a := authority(r); a != nil {
		for _, q := range req.Queries {
			if q.Tenant != a.tenant {
				return 403, apiError{Error: "batch tenant not authorized", Code: "refused"}
			}
		}
	}
	s.mgr.metrics.QueryBatches.Add(1)
	s.mgr.metrics.QueryBatchItems.Add(int64(len(req.Queries)))

	out := make([]BatchAnswer, len(req.Queries))
	ctx := r.Context()
	peerOnly := r.Header.Get("x-hw-noproxy") != ""

	// Phase 1: where does each item go? A peer's own sub-batch (peerOnly)
	// never forwards again: its items all take the single-item path, which
	// under x-hw-noproxy answers what it owns and 421s the rest.
	peerOf := make([]string, len(req.Queries))
	if !peerOnly {
		parallel(len(req.Queries), func(i int) {
			if validTID(req.Queries[i].Tenant) {
				peerOf[i] = s.batchPeer(ctx, req.Queries[i].Tenant)
			}
		})
	}
	groups := map[string][]int{}
	var single []int
	for i, p := range peerOf {
		if p == "" {
			single = append(single, i)
		} else {
			groups[p] = append(groups[p], i)
		}
	}

	// Phase 2: one sub-batch per peer, all peers at once.
	var mu sync.Mutex
	var wg sync.WaitGroup
	for peer, idx := range groups {
		wg.Add(1)
		go func(base string, idx []int) {
			defer wg.Done()
			answers, ok := s.forwardBatch(r, base, req.Queries, idx)
			mu.Lock()
			defer mu.Unlock()
			for k, i := range idx {
				if ok && answers[k].Status != 421 {
					out[i] = answers[k]
				} else {
					single = append(single, i)
				}
			}
		}(s.peers[peer], idx)
	}
	wg.Wait()

	// Phase 3: everything else through /query's own code.
	parallel(len(single), func(k int) {
		i := single[k]
		out[i] = s.queryOne(r, peerOnly, req.Queries[i])
	})
	return 200, batchResponse{Results: out}
}

// batchPeer names the peer that owns tid and can take it in a sub-batch, or
// "" when the item must take the single-item path here: the tenant is open
// here, has no manifest, has no live owner, or its owner is not routable.
// The owner lookup is localOrProxy's (cached owner, else one manifest read),
// so a batch reads no more manifests than the same reads sent one by one.
func (s *Server) batchPeer(ctx context.Context, tid string) string {
	if s.mgr.LocalTenant(tid) != nil {
		return ""
	}
	if owner, ok := s.cachedOwner(tid); ok {
		if _, routable := s.peers[owner]; routable && owner != s.mgr.RouteID() {
			return owner
		}
		return ""
	}
	mf, err := s.mgr.OwnerOf(ctx, tid)
	if err != nil || mf == nil || mf.Owner == "" || mf.Owner == s.mgr.ID() {
		return ""
	}
	if !s.ownerLive(ctx, mf.Owner) {
		return ""
	}
	addr := ownerAddr(mf)
	s.cacheOwner(tid, addr, mf.OwnerUntilMs)
	if _, routable := s.peers[addr]; !routable || addr == s.mgr.RouteID() {
		return ""
	}
	return addr
}

// forwardBatch sends the items idx to the peer at base as one sub-batch with
// x-hw-noproxy. ok is false when the peer could not answer the batch as a
// whole (transport error, an engine without this endpoint, a malformed
// answer); the caller then sends each item down the single-item path.
func (s *Server) forwardBatch(r *http.Request, base string, all []BatchQuery, idx []int) ([]BatchAnswer, bool) {
	sub := batchRequest{Queries: make([]BatchQuery, len(idx))}
	for k, i := range idx {
		sub.Queries[k] = all[i]
	}
	body, err := json.Marshal(sub)
	if err != nil {
		return nil, false
	}
	req, err := http.NewRequestWithContext(r.Context(), http.MethodPost, base+"/v1/query-batch", bytes.NewReader(body))
	if err != nil {
		return nil, false
	}
	req.Header.Set("x-hw-noproxy", "1")
	req.Header.Set("Content-Type", "application/json")
	forwardAuthority(req, r)
	s.mgr.metrics.Proxied.Add(1)
	resp, err := s.client.Do(req)
	if err != nil {
		s.mgr.metrics.ProxyFails.Add(1)
		return nil, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		io.Copy(io.Discard, resp.Body)
		return nil, false
	}
	var got batchResponse
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil || len(got.Results) != len(idx) {
		return nil, false
	}
	return got.Results, true
}

// queryOne answers one item exactly as POST /v1/t/{tid}/query would, by
// running that handler on an in-memory response. peerOnly carries the
// sub-batch's x-hw-noproxy, so a peer answers only what it owns.
func (s *Server) queryOne(r *http.Request, peerOnly bool, q BatchQuery) BatchAnswer {
	body, err := json.Marshal(Statement{SQL: q.SQL, Args: q.Args})
	if err != nil {
		return errorAnswer(400, err.Error())
	}
	path := "/v1/t/" + q.Tenant + "/query"
	sub, err := http.NewRequestWithContext(r.Context(), http.MethodPost, path, bytes.NewReader(body))
	if err != nil {
		return errorAnswer(400, err.Error())
	}
	sub.Header.Set("Content-Type", "application/json")
	forwardAuthority(sub, r)
	if peerOnly {
		sub.Header.Set("x-hw-noproxy", "1")
	}
	sub.SetPathValue("tid", q.Tenant)
	sub.Pattern = "POST /v1/t/{tid}/query"
	rec := &bufferedResponse{header: http.Header{}}
	s.wrap(s.handleQuery)(rec, sub)
	status := rec.status
	if status == 0 {
		status = 200
	}
	out := bytes.TrimSpace(rec.buf.Bytes())
	if !json.Valid(out) {
		// a proxy or transport answer that is not JSON: keep its text
		return errorAnswer(status, string(out))
	}
	return BatchAnswer{Status: status, Body: append(json.RawMessage(nil), out...)}
}

func errorAnswer(status int, msg string) BatchAnswer {
	b, _ := json.Marshal(apiError{Error: msg})
	return BatchAnswer{Status: status, Body: b}
}

// bufferedResponse is an in-memory http.ResponseWriter for queryOne.
type bufferedResponse struct {
	header http.Header
	status int
	buf    bytes.Buffer
}

func (b *bufferedResponse) Header() http.Header { return b.header }

func (b *bufferedResponse) WriteHeader(code int) {
	if b.status == 0 {
		b.status = code
	}
}

func (b *bufferedResponse) Write(p []byte) (int, error) {
	if b.status == 0 {
		b.status = 200
	}
	return b.buf.Write(p)
}

// parallel runs f(0..n-1) with at most batchParallel calls at a time.
func parallel(n int, f func(int)) {
	if n == 0 {
		return
	}
	sem := make(chan struct{}, batchParallel)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			f(i)
		}(i)
	}
	wg.Wait()
}
