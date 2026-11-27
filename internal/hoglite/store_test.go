package hoglite

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// shardPager is a shardPage over an in-memory layout: shard s holds ids
// "<s>-0".."<s>-<count-1>", perPage to a page, and every page costs
// latency. It records the tokens each shard was asked with, the pages
// served, and the most shards ever in flight at once.
type shardPager struct {
	count, perPage int
	latency        time.Duration
	honorCtx       bool // fail a page whose context has ended, as S3 does
	failShard      int  // -1: no failure
	mu             sync.Mutex
	tokens         map[int][]string
	pages          atomic.Int64
	inflight       atomic.Int64
	maxInflight    atomic.Int64
}

func newShardPager(count, perPage int, latency time.Duration) *shardPager {
	return &shardPager{count: count, perPage: perPage, latency: latency, honorCtx: true, failShard: -1, tokens: map[int][]string{}}
}

var errShardListFailed = errors.New("injected LIST failure")

func (p *shardPager) page(ctx context.Context, shard int, tok *string) ([]string, *string, error) {
	n := p.inflight.Add(1)
	defer p.inflight.Add(-1)
	for {
		m := p.maxInflight.Load()
		if n <= m || p.maxInflight.CompareAndSwap(m, n) {
			break
		}
	}
	p.pages.Add(1)
	p.mu.Lock()
	p.tokens[shard] = append(p.tokens[shard], tokString(tok))
	p.mu.Unlock()
	time.Sleep(p.latency)
	if p.honorCtx && ctx.Err() != nil {
		return nil, nil, ctx.Err()
	}
	if shard == p.failShard {
		return nil, nil, errShardListFailed
	}
	start := 0
	if tok != nil {
		fmt.Sscanf(*tok, "%d/%d", &shard, &start) // a token names its shard and the next offset
	}
	var ids []string
	for i := start; i < p.count && i < start+p.perPage; i++ {
		ids = append(ids, fmt.Sprintf("%d-%d", shard, i))
	}
	if start+p.perPage < p.count {
		next := fmt.Sprintf("%d/%d", shard, start+p.perPage)
		return ids, &next, nil
	}
	return ids, nil, nil
}

func tokString(tok *string) string {
	if tok == nil {
		return "<first>"
	}
	return *tok
}

// The concurrent walk lists the same ids in the same order as the serial
// one did: shard order, each shard's pages in order, every page reached
// through the token the previous page of THAT shard returned. It runs
// width shards at once and no more, so 256 shards of 10 ms each take a
// fraction of the 2.56 s the serial walk took.
func TestListShardsOrderPagesAndWidth(t *testing.T) {
	const latency = 10 * time.Millisecond
	p := newShardPager(5, 2, latency) // 3 pages per shard: 2, 2, 1
	start := time.Now()
	ids, err := listShards(context.Background(), tenantShards, listShardWidth, p.page)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatal(err)
	}
	var want []string
	for shard := range tenantShards {
		for i := range 5 {
			want = append(want, fmt.Sprintf("%d-%d", shard, i))
		}
	}
	if len(ids) != len(want) {
		t.Fatalf("listed %d ids, want %d", len(ids), len(want))
	}
	for i := range want {
		if ids[i] != want[i] {
			t.Fatalf("ids[%d] = %q, want %q (shard order, then page order)", i, ids[i], want[i])
		}
	}
	for shard := range tenantShards {
		got := p.tokens[shard]
		want := []string{"<first>", fmt.Sprintf("%d/2", shard), fmt.Sprintf("%d/4", shard)}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("shard %d paged with tokens %v, want %v", shard, got, want)
		}
	}
	if got := p.pages.Load(); got != 3*tenantShards {
		t.Fatalf("%d pages served, want %d", got, 3*tenantShards)
	}
	if m := p.maxInflight.Load(); m > listShardWidth || m < 2 {
		t.Fatalf("%d shards in flight at the peak, want 2..%d", m, listShardWidth)
	}
	serial := 3 * tenantShards * latency // what the serial walk would have slept
	floor := serial / listShardWidth
	t.Logf("256 shards x 3 pages x %v: %v concurrent (floor %v) against %v serial", latency, elapsed, floor, serial)
	if elapsed > serial/4 {
		t.Fatalf("the walk took %v, want well under the serial %v", elapsed, serial)
	}
}

// A failed page is the walk's answer, and the walk stops: the shards not
// yet fed are never listed. A parent context that ends is a failure too,
// even when no page saw it (the walk stopped feeding shards), never a
// shorter listing.
func TestListShardsStopsOnFailure(t *testing.T) {
	p := newShardPager(1, 1, time.Millisecond)
	p.failShard = 20
	ids, err := listShards(context.Background(), tenantShards, listShardWidth, p.page)
	if !errors.Is(err, errShardListFailed) || ids != nil {
		t.Fatalf("walk with a failing shard: %v, %v; want %v and no ids", ids, err, errShardListFailed)
	}
	if n := p.pages.Load(); n >= tenantShards {
		t.Fatalf("%d pages served after a failure at shard %d: the walk did not stop", n, p.failShard)
	}

	// the caller is gone before the walk starts; the pager never looks at
	// its context, so no page fails
	gone, cancel := context.WithCancel(context.Background())
	cancel()
	p = newShardPager(1, 1, 0)
	p.honorCtx = false
	ids, err = listShards(gone, tenantShards, listShardWidth, p.page)
	if !errors.Is(err, context.Canceled) || ids != nil {
		t.Fatalf("walk under a cancelled context: %d ids, %v; want %v and no ids", len(ids), err, context.Canceled)
	}

	// width above the shard count, and a backend with no tenants: the
	// shapes a small deployment and an empty backend produce
	p = newShardPager(0, 1, 0)
	ids, err = listShards(context.Background(), 3, listShardWidth, p.page)
	if err != nil || len(ids) != 0 || p.pages.Load() != 3 {
		t.Fatalf("empty walk: %v, %v, %d pages", ids, err, p.pages.Load())
	}
}
