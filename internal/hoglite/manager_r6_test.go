package hoglite

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// N concurrent Acquires
// after a crash must yield exactly one takeover and one live tenant. The
// cooldown sleep (staggered jitter) must not let a second caller become
// leader and RemoveAll the live tenant.
func TestR6NoDuplicateTakeoverAfterCooldown(t *testing.T) {
	fs := NewFakeStore()
	m := newTestManager(t, fs, "pod-a")
	mustCreate(t, m, "t1")
	mustTx(t, m, "t1", "c", "1", Statement{SQL: "create table x (v int)"})
	mustTx(t, m, "t1", "c", "2", Statement{SQL: "insert into x values (42)"})
	if err := m.CrashTenant("t1"); err != nil {
		t.Fatal(err)
	}
	// wait for the crash to settle (noteClosed + poison entry)
	deadline := time.Now().Add(5 * time.Second)
	for m.LocalTenant("t1") != nil && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	baseObjects := len(fs.ObjectKeys())
	const N = 8
	var wg sync.WaitGroup
	tenants := make([]*Tenant, N)
	errs := make([]error, N)
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ten, err := m.Acquire(context.Background(), "t1", false)
			tenants[i], errs[i] = ten, err
		}(i)
	}
	wg.Wait()
	var live map[*Tenant]bool = map[*Tenant]bool{}
	for i := 0; i < N; i++ {
		if errs[i] != nil {
			t.Fatalf("acquire %d: %v", i, errs[i])
		}
		live[tenants[i]] = true
	}
	if len(live) != 1 {
		t.Fatalf("expected exactly one tenant instance, got %d", len(live))
	}
	// the acked write must survive: exactly one replay happened
	qr := mustQuery(t, m, "t1", "select v from x")
	if fmt.Sprint(qr.Rows) != "[[42]]" {
		t.Fatalf("acked write lost: %v", qr.Rows)
	}
	_ = baseObjects
}

// A claim whose checkpoint
// CAS fails transiently must record a poison entry so retries are damped.
func TestR6FailedClaimCoolsDown(t *testing.T) {
	fs := NewFakeStore()
	m1 := newTestManager(t, fs, "pod-a")
	mustCreate(t, m1, "t2")
	mustTx(t, m1, "t2", "c", "1", Statement{SQL: "create table x (v int)"})
	if err := m1.CrashTenant("t2"); err != nil {
		t.Fatal(err)
	}
	m1.partitioned.Store(true) // simulate: this pod can no longer write
	// a fresh manager on the same store takes over but its claim fails
	var fails atomic.Int64
	fails.Store(10) // fail all CAS during the claim
	fs.FailCAS = func() bool { return fails.Add(-1) >= 0 }
	m2 := newTestManager(t, fs, "pod-b")
	_, err := m2.Acquire(context.Background(), "t2", false)
	if err == nil {
		t.Fatal("expected claim failure")
	}
	m2.mu.Lock()
	_, cooled := m2.poisonedAt["t2"]
	m2.mu.Unlock()
	if !cooled {
		t.Fatal("failed claim did not record a poison cooldown")
	}
}
