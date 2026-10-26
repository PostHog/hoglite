package hoglite

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestThrottleDoesNotPoisonWhileLeaseAlive: during a SlowDown storm the
// segment PUT must keep retrying (hard backoff) as long as the pod lease
// is alive, and the commit must ack once S3 recovers. Before the fix, the
// attempt loop was bounded by the fence captured at cut time, so a
// throttle window longer than ~8s poisoned the tenant even though the
// lease was fine — and 1600 tenants poisoning at once caused the storm.
func TestThrottleDoesNotPoisonWhileLeaseAlive(t *testing.T) {
	fs := NewFakeStore()
	m := newTestManager(t, fs, "pod-a")
	mustCreate(t, m, "tt")
	mustTx(t, m, "tt", "c", "1", Statement{SQL: "create table x (v int)"})

	// throttle for ~3s: longer than the test lease margin would allow a
	// stale-fenced attempt loop to survive
	var throttled atomic.Bool
	throttled.Store(true)
	go func() { time.Sleep(3 * time.Second); throttled.Store(false) }()
	base := fs.FailPutSegment
	fs.FailPutSegment = func(key string) bool {
		if throttled.Load() && strings.HasSuffix(key, ".seg") {
			return true
		}
		if base != nil {
			return base(key)
		}
		return false
	}

	done := make(chan error, 1)
	go func() {
		ten, err := m.Acquire(context.Background(), "tt", false)
		if err != nil {
			done <- err
			return
		}
		_, _, err = ten.SubmitTx(TxRequest{Client: "c", TxID: "2", Stmts: []Statement{{SQL: "insert into x values (1)"}}})
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("commit during throttle: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("commit never acked after throttle cleared")
	}
	// tenant must still be alive and owned
	if ten := m.LocalTenant("tt"); ten == nil || !ten.Alive() {
		t.Fatal("tenant died during throttle")
	}
}
