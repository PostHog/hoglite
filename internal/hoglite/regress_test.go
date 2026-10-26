package hoglite

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

// hasWrites must be set by a real commit so idle release checkpoints.
func TestHasWritesDrivesCheckpoint(t *testing.T) {
	fs := NewFakeStore()
	m := newTestManager(t, fs, "a")
	mustCreate(t, m, "t1")
	makeSchema(t, m, "t1")
	// read-only commit does NOT set hasWrites (reset after the schema write)
	ten, _ := m.Acquire(context.Background(), "t1", false)
	ten.hasWrites = false
	mustTx(t, m, "t1", "c", "r", Statement{SQL: `SELECT 1`})
	if ten.hasWrites {
		t.Fatal("read-only set hasWrites")
	}
	// a real commit sets it
	mustTx(t, m, "t1", "c", "w", insertStmt(1, "v"))
	if !ten.hasWrites {
		t.Fatal("real commit did not set hasWrites")
	}
	// a dedup replay does NOT set it (no new bytes)
	fs2 := NewFakeStore()
	m2 := newTestManager(t, fs2, "b")
	mustCreate(t, m2, "t2")
	makeSchema(t, m2, "t2")
	mustTx(t, m2, "t2", "c", "w", insertStmt(1, "v"))
	ten2, _ := m2.Acquire(context.Background(), "t2", false)
	ten2.hasWrites = false // reset to observe the dedup path alone
	_, dup, err := ten2.SubmitTx(TxRequest{Client: "c", TxID: "w", Stmts: []Statement{insertStmt(1, "v")}})
	if err != nil || !dup {
		t.Fatalf("want dedup hit, dup=%v err=%v", dup, err)
	}
	if ten2.hasWrites {
		t.Fatal("dedup replay set hasWrites")
	}
}

// A cleanup goroutine for a dead incarnation must never clear a
// DELETED-THEN-RECREATED tenant: epoch resets to 1, so the fence must
// also compare instance.
func TestCleanupNeverClearsRecreatedTenant(t *testing.T) {
	fs := NewFakeStore()
	m := newTestManager(t, fs, "a")
	mustCreate(t, m, "t1")
	makeSchema(t, m, "t1")
	old := m.LocalTenant("t1")
	if old == nil {
		t.Fatal("no tenant")
	}
	// capture the old incarnation identity, then poison it (cleanup
	// scheduled via noteClosed would fire; we simulate a delayed one)
	oldEpoch, oldInst := old.claimEpoch, old.claimInst
	old.Poison(errors.New("test poison"))

	// delete + recreate under the same name: epoch resets to 1
	if err := fs.DeleteTenant(context.Background(), "t1"); err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	delete(m.tenants, "t1")
	m.mu.Unlock()
	mustCreate(t, m, "t1")
	makeSchema(t, m, "t1")

	// the delayed cleanup for the OLD incarnation must not touch the new one
	m.clearDeadOwner("t1", oldEpoch, oldInst)
	mf, _, err := m.store.GetManifest(context.Background(), "t1")
	if err != nil || mf == nil {
		t.Fatal("manifest gone")
	}
	if mf.Owner != m.ID() {
		t.Fatalf("recreated tenant lost ownership to a stale cleanup: %+v", mf)
	}
	if _, _, err := m.LocalTenant("t1").SubmitTx(TxRequest{Client: "c", TxID: "x1", Stmts: []Statement{insertStmt(1, "v")}}); err != nil {
		t.Fatalf("fresh incarnation fenced by stale cleanup: %v", err)
	}
}

func TestSaturationMetric(t *testing.T) {
	m := &Metrics{}
	mgr := &Manager{metrics: m, satCommitBudgetUs: 500000, satInflightCap: 128}
	if got := mgr.Saturation(); got != 0 {
		t.Fatalf("idle saturation = %v, want 0", got)
	}
	// latency-dominated
	m.CommitEwmaUs.Store(250000) // 250ms vs 500ms budget
	if got := mgr.Saturation(); got < 0.49 || got > 0.51 {
		t.Fatalf("latency saturation = %v, want ~0.5", got)
	}
	// inflight-dominated wins
	m.InflightSegs.Store(96) // 96/128 = 0.75 > 0.5
	if got := mgr.Saturation(); got < 0.74 || got > 0.76 {
		t.Fatalf("inflight saturation = %v, want ~0.75", got)
	}
	// over the limit
	m.InflightSegs.Store(256)
	if got := mgr.Saturation(); got < 1.99 {
		t.Fatalf("oversaturation = %v, want ~2.0", got)
	}
}

func TestEwmaUpdate(t *testing.T) {
	var a atomic.Int64
	ewmaUpdate(&a, 800)
	if a.Load() != 800 {
		t.Fatalf("first sample should seed: got %d", a.Load())
	}
	for i := 0; i < 100; i++ {
		ewmaUpdate(&a, 1000)
	}
	if got := a.Load(); got < 990 || got > 1000 {
		t.Fatalf("ewma should converge to 1000, got %d", got)
	}
	for i := 0; i < 100; i++ {
		ewmaUpdate(&a, 200)
	}
	if got := a.Load(); got < 200 || got > 210 {
		t.Fatalf("ewma should converge to 200, got %d", got)
	}
}
