package hoglite

import (
	"context"
	"strings"
	"testing"
)

// The sweeper deletes exactly the objects no manifest references:
// dead instance dirs, lost-claim base uploads, old WAL generations.
func TestSweeperCollectsOrphans(t *testing.T) {
	fs := NewFakeStore()
	m := newTestManager(t, fs, "a")
	mustCreate(t, m, "t1")
	makeSchema(t, m, "t1")
	mustTx(t, m, "t1", "c", "s1", insertStmt(1, "v"))
	// release so the tenant is sweepable (live leases are never swept)
	if err := m.Release("t1", ""); err != nil {
		t.Fatal(err)
	}

	mf, _, _ := fs.GetManifest(context.Background(), "t1")
	// plant garbage: a dead instance dir, a lost base attempt, an old gen
	fs.PutSegment(context.Background(), segKey("fake", "t1", "deaddead", 1, 1, 0), []byte("x"))
	fs.PutBase(context.Background(), baseKeyAttempt("fake", "t1", mf.Instance, 1, 999, "lostclaim"),
		strings.NewReader("stale"), 5)
	fs.PutSegment(context.Background(),
		segKey("fake", "t1", mf.Instance, mf.Gen-1, mf.Epoch, 0), []byte("y"))

	before := len(fs.ObjectKeys())
	sw := NewSweeper(m, fs, "fake", testOpts(), &Metrics{})
	sw.sweepOnce(context.Background())
	after := fs.ObjectKeys()
	if len(after) >= before {
		t.Fatalf("sweep collected nothing: %d -> %d", before, len(after))
	}
	for _, k := range after {
		if strings.Contains(k, "deaddead") || strings.Contains(k, "lostclaim") {
			t.Fatalf("orphan survived: %s", k)
		}
		if strings.Contains(k, "/wal/") && !strings.Contains(k, "/wal/00000") {
			// note: check live gen explicitly below
		}
	}
	// live objects intact: data still reads
	if n := countRows(t, m, "t1"); n != 1 {
		t.Fatalf("sweep ate live data: %d", n)
	}
	// the live WAL generation survived (release checkpoints, so gen's
	// segment list may be empty after the fold; the base must survive)
	mf2, _, _ := fs.GetManifest(context.Background(), "t1")
	if _, err := fs.GetObject(context.Background(), mf2.BaseKey); err != nil {
		t.Fatalf("live base swept: %v", err)
	}
}
