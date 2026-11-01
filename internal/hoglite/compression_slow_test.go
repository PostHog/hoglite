//go:build slow

package hoglite

// The slow compression test. CI runs it in its own job (go-race-slow);
// see slow_test.go.

import (
	"context"
	"fmt"
	"testing"
)

// TestOldBuildRefusesZstdGeneration: an old pod that must take over a
// tenant whose current generation is zstd refuses before its claim CAS:
// no manifest publication, no deletion, no change to any object. A new pod
// then serves every acked row.
//
// "large-tx" is the reviewer's data-loss reproduction against e1a0fc8: a
// zstd base, then one transaction that creates a table (page 1) and
// writes a 64 MiB blob. The first S1 commit stored that segment raw (too
// large to compress in memory); SQLite then read page 1 from the raw WAL,
// the old pod opened the tenant, published a base without the "cold"
// table and deleted the zstd base. In a zstd generation every segment is a
// frame, so the old WAL check fails at byte 0. The case also proves that a
// segment above 64 MiB recovers on a new pod.
func TestOldBuildRefusesZstdGeneration(t *testing.T) {
	ctx := context.Background()
	shapes := map[string]struct {
		hot     string // statement for the "hot" table; empty: none
		release bool   // release (base only) instead of crash
	}{
		"base-only":      {release: true},
		"segments":       {},
		"large-tx":       {hot: `INSERT INTO hot VALUES (1, zeroblob(67108864))`},
		"incompressible": {hot: `INSERT INTO hot VALUES (1, randomblob(300000))`},
	}
	// "zstd": deltas off, a format 1 gen, read by a build before
	// compression. "chain": deltas on, a format 2 gen with a delta chain,
	// read by a build before compression and by a build with compression
	// but without deltas. Every reader must refuse.
	type mode struct {
		deltas  bool
		format  int
		readers []string
	}
	modes := map[string]mode{
		"zstd":  {false, manifestFormatZstd, []string{"legacy"}},
		"chain": {true, manifestFormatChain, []string{"legacy", "s1"}},
	}
	for mname, md := range modes {
		for name, sh := range shapes {
			for _, reader := range md.readers {
				t.Run(mname+"/"+name+"/"+reader, func(t *testing.T) {
					oldBuildRefusal(t, ctx, name, sh.hot, sh.release, md.deltas, md.format, reader)
				})
			}
		}
	}
}

func oldBuildRefusal(t *testing.T, ctx context.Context, name, hot string, release, deltas bool, wantFormat int, reader string) {
	sh := struct {
		hot     string
		release bool
	}{hot, release}
	fs := NewFakeStore()
	m, _ := newTestManagerOpts(t, fs, "new", func(o *TenantOpts) {
		o.CheckpointByte = 1 << 30
		o.MaxSegments = 1000
		if !deltas {
			o.DeltaMaxChain = 0
		}
		o.DeltaMaxRatio = 1000 // tiny base: keep the deltas
	})
	mustCreate(t, m, "t1")
	mustTx(t, m, "t1", "c", "schema", Statement{SQL: `CREATE TABLE cold(k INTEGER PRIMARY KEY, v BLOB)`})
	mustTx(t, m, "t1", "c", "cold", Statement{SQL: `INSERT INTO cold VALUES (1, zeroblob(4096))`})
	if sh.hot != "" {
		// fold "cold" into a zstd base, then write the hot tx as
		// the first segment of the next generation
		if err := m.Release("t1", ""); err != nil {
			t.Fatal(err)
		}
		mustTx(t, m, "t1", "c", "hot",
			Statement{SQL: `CREATE TABLE hot(k INTEGER PRIMARY KEY, v BLOB)`},
			Statement{SQL: sh.hot})
	}
	if sh.release {
		if err := m.Release("t1", ""); err != nil {
			t.Fatal(err)
		}
	} else {
		crashTenant(t, m, "t1")
		waitLeaseExpiry()
	}
	forms, mf := segForms(t, fs, "t1")
	if mf.Format != wantFormat || forms.raw != 0 || !isZstd(baseObject(t, fs, "t1")) {
		t.Fatalf("want a pure zstd gen of format %d: format %d segments %+v", wantFormat, mf.Format, forms)
	}
	// "segments" has no checkpoint after the create: a chain gen with an
	// empty chain, which must be refused too
	if deltas && (sh.release || sh.hot != "") {
		if len(mf.Deltas) == 0 {
			t.Fatal("want a delta chain")
		}
		for _, d := range mf.Deltas {
			if b, err := fs.GetObject(ctx, d.Key); err != nil || !isZstd(b) {
				t.Fatalf("delta %s: zstd %v, err %v", d.Key, isZstd(b), err)
			}
		}
	}
	if !sh.release && forms.zst == 0 {
		t.Fatal("want segments")
	}
	if name == "large-tx" {
		// one segment above 64 MiB (the old in-memory cap)
		segs, _ := fs.ListSegments(ctx, "t1", mf.Instance, mf.Gen)
		var largest int
		for _, sg := range segs {
			st, _ := fs.GetObject(ctx, sg.Key)
			d, err := decodeBlob(st, maxSegmentRawBytes)
			if err != nil {
				t.Fatal(err)
			}
			largest = max(largest, len(d))
		}
		if largest <= 64<<20 {
			t.Fatalf("largest segment %d bytes, want > 64 MiB", largest)
		}
	}

	// the deletes after the last CAS run async: let them finish, so the
	// snapshot shows only what the old build could change
	before := settledObjects(fs)
	beforeMf, beforeTag, _ := fs.GetManifest(ctx, "t1")
	var old *Manager
	if reader == "s1" {
		old = newS1Manager(t, fs, "old")
	} else {
		old = newLegacyManager(t, fs, "old")
	}
	if _, err := old.Acquire(ctx, "t1", false); err == nil {
		t.Fatal("old build took over a zstd generation")
	} else {
		t.Logf("old build refused: %v", err)
	}
	// its sweeper must not touch the tenant either
	if reader == "s1" {
		NewSweeper(old, fs, "fake", testOpts(), &Metrics{}).sweepTenant(ctx, "t1")
	} else if g := legacySweepGarbage(fs, beforeMf); len(g) > 0 {
		for _, k := range g {
			if manifestNames(beforeMf, k) {
				t.Fatalf("the sweeper of a build before compression deletes live %s", k)
			}
		}
	}
	if mf2, tag2, _ := fs.GetManifest(ctx, "t1"); tag2 != beforeTag || !manifestEqual(mf2, beforeMf) {
		t.Fatal("old build published a manifest")
	}
	after := snapshotObjects(fs)
	if len(after) != len(before) {
		t.Fatalf("old build changed the object set: %d -> %d objects", len(before), len(after))
	}
	for k, v := range before {
		if after[k] != v {
			t.Fatalf("old build changed or deleted %s", k)
		}
	}

	m2 := newTestManager(t, fs, "new2")
	res := mustQuery(t, m2, "t1", `SELECT COUNT(*), SUM(LENGTH(v)) FROM cold`)
	if fmt.Sprint(res.Rows) != "[[1 4096]]" {
		t.Fatalf("acked cold table after old refusal: %v", res.Rows)
	}
	if sh.hot != "" {
		res := mustQuery(t, m2, "t1", `SELECT COUNT(*) FROM hot`)
		if fmt.Sprint(res.Rows) != "[[1]]" {
			t.Fatalf("hot table: %v", res.Rows)
		}
	}
}
