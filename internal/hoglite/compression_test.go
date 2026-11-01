package hoglite

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// Integration tests for zstd object compression (S1). They run the real
// tenant and manager paths against the fake store.

func newTestManagerOpts(t *testing.T, fs *FakeStore, id string, mut func(*TenantOpts)) (*Manager, *Metrics) {
	t.Helper()
	o := testOpts()
	mut(&o)
	met := &Metrics{}
	return NewManager(fs, "fake", id, "standard", t.TempDir(), o, 1000, met, ControllerOpts{}), met
}

func rawOpts(o *TenantOpts) { o.Compress = false }

// setCompress flips one live tenant's write codec on its actor (the only
// goroutine that reads it), the way a restart with another flag would.
func setCompress(t *testing.T, m *Manager, tid string, on bool) {
	t.Helper()
	ten, err := m.Acquire(context.Background(), tid, false)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	ten.ctlCh <- func() { ten.opts.Compress = on; close(done) }
	<-done
}

// isZstd reports whether a stored object starts with the zstd frame magic
// (test inspection only: production code never sniffs).
func isZstd(b []byte) bool { return len(b) >= 4 && string(b[:4]) == zstdMagic }

type objForms struct{ raw, zst int }

// segForms counts raw and compressed segment objects of the current gen.
func segForms(t *testing.T, fs *FakeStore, tid string) (objForms, *Manifest) {
	t.Helper()
	ctx := context.Background()
	mf, _, _ := fs.GetManifest(ctx, tid)
	segs, err := fs.ListSegments(ctx, tid, mf.Instance, mf.Gen)
	if err != nil {
		t.Fatal(err)
	}
	var f objForms
	for _, s := range segs {
		b, _ := fs.GetObject(ctx, s.Key)
		if isZstd(b) {
			f.zst++
		} else {
			f.raw++
		}
	}
	return f, mf
}

// zstdGen reports whether a manifest format makes every object zstd.
func zstdGen(format int) bool {
	return format == manifestFormatZstd || format == manifestFormatChain
}

func baseObject(t *testing.T, fs *FakeStore, tid string) []byte {
	t.Helper()
	mf, _, _ := fs.GetManifest(context.Background(), tid)
	b, err := fs.GetObject(context.Background(), mf.BaseKey)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestCompressedObjectsRoundTrip: with compression on (the default), every
// new segment and base is one zstd frame, the manifest records the format,
// the metrics show raw > stored, and crash recovery replays them.
func TestCompressedObjectsRoundTrip(t *testing.T) {
	fs := NewFakeStore()
	m, met := newTestManagerOpts(t, fs, "a", func(*TenantOpts) {})
	mustCreate(t, m, "t1")
	makeSchema(t, m, "t1")
	pad := strings.Repeat("compressible-payload-", 40)
	for i := 0; i < 10; i++ {
		mustTx(t, m, "t1", "c", fmt.Sprintf("k%d", i), insertStmt(int64(i), pad))
	}
	forms, mf := segForms(t, fs, "t1")
	if forms.zst == 0 || forms.raw != 0 {
		t.Fatalf("segments: %+v, want all compressed", forms)
	}
	if !isZstd(baseObject(t, fs, "t1")) {
		t.Fatal("base not compressed")
	}
	if !zstdGen(mf.Format) {
		t.Fatalf("manifest format %d", mf.Format)
	}
	if met.SegStoredBytes.Load() >= met.SegBytes.Load() || met.SegStoredBytes.Load() == 0 {
		t.Fatalf("segment bytes raw=%d stored=%d", met.SegBytes.Load(), met.SegStoredBytes.Load())
	}
	if met.BaseStoredBytes.Load() >= met.BaseBytes.Load() || met.BaseStoredBytes.Load() == 0 {
		t.Fatalf("base bytes raw=%d stored=%d", met.BaseBytes.Load(), met.BaseStoredBytes.Load())
	}
	crashTenant(t, m, "t1")
	waitLeaseExpiry()
	m2 := newTestManager(t, fs, "b")
	if n := countRows(t, m2, "t1"); n != 10 {
		t.Fatalf("after recovery: %d rows", n)
	}
	res := mustQuery(t, m2, "t1", `SELECT v FROM kv WHERE k = 7`)
	if res.Rows[0][0] != pad {
		t.Fatal("payload changed in recovery")
	}
	// the temp file of the compressed base never outlives the checkpoint
	matches, _ := filepath.Glob(filepath.Join(m2.baseDir, "t", "t1", "base-*.zst"))
	if len(matches) != 0 {
		t.Fatalf("leftover temp files: %v", matches)
	}
}

// TestGenFormatSticksAcrossFlagFlip: the format belongs to the
// generation. A flag flip mid-generation does not change the form of the
// next segments; only the next checkpoint follows the new flag. Recovery
// chains the zstd segments on RAW lengths (their stored sizes do not
// chain).
func TestGenFormatSticksAcrossFlagFlip(t *testing.T) {
	ctx := context.Background()
	fs := NewFakeStore()
	m, _ := newTestManagerOpts(t, fs, "a", func(o *TenantOpts) {
		o.CheckpointByte = 64 << 20 // keep every segment in one gen
		o.MaxSegments = 1000
	})
	mustCreate(t, m, "t1")
	makeSchema(t, m, "t1")
	pad := strings.Repeat("x", 3000)
	for i := 0; i < 4; i++ {
		mustTx(t, m, "t1", "c", fmt.Sprintf("a%d", i), insertStmt(int64(i), pad))
	}
	setCompress(t, m, "t1", false) // mid-gen: no effect until the next checkpoint
	for i := 4; i < 8; i++ {
		mustTx(t, m, "t1", "c", fmt.Sprintf("b%d", i), insertStmt(int64(i), pad))
	}
	forms, mf := segForms(t, fs, "t1")
	if forms.raw != 0 || forms.zst < 8 || !zstdGen(mf.Format) {
		t.Fatalf("zstd gen after flip off: %+v format %d, want all zstd", forms, mf.Format)
	}
	segs, _ := fs.ListSegments(ctx, "t1", mf.Instance, mf.Gen)
	chained := true
	for i := 1; i < len(segs); i++ {
		if segs[i-1].Offset+segs[i-1].Size != segs[i].Offset {
			chained = false
		}
	}
	if chained {
		t.Fatal("stored sizes still chain; test does not exercise raw-length contiguity")
	}
	crashTenant(t, m, "t1")
	waitLeaseExpiry()

	// the other direction: a raw gen stays raw after a flip on
	m2, _ := newTestManagerOpts(t, fs, "b", func(o *TenantOpts) {
		o.Compress = false
		o.CheckpointByte = 64 << 20
		o.MaxSegments = 1000
	})
	if n := countRows(t, m2, "t1"); n != 8 {
		t.Fatalf("recovery of the zstd gen: %d rows, want 8", n)
	}
	if mf, _, _ := fs.GetManifest(ctx, "t1"); mf.Format != manifestFormatRaw || isZstd(baseObject(t, fs, "t1")) {
		t.Fatalf("claim with compression off: format %d, want a raw gen", mf.Format)
	}
	setCompress(t, m2, "t1", true)
	for i := 8; i < 12; i++ {
		mustTx(t, m2, "t1", "c", fmt.Sprintf("c%d", i), insertStmt(int64(i), pad))
	}
	forms, mf = segForms(t, fs, "t1")
	if forms.zst != 0 || forms.raw < 4 || mf.Format != manifestFormatRaw {
		t.Fatalf("raw gen after flip on: %+v format %d, want all raw", forms, mf.Format)
	}
	// the next checkpoint follows the flag: a full zstd gen
	if err := m2.Release("t1", ""); err != nil {
		t.Fatal(err)
	}
	if mf, _, _ := fs.GetManifest(ctx, "t1"); !zstdGen(mf.Format) || !isZstd(baseObject(t, fs, "t1")) {
		t.Fatalf("checkpoint after flip on: format %d, want a zstd gen", mf.Format)
	}
	m3 := newTestManager(t, fs, "c")
	if n := countRows(t, m3, "t1"); n != 12 {
		t.Fatalf("after reload: %d rows, want 12", n)
	}
}

// TestLegacyRawGenerationRecovers: a generation an old build wrote (raw
// base, raw segments, no manifest format) recovers on a new pod, whose
// claim then publishes a zstd generation.
func TestLegacyRawGenerationRecovers(t *testing.T) {
	ctx := context.Background()
	fs := NewFakeStore()
	old, _ := newTestManagerOpts(t, fs, "old", func(o *TenantOpts) {
		o.Compress = false
		o.CheckpointByte = 64 << 20
		o.MaxSegments = 1000
	})
	mustCreate(t, old, "t1")
	makeSchema(t, old, "t1")
	for i := 0; i < 5; i++ {
		mustTx(t, old, "t1", "c", fmt.Sprintf("o%d", i), insertStmt(int64(i), strings.Repeat("o", 2000)))
	}
	forms, mf := segForms(t, fs, "t1")
	if forms.zst != 0 || forms.raw == 0 || mf.Format != manifestFormatRaw {
		t.Fatalf("old gen: %+v format %d", forms, mf.Format)
	}
	if b := baseObject(t, fs, "t1"); !bytes.HasPrefix(b, []byte("SQLite format 3\x00")) {
		t.Fatal("old base should be a raw SQLite file")
	}
	// the JSON of an old manifest has no format field at all
	raw, _ := json.Marshal(mf)
	if strings.Contains(string(raw), "format") {
		t.Fatalf("raw gen manifest carries a format field: %s", raw)
	}
	crashTenant(t, old, "t1")
	waitLeaseExpiry()
	m := newTestManager(t, fs, "new")
	if n := countRows(t, m, "t1"); n != 5 {
		t.Fatalf("after recovery: %d rows, want 5", n)
	}
	if mf, _, _ := fs.GetManifest(ctx, "t1"); !zstdGen(mf.Format) || !isZstd(baseObject(t, fs, "t1")) {
		t.Fatal("new owner's claim should publish a zstd gen")
	}
}

// TestIncompressibleSegmentsStayZstd: random payloads do not compress, yet
// a zstd generation stores them as frames: never raw.
func TestIncompressibleSegmentsStayZstd(t *testing.T) {
	fs := NewFakeStore()
	m, met := newTestManagerOpts(t, fs, "a", func(o *TenantOpts) {
		o.CheckpointByte = 64 << 20
		o.MaxSegments = 1000
	})
	mustCreate(t, m, "t1")
	mustTx(t, m, "t1", "c", "schema", Statement{SQL: `CREATE TABLE r(k INTEGER PRIMARY KEY, v BLOB)`})
	for i := 0; i < 4; i++ {
		mustTx(t, m, "t1", "c", fmt.Sprintf("r%d", i), Statement{SQL: `INSERT INTO r VALUES (?, randomblob(200000))`, Args: []any{i}})
	}
	forms, mf := segForms(t, fs, "t1")
	if forms.raw != 0 || forms.zst == 0 || !zstdGen(mf.Format) {
		t.Fatalf("segments %+v format %d, want all zstd", forms, mf.Format)
	}
	if met.SegStoredBytes.Load() < met.SegBytes.Load()/2 {
		t.Fatalf("payload was not incompressible: raw %d stored %d", met.SegBytes.Load(), met.SegStoredBytes.Load())
	}
	sum := mustQuery(t, m, "t1", `SELECT SUM(LENGTH(v)), hex(substr(v, 1000, 16)) FROM r WHERE k = 2`)
	crashTenant(t, m, "t1")
	waitLeaseExpiry()
	m2 := newTestManager(t, fs, "b")
	got := mustQuery(t, m2, "t1", `SELECT SUM(LENGTH(v)), hex(substr(v, 1000, 16)) FROM r WHERE k = 2`)
	if fmt.Sprint(got.Rows) != fmt.Sprint(sum.Rows) {
		t.Fatalf("after recovery: %v, want %v", got.Rows, sum.Rows)
	}
	if n := mustQuery(t, m2, "t1", `SELECT COUNT(*) FROM r`); n.Rows[0][0] != int64(4) {
		t.Fatalf("rows: %v", n.Rows)
	}
}

// corruptStored rewrites one stored object with a mutated copy.
func corruptStored(t *testing.T, fs *FakeStore, key string, mut func([]byte) []byte) {
	t.Helper()
	b, err := fs.GetObject(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	if err := fs.PutSegment(context.Background(), key, mut(b)); err != nil {
		t.Fatal(err)
	}
}

// TestCorruptCompressedSegmentRefuses: a compressed segment that does not
// decode is treated exactly like a broken checksum chain today: the claim
// is refused loudly and the manifest does not move.
func TestCorruptCompressedSegmentRefuses(t *testing.T) {
	for name, mut := range map[string]func([]byte) []byte{
		"bitflip":  func(b []byte) []byte { c := append([]byte{}, b...); c[len(c)/2] ^= 0xFF; return c },
		"truncate": func(b []byte) []byte { return b[:len(b)-8] },
		// a raw segment in a zstd generation is corruption, not a legacy
		// object: the generation format decides, never the content
		"raw": func(b []byte) []byte {
			d, err := decodeBlob(b, maxSegmentRawBytes)
			if err != nil {
				panic(err)
			}
			return d
		},
	} {
		t.Run(name, func(t *testing.T) {
			fs := NewFakeStore()
			m := newTestManager(t, fs, "a")
			mustCreate(t, m, "t1")
			makeSchema(t, m, "t1")
			for i := 1; i <= 3; i++ {
				mustTx(t, m, "t1", "c", fmt.Sprintf("a%d", i), insertStmt(int64(i), strings.Repeat("v", 500)))
			}
			mf, _, _ := fs.GetManifest(context.Background(), "t1")
			segs, _ := fs.ListSegments(context.Background(), "t1", mf.Instance, mf.Gen)
			if len(segs) < 2 {
				t.Fatalf("need >=2 segments, got %d", len(segs))
			}
			victim := segs[1].Key
			if b, _ := fs.GetObject(context.Background(), victim); !isZstd(b) {
				t.Fatal("victim segment not compressed")
			}
			crashTenant(t, m, "t1")
			corruptStored(t, fs, victim, mut)
			waitLeaseExpiry()
			before, _, _ := fs.GetManifest(context.Background(), "t1")
			m2 := newTestManager(t, fs, "b")
			_, err := m2.Acquire(context.Background(), "t1", false)
			if err == nil || !strings.Contains(err.Error(), "replay incomplete") || !errors.Is(err, ErrCorruptObject) {
				t.Fatalf("want loud replay-incomplete refusal, got %v", err)
			}
			after, _, _ := fs.GetManifest(context.Background(), "t1")
			if after.Epoch != before.Epoch || after.Gen != before.Gen || after.BaseKey != before.BaseKey {
				t.Fatalf("manifest moved after refused claim: %+v -> %+v", before, after)
			}
			if _, err := fs.GetObject(context.Background(), victim); err != nil {
				t.Fatal("refused claim must not delete history")
			}
		})
	}
}

// TestCorruptCompressedBaseRefuses: a base that does not decode stops the
// claim; nothing is published over it.
func TestCorruptCompressedBaseRefuses(t *testing.T) {
	fs := NewFakeStore()
	m := newTestManager(t, fs, "a")
	mustCreate(t, m, "t1")
	makeSchema(t, m, "t1")
	mustTx(t, m, "t1", "c", "a", insertStmt(1, "v"))
	if err := m.Release("t1", ""); err != nil {
		t.Fatal(err)
	}
	before, _, _ := fs.GetManifest(context.Background(), "t1")
	b := baseObject(t, fs, "t1")
	bad := append([]byte{}, b...)
	bad[len(bad)/2] ^= 0xFF
	fs.PutBase(context.Background(), before.BaseKey, bytes.NewReader(bad), int64(len(bad)))
	m2 := newTestManager(t, fs, "b")
	_, err := m2.Acquire(context.Background(), "t1", false)
	if !errors.Is(err, ErrCorruptObject) {
		t.Fatalf("want ErrCorruptObject, got %v", err)
	}
	after, _, _ := fs.GetManifest(context.Background(), "t1")
	if after.Epoch != before.Epoch || after.BaseKey != before.BaseKey {
		t.Fatal("manifest moved after refused claim")
	}
}

// TestCheckpointReloadCompressed: several checkpoints write compressed
// bases; a fresh pod reloads the latest one.
func TestCheckpointReloadCompressed(t *testing.T) {
	fs := NewFakeStore()
	m := newTestManager(t, fs, "a")
	mustCreate(t, m, "t1")
	makeSchema(t, m, "t1")
	big := strings.Repeat("y", 4000)
	for i := 0; i < 40; i++ {
		mustTx(t, m, "t1", "c", fmt.Sprintf("c%d", i), insertStmt(int64(i), big))
	}
	mf, _, _ := fs.GetManifest(context.Background(), "t1")
	if mf.Gen < 3 {
		t.Fatalf("expected checkpoints, gen=%d", mf.Gen)
	}
	if !isZstd(baseObject(t, fs, "t1")) {
		t.Fatal("checkpoint base not compressed")
	}
	if err := m.Release("t1", ""); err != nil {
		t.Fatal(err)
	}
	m2 := newTestManager(t, fs, "b")
	if n := countRows(t, m2, "t1"); n != 40 {
		t.Fatalf("after reload: %d rows", n)
	}
	if res := mustQuery(t, m2, "t1", `SELECT SUM(LENGTH(v)) FROM kv`); res.Rows[0][0] != int64(40*4000) {
		t.Fatalf("payload bytes: %v", res.Rows[0][0])
	}
}

// TestBaseCompressFailureFallsBackRaw: when the temp file for the
// compressed base cannot be created, the checkpoint publishes a RAW
// generation: raw base, Format 0, and raw segments after it. Never a raw
// base in a zstd generation.
func TestBaseCompressFailureFallsBackRaw(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	fs := NewFakeStore()
	m, _ := newTestManagerOpts(t, fs, "a", func(o *TenantOpts) {
		o.CheckpointByte = 64 << 20
		o.MaxSegments = 1000
	})
	mustCreate(t, m, "t1")
	makeSchema(t, m, "t1")
	mustTx(t, m, "t1", "c", "a", insertStmt(1, "v"))
	ten, err := m.Acquire(context.Background(), "t1", false)
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(m.baseDir, "t", "t1")
	if err := os.Chmod(dir, 0o555); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(dir, 0o755)
	ok := make(chan bool, 1)
	ten.ctlCh <- func() { ok <- ten.checkpointLocked("") }
	if !<-ok {
		t.Fatalf("checkpoint failed: %v", ten.deadErr)
	}
	os.Chmod(dir, 0o755)
	if b := baseObject(t, fs, "t1"); !bytes.HasPrefix(b, []byte("SQLite format 3\x00")) {
		t.Fatal("fallback base should be raw")
	}
	for i := 2; i < 5; i++ {
		mustTx(t, m, "t1", "c", fmt.Sprintf("k%d", i), insertStmt(int64(i), strings.Repeat("v", 500)))
	}
	forms, mf := segForms(t, fs, "t1")
	if mf.Format != manifestFormatRaw || forms.zst != 0 || forms.raw == 0 {
		t.Fatalf("fallback gen: format %d segments %+v, want a raw gen", mf.Format, forms)
	}
	crashTenant(t, m, "t1")
	waitLeaseExpiry()
	m2 := newTestManager(t, fs, "b")
	if n := countRows(t, m2, "t1"); n != 4 {
		t.Fatalf("after reload: %d rows", n)
	}
}

// TestOversizedSegmentRefusedBeforeAck: a batch above the shared segment
// limit cannot go into a zstd generation. The tenant fails before it acks
// the batch, and stores nothing a reader would refuse; acked history
// survives.
func TestOversizedSegmentRefusedBeforeAck(t *testing.T) {
	defer func(v int64) { maxSegmentRawBytes = v }(maxSegmentRawBytes)
	maxSegmentRawBytes = 1 << 20
	fs := NewFakeStore()
	m, _ := newTestManagerOpts(t, fs, "a", func(o *TenantOpts) {
		o.CheckpointByte = 64 << 20
		o.MaxSegments = 1000
	})
	mustCreate(t, m, "t1")
	makeSchema(t, m, "t1")
	mustTx(t, m, "t1", "c", "small", insertStmt(1, "kept"))
	ten, err := m.Acquire(context.Background(), "t1", false)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = ten.SubmitTx(TxRequest{Client: "c", TxID: "big", Stmts: []Statement{
		{SQL: `INSERT INTO kv VALUES (2, hex(randomblob(1048576)))`}}})
	if !errors.Is(err, errSegmentTooLarge) {
		t.Fatalf("want errSegmentTooLarge before ack, got %v", err)
	}
	mf, _, _ := fs.GetManifest(context.Background(), "t1")
	segs, _ := fs.ListSegments(context.Background(), "t1", mf.Instance, mf.Gen)
	for _, sg := range segs {
		b, _ := fs.GetObject(context.Background(), sg.Key)
		if _, err := decodeBlob(b, maxSegmentRawBytes); err != nil {
			t.Fatalf("stored segment %s is unreadable: %v", sg.Key, err)
		}
	}
	crashTenant(t, m, "t1")
	waitLeaseExpiry()
	m2 := newTestManager(t, fs, "b")
	if n := countRows(t, m2, "t1"); n != 1 {
		t.Fatalf("after recovery: %d rows, want only the acked one", n)
	}
}

// TestNewerManifestFormatRefused: a manifest from a newer build is never
// claimed or swept by this build.
func TestNewerManifestFormatRefused(t *testing.T) {
	fs := NewFakeStore()
	m := newTestManager(t, fs, "a")
	mustCreate(t, m, "t1")
	makeSchema(t, m, "t1")
	if err := m.Release("t1", ""); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	mf, etag, _ := fs.GetManifest(ctx, "t1")
	mf.Format = maxManifestFormat + 1
	if _, err := fs.PutManifestCAS(ctx, "t1", mf, etag); err != nil {
		t.Fatal(err)
	}
	// an orphan the sweeper would normally delete
	orphan := segKey("fake", "t1", "deaddead", 1, 1, 0)
	fs.PutSegment(ctx, orphan, []byte("x"))

	m2 := newTestManager(t, fs, "b")
	_, err := m2.Acquire(ctx, "t1", false)
	if !errors.Is(err, ErrUnsupportedFormat) {
		t.Fatalf("want ErrUnsupportedFormat, got %v", err)
	}
	if after, _, _ := fs.GetManifest(ctx, "t1"); after.Epoch != mf.Epoch || after.Owner != "" {
		t.Fatalf("manifest moved: %+v", after)
	}
	sw := NewSweeper(m2, fs, "fake", testOpts(), &Metrics{})
	sw.sweepTenant(ctx, "t1")
	if _, err := fs.GetObject(ctx, orphan); err != nil {
		t.Fatal("sweeper touched a newer-format tenant")
	}
}

// newLegacyManager emulates a pod of the build before compression (e1a0fc8)
// as faithfully as a test in this tree can. Its materialize reads every
// object raw and ignores Manifest.Format: that is the old code, line for
// line (the raw branch of materializeLocked). It writes raw, as the old
// build does. Everything after materialize (openTenant, the claim
// checkpoint, the manifest CAS, old-generation deletion) is the unchanged
// old code. The format gate in Acquire passes for format 1, so it does
// not help here either.
func newLegacyManager(t *testing.T, fs *FakeStore, id string) *Manager {
	m, _ := newTestManagerOpts(t, fs, id, rawOpts)
	m.legacyReadForTest = true
	return m
}

// newS1Manager emulates the build with compression but without page
// deltas: it reads formats 0 and 1 and writes format 1.
func newS1Manager(t *testing.T, fs *FakeStore, id string) *Manager {
	m, _ := newTestManagerOpts(t, fs, id, func(o *TenantOpts) { o.DeltaMaxChain = 0 })
	m.s1ReadForTest = true
	return m
}

// settledObjects waits until the object set has not changed for 300 ms
// (async deletes after a checkpoint) and returns it.
func settledObjects(fs *FakeStore) map[string]string {
	prev := snapshotObjects(fs)
	for stable, i := 0, 0; stable < 3 && i < 100; i++ {
		time.Sleep(100 * time.Millisecond)
		cur := snapshotObjects(fs)
		if len(cur) == len(prev) {
			stable++
		} else {
			stable = 0
		}
		prev = cur
	}
	return prev
}

func snapshotObjects(fs *FakeStore) map[string]string {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	out := make(map[string]string, len(fs.objects))
	for k, v := range fs.objects {
		out[k] = string(v)
	}
	return out
}

// TestLegacyEmulationReadsRawGen: the legacy emulation is not a strawman.
// It takes over a raw generation, as the old build does.
func TestLegacyEmulationReadsRawGen(t *testing.T) {
	fs := NewFakeStore()
	m, _ := newTestManagerOpts(t, fs, "a", rawOpts)
	mustCreate(t, m, "t1")
	makeSchema(t, m, "t1")
	mustTx(t, m, "t1", "c", "k", insertStmt(1, "v"))
	crashTenant(t, m, "t1")
	waitLeaseExpiry()
	old := newLegacyManager(t, fs, "old")
	if n := countRows(t, old, "t1"); n != 1 {
		t.Fatalf("legacy emulation on a raw gen: %d rows", n)
	}
}

// gateStore counts concurrent base downloads and can hold them.
type gateStore struct {
	*FakeStore
	mu        sync.Mutex
	cur, peak int
	hold      chan struct{}
}

func (g *gateStore) GetObjectTo(ctx context.Context, key string, w io.Writer) error {
	if strings.Contains(key, "/base/") || strings.Contains(key, ".db") || strings.HasSuffix(key, ".base") {
		g.mu.Lock()
		g.cur++
		g.peak = max(g.peak, g.cur)
		g.mu.Unlock()
		defer func() { g.mu.Lock(); g.cur--; g.mu.Unlock() }()
		select {
		case <-g.hold:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return g.FakeStore.GetObjectTo(ctx, key, w)
}

// TestMaterializeDecodeLimit: decoding materializations are bounded
// pod-wide, the wait obeys the caller's context, a raw generation does
// not take a slot, and the slots never deadlock with the claim lanes
// (more tenants than slots, all taken over at once).
func TestMaterializeDecodeLimit(t *testing.T) {
	ctx := context.Background()
	fs := NewFakeStore()
	src := newTestManager(t, fs, "src")
	const n = 6
	for i := 0; i < n; i++ {
		tid := fmt.Sprintf("t%d", i)
		mustCreate(t, src, tid)
		makeSchema(t, src, tid)
		mustTx(t, src, tid, "c", "k", insertStmt(1, "v"))
		if err := src.Release(tid, ""); err != nil {
			t.Fatal(err)
		}
	}
	rawSrc, _ := newTestManagerOpts(t, fs, "rawsrc", rawOpts)
	mustCreate(t, rawSrc, "raw")
	if err := rawSrc.Release("raw", ""); err != nil {
		t.Fatal(err)
	}
	if mf, _, _ := fs.GetManifest(ctx, "raw"); mf.Format != manifestFormatRaw {
		t.Fatal("want a raw gen")
	}

	gs := &gateStore{FakeStore: fs, hold: make(chan struct{})}
	close(gs.hold) // downloads pass for now
	m := NewManager(gs, "fake", "dst", "standard", t.TempDir(), testOpts(), 1000, &Metrics{}, ControllerOpts{})
	m.decodeSem = make(chan struct{}, 2)

	// all slots busy: a zstd takeover waits, and gives up with its context
	m.decodeSem <- struct{}{}
	m.decodeSem <- struct{}{}
	cctx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	_, err := m.Acquire(cctx, "t0", false)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, errDecodeSlotWait) {
		t.Fatalf("want the context error while slots are busy, got %v", err)
	}
	m.mu.Lock()
	_, cooled := m.poisonedAt["t0"]
	m.mu.Unlock()
	if cooled {
		t.Fatal("a slot wait must not start a claim cooldown")
	}
	if mf, _, _ := fs.GetManifest(ctx, "t0"); mf.Owner != "" {
		t.Fatalf("manifest moved: owner %q", mf.Owner)
	}
	// a raw gen needs no decoder, so no slot
	if _, err := m.Acquire(ctx, "raw", false); err != nil {
		t.Fatalf("raw gen with all slots busy: %v", err)
	}
	<-m.decodeSem
	<-m.decodeSem

	// more takeovers than slots, held inside the download: at most two
	// decode at once, and all finish (no deadlock with the claim lanes)
	gs.hold = make(chan struct{})
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(tid string) {
			defer wg.Done()
			if _, err := m.Acquire(ctx, tid, false); err != nil {
				errs <- fmt.Errorf("%s: %w", tid, err)
			}
		}(fmt.Sprintf("t%d", i))
	}
	time.Sleep(300 * time.Millisecond)
	gs.mu.Lock()
	cur := gs.cur
	gs.mu.Unlock()
	if cur != 2 {
		t.Fatalf("%d downloads in flight, want exactly the 2 slots", cur)
	}
	close(gs.hold)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if gs.peak > 2 {
		t.Fatalf("peak %d concurrent decoding materializations, limit 2", gs.peak)
	}
	for i := 0; i < n; i++ {
		if c := countRows(t, m, fmt.Sprintf("t%d", i)); c != 1 {
			t.Fatalf("t%d: %d rows", i, c)
		}
	}
}
