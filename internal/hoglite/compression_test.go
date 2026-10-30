package hoglite

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
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
	if mf.Format != manifestFormatZstd {
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

// TestMixedRawAndCompressedSegmentsRecovery: one generation holds raw
// segments (old build) followed by compressed ones. Recovery must chain
// them on RAW offsets: a compressed segment's stored size is smaller than
// the WAL range its key names.
func TestMixedRawAndCompressedSegmentsRecovery(t *testing.T) {
	fs := NewFakeStore()
	m, _ := newTestManagerOpts(t, fs, "a", func(o *TenantOpts) {
		o.Compress = false
		o.CheckpointByte = 64 << 20 // keep every segment in one gen
		o.MaxSegments = 1000
	})
	mustCreate(t, m, "t1")
	makeSchema(t, m, "t1")
	pad := strings.Repeat("x", 3000)
	for i := 0; i < 4; i++ {
		mustTx(t, m, "t1", "c", fmt.Sprintf("r%d", i), insertStmt(int64(i), pad))
	}
	setCompress(t, m, "t1", true)
	for i := 4; i < 8; i++ {
		mustTx(t, m, "t1", "c", fmt.Sprintf("z%d", i), insertStmt(int64(i), pad))
	}
	forms, mf := segForms(t, fs, "t1")
	if forms.raw == 0 || forms.zst == 0 {
		t.Fatalf("want mixed segments, got %+v", forms)
	}
	// stored sizes no longer chain: proves recovery cannot use them
	segs, _ := fs.ListSegments(context.Background(), "t1", mf.Instance, mf.Gen)
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
	m2 := newTestManager(t, fs, "b")
	if n := countRows(t, m2, "t1"); n != 8 {
		t.Fatalf("after recovery: %d rows, want 8", n)
	}
}

// TestOldRawBaseWithCompressedSegments: a tenant last checkpointed by an
// old build (raw base, no manifest format) gets compressed segments from a
// new build and recovers on a third pod.
func TestOldRawBaseWithCompressedSegments(t *testing.T) {
	fs := NewFakeStore()
	old, _ := newTestManagerOpts(t, fs, "old", rawOpts)
	mustCreate(t, old, "t1")
	makeSchema(t, old, "t1")
	mustTx(t, old, "t1", "c", "o1", insertStmt(1, "from-old"))
	if err := old.Release("t1", ""); err != nil {
		t.Fatal(err)
	}
	if b := baseObject(t, fs, "t1"); !bytes.HasPrefix(b, []byte("SQLite format 3\x00")) {
		t.Fatal("old base should be a raw SQLite file")
	}
	if mf, _, _ := fs.GetManifest(context.Background(), "t1"); mf.Format != manifestFormatRaw {
		t.Fatalf("old manifest format %d", mf.Format)
	}
	// a compressing pod's claim checkpoint would rewrite the base
	// compressed. So the claim runs with compression off and the codec
	// flips afterwards: a raw base under compressed segments, the state a
	// mixed deploy produces.
	m, _ := newTestManagerOpts(t, fs, "mixed", func(o *TenantOpts) {
		o.Compress = false
		o.CheckpointByte = 64 << 20
		o.MaxSegments = 1000
	})
	mustTx(t, m, "t1", "c", "o2", insertStmt(2, "claim")) // claim: raw base
	setCompress(t, m, "t1", true)
	for i := 3; i < 7; i++ {
		mustTx(t, m, "t1", "c", fmt.Sprintf("n%d", i), insertStmt(int64(i), strings.Repeat("n", 2000)))
	}
	if b := baseObject(t, fs, "t1"); !bytes.HasPrefix(b, []byte("SQLite format 3\x00")) {
		t.Fatal("base should still be raw")
	}
	if forms, _ := segForms(t, fs, "t1"); forms.zst == 0 {
		t.Fatalf("want compressed segments over the raw base, got %+v", forms)
	}
	crashTenant(t, m, "t1")
	waitLeaseExpiry()
	m2 := newTestManager(t, fs, "new")
	if n := countRows(t, m2, "t1"); n != 6 {
		t.Fatalf("after recovery: %d rows, want 6", n)
	}
	// the new owner's claim rewrote the base compressed
	if !isZstd(baseObject(t, fs, "t1")) {
		t.Fatal("new owner's base should be compressed")
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
// compressed base cannot be created, the checkpoint uploads the raw file
// instead of failing. Raw is always a valid base.
func TestBaseCompressFailureFallsBackRaw(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	fs := NewFakeStore()
	m := newTestManager(t, fs, "a")
	mustCreate(t, m, "t1")
	makeSchema(t, m, "t1")
	mustTx(t, m, "t1", "c", "a", insertStmt(1, "v"))
	dir := filepath.Join(m.baseDir, "t", "t1")
	if err := os.Chmod(dir, 0o555); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(dir, 0o755)
	if err := m.Release("t1", ""); err != nil {
		t.Fatal(err)
	}
	os.Chmod(dir, 0o755)
	if b := baseObject(t, fs, "t1"); !bytes.HasPrefix(b, []byte("SQLite format 3\x00")) {
		t.Fatal("fallback base should be raw")
	}
	m2 := newTestManager(t, fs, "b")
	if n := countRows(t, m2, "t1"); n != 1 {
		t.Fatalf("after reload: %d rows", n)
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

// oldBuildMaterialize is the materialize logic of the build before S1,
// copied verbatim in behavior: raw base copy, raw segment bytes, and
// contiguity on stored lengths. It shows what an OLD pod does with a
// tenant written by a NEW pod during a rolling deploy.
func oldBuildMaterialize(ctx context.Context, fs *FakeStore, tid string, mf *Manifest, dir string) error {
	base, err := os.Create(filepath.Join(dir, "db.sqlite"))
	if err != nil {
		return err
	}
	if err := fs.GetObjectTo(ctx, mf.BaseKey, base); err != nil {
		base.Close()
		return err
	}
	base.Close()
	segs, _ := fs.ListSegments(ctx, tid, mf.Instance, mf.Gen)
	var wal []byte
	for _, s := range segs {
		if s.Offset > int64(len(wal)) {
			break
		}
		if s.Offset < int64(len(wal)) {
			continue
		}
		data, err := fs.GetObject(ctx, s.Key)
		if err != nil {
			return err
		}
		wal = append(wal, data...)
	}
	if len(wal) == 0 {
		return nil
	}
	if err := os.WriteFile(filepath.Join(dir, "db.sqlite-wal"), wal, 0o644); err != nil {
		return err
	}
	if end := walCommitEnd(wal); end != int64(len(wal)) {
		return fmt.Errorf("wal replay incomplete: parsed %d of %d bytes (corruption in stored segments)", end, len(wal))
	}
	return nil
}

// TestOldBuildRefusesCompressedTenant documents the rolling-deploy window:
// an old pod that must take over a tenant written by a new pod fails
// before its claim CAS, in both shapes the tenant can have. It never
// publishes a base, so no history is lost; the tenant waits for a new pod.
func TestOldBuildRefusesCompressedTenant(t *testing.T) {
	ctx := context.Background()
	fs := NewFakeStore()
	m := newTestManager(t, fs, "new")
	mustCreate(t, m, "t1")
	makeSchema(t, m, "t1")
	for i := 0; i < 3; i++ {
		mustTx(t, m, "t1", "c", fmt.Sprintf("k%d", i), insertStmt(int64(i), strings.Repeat("q", 1000)))
	}
	mf, _, _ := fs.GetManifest(ctx, "t1")

	// shape 1: compressed base + compressed segments. The WAL replay check
	// fails at byte 0 (zstd magic is not a WAL header).
	dir := t.TempDir()
	err := oldBuildMaterialize(ctx, fs, "t1", mf, dir)
	if err == nil || !strings.Contains(err.Error(), "replay incomplete") {
		t.Fatalf("old build with segments: want replay-incomplete refusal, got %v", err)
	}

	// shape 2: compressed base, no segments (right after a checkpoint or
	// release). The old build's materialize succeeds, but SQLite refuses the
	// file, so openTenant fails before any claim.
	if err := m.Release("t1", ""); err != nil {
		t.Fatal(err)
	}
	mf, _, _ = fs.GetManifest(ctx, "t1")
	if segs, _ := fs.ListSegments(ctx, "t1", mf.Instance, mf.Gen); len(segs) != 0 {
		t.Fatalf("expected no segments after release, got %d", len(segs))
	}
	dir2 := t.TempDir()
	if err := oldBuildMaterialize(ctx, fs, "t1", mf, dir2); err != nil {
		t.Fatalf("old materialize: %v", err)
	}
	ten, err := openTenant("t1", dir2, mf, "", fs, "fake", "old", "old", testOpts(), &Metrics{}, nil)
	if err == nil {
		ten.Crash()
		t.Fatal("old build opened a compressed base as a database")
	}
	if !strings.Contains(err.Error(), "not a database") {
		t.Fatalf("want SQLite not-a-database error, got %v", err)
	}
	// the manifest is untouched, and a new pod still serves the tenant
	if after, _, _ := fs.GetManifest(ctx, "t1"); after.Epoch != mf.Epoch || after.BaseKey != mf.BaseKey {
		t.Fatal("manifest moved")
	}
	m2 := newTestManager(t, fs, "new2")
	if n := countRows(t, m2, "t1"); n != 3 {
		t.Fatalf("new pod after old refusal: %d rows", n)
	}
}
