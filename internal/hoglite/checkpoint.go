package hoglite

import (
	"context"
	"fmt"
	"math"
	"os"
	"time"
)

// Checkpoint object selection: a page delta on top of the current chain,
// or a full base. See delta.go for the delta format and its argument.

// ckptUpload is the object one checkpoint uploads and the base and chain
// that the new manifest names.
type ckptUpload struct {
	key   string   // object to PUT; "" means none (the chain does not change)
	file  *os.File // stored bytes of the object
	size  int64    // stored size
	raw   int64    // raw size
	delta bool     // the object is a delta (else a full base)

	format   int
	baseKey  string
	baseSize int64
	deltas   []DeltaRef

	cleanup func()
}

func (t *Tenant) logf(format string, args ...any) {
	if t.metrics.Logger != nil {
		t.metrics.Logger.Printf(format, args...)
	}
}

// normalizeDeltaOpts clamps the delta options to values the writer and
// every reader of this build support: a chain of at most maxChainLen, no
// negative cap, and a ratio that is a finite number >= 0 (0: every
// checkpoint uploads a full base).
func normalizeDeltaOpts(o *TenantOpts) {
	if o.DeltaMaxChain < 0 {
		o.DeltaMaxChain = 0
	}
	if o.DeltaMaxChain > maxChainLen {
		o.DeltaMaxChain = maxChainLen
	}
	if math.IsNaN(o.DeltaMaxRatio) || o.DeltaMaxRatio < 0 {
		o.DeltaMaxRatio = 0
	}
	if math.IsInf(o.DeltaMaxRatio, 1) {
		o.DeltaMaxRatio = math.MaxFloat64
	}
}

// deltasOn reports whether this tenant writes chain generations.
func (t *Tenant) deltasOn() bool { return t.opts.Compress && t.opts.DeltaMaxChain > 0 }

// planDelta lists the pages of the committed WAL when this checkpoint may
// upload a delta. It returns nil when the checkpoint must upload a full
// base: deltas off, a create, a generation that is not a chain yet, a
// full chain, or any doubt about the local file. Runs before the fold.
func (t *Tenant) planDelta() *walDelta {
	if !t.deltasOn() || t.etag == "" || t.mf.Format != manifestFormatChain || t.mf.BaseSize <= 0 {
		return nil
	}
	// rebase at the configured cap, and never past what readers accept,
	// whatever the options say
	if len(t.mf.Deltas) >= min(t.opts.DeltaMaxChain, maxChainLen) {
		return nil // rebase
	}
	// SQLite changes the database file only when it folds the WAL. A
	// different size since the last checkpoint means something else wrote
	// it, and a delta from the WAL would miss that write.
	if fi, err := os.Stat(t.dbPath); err != nil || fi.Size() != t.dbBytes {
		t.logf("tenant %s: database file changed outside the WAL (full base)", t.ID)
		return nil
	}
	// the committed end, not submittedOff: commits whose cut the inflight
	// cap deferred are in the fold too, and the delta is their durability
	end, err := t.currentCommitEnd()
	if err != nil {
		t.logf("tenant %s: delta plan: %v (full base)", t.ID, err)
		return nil
	}
	plan, err := scanWALPages(t.wal, end, t.pageSize)
	if err != nil {
		t.logf("tenant %s: delta plan: %v (full base)", t.ID, err)
		return nil
	}
	return plan
}

// foldWAL copies the committed WAL into the database file and truncates
// the WAL. With a delta plan it first checks that SQLite folds exactly
// the frames the plan lists, and afterwards that the file has the size
// the last commit frame names. A failed check drops the plan (full base):
// the file is then still what SQLite wrote, only the delta is in doubt.
// It returns the database file size after the fold.
func (t *Tenant) foldWAL(ctx context.Context, plan *walDelta) (int64, *walDelta, error) {
	if plan != nil {
		// PASSIVE folds every frame (no reader holds a snapshot on the
		// actor) and reports SQLite's own frame count
		var busy, logN, ckptN int64
		if err := t.db.QueryRowContext(ctx, `PRAGMA wal_checkpoint(PASSIVE)`).Scan(&busy, &logN, &ckptN); err != nil {
			return 0, nil, err
		}
		if busy != 0 || logN != plan.frames || ckptN != plan.frames {
			t.logf("tenant %s: sqlite folded %d of %d frames (busy %d), parser found %d (full base)",
				t.ID, ckptN, logN, busy, plan.frames)
			plan = nil
		}
	}
	var busy, logN, ckptN int64
	if err := t.db.QueryRowContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`).Scan(&busy, &logN, &ckptN); err != nil {
		return 0, nil, err
	}
	// a blocked fold leaves WAL bytes behind, and the next gen's segment
	// offsets start at 0: publishing then would corrupt the tenant
	if busy != 0 {
		return 0, nil, fmt.Errorf("fold blocked (busy %d, %d of %d frames)", busy, ckptN, logN)
	}
	if fi, err := os.Stat(t.walPath); err == nil && fi.Size() != 0 {
		return 0, nil, fmt.Errorf("wal still holds %d bytes after the fold", fi.Size())
	}
	fi, err := os.Stat(t.dbPath)
	if err != nil {
		return 0, nil, err
	}
	if plan != nil {
		want := t.dbBytes
		if plan.frames > 0 {
			want = int64(plan.dbPages) * t.pageSize
		}
		if fi.Size() != want {
			t.logf("tenant %s: database is %d bytes after the fold, the WAL says %d (full base)", t.ID, fi.Size(), want)
			plan = nil
		}
	}
	return fi.Size(), plan, nil
}

// prepareCheckpointUpload picks the object for this checkpoint. With a
// plan it builds a delta, unless the chain would exceed DeltaMaxRatio.
// Otherwise it prepares a full base: in a chain gen (format 2, key under
// ckpt/, empty chain) when deltas are on and the base compressed, else in
// a format 0 or 1 gen exactly as before deltas existed. A chain gen
// never has a raw base: that keeps the refusal barrier for builds before
// compression (see delta.go).
func (t *Tenant) prepareCheckpointUpload(f *os.File, dbSize int64, plan *walDelta, newGen int64) *ckptUpload {
	if plan != nil {
		if up := t.prepareDelta(f, plan, newGen); up != nil {
			return up
		}
	}
	upload, size, format, cleanup := t.prepareBaseUpload(f, dbSize)
	up := &ckptUpload{file: upload, size: size, raw: dbSize, format: format, cleanup: cleanup}
	switch {
	case format == manifestFormatZstd && t.deltasOn():
		up.format = manifestFormatChain
		up.key = ckptKey(t.prefix, t.ID, t.mf.Instance, newGen, t.mf.Epoch, t.claimAttempt, "base")
		up.baseSize = size
	case t.claimAttempt != "":
		// acquisition claims upload to a per-attempt key: a racing acquirer
		// with the same gen+epoch can never overwrite our base, and vice
		// versa. The winner's CAS names its exact object.
		up.key = baseKeyAttempt(t.prefix, t.ID, t.mf.Instance, newGen, t.mf.Epoch, t.claimAttempt)
	default:
		up.key = baseKey(t.prefix, t.ID, t.mf.Instance, newGen, t.mf.Epoch)
	}
	up.baseKey = up.key
	return up
}

// prepareDelta writes the delta of plan to a temp file. It returns nil
// (full base) when the delta cannot be built or would push the chain past
// DeltaMaxRatio of the base.
func (t *Tenant) prepareDelta(f *os.File, plan *walDelta, newGen int64) *ckptUpload {
	up := &ckptUpload{
		delta: true, format: manifestFormatChain,
		baseKey: t.mf.BaseKey, baseSize: t.mf.BaseSize,
		deltas:  append([]DeltaRef(nil), t.mf.Deltas...),
		cleanup: func() {},
	}
	if plan.frames == 0 {
		// nothing committed since the last checkpoint, and foldWAL saw the
		// file size unchanged: the new gen starts from the same state, so
		// it names the same chain and no object is uploaded
		return up
	}
	tmp, err := os.CreateTemp(t.dir, "delta-*.zst")
	if err != nil {
		t.logf("tenant %s: delta: %v (full base)", t.ID, err)
		return nil
	}
	var done bool
	cleanup := func() {
		if !done {
			done = true
			tmp.Close()
			os.Remove(tmp.Name())
		}
	}
	h := deltaHeader{
		pageSize: uint32(t.pageSize), dbPages: plan.dbPages, index: uint32(len(t.mf.Deltas)),
		prevBytes: uint64(t.dbBytes), baseKey: t.mf.BaseKey,
	}
	raw, stored, err := writeDelta(tmp, f, h, plan.pages, func() { t.progressMs.Store(time.Now().UnixMilli()) })
	if err != nil {
		cleanup()
		t.logf("tenant %s: delta: %v (full base)", t.ID, err)
		return nil
	}
	var chain int64
	for _, d := range t.mf.Deltas {
		chain += d.Size
	}
	if float64(chain+stored) > t.opts.DeltaMaxRatio*float64(t.mf.BaseSize) {
		cleanup()
		return nil // rebase: recovery would download too much on top of the base
	}
	up.key = ckptKey(t.prefix, t.ID, t.mf.Instance, newGen, t.mf.Epoch, t.claimAttempt, "delta")
	up.file, up.size, up.raw, up.cleanup = tmp, stored, raw, cleanup
	up.deltas = append(up.deltas, DeltaRef{Key: up.key, Size: stored})
	return up
}

// obsoleteObjects lists the base and deltas of old that next no longer
// names. Delete them only after next is published.
func obsoleteObjects(old, next *Manifest) []string {
	keep := map[string]bool{}
	for _, k := range manifestObjects(next) {
		keep[k] = true
	}
	var out []string
	for _, k := range manifestObjects(old) {
		if !keep[k] {
			out = append(out, k)
		}
	}
	return out
}
