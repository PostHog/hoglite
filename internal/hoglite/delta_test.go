package hoglite

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Tests for page-delta checkpoints (manifest format 2).

// legacyKeyGen is keyGen of the builds before compression, verbatim. Their
// sweeper deletes every key of the live instance with 0 <= gen < mf.Gen.
func legacyKeyGen(key, instancePrefix string) int64 {
	if !strings.HasPrefix(key, instancePrefix) {
		return -1
	}
	rel := strings.TrimPrefix(key, instancePrefix)
	parts := strings.Split(rel, "/")
	if len(parts) < 2 {
		return -1
	}
	var g string
	switch parts[0] {
	case "base":
		g, _, _ = strings.Cut(parts[1], "-")
	case "wal":
		g = parts[1]
	default:
		return -1
	}
	n, err := strconv.ParseInt(g, 10, 64)
	if err != nil {
		return -1
	}
	return n
}

// legacySweepGarbage lists what the sweeper of a build before compression
// deletes in the live instance of mf.
func legacySweepGarbage(fs *FakeStore, mf *Manifest) []string {
	var tid string
	for _, k := range fs.ObjectKeys() {
		if i := strings.Index(k, "/i/"+mf.Instance+"/"); i >= 0 {
			tid = k[:i]
			break
		}
	}
	if tid == "" {
		return nil
	}
	live := tid + "/i/" + mf.Instance + "/"
	var out []string
	for _, k := range fs.ObjectKeys() {
		if g := legacyKeyGen(k, live); g >= 0 && g < mf.Gen {
			out = append(out, k)
		}
	}
	return out
}

// tlog sends manager logs to the test log.
type tlog struct{ t *testing.T }

func (l tlog) Printf(format string, args ...any) { l.t.Logf(format, args...) }

// onActorT runs f on the tenant's actor goroutine and waits for it.
func onActorT(t *testing.T, ten *Tenant, f func()) {
	t.Helper()
	done := make(chan struct{})
	ten.ctlCh <- func() { defer close(done); f() }
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("actor did not run the function")
	}
}

// checkpointFold checkpoints on the actor. It returns the local database
// file right after the fold and the manifest the checkpoint published.
func checkpointFold(t *testing.T, ten *Tenant) ([]byte, Manifest) {
	t.Helper()
	var (
		ok    bool
		local []byte
		mf    Manifest
		err   error
	)
	onActorT(t, ten, func() {
		ok = ten.checkpointLocked("")
		if ok {
			local, err = os.ReadFile(ten.dbPath)
			mf = *ten.mf
		}
	})
	if !ok || err != nil {
		t.Fatalf("checkpoint failed: ok %v err %v", ok, err)
	}
	return local, mf
}

// reconstruct builds the start state of mf's generation from the store:
// base, then every delta in order.
func reconstruct(t *testing.T, store Backend, mf *Manifest) []byte {
	t.Helper()
	return reconstructIn(t, store, mf, t.TempDir())
}

func reconstructIn(t *testing.T, store Backend, mf *Manifest, dir string) []byte {
	t.Helper()
	f, err := os.Create(filepath.Join(dir, "db"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	ctx := context.Background()
	if _, _, err := fetchObjectTo(ctx, store, mf.BaseKey, f, mf.Format, maxBaseRawBytes); err != nil {
		t.Fatalf("base: %v", err)
	}
	if err := applyChain(ctx, store, "", mf, dir, f); err != nil {
		t.Fatalf("chain: %v", err)
	}
	b, err := os.ReadFile(f.Name())
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// randomWork writes one random batch straight on the tenant connection
// (actor only). It covers inserts, updates, deletes, overflow pages,
// schema changes, VACUUM (the file shrinks), page reuse from the free
// list, and a rolled-back transaction that spills frames into the WAL.
func randomWork(ten *Tenant, rng *rand.Rand, round int) (err error) {
	exec := func(q string, args ...any) {
		if err != nil {
			return
		}
		if _, e := ten.db.Exec(q, args...); e != nil {
			err = fmt.Errorf("round %d: %s: %w", round, q, e)
		}
	}
	sizes := []int{8, 300, 3000, 20000, 150000}
	table := fmt.Sprintf("r%d", rng.Intn(4))
	exec(`CREATE TABLE IF NOT EXISTS ` + table + `(k INTEGER PRIMARY KEY, v BLOB, s TEXT)`)
	for n := rng.Intn(4) + 1; n > 0; n-- {
		switch op := rng.Intn(10); op {
		case 0, 1, 2:
			for i := rng.Intn(20) + 1; i > 0; i-- {
				exec(`INSERT OR REPLACE INTO `+table+` VALUES (?, randomblob(?), ?)`,
					rng.Intn(500), sizes[rng.Intn(len(sizes))], strings.Repeat("s", rng.Intn(200)))
			}
		case 3:
			exec(`UPDATE `+table+` SET v = zeroblob(?) WHERE k % 7 = ?`, sizes[rng.Intn(len(sizes))], rng.Intn(7))
		case 4:
			exec(`DELETE FROM `+table+` WHERE k % 3 = ?`, rng.Intn(3))
		case 5:
			idx := fmt.Sprintf("i_%s_%d", table, rng.Intn(2))
			if rng.Intn(2) == 0 {
				exec(`CREATE INDEX IF NOT EXISTS ` + idx + ` ON ` + table + `(s)`)
			} else {
				exec(`DROP INDEX IF EXISTS ` + idx)
			}
		case 6:
			if rng.Intn(3) == 0 {
				exec(`DROP TABLE IF EXISTS ` + table)
				exec(`CREATE TABLE IF NOT EXISTS ` + table + `(k INTEGER PRIMARY KEY, v BLOB, s TEXT)`)
			}
		case 7:
			exec(`VACUUM`)
		case 8:
			// a large transaction that rolls back: with a tiny cache SQLite
			// spills its pages into the WAL, and those frames come after
			// the last commit frame
			exec(`PRAGMA cache_size = 4`)
			tx, e := ten.db.Begin()
			if e != nil {
				return e
			}
			for i := 0; i < 30; i++ {
				if _, e := tx.Exec(`INSERT OR REPLACE INTO `+table+` VALUES (?, randomblob(5000), 'rb')`, 1000+i); e != nil {
					tx.Rollback()
					return e
				}
			}
			if e := tx.Rollback(); e != nil {
				return e
			}
			exec(`PRAGMA cache_size = -2000`)
		case 9:
			// nothing this time: a checkpoint with no new frames
		}
	}
	return err
}

// TestDeltaByteIdentity: after every checkpoint, the base plus the delta
// chain in the store is byte-identical to the local database file that
// SQLite folded. Many random workloads, with long chains and with
// frequent rebases. At the end a new pod recovers the tenant.
func TestDeltaByteIdentity(t *testing.T) {
	configs := []struct {
		name  string
		chain int
		ratio float64
	}{
		{"long-chain", 1000, 1000},
		{"rebase-by-count", 3, 1000},
		{"rebase-by-ratio", 1000, 0.3},
	}
	authorizerOffForTest.Store(true) // VACUUM and cache_size
	defer authorizerOffForTest.Store(false)
	seeds := 4
	rounds := 30
	if testing.Short() {
		seeds, rounds = 1, 10
	}
	for _, cfg := range configs {
		for seed := 1; seed <= seeds; seed++ {
			t.Run(fmt.Sprintf("%s/seed%d", cfg.name, seed), func(t *testing.T) {
				rng := rand.New(rand.NewSource(int64(seed)))
				fs := NewFakeStore()
				o := testOpts()
				o.CheckpointByte = 1 << 40
				o.MaxSegments = 1 << 30
				o.DeltaMaxChain = cfg.chain
				o.DeltaMaxRatio = cfg.ratio
				met := &Metrics{Logger: tlog{t}}
				m := NewManager(fs, "fake", "a", "standard", t.TempDir(), o, 1000, met, ControllerOpts{})
				mustCreate(t, m, "t1")
				ten := m.LocalTenant("t1")
				maxChain := 0
				for r := 0; r < rounds; r++ {
					var werr error
					onActorT(t, ten, func() { werr = randomWork(ten, rng, r) })
					if werr != nil {
						t.Fatal(werr)
					}
					local, mf := checkpointFold(t, ten)
					if mf.Format != manifestFormatChain {
						t.Fatalf("round %d: format %d", r, mf.Format)
					}
					got := reconstruct(t, fs, &mf)
					if !bytes.Equal(got, local) {
						t.Fatalf("round %d: reconstruction (%d bytes, chain %d) differs from the local file (%d bytes)",
							r, len(got), len(mf.Deltas), len(local))
					}
					maxChain = max(maxChain, len(mf.Deltas))
					if len(mf.Deltas) > cfg.chain {
						t.Fatalf("chain %d above the cap %d", len(mf.Deltas), cfg.chain)
					}
				}
				if met.DeltaCheckpoints.Load() == 0 {
					t.Fatal("no delta checkpoints")
				}
				if cfg.name != "long-chain" && met.Rebases.Load() == 0 {
					t.Fatal("no rebase")
				}
				if cfg.name == "long-chain" && maxChain < rounds/2 {
					t.Fatalf("longest chain %d: too many full bases", maxChain)
				}
				t.Logf("delta checkpoints %d, rebases %d, longest chain %d, delta stored %d, base stored %d",
					met.DeltaCheckpoints.Load(), met.Rebases.Load(), maxChain, met.DeltaStoredBytes.Load(), met.BaseStoredBytes.Load())

				// one more batch in segments only, then a crash: a new pod
				// applies the chain and replays the segments
				var want string
				var werr error
				onActorT(t, ten, func() {
					werr = randomWork(ten, rng, rounds)
					ten.cutSegment()
				})
				if werr != nil {
					t.Fatal(werr)
				}
				mustTx(t, m, "t1", "c", "last", Statement{SQL: `CREATE TABLE IF NOT EXISTS last(x)`})
				onActorT(t, ten, func() { want = dbDigest(ten) })
				crashTenant(t, m, "t1")
				waitLeaseExpiry()
				m2 := newTestManager(t, fs, "b")
				ten2, err := m2.Acquire(context.Background(), "t1", false)
				if err != nil {
					t.Fatal(err)
				}
				var got string
				onActorT(t, ten2, func() { got = dbDigest(ten2) })
				if got != want {
					t.Fatalf("recovered content differs:\n got %s\nwant %s", got, want)
				}
			})
		}
	}
}

// dbDigest hashes every row of every user table and adds the integrity
// check (actor only).
func dbDigest(ten *Tenant) string {
	var ic string
	if err := ten.db.QueryRow(`PRAGMA integrity_check`).Scan(&ic); err != nil {
		return "error: " + err.Error()
	}
	rows, err := ten.db.Query(`SELECT name FROM sqlite_master WHERE type = 'table' AND name != '_hw_dedup' ORDER BY name`)
	if err != nil {
		return "error: " + err.Error()
	}
	var names []string
	for rows.Next() {
		var n string
		rows.Scan(&n)
		names = append(names, n)
	}
	rows.Close()
	h := sha256.New()
	for _, n := range names {
		fmt.Fprintf(h, "table %s\n", n)
		rows, err := ten.db.Query(fmt.Sprintf(`SELECT * FROM %q ORDER BY rowid`, n))
		if err != nil {
			return "error: " + err.Error()
		}
		cols, _ := rows.Columns()
		for rows.Next() {
			vals := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				return "error: " + err.Error()
			}
			fmt.Fprintf(h, "%v\n", vals)
		}
		rows.Close()
	}
	return fmt.Sprintf("integrity=%s tables=%v sha=%x", ic, names, h.Sum(nil))
}

// hookStore wraps the fake store with checkpoint fault points.
type hookStore struct {
	*FakeStore
	mu sync.Mutex
	// afterPut runs after an object PUT lands; a non-nil error is
	// returned to the writer (the object stays)
	afterPut func(key string) error
	// failCAS refuses a manifest CAS with a transient error before it lands
	failCAS func(m *Manifest) bool
	// failDelete drops deletes of the listed keys (a crash after the CAS)
	failDelete atomic.Bool
	deleted    []string
}

func (h *hookStore) PutBase(ctx context.Context, key string, r io.Reader, size int64) error {
	if err := h.FakeStore.PutBase(ctx, key, r, size); err != nil {
		return err
	}
	h.mu.Lock()
	f := h.afterPut
	h.mu.Unlock()
	if f != nil {
		return f(key)
	}
	return nil
}

func (h *hookStore) PutManifestCAS(ctx context.Context, tid string, m *Manifest, etag string) (string, error) {
	h.mu.Lock()
	f := h.failCAS
	h.mu.Unlock()
	if f != nil && f(m) {
		return "", errors.New("injected transient cas failure")
	}
	return h.FakeStore.PutManifestCAS(ctx, tid, m, etag)
}

func (h *hookStore) DeleteObjects(ctx context.Context, keys ...string) error {
	if h.failDelete.Load() {
		return errors.New("injected delete failure")
	}
	h.mu.Lock()
	h.deleted = append(h.deleted, keys...)
	h.mu.Unlock()
	return h.FakeStore.DeleteObjects(ctx, keys...)
}

func (h *hookStore) set(f func()) {
	h.mu.Lock()
	defer h.mu.Unlock()
	f()
}

func newHookManager(t *testing.T, hs *hookStore, id string, mut func(*TenantOpts)) (*Manager, *Metrics) {
	t.Helper()
	o := testOpts()
	o.CheckpointByte = 1 << 40
	o.MaxSegments = 1 << 30
	if mut != nil {
		mut(&o)
	}
	met := &Metrics{}
	return NewManager(hs, "fake", id, "standard", t.TempDir(), o, 1000, met, ControllerOpts{}), met
}

// chainTenant creates t1 on m and builds a chain of n deltas, each with
// one acked row, then writes `tail` more acked rows that stay in segments
// of the current gen. It returns the number of acked rows.
// chainOpts keeps every checkpoint in one chain (no rebase).
func chainOpts(o *TenantOpts) {
	o.CheckpointByte = 1 << 40
	o.MaxSegments = 1 << 30
	o.DeltaMaxRatio = 1000
}

func chainTenant(t *testing.T, m *Manager, n, tail int) int64 {
	t.Helper()
	mustCreate(t, m, "t1")
	makeSchema(t, m, "t1")
	ten := m.LocalTenant("t1")
	var rows int64
	for i := 0; i < n; i++ {
		mustTx(t, m, "t1", "c", fmt.Sprintf("d%d", i), insertStmt(rows, strings.Repeat("d", 3000)))
		rows++
		checkpointFold(t, ten)
	}
	for i := 0; i < tail; i++ {
		mustTx(t, m, "t1", "c", fmt.Sprintf("s%d", i), insertStmt(rows, strings.Repeat("s", 3000)))
		rows++
	}
	return rows
}

func getManifest(t *testing.T, store Backend, tid string) *Manifest {
	t.Helper()
	mf, _, err := store.GetManifest(context.Background(), tid)
	if err != nil || mf == nil {
		t.Fatalf("manifest: %v", err)
	}
	return mf
}

// mustHaveObjects fails when an object the manifest names is missing.
func mustHaveObjects(t *testing.T, fs *FakeStore, mf *Manifest) {
	t.Helper()
	for _, k := range manifestObjects(mf) {
		if _, err := fs.GetObject(context.Background(), k); err != nil {
			t.Fatalf("named object %s: %v", k, err)
		}
	}
}

func hasKey(fs *FakeStore, key string) bool {
	_, err := fs.GetObject(context.Background(), key)
	return err == nil
}

// TestDeltaChainShape: a checkpoint in a chain gen uploads one delta under
// ckpt/, the chain grows by one entry per checkpoint, the base stays, and
// a checkpoint with nothing new names the same chain and uploads nothing.
func TestDeltaChainShape(t *testing.T) {
	fs := NewFakeStore()
	m, met := newTestManagerOpts(t, fs, "a", chainOpts)
	chainTenant(t, m, 3, 0)
	mf := getManifest(t, fs, "t1")
	if mf.Format != manifestFormatChain || len(mf.Deltas) != 3 || !strings.Contains(mf.BaseKey, "/ckpt/") || !strings.HasSuffix(mf.BaseKey, ".base") {
		t.Fatalf("manifest %+v", mf)
	}
	for i, d := range mf.Deltas {
		if !strings.Contains(d.Key, "/ckpt/") || !strings.HasSuffix(d.Key, ".delta") || legacyKeyGen(d.Key, d.Key[:strings.Index(d.Key, "/ckpt/")+1]) != -1 {
			t.Fatalf("delta %d key %s", i, d.Key)
		}
		b, _ := fs.GetObject(context.Background(), d.Key)
		if int64(len(b)) != d.Size || !isZstd(b) {
			t.Fatalf("delta %d: %d bytes stored, manifest %d, zstd %v", i, len(b), d.Size, isZstd(b))
		}
	}
	mustHaveObjects(t, fs, mf)
	if met.DeltaCheckpoints.Load() != 3 || met.DeltaStoredBytes.Load() == 0 {
		t.Fatalf("metrics: %d delta checkpoints, %d stored bytes", met.DeltaCheckpoints.Load(), met.DeltaStoredBytes.Load())
	}
	ckptKeys := func() (n int) {
		for _, k := range fs.ObjectKeys() {
			if strings.Contains(k, "/ckpt/") {
				n++
			}
		}
		return n
	}
	objs := ckptKeys()
	_, mf2 := checkpointFold(t, m.LocalTenant("t1"))
	if mf2.Gen != mf.Gen+1 || mf2.BaseKey != mf.BaseKey || len(mf2.Deltas) != 3 {
		t.Fatalf("empty checkpoint: %+v", mf2)
	}
	time.Sleep(100 * time.Millisecond) // the async delete after the CAS
	if n := ckptKeys(); n != objs || objs != 4 {
		t.Fatalf("empty checkpoint changed the checkpoint objects %d -> %d", objs, n)
	}
	mustHaveObjects(t, fs, &mf2)
}

// TestDeltaTakeoverMidChain: a new pod takes over a tenant in the middle
// of a chain with segments on top. It applies the chain, replays the
// segments, and its claim appends one delta under a per-attempt key. The
// chain of the old owner stays intact.
func TestDeltaTakeoverMidChain(t *testing.T) {
	fs := NewFakeStore()
	m, _ := newTestManagerOpts(t, fs, "a", chainOpts)
	rows := chainTenant(t, m, 3, 4)
	before := getManifest(t, fs, "t1")
	crashTenant(t, m, "t1")
	waitLeaseExpiry()
	m2, _ := newTestManagerOpts(t, fs, "b", chainOpts)
	if n := countRows(t, m2, "t1"); n != rows {
		t.Fatalf("after takeover: %d rows, want %d", n, rows)
	}
	after := getManifest(t, fs, "t1")
	if after.BaseKey != before.BaseKey || len(after.Deltas) != 4 {
		t.Fatalf("claim: base %s chain %d", after.BaseKey, len(after.Deltas))
	}
	for i := range before.Deltas {
		if after.Deltas[i] != before.Deltas[i] {
			t.Fatalf("claim rewrote delta %d", i)
		}
	}
	claim := after.Deltas[3].Key
	if want := fmt.Sprintf("/ckpt/%06d-%06d-", after.Gen, after.Epoch); !strings.Contains(claim, want) {
		t.Fatalf("claim delta key %s: want a per-attempt key with %s", claim, want)
	}
	mustHaveObjects(t, fs, after)
	// and once more: a chain over three owners
	mustTx(t, m2, "t1", "c", "m2", insertStmt(rows, "m2"))
	rows++
	crashTenant(t, m2, "t1")
	waitLeaseExpiry()
	m3, _ := newTestManagerOpts(t, fs, "c", chainOpts)
	if n := countRows(t, m3, "t1"); n != rows {
		t.Fatalf("second takeover: %d rows, want %d", n, rows)
	}
}

// TestDeltaChainRebase: at DeltaMaxChain deltas the next checkpoint
// uploads a full base and starts a new chain, and the old base and deltas
// are deleted after the CAS. The ratio rule rebases too.
func TestDeltaChainRebase(t *testing.T) {
	t.Run("count", func(t *testing.T) {
		fs := NewFakeStore()
		m, met := newTestManagerOpts(t, fs, "a", func(o *TenantOpts) {
			o.CheckpointByte = 1 << 40
			o.MaxSegments = 1 << 30
			o.DeltaMaxChain = 3
			o.DeltaMaxRatio = 1000
		})
		rows := chainTenant(t, m, 3, 0)
		old := getManifest(t, fs, "t1")
		if len(old.Deltas) != 3 {
			t.Fatalf("chain %d", len(old.Deltas))
		}
		mustTx(t, m, "t1", "c", "x", insertStmt(rows, "x"))
		rows++
		_, mf := checkpointFold(t, m.LocalTenant("t1"))
		if len(mf.Deltas) != 0 || mf.BaseKey == old.BaseKey || mf.Format != manifestFormatChain {
			t.Fatalf("rebase: %+v", mf)
		}
		if met.Rebases.Load() != 2 { // the create and this rebase
			t.Fatalf("rebases %d", met.Rebases.Load())
		}
		waitCond(t, func() bool {
			for _, k := range manifestObjects(old) {
				if hasKey(fs, k) {
					return false
				}
			}
			return true
		})
		crashTenant(t, m, "t1")
		waitLeaseExpiry()
		if n := countRows(t, newTestManager(t, fs, "b"), "t1"); n != rows {
			t.Fatalf("after rebase: %d rows, want %d", n, rows)
		}
	})
	t.Run("ratio", func(t *testing.T) {
		fs := NewFakeStore()
		m, met := newTestManagerOpts(t, fs, "a", func(o *TenantOpts) {
			o.CheckpointByte = 1 << 40
			o.MaxSegments = 1 << 30
			o.DeltaMaxRatio = 0.5
		})
		mustCreate(t, m, "t1")
		ten := m.LocalTenant("t1")
		// a large incompressible base, then small deltas until their sum
		// passes half of it
		mustTx(t, m, "t1", "c", "big", Statement{SQL: `CREATE TABLE big(v BLOB)`},
			Statement{SQL: `INSERT INTO big VALUES (randomblob(400000))`})
		checkpointFold(t, ten)
		mustTx(t, m, "t1", "c", "small", Statement{SQL: `CREATE TABLE small(k INTEGER PRIMARY KEY, v BLOB)`})
		_, mf := checkpointFold(t, ten)
		rebases := met.Rebases.Load()
		for i := 0; i < 200 && met.Rebases.Load() == rebases; i++ {
			mustTx(t, m, "t1", "c", fmt.Sprintf("s%d", i),
				Statement{SQL: `INSERT INTO small VALUES (?, randomblob(30000))`, Args: []any{i}})
			_, mf = checkpointFold(t, ten)
			var sum int64
			for _, d := range mf.Deltas {
				sum += d.Size
			}
			if float64(sum) > 0.5*float64(mf.BaseSize) {
				t.Fatalf("chain %d bytes over half the base %d", sum, mf.BaseSize)
			}
		}
		if met.Rebases.Load() == rebases || len(mf.Deltas) != 0 {
			t.Fatalf("no ratio rebase: chain %d", len(mf.Deltas))
		}
	})
}

func waitCond(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not reached")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestDeltaCrashPoints: a crash at each step of a checkpoint loses no
// acked write and never leaves a manifest that names a missing object.
//   - put-delta: the delta lands, the CAS never happens
//   - rebase-put: the rebase base lands, the CAS never happens
//   - cas-delta / cas-rebase: the CAS fails (ambiguous transient error)
//   - after-cas: the CAS lands, the deletes after it never happen
//
// Then a new pod recovers, and the sweeper collects the leftovers.
func TestDeltaCrashPoints(t *testing.T) {
	cases := []string{"put-delta", "rebase-put", "cas-delta", "cas-rebase", "after-cas-delta", "after-cas-rebase"}
	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			hs := &hookStore{FakeStore: NewFakeStore()}
			rebase := strings.Contains(name, "rebase")
			mut := func(o *TenantOpts) {
				o.DeltaMaxRatio = 1000
				o.DeltaMaxChain = 1000
				if rebase {
					o.DeltaMaxChain = 2
				}
			}
			m, _ := newHookManager(t, hs, "a", mut)
			rows := chainTenant(t, m, 2, 3)
			before := getManifest(t, hs, "t1")
			if len(before.Deltas) != 2 {
				t.Fatalf("chain %d", len(before.Deltas))
			}
			var orphan string
			switch name {
			case "put-delta", "rebase-put":
				suffix := ".delta"
				if rebase {
					suffix = ".base"
				}
				hs.set(func() {
					hs.afterPut = func(key string) error {
						if strings.HasSuffix(key, suffix) {
							orphan = key
							return errors.New("injected crash after put")
						}
						return nil
					}
				})
			case "cas-delta", "cas-rebase":
				hs.set(func() {
					hs.failCAS = func(nm *Manifest) bool { return nm.Gen > before.Gen }
				})
			default:
				hs.failDelete.Store(true)
			}
			ten := m.LocalTenant("t1")
			var ok bool
			onActorT(t, ten, func() { ok = ten.checkpointLocked("") })
			mid := getManifest(t, hs, "t1")
			if strings.HasPrefix(name, "after-cas") {
				if !ok || mid.Gen != before.Gen+1 {
					t.Fatalf("checkpoint ok %v gen %d", ok, mid.Gen)
				}
				if rebase != (len(mid.Deltas) == 0) {
					t.Fatalf("rebase %v but chain %d", rebase, len(mid.Deltas))
				}
			} else {
				if ok {
					t.Fatal("checkpoint should fail")
				}
				if !manifestEqual(mid, before) {
					t.Fatalf("manifest moved: %+v", mid)
				}
			}
			mustHaveObjects(t, hs.FakeStore, mid)
			hs.set(func() { hs.afterPut = nil; hs.failCAS = nil })
			hs.failDelete.Store(false)
			crashTenant(t, m, "t1")
			waitLeaseExpiry()

			m2, _ := newHookManager(t, hs, "b", mut)
			if n := countRows(t, m2, "t1"); n != rows {
				t.Fatalf("after crash: %d rows, want %d", n, rows)
			}
			// release (a checkpoint, owner cleared), then sweep twice
			// with a checkpoint between: every leftover of an older gen
			// goes, every named object stays
			if err := m2.Release("t1", ""); err != nil {
				t.Fatal(err)
			}
			m3, _ := newHookManager(t, hs, "c", mut)
			if n := countRows(t, m3, "t1"); n != rows {
				t.Fatalf("after release: %d rows", n)
			}
			if err := m3.Release("t1", ""); err != nil {
				t.Fatal(err)
			}
			final := getManifest(t, hs, "t1")
			NewSweeper(m3, hs, "fake", testOpts(), &Metrics{}).sweepTenant(context.Background(), "t1")
			mustHaveObjects(t, hs.FakeStore, final)
			if orphan != "" && hasKey(hs.FakeStore, orphan) {
				t.Fatalf("orphan %s survived the sweep", orphan)
			}
			for _, k := range hs.ObjectKeys() {
				if strings.Contains(k, "/ckpt/") && !manifestNames(final, k) {
					t.Fatalf("unnamed checkpoint object %s survived the sweep", k)
				}
			}
			m4, _ := newHookManager(t, hs, "d", mut)
			if n := countRows(t, m4, "t1"); n != rows {
				t.Fatalf("after sweep: %d rows", n)
			}
		})
	}
}

// TestDeltaClaimLoserKeepsChain: two pods race to take over a chain
// tenant. The loser deletes only the delta it uploaded, never the base or
// the deltas of the chain the winner extends.
func TestDeltaClaimLoserKeepsChain(t *testing.T) {
	ctx := context.Background()
	hs := &hookStore{FakeStore: NewFakeStore()}
	m, _ := newHookManager(t, hs, "a", chainOpts)
	rows := chainTenant(t, m, 3, 2)
	crashTenant(t, m, "t1")
	waitLeaseExpiry()

	loser, _ := newHookManager(t, hs, "loser", chainOpts)
	winner, _ := newHookManager(t, hs, "winner", chainOpts)
	var loserKey string
	var once sync.Once
	var winErr error
	hs.set(func() {
		hs.afterPut = func(key string) error {
			once.Do(func() {
				// the loser's claim delta landed: the winner now claims
				// and publishes before the loser's CAS
				loserKey = key
				hs.mu.Lock()
				hs.afterPut = nil
				hs.mu.Unlock()
				_, winErr = winner.Acquire(ctx, "t1", false)
			})
			return nil
		}
	})
	if _, err := loser.Acquire(ctx, "t1", false); err == nil {
		t.Fatal("the loser's claim should fail")
	}
	if winErr != nil {
		t.Fatalf("winner: %v", winErr)
	}
	mf := getManifest(t, hs, "t1")
	if !strings.HasPrefix(mf.Owner, "winner") || len(mf.Deltas) != 4 {
		t.Fatalf("manifest %+v", mf)
	}
	mustHaveObjects(t, hs.FakeStore, mf)
	if !strings.HasSuffix(loserKey, ".delta") || hasKey(hs.FakeStore, loserKey) {
		t.Fatalf("loser key %q: want its own delta deleted", loserKey)
	}
	if n := countRows(t, winner, "t1"); n != rows {
		t.Fatalf("winner: %d rows", n)
	}
}

// TestSweeperWithDeltas: the sweeper keeps every object the manifest
// names, whatever gen its key carries, and collects unnamed checkpoint
// objects of older gens and dead instances. Objects of the current or a
// newer gen stay (a checkpoint may be in flight).
func TestSweeperWithDeltas(t *testing.T) {
	ctx := context.Background()
	fs := NewFakeStore()
	m, _ := newTestManagerOpts(t, fs, "a", chainOpts)
	rows := chainTenant(t, m, 4, 0)
	if err := m.Release("t1", ""); err != nil {
		t.Fatal(err)
	}
	mf := getManifest(t, fs, "t1")
	if len(mf.Deltas) < 4 || mf.Gen < 5 {
		t.Fatalf("manifest %+v", mf)
	}
	ip := tenantPrefix("fake", "t1") + "/i/" + mf.Instance
	plant := map[string]bool{ // key -> want collected
		ckptKey("fake", "t1", mf.Instance, 2, mf.Epoch, "", "delta"):        true,
		ckptKey("fake", "t1", mf.Instance, 1, mf.Epoch, "zz", "base"):       true,
		ckptKey("fake", "t1", mf.Instance, mf.Gen, mf.Epoch+1, "", "delta"): false,
		ckptKey("fake", "t1", mf.Instance, mf.Gen+1, mf.Epoch, "", "delta"): false,
		tenantPrefix("fake", "t1") + "/i/deadinst/ckpt/000001-000001.delta": true,
		ip + "/ckpt/garbage": false,
	}
	for k := range plant {
		if err := fs.PutSegment(ctx, k, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	NewSweeper(m, fs, "fake", testOpts(), &Metrics{}).sweepTenant(ctx, "t1")
	mustHaveObjects(t, fs, mf)
	for k, gone := range plant {
		if hasKey(fs, k) == gone {
			t.Errorf("%s: want collected %v", k, gone)
		}
	}
	m2, _ := newTestManagerOpts(t, fs, "b", chainOpts)
	if n := countRows(t, m2, "t1"); n != rows {
		t.Fatalf("after sweep: %d rows", n)
	}
}

// TestDeltaChainCorruptionRefused: a chain that does not fit together is
// refused loudly before the claim CAS, and nothing is deleted.
func TestDeltaChainCorruptionRefused(t *testing.T) {
	cases := map[string]func(fs *FakeStore, mf *Manifest){
		"swapped": func(fs *FakeStore, mf *Manifest) {
			mf.Deltas[0], mf.Deltas[1] = mf.Deltas[1], mf.Deltas[0]
		},
		"dropped": func(fs *FakeStore, mf *Manifest) { mf.Deltas = mf.Deltas[1:] },
		"size":    func(fs *FakeStore, mf *Manifest) { mf.Deltas[1].Size++ },
		"bitflip": func(fs *FakeStore, mf *Manifest) { fs.Corrupt(mf.Deltas[1].Key) },
		"missing": func(fs *FakeStore, mf *Manifest) {
			fs.DeleteObjects(context.Background(), mf.Deltas[2].Key)
		},
		"other-base": func(fs *FakeStore, mf *Manifest) {
			b, _ := fs.GetObject(context.Background(), mf.BaseKey)
			mf.BaseKey += ".copy"
			fs.PutSegment(context.Background(), mf.BaseKey, b)
		},
		"base-size": func(fs *FakeStore, mf *Manifest) { mf.BaseSize-- },
		"raw-delta": func(fs *FakeStore, mf *Manifest) {
			b, _ := fs.GetObject(context.Background(), mf.Deltas[0].Key)
			d, _ := decodeBlob(b, maxDeltaRawBytes)
			fs.PutSegment(context.Background(), mf.Deltas[0].Key, d)
			mf.Deltas[0].Size = int64(len(d))
		},
		"deltas-in-format-1": func(fs *FakeStore, mf *Manifest) { mf.Format = manifestFormatZstd },
		// a valid zstd frame whose header names the wrong file size before
		// the delta: only the prevBytes binding can catch it
		"prev-bytes": func(fs *FakeStore, mf *Manifest) {
			ctx := context.Background()
			b, _ := fs.GetObject(ctx, mf.Deltas[1].Key)
			d, err := decodeBlob(b, maxDeltaRawBytes)
			if err != nil {
				panic(err)
			}
			prev := binary.BigEndian.Uint64(d[24:32])
			binary.BigEndian.PutUint64(d[24:32], prev+uint64(binary.BigEndian.Uint32(d[8:12])))
			enc := encodeBlob(d)
			fs.PutSegment(ctx, mf.Deltas[1].Key, enc)
			mf.Deltas[1].Size = int64(len(enc))
		},
	}
	for name, mut := range cases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			fs := NewFakeStore()
			m, _ := newTestManagerOpts(t, fs, "a", chainOpts)
			chainTenant(t, m, 3, 1)
			crashTenant(t, m, "t1")
			mf, etag, _ := fs.GetManifest(ctx, "t1")
			mut(fs, mf)
			if _, err := fs.PutManifestCAS(ctx, "t1", mf, etag); err != nil {
				t.Fatal(err)
			}
			waitLeaseExpiry()
			before := snapshotObjects(fs)
			m2, _ := newTestManagerOpts(t, fs, "b", chainOpts)
			actx, cancel := context.WithTimeout(ctx, 5*time.Second)
			_, err := m2.Acquire(actx, "t1", false)
			cancel()
			if err == nil {
				t.Fatal("a broken chain was taken over")
			}
			if errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("want a loud refusal, not a retry loop: %v", err)
			}
			t.Logf("refused: %v", err)
			if name == "prev-bytes" && !strings.Contains(err.Error(), "applies to a") {
				t.Fatalf("want the prevBytes refusal, got %v", err)
			}
			after := snapshotObjects(fs)
			if len(after) != len(before) {
				t.Fatalf("objects changed %d -> %d", len(before), len(after))
			}
			for k, v := range before {
				if after[k] != v {
					t.Fatalf("%s changed", k)
				}
			}
		})
	}
}

// TestDeltasOffWritesOldFormats: without compression, or with deltas off,
// checkpoints write format 0 or 1 exactly as before, and turning deltas
// off on a chain tenant publishes a format 1 gen and drops the chain.
func TestDeltasOffWritesOldFormats(t *testing.T) {
	fs := NewFakeStore()
	raw, _ := newTestManagerOpts(t, fs, "raw", func(o *TenantOpts) { o.Compress = false })
	mustCreate(t, raw, "r")
	makeSchema(t, raw, "r")
	if err := raw.Release("r", ""); err != nil {
		t.Fatal(err)
	}
	if mf := getManifest(t, fs, "r"); mf.Format != manifestFormatRaw || len(mf.Deltas) != 0 || strings.Contains(mf.BaseKey, "/ckpt/") {
		t.Fatalf("compression off: %+v", mf)
	}

	m, _ := newTestManagerOpts(t, fs, "a", chainOpts)
	rows := chainTenant(t, m, 2, 1)
	old := getManifest(t, fs, "t1")
	if err := m.Release("t1", ""); err != nil {
		t.Fatal(err)
	}
	off, _ := newTestManagerOpts(t, fs, "off", func(o *TenantOpts) { o.DeltaMaxChain = 0 })
	if n := countRows(t, off, "t1"); n != rows {
		t.Fatalf("deltas off reads the chain: %d rows", n)
	}
	mf := getManifest(t, fs, "t1")
	if mf.Format != manifestFormatZstd || len(mf.Deltas) != 0 || mf.BaseSize != 0 || !strings.Contains(mf.BaseKey, "/base/") {
		t.Fatalf("deltas off: %+v", mf)
	}
	waitCond(t, func() bool {
		for _, k := range manifestObjects(old) {
			if hasKey(fs, k) {
				return false
			}
		}
		return true
	})
	// an S1 build reads it again
	if err := off.Release("t1", ""); err != nil {
		t.Fatal(err)
	}
	if n := countRows(t, newS1Manager(t, fs, "s1"), "t1"); n != rows {
		t.Fatalf("s1 reader after deltas off: %d rows", n)
	}
}

// TestS1EmulationReadsZstdGen: the S1 emulation is not a strawman. It
// takes over a format 1 gen, and refuses only format 2.
func TestS1EmulationReadsZstdGen(t *testing.T) {
	fs := NewFakeStore()
	m, _ := newTestManagerOpts(t, fs, "a", func(o *TenantOpts) { o.DeltaMaxChain = 0 })
	mustCreate(t, m, "t1")
	makeSchema(t, m, "t1")
	mustTx(t, m, "t1", "c", "k", insertStmt(1, "v"))
	crashTenant(t, m, "t1")
	waitLeaseExpiry()
	if n := countRows(t, newS1Manager(t, fs, "s1"), "t1"); n != 1 {
		t.Fatalf("s1 emulation on a format 1 gen: %d rows", n)
	}
	if f := getManifest(t, fs, "t1").Format; f != manifestFormatZstd {
		t.Fatalf("s1 emulation wrote format %d", f)
	}
}

// TestWALCommitEndReaderMatches: the streaming WAL check gives the same
// commit end as the in-memory parser, on intact, cut and damaged WALs.
func TestWALCommitEndReaderMatches(t *testing.T) {
	db, _, walPath := openRawWAL(t)
	for i := 0; i < 40; i++ {
		if _, err := db.Exec(`INSERT INTO t VALUES (?, randomblob(?))`, i, 100+i*97); err != nil {
			t.Fatal(err)
		}
	}
	wal, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewSource(1))
	variants := [][]byte{wal, nil, wal[:10], wal[:walHdrSize]}
	for i := 0; i < 50; i++ {
		v := append([]byte(nil), wal[:rng.Intn(len(wal)+1)]...)
		if len(v) > 0 && rng.Intn(2) == 0 {
			v[rng.Intn(len(v))] ^= 0x40
		}
		variants = append(variants, v)
	}
	for i, v := range variants {
		got, err := walCommitEndReader(bytes.NewReader(v))
		if err != nil {
			t.Fatal(err)
		}
		if want := walCommitEnd(v); got != want {
			t.Fatalf("variant %d (%d bytes): reader %d, parser %d", i, len(v), got, want)
		}
	}
	if walCommitEnd(wal) != int64(len(wal)) {
		t.Fatal("test WAL should end on a commit")
	}
}

// TestDeltaMeasure prints the bytes one checkpoint uploads for a ~50 MB
// tenant with 128 small commits per checkpoint, with deltas and with full
// bases. Set HL_MEASURE=1 to run it.
func TestDeltaMeasure(t *testing.T) {
	if os.Getenv("HL_MEASURE") == "" {
		t.Skip("set HL_MEASURE=1")
	}
	for _, deltas := range []bool{false, true} {
		fs := NewFakeStore()
		m, met := newTestManagerOpts(t, fs, fmt.Sprint(deltas), func(o *TenantOpts) {
			o.CheckpointByte = 1 << 40
			o.MaxSegments = 1 << 30
			if !deltas {
				o.DeltaMaxChain = 0
			}
		})
		mustCreate(t, m, "t1")
		ten := m.LocalTenant("t1")
		var err error
		onActorT(t, ten, func() {
			_, err = ten.db.Exec(`CREATE TABLE ev(k INTEGER PRIMARY KEY, team INTEGER, name TEXT, props TEXT)`)
			if err == nil {
				_, err = ten.db.Exec(`WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i < 120000)
					INSERT INTO ev SELECT i, i % 50, 'event_' || (i % 300), '{"url":"https://example.com/p/' || hex(randomblob(16)) || '","ua":"Mozilla/5.0","n":' || i || ',"pad":"' || hex(randomblob(110)) || '"}' FROM n`)
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		local, _ := checkpointFold(t, ten)
		base0, stored0 := met.BaseBytes.Load(), met.BaseStoredBytes.Load()
		ms0 := met.CheckpointMs.Load()
		rng := rand.New(rand.NewSource(1))
		const ckpts = 10
		for c := 0; c < ckpts; c++ {
			for i := 0; i < 128; i++ {
				k := rng.Intn(120000) + 1
				if i%2 == 0 {
					mustTx(t, m, "t1", "c", fmt.Sprintf("u%d-%d", c, i), Statement{SQL: `UPDATE ev SET props = ? WHERE k = ?`, Args: []any{fmt.Sprintf(`{"edited":%d,"pad":"%x"}`, i, rng.Int63()), k}})
				} else {
					mustTx(t, m, "t1", "c", fmt.Sprintf("i%d-%d", c, i), Statement{SQL: `INSERT INTO ev VALUES (NULL, 7, 'new', ?)`, Args: []any{strings.Repeat("p", 200)}})
				}
			}
			checkpointFold(t, ten)
		}
		up := met.BaseStoredBytes.Load() - stored0 + met.DeltaStoredBytes.Load()
		raw := met.BaseBytes.Load() - base0 + met.DeltaBytes.Load()
		t.Logf("deltas=%v db=%.1f MB: per checkpoint uploaded %.1f KB stored (%.1f KB raw), %d ms avg, rebases %d",
			deltas, float64(len(local))/1e6, float64(up)/ckpts/1e3, float64(raw)/ckpts/1e3,
			(met.CheckpointMs.Load()-ms0)/ckpts, met.Rebases.Load())
	}
}

// chainLimitRun drives a writer configured far above the reader limit
// (DeltaMaxChain 2048, no ratio limit) up to the limit. The options are
// clamped; even when the live options are raised again past NewManager,
// the chain never gets longer than maxChainLen (the short run also checks
// that the next checkpoint rebases). A crash at a full chain with
// segments on top recovers on a default reader, whose claim then rebases.
func chainLimitRun(t *testing.T, long bool) {
	fs := NewFakeStore()
	m, _ := newTestManagerOpts(t, fs, "writer", func(o *TenantOpts) {
		chainOpts(o)
		o.DeltaMaxChain = 2048
		o.DeltaMaxRatio = 1e9
		if long { // 1024 checkpoints on a loaded host
			o.Lease = 10 * time.Second
			o.SkewMargin = time.Second
		}
	})
	if m.opts.DeltaMaxChain != maxChainLen {
		t.Fatalf("DeltaMaxChain %d not clamped to %d", m.opts.DeltaMaxChain, maxChainLen)
	}
	mustCreate(t, m, "t1")
	makeSchema(t, m, "t1")
	ten := m.LocalTenant("t1")
	// past the constructor too: the checkpoint itself caps the chain
	onActorT(t, ten, func() { ten.opts.DeltaMaxChain = 4 * maxChainLen })
	var rows int64
	fill := func() { // checkpoints until the chain is exactly at the limit
		for i := 0; ; i++ {
			mustTx(t, m, "t1", "c", fmt.Sprint(rows), insertStmt(rows, "x"))
			rows++
			_, mf := checkpointFold(t, ten)
			if len(mf.Deltas) > maxChainLen {
				t.Fatalf("checkpoint %d published a chain of %d, reader limit %d", i, len(mf.Deltas), maxChainLen)
			}
			if len(mf.Deltas) == maxChainLen {
				return
			}
			if i > 2*maxChainLen {
				t.Fatalf("chain does not grow: %d", len(mf.Deltas))
			}
		}
	}
	fill()
	if !long {
		// one checkpoint past a full chain must rebase, then fill again
		mustTx(t, m, "t1", "c", fmt.Sprint(rows), insertStmt(rows, "x"))
		rows++
		if _, mf := checkpointFold(t, ten); len(mf.Deltas) != 0 {
			t.Fatalf("checkpoint past the limit: chain %d, want a rebase", len(mf.Deltas))
		}
		fill()
	}
	for i := 0; i < 3; i++ {
		mustTx(t, m, "t1", "c", fmt.Sprintf("tail%d", i), insertStmt(rows, "tail"))
		rows++
	}
	crashTenant(t, m, "t1")
	m.stopLease()
	reader, _ := newTestManagerOpts(t, fs, "reader", chainOpts)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := reader.Acquire(ctx, "t1", false); err != nil {
		t.Fatalf("reader at the chain limit: %v", err)
	}
	if n := countRows(t, reader, "t1"); n != rows {
		t.Fatalf("after recovery at the limit: %d rows, want %d", n, rows)
	}
	if mf := getManifest(t, fs, "t1"); len(mf.Deltas) != 0 || mf.Format != manifestFormatChain {
		t.Fatalf("claim at a full chain should rebase: chain %d format %d", len(mf.Deltas), mf.Format)
	}
}

// TestDeltaChainReaderLimitFast runs the limit test with a reader limit
// of 6, so the boundary is cheap to reach.
func TestDeltaChainReaderLimitFast(t *testing.T) {
	saved := maxChainLen
	maxChainLen = 6
	defer func() { maxChainLen = saved }()
	chainLimitRun(t, false)
}

// TestDeltaChainReaderLimit runs it at the real limit (1024 deltas).
func TestDeltaChainReaderLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("1024 checkpoints")
	}
	chainLimitRun(t, true)
}

// TestNormalizeDeltaOpts: unsupported option values are clamped.
func TestNormalizeDeltaOpts(t *testing.T) {
	cases := []struct {
		chain     int
		ratio     float64
		wantChain int
		wantRatio float64
	}{
		{32, 0.5, 32, 0.5},
		{-1, -1, 0, 0},
		{maxChainLen + 1, math.NaN(), maxChainLen, 0},
		{1 << 30, math.Inf(1), maxChainLen, math.MaxFloat64},
	}
	for _, c := range cases {
		o := TenantOpts{DeltaMaxChain: c.chain, DeltaMaxRatio: c.ratio}
		normalizeDeltaOpts(&o)
		if o.DeltaMaxChain != c.wantChain || o.DeltaMaxRatio != c.wantRatio {
			t.Errorf("%d/%v -> %d/%v, want %d/%v", c.chain, c.ratio, o.DeltaMaxChain, o.DeltaMaxRatio, c.wantChain, c.wantRatio)
		}
	}
}

// rawDelta builds a raw delta: header h, then one record per page number,
// each page filled with the low byte of its number.
func rawDelta(h deltaHeader, pages []uint32) []byte {
	b := h.marshal()
	for _, pg := range pages {
		var n [4]byte
		binary.BigEndian.PutUint32(n[:], pg)
		b = append(b, n[:]...)
		b = append(b, bytes.Repeat([]byte{byte(pg)}, int(h.pageSize))...)
	}
	return b
}

// TestDeltaApplyRejectsMalformed: every header and record check of the
// delta reader refuses with ErrCorruptObject, on valid zstd frames (the
// codec checksum does not help here).
func TestDeltaApplyRejectsMalformed(t *testing.T) {
	const ps = 512
	const base = "fake/t/x/ckpt/000001-000001.base"
	good := deltaHeader{pageSize: ps, nRecords: 2, dbPages: 4, index: 3, prevBytes: 2 * ps, baseKey: base}
	exp := deltaExpect{baseKey: base, index: 3, pageSize: ps}
	with := func(f func(h *deltaHeader)) deltaHeader { h := good; f(&h); return h }
	cases := map[string]struct {
		raw  []byte
		exp  deltaExpect
		want string // part of the error; "" means it must apply
	}{
		"valid":             {rawDelta(good, []uint32{2, 4}), exp, ""},
		"valid-empty":       {rawDelta(with(func(h *deltaHeader) { h.nRecords = 0; h.dbPages = 2 }), nil), exp, ""},
		"prev-bytes":        {rawDelta(with(func(h *deltaHeader) { h.prevBytes = 3 * ps }), []uint32{2, 4}), exp, "applies to a"},
		"magic":             {append([]byte("XLPGDLT1"), rawDelta(good, []uint32{2, 4})[8:]...), exp, "bad magic"},
		"page-size-odd":     {rawDelta(with(func(h *deltaHeader) { h.pageSize = 1000 }), nil), deltaExpect{baseKey: base, index: 3}, "page size"},
		"page-size-small":   {rawDelta(with(func(h *deltaHeader) { h.pageSize = 256 }), nil), deltaExpect{baseKey: base, index: 3}, "page size"},
		"page-size-big":     {rawDelta(with(func(h *deltaHeader) { h.pageSize = 131072 }), nil), deltaExpect{baseKey: base, index: 3}, "page size"},
		"page-size-differs": {rawDelta(with(func(h *deltaHeader) { h.pageSize = 1024; h.prevBytes = 2048 }), nil), exp, "database has"},
		"index":             {rawDelta(with(func(h *deltaHeader) { h.index = 4 }), []uint32{2, 4}), exp, "position"},
		"base-key":          {rawDelta(with(func(h *deltaHeader) { h.baseKey = base + "x" }), []uint32{2, 4}), exp, "made for base"},
		"records-over-size": {rawDelta(with(func(h *deltaHeader) { h.dbPages = 1 }), []uint32{1, 1}), exp, "records for"},
		"count-too-high":    {rawDelta(with(func(h *deltaHeader) { h.nRecords = 3 }), []uint32{2, 4}), exp, "truncated"},
		"count-too-low":     {rawDelta(with(func(h *deltaHeader) { h.nRecords = 1 }), []uint32{2, 4}), exp, "data after"},
		"trailing-bytes":    {append(rawDelta(good, []uint32{2, 4}), 0), exp, "data after"},
		"unsorted":          {rawDelta(good, []uint32{4, 2}), exp, "after page"},
		"duplicate":         {rawDelta(good, []uint32{2, 2}), exp, "after page"},
		"page-zero":         {rawDelta(good, []uint32{0, 2}), exp, "after page"},
		"page-over-size":    {rawDelta(good, []uint32{2, 5}), exp, "after page"},
		"truncated-record":  {rawDelta(good, []uint32{2, 4})[:deltaFixedHdr+len(base)+4+ps+100], exp, "truncated"},
		"truncated-header":  {rawDelta(good, nil)[:20], exp, "truncated"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			f, err := os.Create(filepath.Join(t.TempDir(), "db"))
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			if err := f.Truncate(2 * ps); err != nil {
				t.Fatal(err)
			}
			_, err = applyDelta(f, bytes.NewReader(encodeBlob(c.raw)), c.exp)
			if c.want == "" {
				if err != nil {
					t.Fatalf("valid delta refused: %v", err)
				}
				return
			}
			if err == nil || !errors.Is(err, ErrCorruptObject) || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("want a corrupt-object refusal with %q, got %v", c.want, err)
			}
		})
	}
	// the valid delta wrote its pages and set the size
	f, _ := os.Create(filepath.Join(t.TempDir(), "db"))
	defer f.Close()
	f.Truncate(2 * ps)
	if _, err := applyDelta(f, bytes.NewReader(encodeBlob(rawDelta(good, []uint32{2, 4}))), exp); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(f.Name())
	want := append(append(append(make([]byte, ps), bytes.Repeat([]byte{2}, ps)...), make([]byte, ps)...), bytes.Repeat([]byte{4}, ps)...)
	if !bytes.Equal(b, want) {
		t.Fatal("valid delta: wrong file content")
	}
}

// windowStore holds the GET of the first delta and records how many
// delta files sit in dir whenever a GET starts.
type windowStore struct {
	*FakeStore
	dir   string
	first string
	mu    sync.Mutex
	peak  int
}

func (w *windowStore) GetObjectTo(ctx context.Context, key string, wr io.Writer) error {
	if strings.HasSuffix(key, ".delta") {
		files, _ := filepath.Glob(filepath.Join(w.dir, "delta-*.zst"))
		w.mu.Lock()
		w.peak = max(w.peak, len(files))
		w.mu.Unlock()
		if key == w.first {
			time.Sleep(300 * time.Millisecond) // later GETs would run ahead
		}
	}
	return w.FakeStore.GetObjectTo(ctx, key, wr)
}

// TestDeltaRecoveryWindow: recovery keeps at most deltaFetchParallel
// delta files on local disk, also when a slow early GET lets later GETs
// finish first. The chain still applies correctly and no file is left.
func TestDeltaRecoveryWindow(t *testing.T) {
	fs := NewFakeStore()
	m, _ := newTestManagerOpts(t, fs, "a", chainOpts)
	chainTenant(t, m, 3*deltaFetchParallel, 0)
	local, mf := checkpointFold(t, m.LocalTenant("t1"))
	if len(mf.Deltas) <= 2*deltaFetchParallel {
		t.Fatalf("chain %d", len(mf.Deltas))
	}
	dir := t.TempDir()
	ws := &windowStore{FakeStore: fs, dir: dir, first: mf.Deltas[0].Key}
	got := reconstructIn(t, ws, &mf, dir)
	if !bytes.Equal(got, local) {
		t.Fatal("reconstruction differs")
	}
	if ws.peak > deltaFetchParallel {
		t.Fatalf("%d delta files on disk at once, window %d", ws.peak, deltaFetchParallel)
	}
	if files, _ := filepath.Glob(filepath.Join(dir, "delta-*.zst")); len(files) != 0 {
		t.Fatalf("left files: %v", files)
	}
	t.Logf("peak delta files on disk: %d (window %d, chain %d)", ws.peak, deltaFetchParallel, len(mf.Deltas))
}
