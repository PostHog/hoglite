//go:build slow

package hoglite

// The slow delta tests: together about half of the package's time under
// -race. CI runs them in their own job (go-race-slow); see slow_test.go.

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"
)

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
	// a cleanup, not a defer: the parallel subtests run after this
	// function returns, and the cleanup waits for them
	t.Cleanup(func() { authorizerOffForTest.Store(false) })
	seeds := 4
	rounds := 30
	if testing.Short() {
		seeds, rounds = 1, 10
	}
	for _, cfg := range configs {
		for seed := 1; seed <= seeds; seed++ {
			t.Run(fmt.Sprintf("%s/seed%d", cfg.name, seed), func(t *testing.T) {
				// Each run is CPU-bound on its own store, manager and
				// seed (about 15s under -race), and shares nothing with
				// the others: in parallel the test takes the longest run
				// per -parallel slot instead of their sum.
				t.Parallel()
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

// TestDeltaChainReaderLimit runs it at the real limit (1024 deltas).
func TestDeltaChainReaderLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("1024 checkpoints")
	}
	chainLimitRun(t, true)
}
