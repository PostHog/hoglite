package hoglite

import (
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

// Property fuzz for the WAL parser: random byte flips, truncations and
// garbage must never panic, never return an out-of-bounds offset, and
// never exceed the pristine parse (corruption can only shorten the chain).
func TestWALParseFuzzProperties(t *testing.T) {
	db, _, walPath := openRawWAL(t)
	defer db.Close()
	for i := 0; i < 20; i++ {
		db.Exec(`INSERT INTO t(v) VALUES(?)`, string(make([]byte, i%7)))
	}
	clean, err := os.ReadFile(walPath)
	if err != nil || len(clean) == 0 {
		t.Skip("no wal")
	}
	cleanEnd := walCommitEnd(clean)
	if cleanEnd <= 0 {
		t.Skip("no commits parsed")
	}

	rng := rand.New(rand.NewSource(42))
	for i := 0; i < 2000; i++ {
		buf := make([]byte, len(clean))
		copy(buf, clean)
		switch i % 4 {
		case 0: // byte flips
			for j := 0; j < 1+rng.Intn(8); j++ {
				buf[rng.Intn(len(buf))] ^= byte(1 << rng.Intn(8))
			}
		case 1: // truncate
			buf = buf[:rng.Intn(len(buf))]
		case 2: // zero a region
			off := rng.Intn(len(buf))
			n := rng.Intn(len(buf) - off)
			clear(buf[off : off+n])
		case 3: // garbage tail append
			buf = append(buf, []byte("junkjunk")...)
		}
		end := walCommitEnd(buf)
		if end < 0 || end > int64(len(buf)) {
			t.Fatalf("out of bounds: end=%d len=%d", end, len(buf))
		}
		if end > cleanEnd {
			t.Fatalf("corruption extended the chain: end=%d cleanEnd=%d", end, cleanEnd)
		}
		// incremental parser must agree with the full parse on mutation too
		var p walParser
		if got := p.CommitEnd(buf); got != end {
			t.Fatalf("incremental %d != full %d", got, end)
		}
	}
}

// The incremental parser must be restartable mid-chain: parse, checkpoint
// (reset), parse — never panic and never disagree with a full parse.
func TestWALParserResetConsistency(t *testing.T) {
	db, _, walPath := openRawWAL(t)
	defer db.Close()
	var p walParser
	for i := 0; i < 10; i++ {
		db.Exec(`INSERT INTO t(v) VALUES('x')`)
		wal, _ := os.ReadFile(walPath)
		full := walCommitEnd(wal)
		var tail []byte
		if p.valid {
			tail = wal[p.commitOff:]
		} else {
			tail = wal
		}
		if got := p.CommitEnd(tail); got != full {
			t.Fatalf("step %d: incremental %d != full %d", i, got, full)
		}
		if i%3 == 2 {
			db.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`)
			p.Reset()
		}
	}
	_ = filepath.Base // silence unused if path unused
}
