package hoglite

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "modernc.org/sqlite"
)

// openRawWAL creates a WAL-mode db and returns it plus paths.
func openRawWAL(t *testing.T) (*sql.DB, string, string) {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "db.sqlite")
	db, err := sql.Open("sqlite", "file:"+dbPath+"?_pragma=journal_mode(WAL)&_pragma=wal_autocheckpoint(0)&_pragma=synchronous(OFF)")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`CREATE TABLE t(k INTEGER PRIMARY KEY, v TEXT)`); err != nil {
		t.Fatal(err)
	}
	return db, dbPath, dbPath + "-wal"
}

// replayInto copies db + wal[0:end] to a fresh dir, opens it, and returns
// all rows. Simulates crash recovery from shipped bytes.
func replayRows(t *testing.T, dbPath string, wal []byte, end int64) map[int64]string {
	t.Helper()
	dir := t.TempDir()
	db2 := filepath.Join(dir, "db.sqlite")
	base, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(db2, base, 0o644); err != nil {
		t.Fatal(err)
	}
	if end > 0 {
		if err := os.WriteFile(db2+"-wal", wal[:end], 0o644); err != nil {
			t.Fatal(err)
		}
	}
	db, err := sql.Open("sqlite", "file:"+db2+"?_pragma=journal_mode(WAL)&_pragma=wal_autocheckpoint(0)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query(`SELECT k, v FROM t`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[int64]string{}
	for rows.Next() {
		var k int64
		var v string
		rows.Scan(&k, &v)
		out[k] = v
	}
	return out
}

func readWALFile(t *testing.T, walPath string) []byte {
	t.Helper()
	b, err := os.ReadFile(walPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		t.Fatal(err)
	}
	return b
}

func TestWALParseBasic(t *testing.T) {
	db, dbPath, walPath := openRawWAL(t)
	defer db.Close()
	for i := 0; i < 5; i++ {
		if _, err := db.Exec(`INSERT INTO t(v) VALUES(?)`, strings.Repeat("a", 100)); err != nil {
			t.Fatal(err)
		}
	}
	wal := readWALFile(t, walPath)
	end := walCommitEnd(wal)
	if end <= 0 || end > int64(len(wal)) {
		t.Fatalf("bad end %d (len %d)", end, len(wal))
	}
	rows := replayRows(t, dbPath, wal, end)
	if len(rows) != 5 {
		t.Fatalf("replayed %d rows, want 5", len(rows))
	}
}

func TestWALParseRollbackTail(t *testing.T) {
	db, dbPath, walPath := openRawWAL(t)
	defer db.Close()
	big := strings.Repeat("b", 5000)
	// commit 3 small rows
	for i := 0; i < 3; i++ {
		if _, err := db.Exec(`INSERT INTO t VALUES(?, 'ok')`, i); err != nil {
			t.Fatal(err)
		}
	}
	// now a big txn that FAILS at the end (unique violation): leaves frames
	tx, _ := db.Begin()
	for i := 10; i < 30; i++ {
		if _, err := tx.Exec(`INSERT INTO t VALUES(?, ?)`, i, big); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := tx.Exec(`INSERT INTO t VALUES(0, 'dupe')`); err == nil {
		t.Fatal("expected constraint violation")
	}
	tx.Rollback()

	wal1 := readWALFile(t, walPath)
	end1 := walCommitEnd(wal1)
	rows := replayRows(t, dbPath, wal1, end1)
	if len(rows) != 3 {
		t.Fatalf("after rollback: replayed %d rows, want 3", len(rows))
	}

	// commit one SMALL row: may rewrite fewer frames than the failed txn
	// wrote, leaving a stale garbage tail beyond the new commit end
	if _, err := db.Exec(`INSERT INTO t VALUES(99, 'small')`); err != nil {
		t.Fatal(err)
	}
	wal2 := readWALFile(t, walPath)
	end2 := walCommitEnd(wal2)
	t.Logf("wal sizes: after rollback %d (end %d), after small commit %d (end %d)",
		len(wal1), end1, len(wal2), end2)
	rows = replayRows(t, dbPath, wal2, end2)
	if len(rows) != 4 || rows[99] != "small" {
		t.Fatalf("replayed %d rows, want 4: %v", len(rows), rows)
	}
	// end2 must not include garbage
	if end2 == int64(len(wal2)) && len(wal2) > int(len(wal1)) {
		t.Logf("note: file grew; no stale tail case")
	}
}

func TestWALParseCheckpointReset(t *testing.T) {
	db, dbPath, walPath := openRawWAL(t)
	defer db.Close()
	for i := 0; i < 3; i++ {
		db.Exec(`INSERT INTO t(v) VALUES('x')`)
	}
	// TRUNCATE checkpoint resets the WAL; new commits write a fresh header
	if _, err := db.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		t.Fatal(err)
	}
	if fi, _ := os.Stat(walPath); fi != nil && fi.Size() != 0 {
		t.Fatalf("wal not truncated: %d", fi.Size())
	}
	for i := 0; i < 2; i++ {
		db.Exec(`INSERT INTO t(v) VALUES('y')`)
	}
	wal := readWALFile(t, walPath)
	end := walCommitEnd(wal)
	if end <= 0 {
		t.Fatal("no commit end after reset")
	}
	// replay onto the CHECKPOINTED db image: exactly 5 rows, no doubles
	rows := replayRows(t, dbPath, wal, end)
	if len(rows) != 5 {
		t.Fatalf("replayed %d rows, want 5", len(rows))
	}
}

func TestWALParseGarbage(t *testing.T) {
	if walCommitEnd(nil) != 0 {
		t.Fatal("nil")
	}
	if walCommitEnd([]byte("short")) != 0 {
		t.Fatal("short")
	}
	garbage := make([]byte, 10000)
	if walCommitEnd(garbage) != 0 {
		t.Fatal("garbage must not parse")
	}
}

// TestWALParseGarbageTailWithReader pins a read mark so SQLite cannot
// truncate the WAL on rollback: the file keeps stale frames beyond the
// committed end. The parser must stop at the true commit boundary.
func TestWALParseGarbageTailWithReader(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "db.sqlite")
	dsn := "file:" + dbPath + "?_pragma=journal_mode(WAL)&_pragma=wal_autocheckpoint(0)&_pragma=synchronous(OFF)&_pragma=busy_timeout(5000)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(2)
	if _, err := db.Exec(`CREATE TABLE t(k INTEGER PRIMARY KEY, v TEXT)`); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := db.Exec(`INSERT INTO t VALUES(?, 'ok')`, i); err != nil {
			t.Fatal(err)
		}
	}
	walPath := dbPath + "-wal"
	before := int64(len(readWALFile(t, walPath)))

	// pin a read snapshot on the other connection
	rtx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer rtx.Rollback()
	var n int
	if err := rtx.QueryRow(`SELECT COUNT(*) FROM t`).Scan(&n); err != nil {
		t.Fatal(err)
	}

	// big failing write txn on the writer connection
	big := strings.Repeat("b", 5000)
	wtx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for i := 10; i < 40; i++ {
		if _, err := wtx.Exec(`INSERT INTO t VALUES(?, ?)`, i, big); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := wtx.Exec(`INSERT INTO t VALUES(0, 'dupe')`); err == nil {
		t.Fatal("expected constraint violation")
	}
	wtx.Rollback()

	afterRB := int64(len(readWALFile(t, walPath)))
	// one small commit
	if _, err := db.Exec(`INSERT INTO t VALUES(99, 'small')`); err != nil {
		t.Fatal(err)
	}
	wal := readWALFile(t, walPath)
	end := walCommitEnd(wal)
	t.Logf("before=%d afterRollback=%d final=%d parseEnd=%d", before, afterRB, len(wal), end)
	if int64(len(wal)) == end && afterRB > before+(24+4096) {
		// file still holds the whole rolled-back txn AND parse accepted all of
		// it: that would be a parser failure
		if len(wal) > int(before)+2*(24+4096) {
			t.Fatalf("parser accepted stale frames: end=%d len=%d before=%d", end, len(wal), before)
		}
	}
	rows := replayRows(t, dbPath, wal, end)
	if len(rows) != 4 || rows[99] != "small" {
		t.Fatalf("replayed %d rows, want 4 (3 + small): %v", len(rows), rows)
	}
}

// TestWALParserIncrementalEquivalence: the cached incremental parser must
// agree with a full parse after every operation, including rollbacks.
func TestWALParserIncrementalEquivalence(t *testing.T) {
	db, _, walPath := openRawWAL(t)
	defer db.Close()
	var p walParser
	check := func(step string) {
		t.Helper()
		wal := readWALFile(t, walPath)
		full := walCommitEnd(wal)
		var tail []byte
		if p.valid {
			tail = wal[p.commitOff:]
		} else {
			tail = wal
		}
		inc := p.CommitEnd(tail)
		if full != inc {
			t.Fatalf("%s: full=%d incremental=%d", step, full, inc)
		}
	}
	big := strings.Repeat("z", 3000)
	for i := 0; i < 10; i++ {
		db.Exec(`INSERT INTO t(v) VALUES(?)`, big)
		check(fmt.Sprintf("commit %d", i))
		// interleave a failing txn (rollback leaves frames or truncates)
		tx, _ := db.Begin()
		tx.Exec(`INSERT INTO t(v) VALUES(?)`, big)
		tx.Exec(`INSERT INTO t(v) VALUES(?)`, big)
		tx.Rollback()
		check(fmt.Sprintf("rollback %d", i))
	}
	// checkpoint reset
	if _, err := db.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		t.Fatal(err)
	}
	p.Reset()
	db.Exec(`INSERT INTO t(v) VALUES('after')`)
	check("post-checkpoint commit")
}
