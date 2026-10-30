package hoglite

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"modernc.org/libc"
	sqlite3 "modernc.org/sqlite/lib"
)

// gapSQL returns five rows at once, then searches without end for a
// sixth row. The search runs inside a later step (Rows.Next), where the
// driver no longer watches the context.
const gapSQL = `WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c)
	SELECT x FROM c WHERE x <= 5 OR x % 1000000000000 = 0`

// trickleSQL emits a row about every 200k iterations, without end.
const trickleSQL = `WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c)
	SELECT x FROM c WHERE x % 200000 = 0`

// endlessInsertSQL writes rows without end inside one statement. The
// blobs make the transaction spill dirty pages into the WAL.
const endlessInsertSQL = `INSERT INTO big SELECT x, randomblob(2000) FROM
	(WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c) SELECT x FROM c)`

func guardOpts() TenantOpts {
	o := testOpts()
	o.QueryTimeout = 300 * time.Millisecond
	o.WriteTimeout = 400 * time.Millisecond
	return o
}

func guardTenant(t *testing.T, o TenantOpts) (*Manager, *Tenant, *Metrics) {
	t.Helper()
	met := &Metrics{}
	m := newReadTestManager(t, NewFakeStore(), o, met)
	t.Cleanup(m.ShutdownAll)
	ten, err := m.Acquire(context.Background(), "t1", true)
	if err != nil {
		t.Fatal(err)
	}
	makeSchema(t, m, "t1")
	return m, ten, met
}

// slotsFree waits until the tenant holds no snapshot and no admission
// slot: a stopped query must give back everything.
func slotsFree(t *testing.T, ten *Tenant, met *Metrics) {
	t.Helper()
	waitFor(t, "slots returned", func() bool {
		return ten.readers.count() == 0 && len(ten.readSlots) == 0 && met.QueriesInflight.Load() == 0
	})
}

func expectTimeout(t *testing.T, err error, elapsed time.Duration) {
	t.Helper()
	if !errors.Is(err, ErrQueryTimeout) {
		t.Fatalf("want ErrQueryTimeout, got %v", err)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("stopped only after %v", elapsed)
	}
}

// The handle lookup must keep working across driver updates.
func TestSQLiteHandle(t *testing.T) {
	db, err := sql.Open("sqlite", "file:"+t.TempDir()+"/h.db?_pragma=journal_mode(WAL)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	h, err := sqliteHandle(conn)
	if err != nil || h == 0 {
		t.Fatalf("handle: %x %v", h, err)
	}
	// the handle is the live connection: an interrupt shows on it
	tls := libc.NewTLS()
	defer tls.Close()
	sqlite3.Xsqlite3_interrupt(tls, h)
	if sqlite3.Xsqlite3_is_interrupted(tls, h) == 0 {
		t.Fatal("interrupt not visible on the handle")
	}
	// the next statement clears it, as the interrupter relies on
	var one int
	if err := conn.QueryRowContext(ctx, `SELECT 1`).Scan(&one); err != nil {
		t.Fatalf("statement after a stale interrupt: %v", err)
	}
}

// After stop returns, no interrupt reaches the connection: the ROLLBACK
// and the next user of the connection run normally.
func TestInterrupterStop(t *testing.T) {
	db, err := sql.Open("sqlite", "file:"+t.TempDir()+"/h.db?_pragma=journal_mode(WAL)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	h, err := sqliteHandle(conn)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		it := watchInterrupt(ctx, h)
		tx, err := conn.BeginTx(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		rows, err := tx.QueryContext(ctx, gapSQL)
		if err != nil {
			t.Fatal(err)
		}
		n := 0
		go func() { time.Sleep(50 * time.Millisecond); cancel() }()
		for rows.Next() {
			n++
		}
		if n != 5 || rows.Err() == nil {
			t.Fatalf("rows=%d err=%v: want 5 rows, then an interrupt", n, rows.Err())
		}
		rows.Close()
		it.stop()
		time.Sleep(3 * interruptEvery) // a late repeat would land here
		if err := tx.Rollback(); err != nil {
			t.Fatalf("rollback after stop: %v", err)
		}
		var c int64
		if err := conn.QueryRowContext(context.Background(),
			`WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c LIMIT 20000) SELECT count(*) FROM c`).Scan(&c); err != nil {
			t.Fatalf("statement after stop: %v", err)
		}
	}
	// an interrupter stopped before its context ends never fires
	ctx, cancel := context.WithCancel(context.Background())
	it := watchInterrupt(ctx, h)
	it.stop()
	cancel()
	time.Sleep(3 * interruptEvery)
	tls := libc.NewTLS()
	defer tls.Close()
	if sqlite3.Xsqlite3_is_interrupted(tls, h) != 0 {
		t.Fatal("stopped interrupter fired")
	}
}

// An endless recursive CTE that yields no row is stopped at the deadline.
func TestReadTimeoutEndlessCTE(t *testing.T) {
	_, ten, met := guardTenant(t, guardOpts())
	t0 := time.Now()
	_, err := ten.SubmitQueryCtx(context.Background(), Statement{SQL: slowSQL})
	expectTimeout(t, err, time.Since(t0))
	slotsFree(t, ten, met)
	if met.ReadTimeouts.Load() != 1 {
		t.Fatalf("read timeouts metric = %d", met.ReadTimeouts.Load())
	}
}

// A query that already returned rows is stopped inside a later step. The
// sixth row of gapSQL is out of reach, so only an interrupt ends it.
func TestReadTimeoutMidStream(t *testing.T) {
	_, ten, met := guardTenant(t, guardOpts())
	for _, q := range []string{gapSQL, trickleSQL} {
		t0 := time.Now()
		res, err := ten.SubmitQueryCtx(context.Background(), Statement{SQL: q})
		expectTimeout(t, err, time.Since(t0))
		if len(res.Rows) != 0 {
			t.Fatalf("partial result returned: %d rows", len(res.Rows))
		}
		slotsFree(t, ten, met)
	}
	// the connection pool is healthy afterwards
	if n := mustQuery(t, nil2m(t, ten), "t1", `SELECT 1`).Rows[0][0].(int64); n != 1 {
		t.Fatal("bad read after timeouts")
	}
}

// nil2m returns the manager of ten for the mustQuery helper.
func nil2m(t *testing.T, ten *Tenant) *Manager {
	t.Helper()
	m, ok := ten.clock.(*Manager)
	if !ok {
		t.Fatal("tenant without manager")
	}
	return m
}

// Client cancel and checkpoint preemption also stop a later step now.
// Before, both ended only at the next row (Q1 known limit).
func TestCancelStopsLaterStep(t *testing.T) {
	o := guardOpts()
	o.QueryTimeout = time.Minute // only the cancel may stop it
	m, ten, met := guardTenant(t, o)

	ctx, cancel := context.WithCancel(context.Background())
	ch := goQuery(ctx, ten, gapSQL)
	waitFor(t, "read pinned", func() bool { return ten.readers.count() == 1 })
	time.Sleep(100 * time.Millisecond)
	cancel()
	if r := recvRead(t, ch, 5*time.Second); !errors.Is(r.err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", r.err)
	}
	slotsFree(t, ten, met)

	// a checkpoint preempts the read and then completes (no skip)
	mustTx(t, m, "t1", "c", "w1", insertStmt(1, "a"))
	ch = goQuery(context.Background(), ten, gapSQL)
	waitFor(t, "read pinned", func() bool { return ten.readers.count() == 1 })
	gen := onActor(ten, func() int64 { return ten.mf.Gen })
	if !checkpointNow(ten) {
		t.Fatal("checkpoint failed")
	}
	if r := recvRead(t, ch, 5*time.Second); !errors.Is(r.err, ErrReadPreempted) {
		t.Fatalf("want ErrReadPreempted, got %v", r.err)
	}
	if g := onActor(ten, func() int64 { return ten.mf.Gen }); g == gen {
		t.Fatal("checkpoint skipped: the read did not stop")
	}
}

func TestReadOnlyTxTimeout(t *testing.T) {
	_, ten, met := guardTenant(t, guardOpts())
	t0 := time.Now()
	_, _, err := ten.SubmitTxCtx(context.Background(), TxRequest{Client: "c", TxID: "r1",
		Stmts: []Statement{{SQL: `SELECT 1`}, {SQL: gapSQL}}})
	expectTimeout(t, err, time.Since(t0))
	slotsFree(t, ten, met)
}

// walState returns the WAL file size and the committed end.
func walState(t *testing.T, ten *Tenant) (size, commitEnd, submitted int64) {
	t.Helper()
	type st struct{ size, end, sub int64 }
	s := onActor(ten, func() st {
		end, err := ten.currentCommitEnd()
		if err != nil {
			t.Error(err)
		}
		return st{ten.walFileSize(), end, ten.submittedOff}
	})
	return s.size, s.end, s.sub
}

// A write that runs past WriteTimeout rolls back: no commit, no receipt,
// no ack, no shipped bytes. Its spilled WAL frames stay beyond the commit
// end and never reach S3. The actor serves the next write at once, and
// recovery from S3 shows only committed state.
func TestWriteTimeoutRollsBack(t *testing.T) {
	fs := NewFakeStore()
	met := &Metrics{}
	o := guardOpts()
	// long enough for the bounded insert below to spill under -race
	o.WriteTimeout = 3 * time.Second
	m := newReadTestManager(t, fs, o, met)
	ten, err := m.Acquire(context.Background(), "t1", true)
	if err != nil {
		t.Fatal(err)
	}
	makeSchema(t, m, "t1")
	mustTx(t, m, "t1", "c", "big", Statement{SQL: `CREATE TABLE big(x INTEGER, b BLOB)`})
	_, end0, sub0 := walState(t, ten)
	puts0 := met.SegPuts.Load()

	t0 := time.Now()
	_, _, err = ten.SubmitTx(TxRequest{Client: "c", TxID: "slow",
		Stmts: []Statement{insertStmt(1, "lost"),
			// about 6 MB, more than the page cache: the transaction spills
			{SQL: `INSERT INTO big SELECT x, randomblob(2000) FROM
				(WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c LIMIT 3000) SELECT x FROM c)`},
			{SQL: endlessInsertSQL}}})
	expectTimeout(t, err, time.Since(t0))
	if met.WriteTimeouts.Load() != 1 {
		t.Fatalf("write timeouts metric = %d", met.WriteTimeouts.Load())
	}
	size, end, sub := walState(t, ten)
	if end != end0 || sub != sub0 || met.SegPuts.Load() != puts0 {
		t.Fatalf("timed-out write moved the WAL: end %d->%d submitted %d->%d puts %d->%d",
			end0, end, sub0, sub, puts0, met.SegPuts.Load())
	}
	if size <= end {
		t.Fatalf("no spilled frames (size %d, commit end %d): the test does not cover a WAL rollback", size, end)
	}
	t.Logf("spilled WAL bytes beyond the commit end: %d", size-end)

	// no receipt: the same txid runs again and commits
	t1 := time.Now()
	res, dup, err := ten.SubmitTx(TxRequest{Client: "c", TxID: "slow", Stmts: []Statement{insertStmt(2, "kept")}})
	if err != nil || dup || len(res) != 1 || res[0].Affected != 1 {
		t.Fatalf("retry of the timed-out txid: res=%v dup=%v err=%v", res, dup, err)
	}
	if d := time.Since(t1); d > 2*time.Second {
		t.Fatalf("actor busy after the timeout: next write took %v", d)
	}
	if got := mustQuery(t, m, "t1", `SELECT group_concat(v) FROM kv`).Rows[0][0]; got != "kept" {
		t.Fatalf("kv = %v, want only the committed row", got)
	}
	if n := mustQuery(t, m, "t1", `SELECT count(*) FROM big`).Rows[0][0].(int64); n != 0 {
		t.Fatalf("big has %d rows from the timed-out write", n)
	}

	// recovery from S3 replays only committed frames
	m.KillLease()
	m2 := newReadTestManager(t, fs, o, &Metrics{})
	t.Cleanup(m2.ShutdownAll)
	if got := mustQuery(t, m2, "t1", `SELECT group_concat(v) FROM kv`).Rows[0][0]; got != "kept" {
		t.Fatalf("after recovery kv = %v", got)
	}
	if n := mustQuery(t, m2, "t1", `SELECT count(*) FROM big`).Rows[0][0].(int64); n != 0 {
		t.Fatalf("after recovery big has %d rows", n)
	}
	mustTx(t, m2, "t1", "c", "after", insertStmt(3, "x"))
}

// A SELECT inside a write transaction that hangs in a later step is
// stopped by the interrupter (the driver does not see the deadline
// there). The earlier INSERT rolls back.
func TestWriteTimeoutLaterStep(t *testing.T) {
	m, ten, _ := guardTenant(t, guardOpts())
	t0 := time.Now()
	_, _, err := ten.SubmitTx(TxRequest{Client: "c", TxID: "w",
		Stmts: []Statement{insertStmt(1, "lost"), {SQL: gapSQL}}})
	expectTimeout(t, err, time.Since(t0))
	if n := countRows(t, m, "t1"); n != 0 {
		t.Fatalf("rolled-back insert visible: %d rows", n)
	}
	mustTx(t, m, "t1", "c", "w", insertStmt(1, "ok"))
	if n := countRows(t, m, "t1"); n != 1 {
		t.Fatalf("rows = %d", n)
	}
}

// Writes that finish inside the deadline are not affected, and a dedup
// replay still answers.
func TestWriteWithinTimeout(t *testing.T) {
	m, ten, met := guardTenant(t, guardOpts())
	for i := 0; i < 20; i++ {
		mustTx(t, m, "t1", "c", fmt.Sprintf("k%d", i), insertStmt(int64(i), "v"))
	}
	_, dup, err := ten.SubmitTx(TxRequest{Client: "c", TxID: "k3", Stmts: []Statement{insertStmt(3, "v")}})
	if err != nil || !dup {
		t.Fatalf("dedup replay: dup=%v err=%v", dup, err)
	}
	if met.WriteTimeouts.Load() != 0 {
		t.Fatal("spurious write timeout")
	}
}

func TestResultRowLimit(t *testing.T) {
	o := guardOpts()
	o.MaxResultRows = 10
	m, ten, met := guardTenant(t, o)
	series := func(n int) string {
		return fmt.Sprintf(`WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c LIMIT %d) SELECT x FROM c`, n)
	}
	if res := mustQuery(t, m, "t1", series(10)); len(res.Rows) != 10 {
		t.Fatalf("rows = %d", len(res.Rows))
	}
	_, err := ten.SubmitQuery(Statement{SQL: series(11)})
	if !errors.Is(err, ErrResultTooLarge) {
		t.Fatalf("11 rows: want ErrResultTooLarge, got %v", err)
	}
	// the budget covers the whole read-only /tx
	_, _, err = ten.SubmitTx(TxRequest{Client: "c", TxID: "r", Stmts: []Statement{{SQL: series(6)}, {SQL: series(6)}}})
	if !errors.Is(err, ErrResultTooLarge) {
		t.Fatalf("read-only tx 6+6 rows: want ErrResultTooLarge, got %v", err)
	}
	// a write transaction has no row limit (guard rows of large commits),
	// only the byte limit (TestResultByteLimit)
	var stmts []Statement
	for i := 0; i < 12; i++ {
		one := int64(0)
		stmts = append(stmts, Statement{SQL: `SELECT count(*) FROM kv`, ExpectScalarInt: &one})
	}
	stmts = append(stmts, Statement{SQL: `WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c LIMIT 11)
			INSERT INTO kv(k, v) SELECT x, 'v' FROM c WHERE true RETURNING k`})
	if _, _, err = ten.SubmitTx(TxRequest{Client: "c", TxID: "w", Stmts: stmts}); err != nil {
		t.Fatalf("write with 23 result rows: %v", err)
	}
	if n := countRows(t, m, "t1"); n != 11 {
		t.Fatalf("write rows = %d", n)
	}
	if met.ResultTooLarge.Load() != 2 {
		t.Fatalf("result too large metric = %d", met.ResultTooLarge.Load())
	}
	slotsFree(t, ten, met)
}

func TestResultByteLimit(t *testing.T) {
	o := guardOpts()
	o.MaxResultBytes = 4096
	m, ten, _ := guardTenant(t, o)
	if res := mustQuery(t, m, "t1", `SELECT hex(zeroblob(1000))`); len(res.Rows) != 1 {
		t.Fatal("small result refused")
	}
	for _, q := range []string{
		`SELECT hex(zeroblob(4000))`, // one large text value
		`SELECT zeroblob(5000)`,      // one large blob
		// many small values
		`WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c LIMIT 200) SELECT x FROM c`,
	} {
		if _, err := ten.SubmitQuery(Statement{SQL: q}); !errors.Is(err, ErrResultTooLarge) {
			t.Fatalf("%s: want ErrResultTooLarge, got %v", q, err)
		}
	}
	// a write whose RETURNING rows pass the byte limit rolls back
	_, _, err := ten.SubmitTx(TxRequest{Client: "c", TxID: "w", Stmts: []Statement{{
		SQL: `WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c LIMIT 50)
			INSERT INTO kv(k, v) SELECT x, hex(zeroblob(100)) FROM c WHERE true RETURNING v`}}})
	if !errors.Is(err, ErrResultTooLarge) {
		t.Fatalf("write over the byte limit: want ErrResultTooLarge, got %v", err)
	}
	if n := countRows(t, m, "t1"); n != 0 {
		t.Fatalf("refused write left %d rows", n)
	}
	if _, dup, err := ten.SubmitTx(TxRequest{Client: "c", TxID: "w", Stmts: []Statement{insertStmt(1, "v")}}); err != nil || dup {
		t.Fatalf("txid of the refused write: dup=%v err=%v", dup, err)
	}
}

// The estimate is at least the JSON size of a whole response made of
// numbers, timestamps and plain text: containers, column names, rows and
// values are all charged.
func TestResultLimitEstimate(t *testing.T) {
	vals := []any{int64(-9223372036854775808), -1.2345678901234567e-300, nil, "hello", []byte("blob"),
		time.Date(2026, 10, 30, 11, 22, 33, 123456789, time.FixedZone("x", 3600))}
	cols := []string{"a", "bb", "ccc", "d", "e", "f"}
	for _, nrows := range []int{0, 1, 3} {
		l := &resultLimit{}
		res := []QueryResult{{Affected: -1 << 63}} // a write statement: no cols, no rows
		if err := l.addResult(nil); err != nil {
			t.Fatal(err)
		}
		qr := QueryResult{Cols: cols, Affected: -1 << 63}
		if err := l.addResult(cols); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < nrows; i++ {
			if err := l.addRow(vals); err != nil {
				t.Fatal(err)
			}
			conv := make([]any, len(vals))
			for i, v := range vals {
				conv[i] = jsonValue(v)
			}
			qr.Rows = append(qr.Rows, conv)
		}
		res = append(res, qr)
		js, _ := json.Marshal(res)
		if l.bytes < int64(len(js)) {
			t.Fatalf("%d rows: estimate %d below JSON size %d: %s", nrows, l.bytes, len(js), js)
		}
	}
	off := &resultLimit{} // zero limits: no limit
	for i := 0; i < 1000; i++ {
		if err := off.addRow(vals); err != nil {
			t.Fatal(err)
		}
	}
}

// Empty results are charged too: column metadata alone can pass the
// byte budget (review of Q2, finding 2).
func TestResultLimitEmptyResults(t *testing.T) {
	o := guardOpts()
	o.MaxResultBytes = 512
	_, ten, met := guardTenant(t, o)
	label := strings.Repeat("label", 60) // 300 bytes
	if _, err := ten.SubmitQuery(Statement{SQL: `SELECT 1 AS "` + label + `" WHERE 0`}); err != nil {
		t.Fatalf("one empty result within budget: %v", err)
	}
	_, err := ten.SubmitQuery(Statement{SQL: `SELECT 1 AS "` + label + `", 2 AS "` + label + `x" WHERE 0`})
	if !errors.Is(err, ErrResultTooLarge) {
		t.Fatalf("column names over budget: want ErrResultTooLarge, got %v", err)
	}
	// many empty results in one read-only /tx share the budget
	var stmts []Statement
	for i := 0; i < 10; i++ {
		stmts = append(stmts, Statement{SQL: `SELECT 1 AS x WHERE 0`})
	}
	if _, _, err := ten.SubmitTx(TxRequest{Client: "c", TxID: "r", Stmts: stmts}); !errors.Is(err, ErrResultTooLarge) {
		t.Fatalf("10 empty results: want ErrResultTooLarge, got %v", err)
	}
	// and so do write statements without rows
	stmts = stmts[:0]
	for i := 0; i < 10; i++ {
		stmts = append(stmts, insertStmt(int64(i), "v"))
	}
	if _, _, err := ten.SubmitTx(TxRequest{Client: "c", TxID: "w", Stmts: stmts}); !errors.Is(err, ErrResultTooLarge) {
		t.Fatalf("10 inserts: want ErrResultTooLarge, got %v", err)
	}
	if met.ResultTooLarge.Load() != 3 {
		t.Fatalf("result too large metric = %d", met.ResultTooLarge.Load())
	}
}

func TestGuardOptDefaults(t *testing.T) {
	if guardOpt(0, 5) != 5 || guardOpt(-1, 5) != 0 || guardOpt(7, 5) != 7 {
		t.Fatal("guardOpt")
	}
	o := guardOpts()
	o.QueryTimeout = -1
	o.MaxResultRows = -1
	_, ten, _ := guardTenant(t, o)
	if ten.opts.QueryTimeout != 0 || ten.opts.MaxResultRows != 0 ||
		ten.opts.WriteTimeout != o.WriteTimeout || ten.opts.MaxResultBytes != defaultMaxResultBytes {
		t.Fatalf("normalized opts: %+v", ten.opts)
	}
}

// Serial reads reuse one worker; concurrent reads never start more
// workers than MaxReaders (Q1 review, Low).
func TestReaderPoolBound(t *testing.T) {
	m, ten, _ := guardTenant(t, guardOpts())
	poolState := func() (int, int) {
		ten.pool.mu.Lock()
		defer ten.pool.mu.Unlock()
		return ten.pool.free, ten.pool.queued
	}
	for i := 0; i < 200; i++ {
		mustQuery(t, m, "t1", `SELECT 1`)
	}
	if free, q := poolState(); free != 1 || q != 0 {
		t.Fatalf("after serial reads: %d workers, %d queued; want 1, 0", free, q)
	}
	// concurrent bursts
	for round := 0; round < 20; round++ {
		errs := make(chan error, 16)
		for i := 0; i < 16; i++ {
			go func() {
				_, err := ten.SubmitQuery(Statement{SQL: `SELECT count(*) FROM sqlite_master`})
				errs <- err
			}()
		}
		for i := 0; i < 16; i++ {
			if err := <-errs; err != nil {
				t.Fatal(err)
			}
		}
	}
	waitFor(t, "workers free", func() bool { _, q := poolState(); return ten.readers.count() == 0 && q == 0 })
	if free, _ := poolState(); free < 1 || free > ten.opts.MaxReaders {
		t.Fatalf("%d workers, MaxReaders %d", free, ten.opts.MaxReaders)
	}
}

// HTTP mapping: a timeout is 504, a too large result is 413. Neither is
// 503 (the adapter would retry) nor 409/422 (guard failure, SQL error).
func TestGuardHTTPStatus(t *testing.T) {
	o := guardOpts()
	o.MaxResultRows = 5 // gapSQL returns 5 rows before it hangs
	m, _, _ := guardTenant(t, o)
	srv := httptest.NewServer(NewServer(m, nil).Handler())
	t.Cleanup(srv.Close)
	post := func(path string, body any) (int, apiError) {
		b, _ := json.Marshal(body)
		resp, err := http.Post(srv.URL+path, "application/json", bytes.NewReader(b))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var ae apiError
		json.NewDecoder(resp.Body).Decode(&ae)
		return resp.StatusCode, ae
	}
	cases := []struct {
		path string
		body any
		code int
		kind string
	}{
		{"/v1/t/t1/query", Statement{SQL: gapSQL}, 504, "query_timeout"},
		{"/v1/t/t1/tx", TxRequest{Client: "c", TxID: "r", Stmts: []Statement{{SQL: gapSQL}}}, 504, "query_timeout"},
		{"/v1/t/t1/tx", TxRequest{Client: "c", TxID: "w", Stmts: []Statement{insertStmt(1, "a"), {SQL: gapSQL}}}, 504, "query_timeout"},
		{"/v1/t/t1/query", Statement{SQL: `VALUES (1),(2),(3),(4),(5),(6)`}, 413, "result_too_large"},
		{"/v1/t/t1/tx", TxRequest{Client: "c", TxID: "r2", Stmts: []Statement{{SQL: `VALUES (1),(2),(3),(4),(5),(6)`}}}, 413, "result_too_large"},
	}
	for _, c := range cases {
		code, ae := post(c.path, c.body)
		if code != c.code || ae.Code != c.kind {
			t.Fatalf("%s %+v: got %d %+v, want %d %s", c.path, c.body, code, ae, c.code, c.kind)
		}
	}
}

// A closed tenant returns the tenant slot of a non-durable outcome in both
// schedules: the outcome was queued in readDone before the close, or the
// worker finished after it (review of Q2, finding 3).
func TestNonDurableCloseReturnsSlots(t *testing.T) {
	for _, finishBeforeClose := range []bool{true, false} {
		name := "worker-after-close"
		if finishBeforeClose {
			name = "queued-before-close"
		}
		t.Run(name, func(t *testing.T) {
			fs := NewFakeStore()
			met := &Metrics{}
			m := newReadTestManager(t, fs, guardOpts(), met)
			t.Cleanup(m.ShutdownAll)
			ten, err := m.Acquire(context.Background(), "t1", true)
			if err != nil {
				t.Fatal(err)
			}
			makeSchema(t, m, "t1")
			upload := make(chan struct{})
			defer close(upload)
			fs.FailPutSegment = func(string) bool { <-upload; return false }
			wr := make(chan error, 1)
			go func() {
				_, _, e := ten.SubmitTx(TxRequest{Client: "c", TxID: "w", Stmts: []Statement{insertStmt(1, "pending")}})
				wr <- e
			}()
			waitFor(t, "unshipped write", func() bool {
				return onActor(ten, func() bool { end, _ := ten.currentCommitEnd(); return end > ten.durableOff })
			})
			hook, pinned, workerGate := blockingHook()
			ten.readHook = hook
			rd := goQuery(context.Background(), ten, `SELECT count(*) FROM kv`)
			waitPinned(t, pinned)
			actorEntered, actorGate, actorDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
			ten.ctlCh <- func() { close(actorEntered); <-actorGate; ten.failLocked(ErrClosed); close(actorDone) }
			<-actorEntered
			if finishBeforeClose {
				close(workerGate)
				waitFor(t, "queued outcome", func() bool { return len(ten.readDone) == 1 })
			}
			close(actorGate)
			<-actorDone
			if !finishBeforeClose {
				close(workerGate)
			}
			if r := recvRead(t, rd, time.Second); r.err == nil {
				t.Fatal("non-durable success after close")
			}
			<-wr
			waitFor(t, "slots returned after close", func() bool {
				ten.pool.mu.Lock()
				defer ten.pool.mu.Unlock()
				return ten.pool.free == 0 && ten.pool.queued == 0 && met.QueriesInflight.Load() == 0 &&
					len(ten.readSlots) == 0 && len(ten.readDone) == 0
			})
		})
	}
}

// The commit decision happens after the statements: a transaction whose
// statements and receipt all succeeded but whose deadline passed before
// the decision rolls back, writes no receipt, and its txid runs again
// (review of Q2, finding 4). txHook models a scheduler pause there.
func TestWriteDeadlineAfterStatements(t *testing.T) {
	m, ten, met := guardTenant(t, guardOpts())
	onActor(ten, func() bool {
		ten.opts.WriteTimeout = 30 * time.Millisecond
		ten.txHook = func() { time.Sleep(60 * time.Millisecond) }
		return true
	})
	req := TxRequest{Client: "c", TxID: "boundary", Stmts: []Statement{insertStmt(1, "must-rollback")}}
	_, _, err := ten.SubmitTx(req)
	onActor(ten, func() bool { ten.txHook = nil; ten.opts.WriteTimeout = guardOpts().WriteTimeout; return true })
	if !errors.Is(err, ErrQueryTimeout) {
		t.Fatalf("finished statements past the deadline: want ErrQueryTimeout, got %v", err)
	}
	if met.WriteTimeouts.Load() != 1 {
		t.Fatalf("write timeouts metric = %d", met.WriteTimeouts.Load())
	}
	if n := countRows(t, m, "t1"); n != 0 {
		t.Fatalf("timed-out row visible: %d rows", n)
	}
	receipts := onActor(ten, func() (n int64) {
		ten.db.QueryRow(`SELECT count(*) FROM _hw_dedup WHERE txid='boundary'`).Scan(&n)
		return n
	})
	if receipts != 0 {
		t.Fatalf("receipt of the timed-out tx recorded")
	}
	if _, dup, err := ten.SubmitTx(req); err != nil || dup {
		t.Fatalf("retry: dup=%v err=%v", dup, err)
	}
	if n := countRows(t, m, "t1"); n != 1 {
		t.Fatalf("retry rows = %d", n)
	}
}

// A dedup hit replays the stored result as it is, also when the result
// limit was lowered since (I2). A fresh transaction meets the new limit.
func TestDedupReplayIgnoresNewLimit(t *testing.T) {
	_, ten, _ := guardTenant(t, guardOpts())
	req := TxRequest{Client: "c", TxID: "large", Stmts: []Statement{insertStmt(1, "v"), {SQL: `VALUES (1),(2),(3),(4),(5)`}}}
	if _, _, err := ten.SubmitTx(req); err != nil {
		t.Fatal(err)
	}
	onActor(ten, func() bool { ten.opts.MaxResultBytes = 200; return true })
	res, dup, err := ten.SubmitTx(req)
	if err != nil || !dup || len(res) != 2 || len(res[1].Rows) != 5 {
		t.Fatalf("replay: dup=%v err=%v res=%v", dup, err, res)
	}
	fresh := TxRequest{Client: "c", TxID: "fresh", Stmts: []Statement{insertStmt(2, "v"), {SQL: `VALUES (1),(2),(3),(4),(5)`}}}
	if _, _, err := ten.SubmitTx(fresh); !errors.Is(err, ErrResultTooLarge) {
		t.Fatalf("fresh write: want ErrResultTooLarge, got %v", err)
	}
}

// The handle lookup works with the linked driver, and /info and /metrics
// say so; a failed lookup is counted.
func TestInterruptSupportVisible(t *testing.T) {
	if err := interruptSupport(); err != nil {
		t.Fatalf("interrupt unavailable with the linked driver: %v", err)
	}
	m, ten, met := guardTenant(t, guardOpts())
	if v, ok := m.Stats()["sqlite_interrupt"].(bool); !ok || !v {
		t.Fatalf("/info sqlite_interrupt = %v", m.Stats()["sqlite_interrupt"])
	}
	srv := httptest.NewServer(NewServer(m, nil).Handler())
	t.Cleanup(srv.Close)
	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	var b bytes.Buffer
	b.ReadFrom(resp.Body)
	resp.Body.Close()
	for _, want := range []string{"hoglite_sqlite_interrupt_available 1\n", "hoglite_interrupt_handle_misses_total 0\n"} {
		if !strings.Contains(b.String(), want) {
			t.Fatalf("metrics lack %q", want)
		}
	}
	// the handle of a sqlite connection is found and not counted
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	c, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if h := ten.connHandle(c); h == 0 || met.InterruptMisses.Load() != 0 {
		t.Fatalf("connHandle = %x, misses %d", h, met.InterruptMisses.Load())
	}
	// a connection without the expected field (as after a driver change)
	// gives no handle and is counted
	other := sql.OpenDB(noHandleConnector{})
	defer other.Close()
	oc, err := other.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer oc.Close()
	if h := ten.connHandle(oc); h != 0 || met.InterruptMisses.Load() != 1 {
		t.Fatalf("connHandle = %x, misses %d", h, met.InterruptMisses.Load())
	}
}

// noHandleConnector yields driver connections that are not sqlite's.
type noHandleConnector struct{}

type noHandleConn struct{ db uintptr }

func (noHandleConnector) Connect(context.Context) (driver.Conn, error) {
	return &noHandleConn{db: 1}, nil
}
func (noHandleConnector) Driver() driver.Driver           { return nil }
func (*noHandleConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("no") }
func (*noHandleConn) Close() error                        { return nil }
func (*noHandleConn) Begin() (driver.Tx, error)           { return nil, errors.New("no") }
