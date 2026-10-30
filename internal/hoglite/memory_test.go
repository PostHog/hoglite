package hoglite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"strings"
	"testing"
	"time"
	"unsafe"

	"modernc.org/libc"
	sqlite3 "modernc.org/sqlite/lib"
)

// withHeapLimit sets the SQLite hard heap limit to the current use plus
// extra bytes (no soft limit) and restores the old limits when the test
// ends. The limit is process wide; tests of this package do not run in
// parallel.
func withHeapLimit(t *testing.T, extra int64) (restore func()) {
	t.Helper()
	used, _ := SQLiteMemory()
	return withHeapLimitAt(t, used+extra)
}

// withHeapLimitAt sets the hard heap limit to limit bytes. Below the
// current use, an allocation first frees unused page cache memory of all
// connections (sqlite3_release_memory) and fails only if that is not
// enough.
func withHeapLimitAt(t *testing.T, limit int64) (restore func()) {
	t.Helper()
	hard, soft := SQLiteHeapLimits()
	if err := SetSQLiteHeapLimits(limit, 0); err != nil {
		t.Fatal(err)
	}
	restored := false
	restore = func() {
		if !restored {
			restored = true
			SetSQLiteHeapLimits(hard, soft)
		}
	}
	t.Cleanup(restore)
	return restore
}

// bigStringSQL grows one string inside SQLite to about 100 MB in 10 KB
// steps (zeroblob keeps it cheap under -race).
const bigStringSQL = `SELECT length(group_concat(zeroblob(10000))) FROM
	(WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c LIMIT 10000) SELECT x FROM c)`

func expectNoMem(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, ErrOutOfMemory) {
		t.Fatalf("want ErrOutOfMemory, got %v", err)
	}
	if code, ae, ok := guardStatus(err); !ok || code != 507 || ae.Code != "out_of_memory" {
		t.Fatalf("status of %v: %d %+v", err, code, ae)
	}
}

// modernc v1.61.0 is built with SQLITE_DEFAULT_MEMSTATUS=0. Without the
// init switch, sqlite3_memory_used stays 0 and the heap limit pragmas do
// nothing. The switch must have worked in this binary.
func TestSQLiteMemStatus(t *testing.T) {
	if err := SQLiteMemStatus(); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`CREATE TABLE x(a)`); err != nil {
		t.Fatal(err)
	}
	if used, hw := SQLiteMemory(); used <= 0 || hw < used {
		t.Fatalf("sqlite memory used %d, highwater %d: memory statistics are off", used, hw)
	}
	restore := withHeapLimit(t, 16<<20)
	var n int64
	err = db.QueryRow(`SELECT length(randomblob(100000000))`).Scan(&n)
	if !isNoMem(err) {
		t.Fatalf("100 MB blob under a 16 MB headroom: n=%d err=%v", n, err)
	}
	restore()
	if err := db.QueryRow(`SELECT length(randomblob(1000))`).Scan(&n); err != nil || n != 1000 {
		t.Fatalf("connection after SQLITE_NOMEM: n=%d err=%v", n, err)
	}
}

// The SQLite heap lives outside the Go runtime (modernc.org/memory maps it
// with mmap). The Go memory limit therefore cannot bound it, and the Go
// limit must leave room for it (PlanMemory).
func TestSQLiteHeapOutsideGo(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	goTotal := func() int64 {
		s := []metrics.Sample{{Name: "/memory/classes/total:bytes"}}
		metrics.Read(s)
		return int64(s[0].Value.Uint64())
	}
	runtime.GC()
	go0 := goTotal()
	sq0, _ := SQLiteMemory()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	// the open cursor keeps the 64 MB string alive in SQLite
	rows, err := tx.Query(`SELECT length(zeroblob(1) || zeroblob(64000000))`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal(rows.Err())
	}
	sq1, _ := SQLiteMemory()
	go1 := goTotal()
	if sq1-sq0 < 60<<20 {
		t.Fatalf("sqlite heap grew by %d bytes only", sq1-sq0)
	}
	if go1-go0 > 32<<20 {
		t.Fatalf("Go runtime memory grew by %d MiB: the SQLite heap is visible to Go", (go1-go0)>>20)
	}
	t.Logf("sqlite heap +%d MiB, Go runtime memory +%d MiB", (sq1-sq0)>>20, (go1-go0)>>20)
}

// A read that needs more than the heap limit fails with ErrOutOfMemory
// (507). The tenant stays open, its slots return, and the next read works.
func TestHeapLimitFailsRead(t *testing.T) {
	m, ten, met := guardTenant(t, testOpts())
	mustTx(t, m, "t1", "c", "a", insertStmt(1, "a"))
	restore := withHeapLimit(t, 32<<20)
	_, err := ten.SubmitQuery(Statement{SQL: bigStringSQL})
	expectNoMem(t, err)
	// read-only /tx: the same rule on the reader path
	_, _, err = ten.SubmitTx(TxRequest{Client: "c", TxID: "r",
		Stmts: []Statement{{SQL: `SELECT count(*) FROM kv`}, {SQL: bigStringSQL}}})
	expectNoMem(t, err)
	restore()
	if n := met.OutOfMemory.Load(); n != 2 {
		t.Fatalf("out of memory metric = %d, want 2", n)
	}
	slotsFree(t, ten, met)
	if !ten.Alive() {
		t.Fatal("tenant closed after a failed read")
	}
	if got := countRows(t, m, "t1"); got != 1 {
		t.Fatalf("rows = %d", got)
	}
}

// A write transaction that hits the heap limit rolls back as a whole:
// no receipt, no ack, no shipped bytes, and frames it spilled into the
// WAL stay beyond the commit end. The tenant is not poisoned; the same
// txid commits on retry, and recovery from S3 shows only committed rows.
func TestHeapLimitRollsBackWrite(t *testing.T) {
	fs := NewFakeStore()
	met := &Metrics{}
	o := testOpts()
	o.CacheKiB = 512 // the 6 MB insert below spills
	o.WriteTimeout = time.Minute
	m := newReadTestManager(t, fs, o, met)
	ten, err := m.Acquire(context.Background(), "t1", true)
	if err != nil {
		t.Fatal(err)
	}
	makeSchema(t, m, "t1")
	mustTx(t, m, "t1", "c", "big", Statement{SQL: `CREATE TABLE big(x INTEGER, b BLOB)`})
	mustTx(t, m, "t1", "c", "k1", insertStmt(1, "kept"))
	waitFor(t, "acks", func() bool { _, end, sub := walState(t, ten); return end == sub })
	_, end0, sub0 := walState(t, ten)
	puts0 := met.SegPuts.Load()

	restore := withHeapLimit(t, 48<<20)
	_, _, err = ten.SubmitTx(TxRequest{Client: "c", TxID: "oom",
		Stmts: []Statement{insertStmt(2, "lost"),
			{SQL: `INSERT INTO big SELECT x, zeroblob(2000) FROM
				(WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c LIMIT 3000) SELECT x FROM c)`},
			{SQL: `INSERT INTO kv(v) ` + strings.Replace(bigStringSQL, "SELECT length(", "SELECT (", 1)}}})
	restore()
	expectNoMem(t, err)
	if !ten.Alive() {
		t.Fatal("tenant poisoned by a statement that hit the heap limit")
	}
	size, end, sub := walState(t, ten)
	if end != end0 || sub != sub0 || met.SegPuts.Load() != puts0 {
		t.Fatalf("failed write moved the WAL: end %d->%d submitted %d->%d puts %d->%d",
			end0, end, sub0, sub, puts0, met.SegPuts.Load())
	}
	if size <= end {
		t.Fatalf("no spilled frames (size %d, commit end %d): the test does not cover a WAL rollback", size, end)
	}
	res, dup, err := ten.SubmitTx(TxRequest{Client: "c", TxID: "oom", Stmts: []Statement{insertStmt(3, "retry")}})
	if err != nil || dup || res[0].Affected != 1 {
		t.Fatalf("retry of the failed txid: res=%v dup=%v err=%v", res, dup, err)
	}
	check := func(m *Manager) {
		t.Helper()
		if got := mustQuery(t, m, "t1", `SELECT group_concat(v, ',') FROM (SELECT v FROM kv ORDER BY k)`).Rows[0][0]; got != "kept,retry" {
			t.Fatalf("kv = %v", got)
		}
		if n := mustQuery(t, m, "t1", `SELECT count(*) FROM big`).Rows[0][0].(int64); n != 0 {
			t.Fatalf("big has %d rows from the failed write", n)
		}
	}
	check(m)
	m.KillLease()
	m2 := newReadTestManager(t, fs, o, &Metrics{})
	t.Cleanup(m2.ShutdownAll)
	check(m2)
}

// A COMMIT that fails without writing (a deferred foreign key) leaves the
// commit end unchanged: the client gets the error, the tenant stays open.
// Poisoning here would let any client fail a tenant over.
func TestFailedCommitWithoutFramesKeepsTenant(t *testing.T) {
	m, ten, met := guardTenant(t, testOpts())
	mustTx(t, m, "t1", "c", "fk",
		Statement{SQL: `CREATE TABLE parent(id INTEGER PRIMARY KEY)`},
		Statement{SQL: `CREATE TABLE child(pid INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)`})
	puts0 := met.SegPuts.Load()
	_, _, err := ten.SubmitTx(TxRequest{Client: "c", TxID: "bad",
		Stmts: []Statement{{SQL: `INSERT INTO child VALUES (42)`}}})
	if err == nil || !strings.Contains(err.Error(), "FOREIGN KEY") {
		t.Fatalf("want a deferred foreign key failure, got %v", err)
	}
	if !ten.Alive() {
		t.Fatal("tenant poisoned by a COMMIT that wrote nothing")
	}
	mustTx(t, m, "t1", "c", "good", Statement{SQL: `INSERT INTO parent VALUES (42)`}, Statement{SQL: `INSERT INTO child VALUES (42)`})
	if n := mustQuery(t, m, "t1", `SELECT count(*) FROM child`).Rows[0][0].(int64); n != 1 {
		t.Fatalf("child rows = %d", n)
	}
	if met.SegPuts.Load() > puts0+1 {
		t.Fatalf("segment puts %d -> %d", puts0, met.SegPuts.Load())
	}
}

// A COMMIT can fail with SQLITE_NOMEM after its frames and the commit
// frame reached the WAL file (here: the wal-index needs a new page for
// frame 4097 and more). SQLite reports an error, but the parser and the
// shipper read the file. The tenant must fail before a cut ships the
// frames: the client heard an error, and its retry must not find a
// receipt. Recovery from S3 shows the state before the transaction.
func TestFailedCommitWithFramesPoisons(t *testing.T) {
	fs := NewFakeStore()
	met := &Metrics{Logger: tLogger{t}}
	o := testOpts()
	o.CacheKiB = 128 << 10 // no spill: the COMMIT writes all frames
	o.WriteTimeout = time.Minute
	m := newReadTestManager(t, fs, o, met)
	ten, err := m.Acquire(context.Background(), "t1", true)
	if err != nil {
		t.Fatal(err)
	}
	makeSchema(t, m, "t1")
	mustTx(t, m, "t1", "c", "big", Statement{SQL: `CREATE TABLE big(x INTEGER, b BLOB)`})
	waitFor(t, "acks", func() bool { _, end, sub := walState(t, ten); return end == sub })
	_, end0, _ := walState(t, ten)
	puts0 := met.SegPuts.Load()
	restore := func() {}
	ten.txHook = func() { restore = withHeapLimit(t, 0) } // after the statements, before COMMIT
	_, _, err = ten.SubmitTx(TxRequest{Client: "c", TxID: "x", Stmts: []Statement{insertStmt(1, "lost"),
		{SQL: `INSERT INTO big SELECT x, zeroblob(3000) FROM
			(WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c LIMIT 6000) SELECT x FROM c)`}}})
	restore()
	expectNoMem(t, err)
	waitFor(t, "poison", func() bool { return !ten.Alive() })
	if met.SegPuts.Load() != puts0 {
		t.Fatalf("segment puts %d -> %d: the failed commit was shipped", puts0, met.SegPuts.Load())
	}
	if !strings.Contains(ten.deadError().Error(), "new commit frame") {
		t.Fatalf("poison cause = %v (commit end before: %d)", ten.deadError(), end0)
	}
	m.KillLease()
	m2 := newReadTestManager(t, fs, o, &Metrics{})
	t.Cleanup(m2.ShutdownAll)
	if n := countRows(t, m2, "t1"); n != 0 {
		t.Fatalf("kv rows after recovery = %d", n)
	}
	mustTx(t, m2, "t1", "c", "x", insertStmt(1, "retry")) // no receipt: runs again
	if n := countRows(t, m2, "t1"); n != 1 {
		t.Fatalf("kv rows after retry = %d", n)
	}
}

type tLogger struct{ t *testing.T }

func (l tLogger) Printf(f string, a ...any) { l.t.Logf(f, a...) }

func TestCgroupMemoryLimit(t *testing.T) {
	cases := []struct {
		dir   string
		limit int64
		src   string // suffix of the source file
	}{
		{"v2", 2 << 30, "sys/fs/cgroup/memory.max"},
		{"v2-max", 0, ""},
		{"v2-nested", 1 << 30, "kubepods/pod1/memory.max"}, // the parent is tighter
		{"v2-missing", 512 << 20, "sys/fs/cgroup/memory.max"},
		{"v1", 3 << 30, "memory/memory.limit_in_bytes"},
		{"v1-unlimited", 0, ""},
		{"v1-nested", 768 << 20, "docker/abc/memory.limit_in_bytes"},
		{"none", 0, ""},
	}
	for _, c := range cases {
		t.Run(c.dir, func(t *testing.T) {
			limit, src, err := cgroupMemoryLimit(filepath.Join("testdata/cgroup", c.dir))
			if err != nil {
				t.Fatal(err)
			}
			if limit != c.limit || !strings.HasSuffix(src, c.src) || (c.src == "") != (src == "") {
				t.Fatalf("got %d from %q, want %d from ...%s", limit, src, c.limit, c.src)
			}
		})
	}
	if _, _, err := cgroupMemoryLimit("testdata/cgroup/v2-bad"); err == nil || !strings.Contains(err.Error(), "lots") {
		t.Fatalf("bad memory.max: err = %v", err)
	}
}

func TestParseCgroupLimit(t *testing.T) {
	for in, want := range map[string]int64{
		"max\n": 0, "": 0, "1073741824\n": 1 << 30, " 4096 ": 4096,
		"9223372036854771712": 0, "18446744073709551615": 0, "1152921504606846976": 0,
	} {
		if got, err := parseCgroupLimit(in); err != nil || got != want {
			t.Errorf("parseCgroupLimit(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	for _, in := range []string{"-1", "12k", "max max"} {
		if _, err := parseCgroupLimit(in); err == nil {
			t.Errorf("parseCgroupLimit(%q): no error", in)
		}
	}
}

func frac(v int64, f float64) int64 { return int64(float64(v) * f) }

func TestPlanMemory(t *testing.T) {
	const g = int64(1) << 30
	cases := []struct {
		name       string
		cg         int64
		env        string
		cfg        MemoryConfig
		goLim      int64
		hard, soft int64
		err        bool
	}{
		{"defaults", 10 * g, "", MemoryConfig{}, frac(10*g, 0.9) - frac(10*g, 0.4), frac(10*g, 0.4), frac(frac(10*g, 0.4), 0.8), false},
		{"GOMEMLIMIT wins", 10 * g, "3GiB", MemoryConfig{}, 0, frac(10*g, 0.4), frac(frac(10*g, 0.4), 0.8), false},
		{"no cgroup", 0, "", MemoryConfig{}, 0, 0, 0, false},
		{"no cgroup, explicit sqlite", 0, "", MemoryConfig{SQLiteHard: g}, 0, g, frac(g, 0.8), false},
		{"explicit sqlite and soft", 10 * g, "", MemoryConfig{SQLiteHard: 2 * g, SQLiteSoft: g}, frac(10*g, 0.9) - 2*g, 2 * g, g, false},
		{"soft above hard", 10 * g, "", MemoryConfig{SQLiteHard: g, SQLiteSoft: 2 * g}, frac(10*g, 0.9) - g, g, g, false},
		{"sqlite off", 10 * g, "", MemoryConfig{SQLiteHard: -1}, frac(10*g, 0.9), 0, 0, false},
		{"soft off", 10 * g, "", MemoryConfig{SQLiteSoft: -1}, frac(10*g, 0.9) - frac(10*g, 0.4), frac(10*g, 0.4), frac(10*g, 0.4), false},
		{"fraction", 10 * g, "", MemoryConfig{Fraction: 0.85, SQLiteFraction: 0.3}, frac(10*g, 0.85) - frac(10*g, 0.3), frac(10*g, 0.3), frac(frac(10*g, 0.3), 0.8), false},
		{"sqlite eats the Go heap", 10 * g, "", MemoryConfig{SQLiteHard: 9 * g}, 0, 0, 0, true},
		{"bad fractions", 10 * g, "", MemoryConfig{Fraction: 0.5, SQLiteFraction: 0.6}, 0, 0, 0, true},
		{"fraction above 1", 10 * g, "", MemoryConfig{Fraction: 1.5}, 0, 0, 0, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p, err := PlanMemory(c.cg, "fixture", c.env, c.cfg)
			if c.err {
				if err == nil {
					t.Fatalf("no error: %+v", p)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if p.GoLimit != c.goLim || p.SQLiteHard != c.hard || p.SQLiteSoft != c.soft {
				t.Fatalf("got go %d hard %d soft %d, want %d %d %d (%s)", p.GoLimit, p.SQLiteHard, p.SQLiteSoft, c.goLim, c.hard, c.soft, p)
			}
			if p.String() == "" || (c.env != "" && !strings.Contains(p.String(), "GOMEMLIMIT")) {
				t.Fatalf("log line %q", p)
			}
		})
	}
}

// ApplyMemoryPlan sets the Go soft limit and the SQLite limits; both are
// visible in the metrics and in /v1/info.
func TestApplyMemoryPlan(t *testing.T) {
	oldGo := debug.SetMemoryLimit(-1)
	t.Cleanup(func() { debug.SetMemoryLimit(oldGo) })
	hard, soft := SQLiteHeapLimits()
	t.Cleanup(func() { SetSQLiteHeapLimits(hard, soft) })
	p, err := PlanMemory(64<<30, "fixture", "", MemoryConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := ApplyMemoryPlan(p); err != nil {
		t.Fatal(err)
	}
	if got := goMemoryLimit(); got != p.GoLimit {
		t.Fatalf("Go memory limit %d, want %d", got, p.GoLimit)
	}
	if h, s := SQLiteHeapLimits(); h != p.SQLiteHard || s != p.SQLiteSoft {
		t.Fatalf("sqlite limits %d/%d, want %d/%d", h, s, p.SQLiteHard, p.SQLiteSoft)
	}
	m, _, met := guardTenant(t, testOpts())
	met.OutOfMemory.Add(3)
	srv := httptest.NewServer(NewServer(m, nil).Handler())
	t.Cleanup(srv.Close)
	get := func(path string) []byte {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return b
	}
	var info struct {
		Memory map[string]any `json:"memory"`
	}
	if err := json.Unmarshal(get("/v1/info"), &info); err != nil {
		t.Fatal(err)
	}
	if info.Memory["sqlite_heap_limit"] != float64(p.SQLiteHard) || info.Memory["go_memory_limit"] != float64(p.GoLimit) ||
		info.Memory["out_of_memory_errors"] != float64(3) || info.Memory["sqlite_memstatus"] != "ok" ||
		info.Memory["sqlite_memory_used"].(float64) <= 0 {
		t.Fatalf("/v1/info memory = %v", info.Memory)
	}
	metricsText := string(get("/metrics"))
	for _, want := range []string{
		fmt.Sprintf("hoglite_sqlite_heap_limit_bytes %d\n", p.SQLiteHard),
		fmt.Sprintf("hoglite_go_memory_limit_bytes %d\n", p.GoLimit),
		"hoglite_out_of_memory_total 3\n", "hoglite_sqlite_memory_used_bytes ", "hoglite_sqlite_memory_highwater_bytes ",
	} {
		if !strings.Contains(metricsText, want) {
			t.Fatalf("/metrics lacks %q", want)
		}
	}
}

// SQLITE_NOMEM is 507 out_of_memory on /query and /tx: not 503 (the
// adapter would retry), not 422 or 409 (constraint or guard failure).
func TestOutOfMemoryHTTPStatus(t *testing.T) {
	m, _, _ := guardTenant(t, testOpts())
	srv := httptest.NewServer(NewServer(m, nil).Handler())
	t.Cleanup(srv.Close)
	withHeapLimit(t, 32<<20)
	for _, c := range []struct {
		path string
		body any
	}{
		{"/v1/t/t1/query", Statement{SQL: bigStringSQL}},
		{"/v1/t/t1/tx", TxRequest{Client: "c", TxID: "r", Stmts: []Statement{{SQL: bigStringSQL}}}},
		{"/v1/t/t1/tx", TxRequest{Client: "c", TxID: "w", Stmts: []Statement{insertStmt(1, "a"), {SQL: bigStringSQL}}}},
	} {
		b, _ := json.Marshal(c.body)
		resp, err := http.Post(srv.URL+c.path, "application/json", bytes.NewReader(b))
		if err != nil {
			t.Fatal(err)
		}
		var ae apiError
		json.NewDecoder(resp.Body).Decode(&ae)
		resp.Body.Close()
		if resp.StatusCode != 507 || ae.Code != "out_of_memory" {
			t.Fatalf("%s: got %d %+v", c.path, resp.StatusCode, ae)
		}
	}
}

// cacheUsed returns the page cache bytes of the connection with handle h
// (SQLITE_DBSTATUS_CACHE_USED). The authorizer forbids PRAGMA cache_size,
// so the test checks the effect of the bound instead.
func cacheUsed(t *testing.T, h uintptr) int64 {
	t.Helper()
	tls := libc.NewTLS()
	defer tls.Close()
	// a package variable: the heap does not move, and C code can write it
	p := uintptr(unsafe.Pointer(&dbStatusOut[0]))
	if rc := sqlite3.Xsqlite3_db_status(tls, h, sqlite3.SQLITE_DBSTATUS_CACHE_USED, p, p+4, 0); rc != 0 {
		t.Fatalf("sqlite3_db_status: %d", rc)
	}
	return int64(dbStatusOut[0])
}

var dbStatusOut [2]int32

// CacheKiB bounds the page cache of the tenant connections. With
// SQLITE_ENABLE_MEMORY_MANAGEMENT all caches share one page group: a
// connection can take the pages of an idle one, so the bound holds for
// the sum, not per connection. After a scan of a 4 MB table on every
// connection, the tenant holds about connections x CacheKiB.
func TestCacheSizeApplied(t *testing.T) {
	for _, c := range []struct{ opt, kib int64 }{
		{0, defaultCacheKiB},
		{256, 256},
		{-1, 2000}, // SQLite default: -2000
	} {
		o := testOpts()
		o.CacheKiB = int(c.opt)
		m, ten, _ := guardTenant(t, o)
		mustTx(t, m, "t1", "c", "fill", Statement{SQL: `CREATE TABLE big(b BLOB)`},
			Statement{SQL: `INSERT INTO big SELECT zeroblob(1000) FROM
				(WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c LIMIT 4000) SELECT x FROM c)`})
		var conns []*sql.Conn
		take := func(db *sql.DB) {
			rc, err := db.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			conns = append(conns, rc)
		}
		take(ten.db)
		for i := 0; i < ten.opts.MaxReaders; i++ {
			take(ten.roDB)
		}
		for _, rc := range conns {
			var sum int64
			if err := rc.QueryRowContext(context.Background(), `SELECT sum(length(b)) FROM big`).Scan(&sum); err != nil || sum != 4000000 {
				t.Fatalf("scan: %d %v", sum, err)
			}
		}
		var total int64
		for _, rc := range conns {
			total += cacheUsed(t, ten.connHandle(rc))
			rc.Close()
		}
		n := int64(len(conns))
		if lo, hi := n*c.kib*700, n*c.kib*1300; total < lo || total > hi {
			t.Fatalf("CacheKiB %d: %d connections hold %d KiB of page cache, want %d..%d KiB", c.opt, n, total>>10, lo>>10, hi>>10)
		}
		m.ShutdownAll()
	}
}

// A checkpoint that hits the heap limit is skipped while the WAL still
// holds every shipped frame: the tenant stays open, and a later
// checkpoint folds the WAL.
func TestCheckpointNoMemSkips(t *testing.T) {
	fs := NewFakeStore()
	met := &Metrics{Logger: tLogger{t}}
	o := testOpts()
	o.CheckpointByte = 1 << 30
	o.MaxSegments = 1 << 20
	m := newReadTestManager(t, fs, o, met)
	ten, err := m.Acquire(context.Background(), "t1", true)
	if err != nil {
		t.Fatal(err)
	}
	makeSchema(t, m, "t1")
	for i := 0; i < 20; i++ {
		mustTx(t, m, "t1", "c", fmt.Sprint("k", i), insertStmt(int64(i), strings.Repeat("v", 5000)))
	}
	waitFor(t, "acks", func() bool { _, end, sub := walState(t, ten); return end == sub })
	ck0 := met.Checkpoints.Load()
	if size, _, _ := walState(t, ten); size < 100000 {
		t.Fatalf("WAL size %d: too small for the test", size)
	}
	restore := withHeapLimitAt(t, 64<<10)
	ok := onActor(ten, func() bool { return ten.checkpointLocked("") })
	restore()
	if !ok || !ten.Alive() {
		t.Fatalf("checkpoint under the heap limit: ok=%v alive=%v", ok, ten.Alive())
	}
	if met.Checkpoints.Load() != ck0 || met.OutOfMemory.Load() == 0 {
		t.Fatalf("checkpoints %d -> %d, out of memory %d: the limit did not stop the checkpoint",
			ck0, met.Checkpoints.Load(), met.OutOfMemory.Load())
	}
	if !onActor(ten, func() bool { return ten.checkpointLocked("") }) || met.Checkpoints.Load() != ck0+1 {
		t.Fatalf("checkpoint after the limit: checkpoints %d", met.Checkpoints.Load())
	}
	if size, _, _ := walState(t, ten); size != 0 {
		t.Fatalf("WAL size %d after the checkpoint", size)
	}
	m.KillLease()
	m2 := newReadTestManager(t, fs, o, &Metrics{})
	t.Cleanup(m2.ShutdownAll)
	if n := countRows(t, m2, "t1"); n != 20 {
		t.Fatalf("rows after recovery = %d", n)
	}
}
