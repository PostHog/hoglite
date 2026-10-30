package hoglite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"modernc.org/libc"
	"modernc.org/sqlite"
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
		// with mountinfo: the mount shows a subtree of the hierarchy
		{"v2-subtree", 256 << 20, "sys/fs/cgroup/child/memory.max"}, // not the mount root (review of Q3)
		{"v2-mountpoint", 384 << 20, "run/my cg/a/memory.max"},
		{"v1-subtree", 128 << 20, "memory/worker/memory.limit_in_bytes"},
		{"v2-dotdot", 640 << 20, "sys/fs/cgroup/memory.max"},
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

// ---- review of Q3: gaps and guards that the tests above did not pin ----

// disableLookaside turns off the lookaside allocator of connection h.
// Lookaside serves small allocations (for example those of ROLLBACK and
// BEGIN) outside the heap limit, so without this they do not fail.
func disableLookaside(h uintptr) error {
	tls := libc.NewTLS()
	defer tls.Close()
	va := libc.Xmalloc(tls, 24)
	defer libc.Xfree(tls, va)
	if rc := sqlite3.Xsqlite3_db_config(tls, h, sqlite3.SQLITE_DBCONFIG_LOOKASIDE, libc.VaList(va, uintptr(0), int32(0), int32(0))); rc != 0 {
		return fmt.Errorf("disable lookaside: rc %d", rc)
	}
	return nil
}

// noLookasideForTest disables lookaside on every connection that a
// tenant of this test opens. Its files are below the temp parent of t:
// all t.TempDir calls of one test share that parent, whatever the test
// name becomes in the path. hits counts the connections that the hook
// changed; tests assert it, so a hook that never matches fails.
func noLookasideForTest(t *testing.T) (hits func() int64) {
	marker := filepath.Dir(t.TempDir()) + string(os.PathSeparator)
	var n atomic.Int64
	sqlite.RegisterConnectionHook(func(c sqlite.ExecQuerierContext, dsn string) error {
		if !strings.Contains(dsn, marker) {
			return nil
		}
		v := reflect.ValueOf(c)
		if v.Kind() != reflect.Pointer || !v.Elem().FieldByName("db").IsValid() {
			return errNoHandle
		}
		if err := disableLookaside(uintptr(v.Elem().FieldByName("db").Uint())); err != nil {
			return err
		}
		n.Add(1)
		return nil
	})
	return n.Load
}

// NaN and infinite fractions fail every comparison; they must not pass
// the range check and give a huge Go limit (review of Q3, finding 4).
func TestPlanMemoryRejectsNonFinite(t *testing.T) {
	for _, f := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		for _, c := range []MemoryConfig{{Fraction: f}, {SQLiteFraction: f}, {Fraction: f, SQLiteHard: 1 << 20}} {
			for _, cg := range []int64{0, 1 << 30} {
				for _, env := range []string{"", "1GiB"} {
					if p, err := PlanMemory(cg, "fixture", env, c); err == nil {
						t.Errorf("cg %d env %q config %+v: no error, plan %s", cg, env, c, p)
					}
				}
			}
		}
	}
	for _, c := range []MemoryConfig{{Fraction: -0.1}, {SQLiteFraction: -0.1}, {Fraction: 1.01}, {Fraction: 0.5, SQLiteFraction: 0.5}} {
		if _, err := PlanMemory(0, "", "", c); err == nil {
			t.Errorf("config %+v: no error", c)
		}
	}
}

// Without memory counting, a plan with SQLite limits fails, and the Go
// limit and the SQLite limits stay as they were. The opt-out plan works.
func TestApplyMemoryPlanFailsClosed(t *testing.T) {
	saved := memStatusErr
	memStatusErr = errors.New("forced: counting off")
	t.Cleanup(func() { memStatusErr = saved })
	goBefore := debug.SetMemoryLimit(-1)
	t.Cleanup(func() { debug.SetMemoryLimit(goBefore) })
	hard, soft := SQLiteHeapLimits()
	p, err := PlanMemory(8<<30, "fixture", "", MemoryConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := ApplyMemoryPlan(p); err == nil {
		t.Fatal("plan with SQLite limits applied without memory counting")
	}
	if g := debug.SetMemoryLimit(-1); g != goBefore {
		t.Fatalf("Go limit changed to %d by a failed plan", g)
	}
	if h, s := SQLiteHeapLimits(); h != hard || s != soft {
		t.Fatalf("sqlite limits changed to %d/%d by a failed plan", h, s)
	}
	off, err := PlanMemory(8<<30, "fixture", "", MemoryConfig{SQLiteHard: -1})
	if err != nil {
		t.Fatal(err)
	}
	if err := ApplyMemoryPlan(off); err != nil {
		t.Fatalf("opt-out plan: %v", err)
	}
	if g := debug.SetMemoryLimit(-1); g != off.GoLimit {
		t.Fatalf("Go limit %d, want %d", g, off.GoLimit)
	}
}

// A package that opens SQLite in its init runs before hoglite's init in
// some binaries. Memory counting then cannot start, and StartMemoryCaps
// must fail instead of running without the cap (review of Q3, finding
// 3). The opt-out still starts. A separate process, because init order
// is per binary.
func TestStartMemoryCapsFailsClosed(t *testing.T) {
	if testing.Short() {
		t.Skip("builds a binary")
	}
	bin := filepath.Join(t.TempDir(), "earlyinit")
	gobin := filepath.Join(runtime.GOROOT(), "bin", "go")
	if out, err := exec.Command(gobin, "build", "-o", bin, "./testdata/earlyinit").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	run := func(hard string) string {
		cmd := exec.Command(bin, hard)
		for _, kv := range os.Environ() {
			if !strings.HasPrefix(kv, "GOMEMLIMIT=") {
				cmd.Env = append(cmd.Env, kv)
			}
		}
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("run: %v\n%s", err, out)
		}
		return string(out)
	}
	out := run(fmt.Sprint(64 << 20))
	if !strings.Contains(out, "SQLite already initialized") {
		t.Fatalf("the early package did not initialize SQLite first:\n%s", out)
	}
	if !strings.Contains(out, "start=memory: sqlite heap limit has no effect") ||
		!strings.Contains(out, "hard=0 soft=0 go=9223372036854775807") {
		t.Fatalf("explicit limit without counting:\n%s", out)
	}
	if out := run("-1"); !strings.Contains(out, "start=<nil>") {
		t.Fatalf("opt-out:\n%s", out)
	}
}

// SQLITE_NOMEM in each step of a read pin is 507 out_of_memory: the
// prepare of the pin probe (first read), the open of a pool connection
// (warm probe, no idle connection), and BEGIN (review of Q3, finding 2).
func TestPinOutOfMemoryHTTP(t *testing.T) {
	for _, path := range []string{"query", "tx"} {
		for _, step := range []string{"prepare", "connection", "begin"} {
			t.Run(path+"/"+step, func(t *testing.T) {
				hits := func() int64 { return 0 }
				if step == "begin" {
					hits = noLookasideForTest(t)
				}
				m, ten, met := guardTenant(t, testOpts())
				if step != "prepare" {
					mustQuery(t, m, "t1", "SELECT 1")
				}
				if step == "connection" {
					ten.roDB.SetMaxIdleConns(0)
				}
				openBefore := ten.roDB.Stats().OpenConnections
				srv := httptest.NewServer(NewServer(m, nil).Handler())
				defer srv.Close()
				var body []byte
				if path == "tx" {
					body, _ = json.Marshal(TxRequest{Client: "c", TxID: "r", Stmts: []Statement{{SQL: "SELECT 1"}}})
				} else {
					body, _ = json.Marshal(Statement{SQL: "SELECT 1"})
				}
				restore := withHeapLimitAt(t, 1)
				resp, err := http.Post(srv.URL+"/v1/t/t1/"+path, "application/json", bytes.NewReader(body))
				restore()
				if err != nil {
					t.Fatal(err)
				}
				defer resp.Body.Close()
				var ae apiError
				json.NewDecoder(resp.Body).Decode(&ae)
				slotsFree(t, ten, met)
				if resp.StatusCode != 507 || ae.Code != "out_of_memory" || met.OutOfMemory.Load() != 1 {
					t.Fatalf("got %d %+v, out of memory metric %d", resp.StatusCode, ae, met.OutOfMemory.Load())
				}
				if step == "begin" {
					// the warm pin opened no connection: the failure came
					// from BEGIN or the probe on a connection without
					// lookaside
					if hits() == 0 || ten.roDB.Stats().OpenConnections != openBefore {
						t.Fatalf("setup: lookaside hook hits %d, read connections %d -> %d",
							hits(), openBefore, ten.roDB.Stats().OpenConnections)
					}
				}
				if !ten.Alive() || countRows(t, m, "t1") != 0 {
					t.Fatal("tenant unusable after the failed pin")
				}
			})
		}
	}
}

// SQLITE_NOMEM in BEGIN keeps the tenant open, for a write and a read;
// the write's txid commits on retry.
func TestBeginOutOfMemoryKeepsTenant(t *testing.T) {
	hits := noLookasideForTest(t)
	m, ten, met := guardTenant(t, testOpts())
	mustQuery(t, m, "t1", "SELECT 1")
	if hits() < 2 {
		t.Fatalf("setup: lookaside hook hits %d, want the write and a read connection", hits())
	}
	restore := withHeapLimitAt(t, 1)
	_, _, we := ten.SubmitTx(TxRequest{Client: "c", TxID: "begin", Stmts: []Statement{insertStmt(1, "lost")}})
	_, re := ten.SubmitQuery(Statement{SQL: "SELECT 1"})
	restore()
	expectNoMem(t, we)
	expectNoMem(t, re)
	slotsFree(t, ten, met)
	if !ten.Alive() {
		t.Fatal("BEGIN failure poisoned the tenant")
	}
	mustTx(t, m, "t1", "c", "begin", insertStmt(1, "retry"))
	if n := countRows(t, m, "t1"); n != 1 {
		t.Fatalf("rows = %d", n)
	}
}

// COMMIT and ROLLBACK both fail: the write connection stays inside the
// transaction. endOpenTx must see it and fail the tenant, and nothing
// ships. Without the check the next BEGIN fails forever.
func TestRollbackOutOfMemoryPoisonsWrite(t *testing.T) {
	hits := noLookasideForTest(t)
	_, ten, met := guardTenant(t, testOpts())
	if hits() == 0 {
		t.Fatal("setup: lookaside hook never matched")
	}
	puts0 := met.SegPuts.Load()
	restore := func() {}
	onActor(ten, func() bool { ten.txHook = func() { restore = withHeapLimitAt(t, 1) }; return true })
	_, _, err := ten.SubmitTx(TxRequest{Client: "c", TxID: "rollback", Stmts: []Statement{insertStmt(1, "lost")}})
	restore()
	expectNoMem(t, err)
	waitFor(t, "poison", func() bool { return !ten.Alive() })
	if !strings.Contains(ten.deadError().Error(), "left inside a transaction") {
		t.Fatalf("poison cause = %v", ten.deadError())
	}
	if met.SegPuts.Load() != puts0 {
		t.Fatal("the failed write was shipped")
	}
}

// A read connection whose ROLLBACK failed is discarded, not returned to
// the pool, and a new connection replaces it.
func TestReadConnDiscardedAfterFailedRollback(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	c, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	h, err := sqliteHandle(c)
	if err != nil {
		t.Fatal(err)
	}
	if err := disableLookaside(h); err != nil {
		t.Fatal(err)
	}
	tx, err := c.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	restore := withHeapLimitAt(t, 1)
	err = tx.Rollback()
	stillOpen := !sqliteAutocommit(h)
	if isNoMem(err) && stillOpen {
		closeReadConn(c, h)
	}
	restore()
	if !isNoMem(err) || !stillOpen {
		t.Fatalf("setup: rollback = %v, transaction still open = %v", err, stillOpen)
	}
	if n := db.Stats().OpenConnections; n != 0 {
		t.Fatalf("the read connection went back to the pool: open = %d", n)
	}
	if _, err := db.Exec("SELECT 1"); err != nil {
		t.Fatalf("replacement connection: %v", err)
	}
}

// The checkpoint skip after SQLITE_NOMEM refuses a claim, a class change
// and a WAL file shorter than the shipped offset.
func TestCheckpointSkipGates(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("SELECT 1"); err != nil {
		t.Fatal(err)
	}
	restore := withHeapLimitAt(t, 1)
	var x int
	noMem := db.QueryRow("SELECT length(randomblob(1000000))").Scan(&x)
	restore()
	if !isNoMem(noMem) {
		t.Fatalf("setup: need a real SQLITE_NOMEM, got %v", noMem)
	}
	m, ten, _ := guardTenant(t, testOpts())
	mustTx(t, m, "t1", "c", "a", insertStmt(1, "a"))
	if _, _, sub := walState(t, ten); sub == 0 {
		t.Fatal("setup: nothing shipped")
	}
	if !onActor(ten, func() bool { return ten.skipNoMemCheckpoint(noMem, "") }) {
		t.Fatal("control: a plain checkpoint does not skip")
	}
	if onActor(ten, func() bool {
		old := ten.claimed
		ten.claimed = false
		defer func() { ten.claimed = old }()
		return ten.skipNoMemCheckpoint(noMem, "")
	}) {
		t.Error("a claim skipped its checkpoint")
	}
	if onActor(ten, func() bool { return ten.skipNoMemCheckpoint(noMem, "whale") }) {
		t.Error("a class change skipped its checkpoint")
	}
	if onActor(ten, func() bool { return ten.skipNoMemCheckpoint(errors.New("other"), "") }) {
		t.Error("an error other than SQLITE_NOMEM skipped")
	}
	type res struct {
		ok  bool
		err error
	}
	r := onActor(ten, func() res {
		if err := os.Truncate(ten.walPath, 0); err != nil {
			return res{err: err}
		}
		ok := ten.skipNoMemCheckpoint(noMem, "")
		ten.failLocked(errors.New("test truncated the local WAL"))
		return res{ok: ok}
	})
	if r.err != nil {
		t.Fatal(r.err)
	}
	if r.ok {
		t.Error("a WAL shorter than the shipped offset skipped")
	}
}

// advanceWatermark clears the slots it takes out of t.segs and
// t.pending, so their backing arrays do not pin acked data. It keeps the
// entries that still wait.
func TestWatermarkClearsSlots(t *testing.T) {
	reply := make(chan txOutcome, 1)
	seg := &segment{done: true, end: 10, waiters: []chan txOutcome{reply}, results: []txOutcome{{dup: true}}}
	segs := []*segment{seg}
	pending := []pendingAck{
		{end: 10, reply: make(chan txOutcome, 1), outcome: txOutcome{dup: true}},
		{end: 11, reply: make(chan txOutcome, 1), outcome: txOutcome{err: errors.New("kept")}},
		{end: 9, reply: make(chan txOutcome, 1)},
	}
	keep := pending[1].reply
	ten := &Tenant{mf: &Manifest{OwnerUntilMs: time.Now().Add(time.Hour).UnixMilli()}, segs: segs, pending: pending}
	ten.advanceWatermark()
	if segs[0] != nil {
		t.Fatal("segment slot kept")
	}
	if !(<-reply).dup {
		t.Fatal("segment waiter got the wrong result")
	}
	if len(ten.pending) != 1 || ten.pending[0].reply != keep || ten.pending[0].outcome.err == nil {
		t.Fatal("pending compaction lost the waiting entry")
	}
	for i := 1; i < len(pending); i++ {
		if pending[i].reply != nil || pending[i].outcome.err != nil || pending[i].outcome.dup {
			t.Fatalf("pending slot %d kept", i)
		}
	}
}

// cgroupFixture writes files below a new temp dir and returns it.
func cgroupFixture(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	for p, s := range files {
		p = filepath.Join(root, p)
		if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(s), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

// mountinfo layouts from the re-review of Q3 (R1, R2) and controls.
func TestCgroupMountinfoEdges(t *testing.T) {
	const gib, mib256 = "1073741824\n", "268435456\n"
	full := "29 1 0:26 / /sys/fs/cgroup rw - cgroup2 cgroup rw\n"
	leaf := "30 1 0:26 /top/leaf /run/leaf rw - cgroup2 cgroup rw\n"
	ancestor := map[string]string{"proc/self/cgroup": "0::/top/leaf\n",
		"sys/fs/cgroup/top/memory.max": mib256, "sys/fs/cgroup/top/leaf/memory.max": gib, "run/leaf/memory.max": gib}
	with := func(base map[string]string, kv ...string) map[string]string {
		out := map[string]string{}
		for k, v := range base {
			out[k] = v
		}
		for i := 0; i < len(kv); i += 2 {
			out[kv[i]] = kv[i+1]
		}
		return out
	}
	cases := []struct {
		name  string
		files map[string]string
		want  int64
	}{
		// R1: a deeper mount hides the parent limit that the full mount shows
		{"tighter ancestor, full mount first", with(ancestor, "proc/self/mountinfo", full+leaf), 256 << 20},
		{"tighter ancestor, leaf mount first", with(ancestor, "proc/self/mountinfo", leaf+full), 256 << 20},
		{"tighter leaf, both mounts", with(ancestor, "proc/self/mountinfo", full+leaf,
			"sys/fs/cgroup/top/memory.max", gib, "sys/fs/cgroup/top/leaf/memory.max", mib256, "run/leaf/memory.max", mib256), 256 << 20},
		{"v1 tighter ancestor", map[string]string{"proc/self/cgroup": "11:memory:/top/leaf\n",
			"proc/self/mountinfo":                            "30 1 0:27 /top/leaf /run/leaf rw - cgroup cgroup rw,memory\n31 1 0:27 / /sys/fs/cgroup/memory rw - cgroup cgroup rw,memory\n",
			"sys/fs/cgroup/memory/top/memory.limit_in_bytes": mib256, "sys/fs/cgroup/memory/top/leaf/memory.limit_in_bytes": gib,
			"run/leaf/memory.limit_in_bytes": gib}, 256 << 20},
		// R2: ".." inside a name is not a parent element
		{"dots within a legal name", map[string]string{"proc/self/cgroup": "0::/build..jobs/worker\n",
			"proc/self/mountinfo":   "29 1 0:26 /build..jobs /run/cgroup rw - cgroup2 cgroup rw\n",
			"run/cgroup/memory.max": gib, "run/cgroup/worker/memory.max": mib256}, 256 << 20},
		{"hybrid: memory on v1", map[string]string{"proc/self/cgroup": "11:memory:/tenant/leaf\n0::/ignored\n",
			"proc/self/mountinfo":       "29 1 0:26 / /run/v2 rw - cgroup2 cgroup rw\n30 1 0:27 /tenant /run/v1 rw - cgroup cgroup rw,memory\n",
			"run/v2/ignored/memory.max": gib, "run/v1/leaf/memory.limit_in_bytes": mib256}, 256 << 20},
		{"escaped root and mount point", map[string]string{"proc/self/cgroup": "0::/root name/a\\b/leaf\n",
			"proc/self/mountinfo":     "29 1 0:26 /root\\040name/a\\134b /run/cg\\040space rw shared:1 - cgroup2 none rw\n",
			"run/cg space/memory.max": gib, "run/cg space/leaf/memory.max": mib256}, 256 << 20},
		{"no limit", map[string]string{"proc/self/cgroup": "0::/\n", "proc/self/mountinfo": full, "sys/fs/cgroup/memory.max": "max\n"}, 0},
		{"no cgroup mounts", map[string]string{"proc/self/cgroup": "", "proc/self/mountinfo": "11 1 0:23 / /proc rw - proc proc rw\n"}, 0},
		{"no mountinfo: standard mount", map[string]string{"proc/self/cgroup": "0::/a\n",
			"sys/fs/cgroup/cgroup.controllers": "memory\n", "sys/fs/cgroup/a/memory.max": mib256}, 256 << 20},
		{"malformed unrelated lines", map[string]string{"proc/self/cgroup": "0::/a\n",
			"proc/self/mountinfo": "not mountinfo\n1 2 - cgroup2\n29 1 0:26 / /run/cg rw - cgroup2 cgroup rw\n",
			"run/cg/a/memory.max": mib256}, 256 << 20},
		{"no mount contains the process: smallest mount point", map[string]string{"proc/self/cgroup": "0::/x/y\n",
			"proc/self/mountinfo": "29 1 0:26 /a /run/a rw - cgroup2 cgroup rw\n30 1 0:26 /.. /run/b rw - cgroup2 cgroup rw\n",
			"run/a/memory.max":    gib, "run/b/memory.max": mib256}, 256 << 20},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			n, src, err := cgroupMemoryLimit(cgroupFixture(t, c.files))
			if err != nil || n != c.want {
				t.Fatalf("got %d from %q (%v), want %d", n, src, err, c.want)
			}
		})
	}
}

// A limit file or a metadata file that exists but cannot be read is an
// error, not "no limit" (re-review of Q3, R3). StartMemoryCaps then fails
// unless the SQLite limit is explicit.
func TestCgroupReadFailure(t *testing.T) {
	base := map[string]string{"proc/self/cgroup": "0::/leaf\n",
		"proc/self/mountinfo": "29 1 0:26 / /run/cgroup rw - cgroup2 cgroup rw\n", "run/cgroup/memory.max": "max\n"}
	cases := []struct {
		name   string
		damage func(t *testing.T, root string)
	}{
		{"limit file unreadable", func(t *testing.T, root string) {
			p := filepath.Join(root, "run/cgroup/leaf/memory.max")
			os.MkdirAll(filepath.Dir(p), 0o700)
			if err := os.WriteFile(p, []byte("268435456\n"), 0); err != nil {
				t.Fatal(err)
			}
		}},
		{"limit file is a directory", func(t *testing.T, root string) {
			os.MkdirAll(filepath.Join(root, "run/cgroup/leaf/memory.max"), 0o700)
		}},
		{"mountinfo unreadable", func(t *testing.T, root string) {
			os.Chmod(filepath.Join(root, "proc/self/mountinfo"), 0)
		}},
		{"proc cgroup unreadable", func(t *testing.T, root string) {
			os.Chmod(filepath.Join(root, "proc/self/cgroup"), 0)
		}},
		{"cgroup path through a file", func(t *testing.T, root string) {
			// stat of the process dir fails with ENOTDIR, not ENOENT
			os.WriteFile(filepath.Join(root, "proc/self/cgroup"), []byte("0::/leaf/sub\n"), 0o600)
			os.WriteFile(filepath.Join(root, "run/cgroup/leaf"), nil, 0o600)
		}},
		{"cgroup dir not searchable", func(t *testing.T, root string) {
			d := filepath.Join(root, "run/cgroup/leaf")
			os.MkdirAll(d, 0o700)
			os.WriteFile(filepath.Join(d, "memory.max"), []byte("268435456\n"), 0o600)
			os.Chmod(filepath.Join(root, "run/cgroup"), 0o600)
			t.Cleanup(func() { os.Chmod(filepath.Join(root, "run/cgroup"), 0o700) })
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if os.Geteuid() == 0 && c.name != "limit file is a directory" && c.name != "cgroup path through a file" {
				t.Skip("root reads files without permission")
			}
			root := cgroupFixture(t, base)
			c.damage(t, root)
			if n, src, err := cgroupMemoryLimit(root); err == nil {
				t.Fatalf("no error: %d from %q", n, src)
			}
		})
	}
	// absence is not an error: no limit file in the root cgroup, no
	// mountinfo, no cgroup at all
	for name, files := range map[string]map[string]string{
		"no root limit file": {"proc/self/cgroup": "0::/\n", "proc/self/mountinfo": base["proc/self/mountinfo"], "run/cgroup/cgroup.procs": ""},
		"nothing":            {},
	} {
		if n, _, err := cgroupMemoryLimit(cgroupFixture(t, files)); err != nil || n != 0 {
			t.Fatalf("%s: %d, %v", name, n, err)
		}
	}
}
