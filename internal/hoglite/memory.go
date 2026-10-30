package hoglite

// Memory caps. One pod serves thousands of tenants in one process. When
// the process passes the container memory limit, the kernel OOM killer
// stops it, and every tenant on the pod fails over. The caps here turn
// most of that pressure into an error for the query that needs the
// memory.
//
// The process has two heaps, and each needs its own cap:
//
//   - The Go heap. debug.SetMemoryLimit makes the GC work harder near the
//     limit. It is a soft limit: Go never fails an allocation.
//   - The SQLite heap. modernc.org/sqlite runs SQLite on modernc.org/libc,
//     whose malloc takes memory from modernc.org/memory. That allocator
//     maps pages with mmap outside the Go runtime. The Go limit, the GC
//     and runtime/metrics do not see this memory
//     (TestSQLiteHeapOutsideGo).
//
// SQLite can refuse its own allocations: sqlite3_hard_heap_limit64 makes
// an allocation past the limit fail, and the statement fails with
// SQLITE_NOMEM. The limit only works when SQLite counts its memory
// (SQLITE_CONFIG_MEMSTATUS). modernc v1.61.0 is built with
// SQLITE_DEFAULT_MEMSTATUS=0, so by default the heap limit pragmas are
// silently ignored and sqlite3_memory_used returns 0. The counting can
// only be switched on before SQLite initializes, that is before the
// first connection of the process opens. init below does it.
//
// The build also has SQLITE_ENABLE_MEMORY_MANAGEMENT. Near the hard
// limit, an allocation first frees unused page cache memory of all
// connections (sqlite3_release_memory) and fails only when that is not
// enough. Above the soft limit, SQLite recycles page cache slots instead
// of taking new memory, and sorts spill to temporary files earlier
// (SQLITE_TEMP_STORE=1: files). All connections share one page cache
// group, so an active connection can take the pages of idle ones. The
// page cache bound per connection (TenantOpts.CacheKiB) therefore bounds
// the sum: open connections x CacheKiB.
//
// Which statement gets SQLITE_NOMEM is not exact. The limit is process
// wide, so a small allocation of another tenant can be the one that
// passes it. The cache release and the soft limit reduce that risk.

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"unsafe"

	"modernc.org/libc"
	"modernc.org/libc/sys/types"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

// ErrOutOfMemory marks a statement that failed because SQLite reached its
// heap limit (SQLITE_NOMEM). The statement and its transaction rolled
// back. The tenant stays open.
var ErrOutOfMemory = errors.New("out of memory")

// memStatusErr is nil when SQLite counts its memory. Without the counting
// the heap limits have no effect.
var memStatusErr error

func init() {
	memStatusErr = enableSQLiteMemStatus()
}

// enableSQLiteMemStatus switches on SQLITE_CONFIG_MEMSTATUS. SQLite
// accepts it only before sqlite3_initialize, which the first open runs.
// Package init runs before any code of this package opens a connection.
// A program whose other packages open SQLite during their own init gets
// SQLITE_MISUSE here, and the heap limits stay off (logged at Manager
// start, visible in /v1/info).
func enableSQLiteMemStatus() error {
	tls := libc.NewTLS()
	defer tls.Close()
	va := libc.Xmalloc(tls, types.Size_t(unsafe.Sizeof(uintptr(0))))
	if va == 0 {
		return errors.New("sqlite memstatus: out of memory")
	}
	defer libc.Xfree(tls, va)
	if rc := sqlite3.Xsqlite3_config(tls, sqlite3.SQLITE_CONFIG_MEMSTATUS, libc.VaList(va, int32(1))); rc != sqlite3.SQLITE_OK {
		return fmt.Errorf("sqlite memstatus: sqlite3_config returned %d (SQLite already initialized)", rc)
	}
	return nil
}

// SQLiteMemStatus reports whether SQLite counts its memory (nil), or why
// not. Without it the heap limits do nothing.
func SQLiteMemStatus() error { return memStatusErr }

// SQLiteMemory returns the bytes that SQLite holds now and the high-water
// mark since the process started. Both are 0 without SQLiteMemStatus.
func SQLiteMemory() (used, highwater int64) {
	tls := libc.NewTLS()
	defer tls.Close()
	return sqlite3.Xsqlite3_memory_used(tls), sqlite3.Xsqlite3_memory_highwater(tls, 0)
}

// SQLiteHeapLimits returns the hard and soft heap limits of the process
// (0 = none).
func SQLiteHeapLimits() (hard, soft int64) {
	tls := libc.NewTLS()
	defer tls.Close()
	return sqlite3.Xsqlite3_hard_heap_limit64(tls, -1), sqlite3.Xsqlite3_soft_heap_limit64(tls, -1)
}

// SetSQLiteHeapLimits sets the process-wide hard and soft heap limits of
// SQLite in bytes. 0 removes a limit. With a hard limit, SQLite sets a
// soft limit of 0 or above the hard limit to the hard limit.
func SetSQLiteHeapLimits(hard, soft int64) error {
	if hard < 0 || soft < 0 {
		return errors.New("sqlite heap limit: negative value")
	}
	if (hard > 0 || soft > 0) && memStatusErr != nil {
		return fmt.Errorf("sqlite heap limit has no effect: %w", memStatusErr)
	}
	tls := libc.NewTLS()
	defer tls.Close()
	sqlite3.Xsqlite3_hard_heap_limit64(tls, hard)
	sqlite3.Xsqlite3_soft_heap_limit64(tls, soft)
	return nil
}

// isNoMem reports whether err is an SQLITE_NOMEM error of the driver.
func isNoMem(err error) bool {
	var se *sqlite.Error
	return errors.As(err, &se) && se.Code()&0xff == sqlite3.SQLITE_NOMEM
}

// memError is an SQLITE_NOMEM error. It matches ErrOutOfMemory and keeps
// the driver error.
type memError struct{ err error }

func (e *memError) Error() string {
	return "out of memory: sqlite heap limit reached (" + e.err.Error() + ")"
}
func (e *memError) Unwrap() []error { return []error{ErrOutOfMemory, e.err} }

// memErr turns an SQLITE_NOMEM error into an ErrOutOfMemory error and
// counts it. Other errors pass unchanged.
func memErr(err error, m *Metrics) error {
	if err == nil || errors.Is(err, ErrOutOfMemory) || !isNoMem(err) {
		return err
	}
	if m != nil {
		m.OutOfMemory.Add(1)
	}
	return &memError{err: err}
}

// sqliteAutocommit reports whether the connection with handle db has no
// open transaction. The caller must hold the connection, with no
// statement running on it.
func sqliteAutocommit(db uintptr) bool {
	tls := libc.NewTLS()
	defer tls.Close()
	return sqlite3.Xsqlite3_get_autocommit(tls, db) != 0
}

// cgroupUnlimited is the smallest value treated as "no limit". cgroup v1
// reports an unset limit as a page-rounded maximum int64.
const cgroupUnlimited = int64(1) << 60

// CgroupMemoryLimit returns the memory limit of the cgroup of this
// process in bytes, or 0 when there is none. src names the file that set
// the limit.
func CgroupMemoryLimit() (limit int64, src string, err error) {
	return cgroupMemoryLimit("/")
}

// cgroupMemoryLimit reads the limit below root (a fixture directory in
// tests). It tries cgroup v2 (memory.max), then v1
// (memory.limit_in_bytes). The cgroup path comes from /proc/self/cgroup.
// With a cgroup namespace, or when the container sees only its own
// cgroup, that path does not exist below the mount; the mount root is
// then the cgroup of the process. Limits of parent cgroups also apply, so
// the smallest limit from the cgroup up to the mount root wins.
func cgroupMemoryLimit(root string) (int64, string, error) {
	v2path, v1path := "", ""
	if b, err := os.ReadFile(filepath.Join(root, "proc/self/cgroup")); err == nil {
		for _, line := range strings.Split(string(b), "\n") {
			parts := strings.SplitN(line, ":", 3)
			if len(parts) != 3 {
				continue
			}
			if parts[0] == "0" && parts[1] == "" {
				v2path = parts[2]
			}
			for _, c := range strings.Split(parts[1], ",") {
				if c == "memory" {
					v1path = parts[2]
				}
			}
		}
	}
	if _, err := os.Stat(filepath.Join(root, "sys/fs/cgroup/cgroup.controllers")); err == nil {
		return cgroupWalk(filepath.Join(root, "sys/fs/cgroup"), v2path, "memory.max")
	}
	mount := filepath.Join(root, "sys/fs/cgroup/memory")
	if _, err := os.Stat(mount); err == nil {
		return cgroupWalk(mount, v1path, "memory.limit_in_bytes")
	}
	return 0, "", nil // no cgroup memory controller: no limit
}

// cgroupWalk returns the smallest limit in file from mount/rel up to
// mount. A rel that does not exist below mount starts at mount.
func cgroupWalk(mount, rel, file string) (int64, string, error) {
	dir := filepath.Join(mount, filepath.Clean("/"+rel))
	if _, err := os.Stat(dir); err != nil {
		dir = mount
	}
	var best int64
	var src string
	for {
		p := filepath.Join(dir, file)
		if b, err := os.ReadFile(p); err == nil {
			v, err := parseCgroupLimit(string(b))
			if err != nil {
				return 0, "", fmt.Errorf("%s: %w", p, err)
			}
			if v > 0 && (best == 0 || v < best) {
				best, src = v, p
			}
		}
		if dir == mount || len(dir) <= len(mount) {
			return best, src, nil
		}
		dir = filepath.Dir(dir)
	}
}

// parseCgroupLimit parses memory.max or memory.limit_in_bytes. "max" and
// values from cgroupUnlimited up mean no limit (0).
func parseCgroupLimit(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "max" || s == "" {
		return 0, nil
	}
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("bad memory limit %q", s)
	}
	if v >= uint64(cgroupUnlimited) {
		return 0, nil
	}
	return int64(v), nil
}

// MemoryConfig selects the memory caps of the process. Fractions are
// shares of the cgroup memory limit.
type MemoryConfig struct {
	// Fraction is the share of the cgroup limit for the Go heap and the
	// SQLite heap together. The rest is for stacks, file mappings,
	// allocator slack and the kernel. Default 0.9.
	Fraction float64
	// SQLiteFraction is the share of the cgroup limit for the SQLite hard
	// heap limit. Default 0.4.
	SQLiteFraction float64
	// SQLiteHard is an explicit hard heap limit in bytes. 0 derives it
	// from SQLiteFraction, a negative value turns it off.
	SQLiteHard int64
	// SQLiteSoft is the soft heap limit in bytes. 0 selects 80% of the
	// hard limit. A negative value turns it off; with a hard limit,
	// SQLite then uses the hard limit as the soft limit.
	SQLiteSoft int64
}

// defaultCacheKiB is the default page cache bound per connection.
const defaultCacheKiB = 1024

const (
	defaultMemFraction    = 0.9
	defaultSQLiteFraction = 0.4
	softOfHard            = 0.8
)

// MemoryPlan is the outcome of PlanMemory.
type MemoryPlan struct {
	CgroupLimit  int64  // 0 = none found
	CgroupSource string // file that set CgroupLimit
	// GoLimit is the Go soft memory limit that ApplyMemoryPlan sets.
	// 0 = do not set (no cgroup limit, or GOMEMLIMIT set by the operator).
	GoLimit       int64
	GoLimitReason string
	SQLiteHard    int64 // 0 = off
	SQLiteSoft    int64 // 0 = off
}

// String is the startup log line.
func (p MemoryPlan) String() string {
	cg := "none"
	if p.CgroupLimit > 0 {
		cg = fmt.Sprintf("%d MiB (%s)", p.CgroupLimit>>20, p.CgroupSource)
	}
	go_ := "unchanged"
	if p.GoLimit > 0 {
		go_ = fmt.Sprintf("%d MiB", p.GoLimit>>20)
	}
	return fmt.Sprintf("memory: cgroup limit %s; Go soft limit %s (%s); sqlite heap hard %s soft %s",
		cg, go_, p.GoLimitReason, mib(p.SQLiteHard), mib(p.SQLiteSoft))
}

func mib(v int64) string {
	if v <= 0 {
		return "off"
	}
	return fmt.Sprintf("%d MiB", v>>20)
}

// PlanMemory derives the caps from the cgroup limit cg (0 = none), the
// GOMEMLIMIT environment value and c. It does not change the process.
//
// The Go soft limit is Fraction x cg minus the SQLite hard limit, because
// the Go limit does not count the SQLite heap. An explicit GOMEMLIMIT
// wins: the runtime already applied it, and the operator chose it.
func PlanMemory(cg int64, src, gomemlimit string, c MemoryConfig) (MemoryPlan, error) {
	if c.Fraction == 0 {
		c.Fraction = defaultMemFraction
	}
	if c.SQLiteFraction == 0 {
		c.SQLiteFraction = defaultSQLiteFraction
	}
	if c.Fraction < 0 || c.Fraction > 1 || c.SQLiteFraction < 0 || c.SQLiteFraction >= c.Fraction {
		return MemoryPlan{}, fmt.Errorf("memory fractions: need 0 < sqlite fraction (%v) < fraction (%v) <= 1", c.SQLiteFraction, c.Fraction)
	}
	p := MemoryPlan{CgroupLimit: cg, CgroupSource: src}
	switch {
	case c.SQLiteHard > 0:
		p.SQLiteHard = c.SQLiteHard
	case c.SQLiteHard == 0 && cg > 0:
		p.SQLiteHard = int64(float64(cg) * c.SQLiteFraction)
	}
	switch {
	case c.SQLiteSoft > 0:
		p.SQLiteSoft = c.SQLiteSoft
		if p.SQLiteHard > 0 && p.SQLiteSoft > p.SQLiteHard {
			p.SQLiteSoft = p.SQLiteHard
		}
	case c.SQLiteSoft == 0:
		p.SQLiteSoft = int64(float64(p.SQLiteHard) * softOfHard)
	default:
		p.SQLiteSoft = p.SQLiteHard // what SQLite does with soft 0 (0 without a hard limit)
	}
	switch {
	case strings.TrimSpace(gomemlimit) != "":
		p.GoLimitReason = "GOMEMLIMIT set by the operator"
	case cg <= 0:
		p.GoLimitReason = "no cgroup limit"
	default:
		budget := int64(float64(cg) * c.Fraction)
		p.GoLimit = budget - p.SQLiteHard
		if p.GoLimit < cg/10 {
			return MemoryPlan{}, fmt.Errorf("memory: sqlite heap limit %d MiB leaves %d MiB of %d MiB for the Go heap",
				p.SQLiteHard>>20, p.GoLimit>>20, cg>>20)
		}
		p.GoLimitReason = fmt.Sprintf("%.2f x cgroup limit - sqlite hard limit", c.Fraction)
	}
	return p, nil
}

// ApplyMemoryPlan sets the Go soft memory limit and the SQLite heap
// limits of the process.
func ApplyMemoryPlan(p MemoryPlan) error {
	if p.GoLimit > 0 {
		debug.SetMemoryLimit(p.GoLimit)
	}
	return SetSQLiteHeapLimits(p.SQLiteHard, p.SQLiteSoft)
}

// goMemoryLimit returns the current Go soft memory limit (0 = none).
func goMemoryLimit() int64 {
	v := debug.SetMemoryLimit(-1)
	if v == math.MaxInt64 {
		return 0
	}
	return v
}

// memoryInfo is the memory section of /v1/info.
func memoryInfo(m *Metrics) map[string]any {
	used, hw := SQLiteMemory()
	hard, soft := SQLiteHeapLimits()
	st := "ok"
	if memStatusErr != nil {
		st = memStatusErr.Error()
	}
	return map[string]any{
		"sqlite_memory_used":      used,
		"sqlite_memory_highwater": hw,
		"sqlite_heap_limit":       hard,
		"sqlite_soft_heap_limit":  soft,
		"sqlite_memstatus":        st,
		"go_memory_limit":         goMemoryLimit(),
		"out_of_memory_errors":    m.OutOfMemory.Load(),
	}
}
