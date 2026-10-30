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
	"io/fs"
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
// SQLITE_MISUSE here. The heap limits then cannot work, and
// StartMemoryCaps fails unless the operator turned the SQLite limit off
// (TestStartMemoryCapsFailsClosed). Manager start logs it, and /v1/info
// shows it.
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
// tests). The cgroup path of the process comes from /proc/self/cgroup.
// That path is relative to the root of the hierarchy. A mount can show a
// subtree of the hierarchy, so /proc/self/mountinfo gives the mount point
// and the hierarchy path that the mount shows (its root). The process
// directory is the mount point plus the process path below that root.
// The memory controller of cgroup v1 wins over cgroup v2 (hybrid layout).
//
// Limits of parent cgroups also apply. One mount can show more of the
// ancestors than another, so the result is the smallest limit over every
// mount of the hierarchy that contains the process, each from the
// process cgroup up to its mount point. When no mount contains the
// process (a cgroup namespace), the mount points are the closest visible
// cgroups. Without mountinfo, the standard mount points apply
// (/sys/fs/cgroup, /sys/fs/cgroup/memory).
//
// A file that does not exist is skipped; any other read error is
// returned, so that the start can fail instead of running without a cap.
func cgroupMemoryLimit(root string) (int64, string, error) {
	v2path, v1path := "", ""
	b, err := readOptional(filepath.Join(root, "proc/self/cgroup"))
	if err != nil {
		return 0, "", err
	}
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
	mounts, err := readMountinfo(filepath.Join(root, "proc/self/mountinfo"))
	if err != nil {
		return 0, "", err
	}
	if v1path != "" {
		if ms := filterMounts(mounts, func(m mountEntry) bool {
			return m.fstype == "cgroup" && hasOpt(m.superopts, "memory")
		}); len(ms) > 0 {
			return cgroupMounts(root, ms, v1path, "memory.limit_in_bytes")
		}
	}
	if v2path != "" {
		if ms := filterMounts(mounts, func(m mountEntry) bool { return m.fstype == "cgroup2" }); len(ms) > 0 {
			return cgroupMounts(root, ms, v2path, "memory.max")
		}
	}
	// no mountinfo, or no cgroup mount in it: the standard mount points
	if ok, err := exists(filepath.Join(root, "sys/fs/cgroup/cgroup.controllers")); err != nil {
		return 0, "", err
	} else if ok {
		return cgroupWalk(filepath.Join(root, "sys/fs/cgroup"), v2path, "memory.max")
	}
	mount := filepath.Join(root, "sys/fs/cgroup/memory")
	if ok, err := exists(mount); err != nil {
		return 0, "", err
	} else if ok {
		return cgroupWalk(mount, v1path, "memory.limit_in_bytes")
	}
	return 0, "", nil // no cgroup memory controller: no limit
}

// readOptional reads a file. A file that does not exist gives no data
// and no error.
func readOptional(path string) ([]byte, error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	return b, err
}

// exists reports whether path exists. Errors other than "does not
// exist" are returned.
func exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	return false, err
}

// mountEntry is one line of /proc/self/mountinfo.
type mountEntry struct{ root, point, fstype, superopts string }

// readMountinfo parses mountinfo. A missing file gives no entries.
func readMountinfo(path string) ([]mountEntry, error) {
	b, err := readOptional(path)
	if err != nil {
		return nil, err
	}
	var out []mountEntry
	for _, line := range strings.Split(string(b), "\n") {
		// ID parent major:minor root mountpoint options [optional...] - fstype source superopts
		f := strings.Fields(line)
		sep := -1
		for i, x := range f {
			if x == "-" && i >= 6 {
				sep = i
				break
			}
		}
		if sep < 0 || len(f) < sep+4 {
			continue
		}
		out = append(out, mountEntry{root: unescapeMount(f[3]), point: unescapeMount(f[4]),
			fstype: f[sep+1], superopts: f[sep+3]})
	}
	return out, nil
}

// unescapeMount decodes the octal escapes of mountinfo (\040 is a space).
func unescapeMount(s string) string {
	if !strings.Contains(s, "\\") {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+3 < len(s) {
			if v, err := strconv.ParseUint(s[i+1:i+4], 8, 8); err == nil {
				b.WriteByte(byte(v))
				i += 3
				continue
			}
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

func hasOpt(opts, want string) bool {
	for _, o := range strings.Split(opts, ",") {
		if o == want {
			return true
		}
	}
	return false
}

func filterMounts(mounts []mountEntry, match func(mountEntry) bool) []mountEntry {
	var out []mountEntry
	for _, m := range mounts {
		if match(m) {
			out = append(out, m)
		}
	}
	return out
}

// cgroupMounts returns the smallest limit over the mounts ms of one
// hierarchy for the process path p (see cgroupMemoryLimit).
func cgroupMounts(root string, ms []mountEntry, p, file string) (int64, string, error) {
	var under []mountEntry
	for _, m := range ms {
		if underCgroup(m.root, p) {
			under = append(under, m)
		}
	}
	var best int64
	var src string
	take := func(v int64, s string) {
		if v > 0 && (best == 0 || v < best) {
			best, src = v, s
		}
	}
	if len(under) == 0 {
		// no mount shows the process cgroup: each mount point is an
		// ancestor or the closest visible cgroup
		for _, m := range ms {
			v, s, err := cgroupWalk(filepath.Join(root, m.point), "/", file)
			if err != nil {
				return 0, "", err
			}
			take(v, s)
		}
		return best, src, nil
	}
	for _, m := range under {
		v, s, err := cgroupWalk(filepath.Join(root, m.point), relCgroupPath(m.root, p), file)
		if err != nil {
			return 0, "", err
		}
		take(v, s)
	}
	return best, src, nil
}

// underCgroup reports whether the hierarchy path p is root or below it.
// A root with a ".." element (a cgroup namespace shows the mount of a
// cgroup above its own root this way) never contains p.
func underCgroup(root, p string) bool {
	for _, e := range strings.Split(root, "/") {
		if e == ".." {
			return false
		}
	}
	root, p = filepath.Clean("/"+root), filepath.Clean("/"+p)
	return root == "/" || p == root || strings.HasPrefix(p, root+"/")
}

// relCgroupPath returns the process path p relative to the mount root
// (p must be under root, see underCgroup).
func relCgroupPath(root, p string) string {
	root, p = filepath.Clean("/"+root), filepath.Clean("/"+p)
	if root == "/" {
		return p
	}
	return "/" + strings.TrimPrefix(strings.TrimPrefix(p, root), "/")
}

// cgroupWalk returns the smallest limit in file from mount/rel up to
// mount. A rel that does not exist below mount starts at mount (a view
// that hides the process cgroup).
func cgroupWalk(mount, rel, file string) (int64, string, error) {
	dir := filepath.Join(mount, filepath.Clean("/"+rel))
	if ok, err := exists(dir); err != nil {
		return 0, "", err
	} else if !ok {
		dir = mount
	}
	var best int64
	var src string
	for {
		p := filepath.Join(dir, file)
		b, err := os.ReadFile(p)
		switch {
		case errors.Is(err, fs.ErrNotExist):
			// the root cgroup has no limit file
		case err != nil:
			return 0, "", err
		default:
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
	// NaN fails every comparison, so the checks are written to accept
	// only finite values in range.
	for _, f := range []float64{c.Fraction, c.SQLiteFraction} {
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return MemoryPlan{}, fmt.Errorf("memory fractions: %v is not a finite number", f)
		}
	}
	if c.Fraction == 0 {
		c.Fraction = defaultMemFraction
	}
	if c.SQLiteFraction == 0 {
		c.SQLiteFraction = defaultSQLiteFraction
	}
	if !(c.Fraction > 0 && c.Fraction <= 1 && c.SQLiteFraction > 0 && c.SQLiteFraction < c.Fraction) {
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

// ApplyMemoryPlan sets the SQLite heap limits and the Go soft memory
// limit of the process. It sets SQLite first and reads the limits back.
// If SQLite cannot take them (memory counting is off, see
// SQLiteMemStatus), it returns an error and leaves the Go limit as it
// is. A plan without SQLite limits (operator opt-out) needs no counting.
func ApplyMemoryPlan(p MemoryPlan) error {
	if err := SetSQLiteHeapLimits(p.SQLiteHard, p.SQLiteSoft); err != nil {
		return err
	}
	// SQLite clamps a soft limit to the hard limit (SetSQLiteHeapLimits)
	wantSoft := p.SQLiteSoft
	if p.SQLiteHard > 0 && (wantSoft == 0 || wantSoft > p.SQLiteHard) {
		wantSoft = p.SQLiteHard
	}
	if h, s := SQLiteHeapLimits(); h != p.SQLiteHard || s != wantSoft {
		return fmt.Errorf("sqlite heap limits did not apply: hard %d soft %d, want %d %d", h, s, p.SQLiteHard, wantSoft)
	}
	if p.GoLimit > 0 {
		debug.SetMemoryLimit(p.GoLimit)
	}
	return nil
}

// EffectiveMemory describes the caps in force now: the startup log line
// after ApplyMemoryPlan.
func EffectiveMemory() string {
	hard, soft := SQLiteHeapLimits()
	goLim := "none"
	if v := goMemoryLimit(); v > 0 {
		goLim = fmt.Sprintf("%d MiB", v>>20)
	}
	return fmt.Sprintf("memory caps in force: Go soft limit %s; sqlite heap hard %s soft %s", goLim, mib(hard), mib(soft))
}

// StartMemoryCaps sets the memory caps at process start, before any
// tenant opens. It reads the cgroup limit and GOMEMLIMIT, plans with c
// and applies the plan. It returns the plan, and an error when the caps
// that c asks for cannot be set. The caller must then stop: running
// without them would let one query grow the pod to the OOM killer.
//
// A cgroup read error is fatal when the SQLite limit derives from the
// cgroup (c.SQLiteHard == 0). An explicit SQLite limit, or the opt-out
// (c.SQLiteHard < 0), plans without the cgroup limit; the Go limit is then
// only what GOMEMLIMIT sets.
func StartMemoryCaps(c MemoryConfig) (MemoryPlan, error) {
	cg, src, err := CgroupMemoryLimit()
	if err != nil {
		if c.SQLiteHard == 0 {
			return MemoryPlan{}, fmt.Errorf("memory: cgroup memory limit: %w (set the sqlite heap limit explicitly, or turn it off)", err)
		}
		cg, src = 0, ""
	}
	p, err := PlanMemory(cg, src, os.Getenv("GOMEMLIMIT"), c)
	if err != nil {
		return MemoryPlan{}, err
	}
	if err := ApplyMemoryPlan(p); err != nil {
		return p, fmt.Errorf("memory: %w", err)
	}
	return p, nil
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
