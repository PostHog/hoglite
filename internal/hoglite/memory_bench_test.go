package hoglite

import (
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"testing"
	"time"
)

// TestIdleTenantMemory measures the memory of open idle tenants: SQLite
// heap and Go heap per tenant after each tenant wrote and read about
// 1 MB. Set HL_BENCH=1; HL_CACHE_KIB sets TenantOpts.CacheKiB,
// HL_TENANTS the tenant count.
func TestIdleTenantMemory(t *testing.T) {
	if os.Getenv("HL_BENCH") == "" {
		t.Skip("memory bench: set HL_BENCH=1 to run")
	}
	n := 500
	if v, _ := strconv.Atoi(os.Getenv("HL_TENANTS")); v > 0 {
		n = v
	}
	o := testOpts()
	o.IdleRelease = time.Hour
	o.CheckpointByte = 64 << 20
	o.MaxSegments = 1 << 20
	if v, err := strconv.Atoi(os.Getenv("HL_CACHE_KIB")); err == nil {
		o.CacheKiB = v
	}
	fs := NewFakeStore()
	m := newReadTestManager(t, fs, o, &Metrics{})
	t.Cleanup(m.ShutdownAll)
	runtime.GC()
	sq0, _ := SQLiteMemory()
	var ms0 runtime.MemStats
	runtime.ReadMemStats(&ms0)
	for i := 0; i < n; i++ {
		tid := fmt.Sprintf("m%04d", i)
		mustCreate(t, m, tid)
		makeSchema(t, m, tid)
		// about 1 MB of rows: more than a small page cache holds
		mustTx(t, m, tid, "c", "fill", Statement{SQL: `INSERT INTO kv(v) SELECT hex(randomblob(250)) FROM
			(WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c LIMIT 2000) SELECT x FROM c)`})
		mustQuery(t, m, tid, `SELECT count(*), max(length(v)) FROM kv`)
		mustQuery(t, m, tid, `SELECT sum(length(v)) FROM kv WHERE k % 7 = 0`)
	}
	time.Sleep(500 * time.Millisecond) // segment acks settle
	runtime.GC()
	sq1, hw := SQLiteMemory()
	var ms1 runtime.MemStats
	runtime.ReadMemStats(&ms1)
	if p := os.Getenv("HL_HEAP_PROFILE"); p != "" {
		f, _ := os.Create(p)
		pprof.WriteHeapProfile(f)
		f.Close()
	}
	// the fake store keeps every object in the Go heap: not tenant memory
	var stored int64
	fs.mu.Lock()
	for _, b := range fs.objects {
		stored += int64(cap(b))
	}
	fs.mu.Unlock()
	t.Logf("tenants=%d cache_kib=%d: sqlite heap %.1f KiB/tenant (total %d MiB, highwater %d MiB); Go heap %.1f KiB/tenant without %d MiB of fake S3 objects",
		n, o.CacheKiB, float64(sq1-sq0)/1024/float64(n), (sq1-sq0)>>20, hw>>20,
		float64(int64(ms1.HeapAlloc)-int64(ms0.HeapAlloc)-stored)/1024/float64(n), stored>>20)
}
