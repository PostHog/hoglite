package hoglite

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func latMs() int {
	n, _ := strconv.Atoi(os.Getenv("BENCH_LAT_MS"))
	if n == 0 {
		n = 70
	}
	return n
}

func TestBenchCommitThroughput(t *testing.T) {
	if testing.Short() || os.Getenv("HL_BENCH") == "" {
		t.Skip("throughput bench: set HL_BENCH=1 to run")
	}
	fs := NewFakeStore()
	fs.Latency = time.Duration(latMs()) * time.Millisecond
	m := newTestManager(t, fs, "a")
	mustCreate(t, m, "bench")
	mustTx(t, m, "bench", "c", "schema", Statement{SQL: `CREATE TABLE kv(k INTEGER PRIMARY KEY, v TEXT)`})

	workers := 12
	if os.Getenv("BENCH_SERIAL") != "" {
		workers = 1
	}
	const dur = 10 * time.Second
	var commits, reads atomic.Int64
	stop := time.Now().Add(dur)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			ten, _ := m.Acquire(context.Background(), "bench", false)
			for n := 0; time.Now().Before(stop); n++ {
				_, _, err := ten.SubmitTx(TxRequest{Client: fmt.Sprint(w), TxID: fmt.Sprint(n),
					Stmts: []Statement{{SQL: `INSERT INTO kv VALUES(?, ?)`, Args: []any{int64(w)*1000000000 + int64(n), "x"}}}})
				if err != nil {
					t.Error(err)
					return
				}
				commits.Add(1)
			}
		}(w)
	}
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ten, _ := m.Acquire(context.Background(), "bench", false)
			for time.Now().Before(stop) {
				if _, err := ten.SubmitQuery(Statement{SQL: `SELECT COUNT(*) FROM kv`}); err == nil {
					reads.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	t.Logf("commits/s=%.0f reads/s=%.0f", float64(commits.Load())/dur.Seconds(), float64(reads.Load())/dur.Seconds())
}
