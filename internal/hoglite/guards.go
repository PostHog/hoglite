package hoglite

// Per-query resource guards: execution deadlines and result size limits.
//
// Deadline. Every read (/query, read-only /tx) and every write transaction
// runs under a context deadline (TenantOpts.QueryTimeout, WriteTimeout).
// The deadline alone is not enough. The driver (modernc.org/sqlite
// v1.61.0) watches the context only while a call into it runs: Exec, the
// first step of a Query, BEGIN. database/sql calls Rows.Next without a
// context, so a query that already returned its first row cannot be
// stopped. A later step can run for a very long time, for example a
// recursive CTE that finds no further row.
//
// The fix is the standard SQLite mechanism: sqlite3_interrupt on the
// connection handle, called from another goroutine. It is an atomic store
// of a flag. The VM checks the flag at each jump and fails the step with
// SQLITE_INTERRUPT. The driver does not export the call or the handle,
// but its exported lib package exports sqlite3_interrupt, and reflect can
// read the unexported handle field of the driver connection. No fork is
// needed. TestSQLiteHandle fails when a driver update moves the field.
//
// Interrupt rules that this code relies on (SQLite source, vdbe.c,
// tokenize.c):
//   - A statement that starts while no other statement is active clears
//     a pending interrupt. An interrupt between two statements is
//     therefore lost. The interrupter repeats it until the caller stops
//     it, so the next statement is hit too.
//   - An interrupted INSERT, UPDATE or DELETE inside an explicit
//     transaction rolls back the whole transaction. The write path never
//     commits after an error, so this only makes the later ROLLBACK a
//     no-op.
//   - The caller stops the interrupter before ROLLBACK, COMMIT and the
//     return of the connection. A late interrupt therefore never hits a
//     statement that is not part of the guarded work, and the handle is
//     never used after the connection closes.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"modernc.org/libc"
	sqlite3 "modernc.org/sqlite/lib"
)

// ErrQueryTimeout is the cause of a read or write transaction that ran
// past its deadline. The work was stopped and rolled back.
var ErrQueryTimeout = errors.New("query timeout")

// ErrResultTooLarge fails a query whose result passes the row or byte
// limit. The result is never truncated silently.
var ErrResultTooLarge = errors.New("result too large")

const (
	defaultQueryTimeout   = 5 * time.Second
	defaultWriteTimeout   = 10 * time.Second
	defaultMaxResultRows  = 100_000
	defaultMaxResultBytes = 64 << 20
)

// interruptEvery is how often a fired interrupter repeats the interrupt.
// A repeat only matters when the first one landed between two statements.
const interruptEvery = 5 * time.Millisecond

var errNoHandle = errors.New("sqlite connection handle not found")

// sqliteHandle returns the sqlite3* handle of the driver connection behind
// c. The handle stays valid while the caller holds c: database/sql closes
// a driver connection only after it returns to the pool.
func sqliteHandle(c *sql.Conn) (uintptr, error) {
	var h uintptr
	err := c.Raw(func(dc any) error {
		v := reflect.ValueOf(dc)
		if v.Kind() != reflect.Pointer || v.Elem().Kind() != reflect.Struct {
			return errNoHandle
		}
		ty := v.Elem().Type()
		if ty.PkgPath() != "modernc.org/sqlite" || ty.Name() != "conn" {
			return errNoHandle
		}
		f := v.Elem().FieldByName("db")
		if !f.IsValid() || f.Kind() != reflect.Uintptr {
			return errNoHandle
		}
		h = uintptr(f.Uint())
		if h == 0 {
			return errNoHandle
		}
		return nil
	})
	return h, err
}

// interrupter calls sqlite3_interrupt on one connection when its context
// ends, and repeats it until stop. A nil interrupter does nothing; the
// driver's own context handling then remains the only cancellation.
type interrupter struct {
	db    uintptr
	after func() bool // context.AfterFunc stop

	mu      sync.Mutex
	stopped bool
}

// watchInterrupt arms an interrupter for db on ctx. Arming starts no
// goroutine; one starts only when ctx ends before stop.
func watchInterrupt(ctx context.Context, db uintptr) *interrupter {
	if db == 0 || ctx.Done() == nil {
		return nil
	}
	it := &interrupter{db: db}
	it.after = context.AfterFunc(ctx, it.fire)
	return it
}

func (it *interrupter) fire() {
	for {
		it.mu.Lock()
		if it.stopped {
			it.mu.Unlock()
			return
		}
		// A fresh TLS, as in the driver: the connection's own TLS may be
		// in use by the statement that runs on another goroutine.
		tls := libc.NewTLS()
		sqlite3.Xsqlite3_interrupt(tls, it.db)
		tls.Close()
		it.mu.Unlock()
		time.Sleep(interruptEvery)
	}
}

// stop disarms the interrupter. After stop returns, no further interrupt
// reaches the connection, so the caller may run ROLLBACK or COMMIT and
// release the connection.
func (it *interrupter) stop() {
	if it == nil {
		return
	}
	if it.after() {
		return // fire never started
	}
	it.mu.Lock()
	it.stopped = true
	it.mu.Unlock()
}

// resultLimit is the row and byte budget of one response: one /query
// result, or all results of one /tx together. A zero field means no limit.
type resultLimit struct {
	maxRows  int
	maxBytes int64
	rows     int
	bytes    int64
}

func (t *Tenant) newResultLimit() *resultLimit {
	return &resultLimit{maxRows: t.opts.MaxResultRows, maxBytes: t.opts.MaxResultBytes}
}

// Per-value cost estimates. They count the decoded value in memory and are
// at least its JSON size for numbers, booleans and null. Text and blobs
// count their length plus the same overhead. JSON escapes can make a
// string longer than that, but memory, the resource this limit protects,
// is still counted correctly. The count costs no allocation.
const (
	rowCost   = 24 // slice header of one row
	valueCost = 24 // interface value (16) plus up to 8 bytes of payload
)

// addRow charges one scanned row. It fails once the row or byte count
// passes its limit.
func (l *resultLimit) addRow(vals []any) error {
	if l == nil {
		return nil
	}
	l.rows++
	if l.maxRows > 0 && l.rows > l.maxRows {
		return fmt.Errorf("%w: more than %d rows", ErrResultTooLarge, l.maxRows)
	}
	n := int64(rowCost)
	for _, v := range vals {
		n += valueCost
		switch x := v.(type) {
		case string:
			n += int64(len(x))
		case []byte:
			n += int64(len(x))
		}
	}
	l.bytes += n
	if l.maxBytes > 0 && l.bytes > l.maxBytes {
		return fmt.Errorf("%w: more than %d bytes", ErrResultTooLarge, l.maxBytes)
	}
	return nil
}

// timeoutCause is the context cause of a deadline of length d.
func timeoutCause(kind string, d time.Duration) error {
	return fmt.Errorf("%w: %s exceeded %v", ErrQueryTimeout, kind, d)
}

// withDeadline bounds ctx by d when d > 0.
func withDeadline(ctx context.Context, d time.Duration, kind string) (context.Context, context.CancelFunc) {
	if d <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeoutCause(ctx, d, timeoutCause(kind, d))
}

// guardOpt applies the convention of the guard options: zero selects the
// default, a negative value turns the guard off (stored as zero).
func guardOpt[T ~int | ~int64](v, def T) T {
	switch {
	case v == 0:
		return def
	case v < 0:
		return 0
	}
	return v
}
