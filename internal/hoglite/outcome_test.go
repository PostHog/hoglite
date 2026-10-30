package hoglite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

// realSQLiteErr returns the error SQLite raises for sqlText on a scratch
// database: a real *sqlite.Error with its real code.
func realSQLiteErr(t *testing.T, setup, sqlText string) error {
	t.Helper()
	return realSQLiteErrDSN(t, ":memory:", setup, sqlText)
}

func realSQLiteErrDSN(t *testing.T, dsn, setup, sqlText string) error {
	t.Helper()
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if setup != "" {
		if _, err := db.Exec(setup); err != nil {
			t.Fatal(err)
		}
	}
	_, err = db.Exec(sqlText)
	if err == nil {
		t.Fatalf("%q did not fail", sqlText)
	}
	return err
}

// The wire mapping of a failed /tx and /query: an outcome that is not
// known to be non-durable never reads as a refusal, and a read the engine
// could not serve right now (a checkpoint drain cancelling an active read
// snapshot surfaces as ctx.Err) is retryable on BOTH read paths.
func TestErrorResponseTaxonomy(t *testing.T) {
	syntax := realSQLiteErr(t, "", "SELEC 1")
	constraint := realSQLiteErr(t, "CREATE TABLE kv(k INTEGER PRIMARY KEY)",
		"INSERT INTO kv VALUES(1); INSERT INTO kv VALUES(1)")
	// what a write through the query_only read connection raises
	readOnlyWrite := realSQLiteErrDSN(t, "file:"+t.TempDir()+"/ro.db?_pragma=query_only(1)", "",
		"CREATE TABLE x(k)")
	drainCancel := context.Canceled
	drainDeadline := fmt.Errorf("read snapshot: %w", context.DeadlineExceeded)

	tx := []struct {
		name     string
		err      error
		readOnly bool
		code     int
		wire     string
	}{
		{"guard miss", &ExpectFailedError{Index: 0, Guard: "affected"}, false, 409, CodeExpectFailed},
		{"applied, pod fail-stop", &OutcomeUnknownError{Err: errors.New("pod lease write deadline passed")}, false, 504, CodeOutcomeUnknown},
		{"applied, lease generation moved", outcomeUnknown(fmt.Errorf("%w: pending ack after lease margin", ErrNotOwner)), false, 504, CodeOutcomeUnknown},
		{"applied, checkpoint cas", outcomeUnknown(errors.New("checkpoint cas: injected")), false, 504, CodeOutcomeUnknown},
		{"applied, wraps a refusal-looking error", &OutcomeUnknownError{Err: &RefusedError{Err: syntax}}, false, 504, CodeOutcomeUnknown},
		{"refused: syntax", classifyStmtErr(syntax), false, 422, CodeRefused},
		{"refused: constraint", classifyStmtErr(constraint), false, 422, CodeRefused},
		{"refused: engine validator", classifyStmtErr(errors.New("one statement per Statement")), false, 422, CodeRefused},
		{"never applied: lease gate", ErrNotOwner, false, 503, CodeNotApplied},
		{"never applied: tenant closed", ErrClosed, false, 503, CodeNotApplied},
		{"unclassified write error", errors.New("disk I/O error"), false, 503, CodeNotApplied},
		// read-only /tx
		{"read-only: drain cancel", drainCancel, true, 503, CodeNotApplied},
		{"read-only: drain deadline", drainDeadline, true, 503, CodeNotApplied},
		{"read-only: cancel wrapped as refusal", &RefusedError{Err: drainCancel}, true, 503, CodeNotApplied},
		{"read-only: cancel through classifyStmtErr", classifyStmtErr(drainCancel), true, 503, CodeNotApplied},
		{"read-only: untyped syntax error is final", syntax, true, 422, CodeRefused},
		{"read-only: lease lost", ErrNotOwner, true, 503, CodeNotApplied},
		// a write's untyped SQLite error is not assumed to be a refusal
		{"write: untyped syntax error is retryable", syntax, false, 503, CodeNotApplied},
	}
	for _, c := range tx {
		code, body := txErrorResponse(c.err, c.readOnly, "pod")
		if code != c.code || body.Code != c.wire {
			t.Errorf("/tx %s: got %d %q, want %d %q", c.name, code, body.Code, c.code, c.wire)
		}
	}

	// the statement classifier itself never types a cancelled read as a
	// refusal (the handler would catch it, but other callers may not)
	for _, e := range []error{drainCancel, drainDeadline} {
		var rf *RefusedError
		if errors.As(classifyStmtErr(e), &rf) {
			t.Errorf("classifyStmtErr(%v) typed a cancelled read as a refusal", e)
		}
	}

	q := []struct {
		name  string
		err   error
		alive bool
		code  int
		wire  string
	}{
		{"drain cancel", drainCancel, true, 503, CodeNotApplied},
		{"drain deadline", drainDeadline, true, 503, CodeNotApplied},
		{"lease lost", ErrNotOwner, true, 503, CodeNotApplied},
		{"tenant closed", ErrClosed, true, 503, CodeNotApplied},
		{"tenant died", errors.New("anything"), false, 503, CodeNotApplied},
		{"syntax", syntax, true, 409, CodeRefused},
		{"write through the read connection", readOnlyWrite, true, 409, CodeRefused},
		{"wrapped syntax error", fmt.Errorf("step: %w", syntax), true, 409, CodeRefused},
		{"engine validator", errors.New("statement class not allowed through /query"), true, 409, CodeRefused},
	}
	for _, c := range q {
		code, body := queryErrorResponse(c.err, c.alive, "pod")
		if code != c.code || body.Code != c.wire {
			t.Errorf("/query %s: got %d %q, want %d %q", c.name, code, body.Code, c.code, c.wire)
		}
	}
}

// The taxonomy next to the query guards and the checkpoint drain: a guard
// refusal keeps its own code and status (a definite rollback), a
// preempted read is retryable, and an applied-but-unconfirmed commit is
// outcome_unknown even when its cause is a guard error. 504 is both
// query_timeout and outcome_unknown, so the code is the contract.
func TestErrorResponseWithGuards(t *testing.T) {
	timeout := timeoutCause("write transaction", time.Second)
	tooLarge := fmt.Errorf("%w: more than 10 bytes", ErrResultTooLarge)
	oom := fmt.Errorf("%w: statement", ErrOutOfMemory)
	tx := []struct {
		name     string
		err      error
		readOnly bool
		code     int
		wire     string
	}{
		{"write timeout", timeout, false, 504, "query_timeout"},
		{"write timeout through classifyStmtErr", classifyStmtErr(timeout), false, 504, "query_timeout"},
		{"result too large", classifyStmtErr(tooLarge), false, 413, "result_too_large"},
		{"out of memory, rolled back", classifyStmtErr(oom), false, 507, "out_of_memory"},
		{"out of memory, commit frame may exist", outcomeUnknown(oom), false, 504, CodeOutcomeUnknown},
		{"read-only: preempted by checkpoint", ErrReadPreempted, true, 503, CodeNotApplied},
		{"read-only: preempted through classifyStmtErr", classifyStmtErr(ErrReadPreempted), true, 503, CodeNotApplied},
		{"read-only: timeout", timeout, true, 504, "query_timeout"},
		{"guard miss through classifyStmtErr", classifyStmtErr(&ExpectFailedError{Index: 1}), false, 409, CodeExpectFailed},
	}
	for _, c := range tx {
		code, body := txErrorResponse(c.err, c.readOnly, "pod")
		if code != c.code || body.Code != c.wire {
			t.Errorf("/tx %s: got %d %q, want %d %q", c.name, code, body.Code, c.code, c.wire)
		}
	}
	q := []struct {
		name string
		err  error
		code int
		wire string
	}{
		{"preempted by checkpoint", ErrReadPreempted, 503, CodeNotApplied},
		{"timeout", timeout, 504, "query_timeout"},
		{"result too large", tooLarge, 413, "result_too_large"},
		{"out of memory", oom, 507, "out_of_memory"},
	}
	for _, c := range q {
		code, body := queryErrorResponse(c.err, true, "pod")
		if code != c.code || body.Code != c.wire {
			t.Errorf("/query %s: got %d %q, want %d %q", c.name, code, body.Code, c.code, c.wire)
		}
	}
}
