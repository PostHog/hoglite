package hoglite

import (
	"context"
	"errors"

	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

// A /tx answer is one of three outcomes, and the wire keeps them apart:
//
//   - committed: 200, durable before the ack.
//   - definitely not committed: the transaction was rolled back or never
//     applied. A guard miss is 409 expect_failed, a refused statement set
//     is 422 refused (both deterministic: the same payload fails again),
//     and anything transient that rolled back or never ran is 503
//     not_applied (retry).
//   - outcome unknown: the transaction COMMITTED LOCALLY and the tenant
//     then failed before durability was confirmed (segment or base PUT
//     failure, checkpoint CAS, lease lapse or generation move, pod-wide
//     fail-stop). Its WAL bytes may already be in S3, so the next owner
//     may replay it. 504 outcome_unknown: retry with the SAME client+txid
//     and fingerprint; the retry either finds the replayed commit in
//     _hw_dedup (and answers its original result, dup=true) or applies it
//     exactly once.
//
// The rule the classification serves: an outcome that is not known to be
// non-durable never reads as a refusal. 422 is only ever a RefusedError,
// and an unclassified error is 503 (retryable), never 422.
//
// The query guards (guards.go, memory.go) keep their own codes and
// statuses: 504 query_timeout, 413 result_too_large, 507 out_of_memory.
// Each is a definite rollback. outcome_unknown is also 504, so clients
// must switch on the code, not on the status. outcome_unknown wins over a
// guard code: a COMMIT that failed with SQLITE_NOMEM after its frames may
// have reached the WAL is unknown, not out_of_memory.

const (
	CodeExpectFailed   = "expect_failed"
	CodeRefused        = "refused"
	CodeNotApplied     = "not_applied"
	CodeOutcomeUnknown = "outcome_unknown"

	// StatusOutcomeUnknown is the HTTP status of an ambiguous /tx answer.
	StatusOutcomeUnknown = 504
)

// OutcomeUnknownError wraps the failure that ended a tenant after the
// transaction was applied locally but before its durability was known.
type OutcomeUnknownError struct{ Err error }

func (e *OutcomeUnknownError) Error() string {
	return "commit outcome unknown (applied locally, durability unconfirmed): " + e.Err.Error()
}

func (e *OutcomeUnknownError) Unwrap() error { return e.Err }

// RefusedError is a deterministic refusal of the statement set: the
// transaction rolled back and the same payload can only fail again (SQL
// error, constraint violation, forbidden statement class, idempotency key
// reused with a different payload).
type RefusedError struct{ Err error }

func (e *RefusedError) Error() string { return e.Err.Error() }

func (e *RefusedError) Unwrap() error { return e.Err }

// outcomeUnknown wraps err unless it already carries the classification.
func outcomeUnknown(err error) error {
	var ou *OutcomeUnknownError
	if err == nil || errors.As(err, &ou) {
		return err
	}
	return &OutcomeUnknownError{Err: err}
}

// classifyStmtErr types an error from executing one statement inside the
// (still open, to be rolled back) transaction: a refusal when the
// statement itself is at fault, otherwise left untyped (transient; the
// rollback makes it not-applied). Errors the engine raises itself while
// executing a statement (validators, forbidden class, row decoding) are
// refusals; a cancelled or timed-out context never is.
func classifyStmtErr(err error) error {
	var se *sqlite.Error
	var ef *ExpectFailedError
	switch {
	case cancelled(err), isGuardErr(err), errors.As(err, &ef):
		// a cancelled read, a query guard refusal and a guard miss
		// keep their own type and wire answer
		return err
	case errors.As(err, &se):
		if stmtRefusal(err) {
			return &RefusedError{Err: err}
		}
		return err
	default:
		return &RefusedError{Err: err}
	}
}

// stmtRefusal reports whether err POSITIVELY refuses the statement: a
// RefusedError, or one of SQLite's statement-level codes (syntax or
// missing object, constraint, type mismatch, bind range, too big,
// authorizer denial). I/O, full disk, busy, corruption, interrupts,
// context cancellation (a checkpoint drain cancels in-flight reads), lease
// loss and a poisoned tenant are not: the caller retries.
func stmtRefusal(err error) bool {
	var rf *RefusedError
	if errors.As(err, &rf) {
		return true
	}
	var se *sqlite.Error
	if errors.As(err, &se) {
		switch se.Code() & 0xff {
		case sqlite3.SQLITE_ERROR, sqlite3.SQLITE_CONSTRAINT, sqlite3.SQLITE_MISMATCH,
			sqlite3.SQLITE_RANGE, sqlite3.SQLITE_TOOBIG, sqlite3.SQLITE_AUTH:
			return true
		}
	}
	return false
}

// transientReadErr reports whether a READ failed for a reason that says
// nothing about the statement: a cancelled or timed-out context (a
// checkpoint drain cancels in-flight reads), lease loss, a closed tenant,
// or a SQLite failure that is not statement-level (busy, interrupt, I/O).
// Reads commit nothing, so classifying them can never hide a durable
// write; the cost of a wrong answer here is only a final error where a
// retry would have succeeded, which is exactly what this guards against.
func transientReadErr(err error) bool {
	if cancelled(err) || errors.Is(err, ErrNotOwner) || errors.Is(err, ErrClosed) {
		return true
	}
	var rf *RefusedError
	if errors.As(err, &rf) {
		return false
	}
	var se *sqlite.Error
	if !errors.As(err, &se) || stmtRefusal(err) {
		return false
	}
	// a read connection is query_only: SQLITE_READONLY means the
	// statement tried to write, which is final, not transient
	return se.Code()&0xff != sqlite3.SQLITE_READONLY
}

// txErrorResponse maps a failed /tx to its wire answer. Three outcomes,
// kept apart (see the top of this file). The order is load-bearing:
//
//  1. a guard miss is final (409 expect_failed);
//  2. an applied-but-unconfirmed commit is 504 outcome_unknown, NEVER a
//     refusal, whatever else it wraps;
//  3. a query guard refusal keeps its code (query_timeout,
//     result_too_large, out_of_memory): the transaction rolled back;
//  4. a cancelled or timed-out context (a checkpoint drain cancelling an
//     active read snapshot, normalised to ctx.Err) is 503 not_applied
//     even if something wrapped it as a refusal: it says nothing about
//     the statement;
//  5. a RefusedError is 422 refused, final;
//  6. a read-only transaction commits nothing, so a positive
//     statement-level SQLite error reaching here untyped is also final;
//  7. everything else (never applied, or rolled back on a transient
//     error: lease gate, tenant closed or poisoned, local I/O) is 503
//     not_applied: retryable, and nothing to reconcile.
func txErrorResponse(err error, readOnly bool, owner string) (int, apiError) {
	var ef *ExpectFailedError
	var ou *OutcomeUnknownError
	var rf *RefusedError
	switch {
	case errors.As(err, &ef):
		return 409, apiError{Error: "expect_failed: " + err.Error(), Code: CodeExpectFailed, Owner: owner,
			GuardIndex: ef.Index + 1, GuardKind: ef.Guard}
	case errors.As(err, &ou):
		return StatusOutcomeUnknown, apiError{Error: err.Error(), Code: CodeOutcomeUnknown, Owner: owner}
	case isGuardErr(err):
		code, ae, _ := guardStatus(err)
		ae.Owner = owner
		return code, ae
	case cancelled(err):
		return 503, apiError{Error: err.Error(), Code: CodeNotApplied, Owner: owner}
	case errors.As(err, &rf):
		return 422, apiError{Error: err.Error(), Code: CodeRefused}
	case readOnly && stmtRefusal(err):
		return 422, apiError{Error: err.Error(), Code: CodeRefused}
	default:
		return 503, apiError{Error: err.Error(), Code: CodeNotApplied, Owner: owner}
	}
}

// queryErrorResponse maps a failed /query. A read the tenant could not
// serve right now (cancelled by a checkpoint drain, lease lost, tenant
// poisoned or closed, busy, interrupted, I/O) is 503 not_applied and the
// caller retries; only a statement error is final (409 refused). A dead
// tenant makes every error retryable: the next owner may serve it.
func queryErrorResponse(err error, tenantAlive bool, owner string) (int, apiError) {
	if code, ae, ok := guardStatus(err); ok {
		ae.Owner = owner
		return code, ae
	}
	if transientReadErr(err) || !tenantAlive {
		return 503, apiError{Error: err.Error(), Code: CodeNotApplied, Owner: owner}
	}
	return 409, apiError{Error: err.Error(), Code: CodeRefused}
}

// cancelled reports whether err only says that the work was cut short: a
// cancelled or timed-out request context, or a read that a checkpoint
// preempted (ErrReadPreempted). None of them says anything about the
// statement. The query timeout (ErrQueryTimeout) is not among them: it is
// a guard refusal with its own code.
func cancelled(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, ErrReadPreempted)
}

// isGuardErr reports whether err is a query guard refusal (guardStatus).
func isGuardErr(err error) bool {
	_, _, ok := guardStatus(err)
	return ok
}
