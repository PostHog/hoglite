package main

import (
	"errors"
	"net/http"
)

// failure is the REST answer for one failed guard.
type failure struct {
	Status int
	Code   string
	Detail string
	Extra  map[string]any
}

func (f *failure) write(w http.ResponseWriter) {
	apiErrDetail(w, f.Status, f.Code, f.Detail, f.Extra)
}

// txb builds one hoglite transaction.
// Each guard statement carries the failure it means.
// A failed guard rolls back the whole transaction.
type txb struct {
	stmts []stmt
	fails map[int]*failure
}

func newTxb() *txb { return &txb{fails: map[int]*failure{}} }

// add appends a plain statement.
func (b *txb) add(s stmt) int {
	b.stmts = append(b.stmts, s)
	return len(b.stmts) - 1
}

// expectZero adds a COUNT query that must return 0.
func (b *txb) expectZero(sql string, args []any, f *failure) {
	b.fails[b.add(stmt{SQL: sql, Args: args, ExpectScalarInt: i64p(0)})] = f
}

// expectOne adds a COUNT query that must return 1.
func (b *txb) expectOne(sql string, args []any, f *failure) {
	b.fails[b.add(stmt{SQL: sql, Args: args, ExpectScalarInt: i64p(1)})] = f
}

// affectOne adds a write that must touch exactly one row.
func (b *txb) affectOne(sql string, args []any, f *failure) {
	b.fails[b.add(stmt{SQL: sql, Args: args, ExpectAffected: i64p(1)})] = f
}

// classify maps a hoglite error to the guard's failure.
// It returns nil when the error is not a known guard.
func (b *txb) classify(err error) *failure {
	if i := failedGuard(err); i >= 0 {
		if f, ok := b.fails[i]; ok {
			return f
		}
	}
	return nil
}

// sqlError reports whether hoglite refused the SQL itself (422).
// Constraint violations land here.
func sqlError(err error) bool {
	var he *hlError
	return errors.As(err, &he) && he.Status == 422
}

var errNotFound = &failure{Status: 404, Code: "not_found"}
