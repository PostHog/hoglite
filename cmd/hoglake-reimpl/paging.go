package main

import (
	"cmp"
	"errors"
	"flag"
	"slices"
)

// ---- internal paging ----
//
// hoglite refuses a result above its row or byte limit (-max-result-rows,
// -max-result-bytes; 413) and stops a read at its deadline
// (-query-timeout; 504). A listing that grows with the table (files,
// scan plan, changes) therefore cannot be one query. These handlers read
// it in bounded pages and assemble the same response as before.
//
// Consistency: all pages read at one pinned snapshot `hi`. files and dvs
// rows are insert-only and carry begin_snapshot, the snapshot of the
// commit that inserted them. A commit after the pin inserts rows with
// begin_snapshot > hi, so every page sees the same set of rows.
//
// Keyset: pages walk the index (table_id, begin_snapshot) of files or dvs
// in its order (begin_snapshot, rowid). SQLite does not seek on a row
// value that includes the rowid, so a page continues in two steps: the
// rest of the last snapshot (begin_snapshot = ? AND rowid > ?), then the
// later snapshots (begin_snapshot > ?). Both are index range scans; a
// page costs O(page), whatever the table size. The handlers then sort the
// rows into the order of their response.

var pageRows = flag.Int("page-rows", 10000,
	"rows per internal page of files, scan and changes listings; keep well below hoglite -max-result-rows")

// pageQuery describes a paged read of one table's files or dvs rows.
type pageQuery struct {
	// sel is "SELECT ... FROM <table> <alias> [joins] WHERE <filter>",
	// without ORDER BY or LIMIT; args are its placeholders.
	sel  string
	args []any
	// snapCol and idCol name the begin_snapshot and rowid columns of the
	// paged table, e.g. "f.begin_snapshot" and "f.id".
	snapCol, idCol string
	// lo is the exclusive lower bound of begin_snapshot. The caller puts
	// the upper bound (the pinned snapshot) into sel.
	lo int64
	// key returns begin_snapshot and rowid of a result row.
	key func(row []any) (snap, id int64)
}

// pageAll returns all rows of q in (begin_snapshot, rowid) order.
func pageAll(c string, q pageQuery) ([][]any, error) {
	n := max(*pageRows, 1)
	var out [][]any
	// after the first page: the last row read
	var curSnap, lastID int64
	inSnap := false // continue inside curSnap
	lo := q.lo
	for {
		var sql string
		var args []any
		if inSnap {
			sql = q.sel + ` AND ` + q.snapCol + ` = ? AND ` + q.idCol + ` > ? ORDER BY ` + q.idCol + ` LIMIT ?`
			args = append(slices.Clip(q.args), curSnap, lastID, n)
		} else {
			sql = q.sel + ` AND ` + q.snapCol + ` > ? ORDER BY ` + q.snapCol + `, ` + q.idCol + ` LIMIT ?`
			args = append(slices.Clip(q.args), lo, n)
		}
		res, err := hlQuery(c, sql, args...)
		var he *hlError
		if errors.As(err, &he) && he.Status == 413 && n > 1 {
			n /= 2 // rows larger than planned (long paths): smaller pages
			continue
		}
		if err != nil {
			return nil, err
		}
		out = append(out, res.Rows...)
		if len(res.Rows) > 0 {
			curSnap, lastID = q.key(res.Rows[len(res.Rows)-1])
		}
		switch {
		case len(res.Rows) == n:
			inSnap = true // the page may have ended inside curSnap
		case inSnap:
			inSnap, lo = false, curSnap // curSnap is done
		default:
			return out, nil // no later snapshot has rows
		}
	}
}

// sortRows orders rows by the int64 keys that key returns, stably.
func sortRows(rows [][]any, key func(row []any) [3]int64) {
	type kr struct {
		k   [3]int64
		row []any
	}
	ks := make([]kr, len(rows))
	for i, r := range rows {
		ks[i] = kr{key(r), r}
	}
	slices.SortStableFunc(ks, func(a, b kr) int {
		for i := range a.k {
			if c := cmp.Compare(a.k[i], b.k[i]); c != 0 {
				return c
			}
		}
		return 0
	})
	for i := range ks {
		rows[i] = ks[i].row
	}
}

// pinSnap is atSnap clamped to head. Every page of a paged read uses the
// value it returns. A requested snapshot beyond head reads the same rows
// as head, and the clamp keeps commits during the paging out of it.
const pinSnap = `WITH s(id) AS (SELECT MIN(COALESCE(?, next_snapshot - 1), next_snapshot - 1) FROM meta WHERE id=1) `

// resolveTableAt returns the id of the table incarnation live at snap
// (nil = head) and the pinned snapshot. ok is false when no such table
// exists.
func resolveTableAt(c string, snap *int64, ns, tbl string) (tid, pin int64, ok bool, err error) {
	res, err := hlQuery(c, pinSnap+`SELECT t.id, s.id FROM s, tables t WHERE`+tableAt, snapArg(snap), ns, tbl)
	if err != nil || len(res.Rows) == 0 {
		return 0, 0, false, err
	}
	return i64(res.Rows[0][0]), i64(res.Rows[0][1]), true, nil
}

// filePage pages the files of table tid with lo < begin_snapshot <= hi.
// Rows are fileCols plus extra columns.
func filePage(tid, lo, hi int64, extraCols, joins string, joinArgs ...any) pageQuery {
	return pageQuery{
		sel: `SELECT ` + fileCols + extraCols + ` FROM files f` + joins +
			` WHERE f.table_id = ? AND f.begin_snapshot <= ?`,
		args:    append(slices.Clip(joinArgs), tid, hi),
		snapCol: "f.begin_snapshot", idCol: "f.id", lo: lo,
		key: func(row []any) (int64, int64) { return i64(row[6]), i64(row[0]) },
	}
}

// fileOrder is the response order of scan and changes files:
// (begin_snapshot, row_id_start, id).
func fileOrder(row []any) [3]int64 { return [3]int64{i64(row[6]), i64(row[4]), i64(row[0])} }
