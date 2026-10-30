package main

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// ---- snapshot resolution ----

// travel resolves the snapshot/at_timestamp query params.
// nil means head. A timestamp resolves to the newest snapshot at or
// before it.
func travel(r *http.Request, c string) (*int64, *failure) {
	q := r.URL.Query()
	sv, tv := q.Get("snapshot"), q.Get("at_timestamp")
	if sv != "" && tv != "" {
		return nil, &failure{Status: 422, Code: "validation", Detail: "snapshot and at_timestamp are mutually exclusive"}
	}
	if sv != "" {
		n, err := strconv.ParseInt(sv, 10, 64)
		if err != nil || n < 0 {
			return nil, &failure{Status: 400, Code: "bad_request", Detail: "snapshot must be a non-negative integer"}
		}
		return &n, nil
	}
	if tv == "" {
		return nil, nil
	}
	ts, err := time.Parse(time.RFC3339Nano, tv)
	if err != nil {
		return nil, &failure{Status: 400, Code: "bad_request", Detail: "at_timestamp must be an ISO-8601 instant with offset"}
	}
	// Stored timestamps are UTC text with millisecond precision.
	key := ts.UTC().Format("2006-01-02T15:04:05.000Z")
	res, err := hlQuery(c, `SELECT MAX(id) FROM snapshots WHERE ts <= ?`, key)
	if err != nil {
		return nil, &failure{Status: 500, Code: "internal", Detail: err.Error()}
	}
	if len(res.Rows) == 0 || res.Rows[0][0] == nil {
		return nil, &failure{Status: 410, Code: "expired", Detail: "at_timestamp is before the earliest snapshot"}
	}
	n := i64(res.Rows[0][0])
	return &n, nil
}

// snapArg is the SQL arg for a resolved snapshot: NULL means head.
func snapArg(s *int64) any {
	if s == nil {
		return nil
	}
	return *s
}

// atSnap binds the resolved snapshot as CTE "s(id)".
// It takes one arg: the snapshot or NULL for head.
const atSnap = `WITH s(id) AS (SELECT COALESCE(?, next_snapshot - 1) FROM meta WHERE id=1) `

// tableAt filters t to the incarnation live at s.id by name.
// It takes two args: namespace, name.
const tableAt = ` t.namespace=? AND t.name=? AND t.created_snapshot <= s.id
	AND (t.dropped_snapshot IS NULL OR t.dropped_snapshot > s.id) `

// ---- columns ----

// assignFieldIDs numbers columns depth-first pre-order from 1.
// Ordinals are 1-based per sibling group. This matches hoglake.
func assignFieldIDs(cols []map[string]any) error {
	n := 0
	var walk func([]map[string]any) error
	walk = func(cs []map[string]any) error {
		for i, c := range cs {
			if s, _ := c["name"].(string); s == "" {
				return fmt.Errorf("column without a name")
			}
			if s, _ := c["type"].(string); s == "" {
				return fmt.Errorf("column %v without a type", c["name"])
			}
			n++
			c["field_id"] = n
			c["ordinal"] = i + 1
			if _, ok := c["nullable"]; !ok {
				c["nullable"] = true
			}
			if kids, ok := c["children"].([]any); ok {
				sub := make([]map[string]any, 0, len(kids))
				for _, k := range kids {
					m, ok := k.(map[string]any)
					if !ok {
						return fmt.Errorf("column %v: malformed children", c["name"])
					}
					sub = append(sub, m)
				}
				if err := walk(sub); err != nil {
					return err
				}
			}
		}
		return nil
	}
	return walk(cols)
}

// newUUID returns a random RFC 4122 v4 uuid.
func newUUID() string {
	b := make([]byte, 16)
	rand.Read(b)
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

// ---- DDL ----

func createTable(w http.ResponseWriter, r *http.Request) {
	c, ns := r.PathValue("c"), r.PathValue("ns")
	var req struct {
		Name    string           `json:"name"`
		Columns []map[string]any `json:"columns"`
		Author  string           `json:"author"`
		Message string           `json:"message"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Name == "" || len(req.Columns) == 0 {
		apiErr(w, 422, "name and columns required")
		return
	}
	if err := assignFieldIDs(req.Columns); err != nil {
		apiErr(w, 422, err.Error())
		return
	}
	uuid := newUUID()
	colsJSON, _ := json.Marshal(req.Columns)
	b := newTxb()
	b.expectOne(`SELECT COUNT(*) FROM namespaces WHERE name=?`, []any{ns},
		&failure{Status: 404, Code: "not_found", Detail: "namespace not found: " + ns})
	// a live table of this name with the SAME columns is a duplicate create:
	// return the existing row without allocating a snapshot (early return)
	if res2, err := hlQuery(c, `SELECT table_uuid, created_snapshot FROM tables WHERE namespace=? AND name=? AND dropped_snapshot IS NULL`, ns, req.Name); err == nil && len(res2.Rows) > 0 {
		live := res2.Rows[0]
		if live[1] != nil {
			// check schema match on the live row
			res3, err2 := hlQuery(c, `SELECT columns_json FROM tables WHERE namespace=? AND name=? AND dropped_snapshot IS NULL`, ns, req.Name)
			if err2 == nil && len(res3.Rows) > 0 && res3.Rows[0][0].(string) == string(colsJSON) {
				writeJSON(w, 201, map[string]any{
					"name": req.Name, "namespace": ns, "table_uuid": live[0],
					"columns": req.Columns, "snapshot_id": i64(live[1]), "read_snapshot_id": i64(live[1]),
					"record_count": 0, "file_count": 0, "file_size_bytes": 0,
				})
				return
			}
		}
		// different schema: conflict
		apiErrDetail(w, 409, "already_exists", "table already exists with a different schema: "+ns+"."+req.Name, nil)
		return
	}
	// fresh create: allocate the snapshot FIRST, then insert at the new head
	b.affectOne(allocSQL, nil, errNotFound)
	b.add(stmt{SQL: `INSERT INTO tables(namespace, name, table_uuid, columns_json, created_snapshot)
		VALUES(?, ?, ?, ?, ` + headSnap + `)`, Args: []any{ns, req.Name, uuid, string(colsJSON)}})
	// a parallel create with a different schema must have lost the race
	b.expectZero(`SELECT COUNT(*) FROM tables WHERE namespace=? AND name=? AND dropped_snapshot IS NULL
		AND columns_json != ?`,
		[]any{ns, req.Name, string(colsJSON)},
		&failure{Status: 409, Code: "already_exists", Detail: "table already exists with a different schema: " + ns + "." + req.Name})
	b.add(stmt{SQL: `INSERT INTO changes(snapshot_id, kind, object_id)
		SELECT created_snapshot, 'table_created', id FROM tables WHERE table_uuid=?`, Args: []any{uuid}})
	b.add(snapshotRow(req.Author, req.Message))
	// read back the live row by name so a retry returns the existing table
	b.add(stmt{SQL: `SELECT table_uuid, created_snapshot FROM tables WHERE namespace=? AND name=? AND dropped_snapshot IS NULL`,
		Args: []any{ns, req.Name}})
	idx := b.add(readback)
	res, err := hlTx(c, b.stmts)
	if err != nil {
		log.Printf("createTable %s.%s failed: %v", ns, req.Name, err)
		if f := b.classify(err); f != nil {
			f.write(w)
			return
		}
		if sqlError(err) {
			apiErrDetail(w, 409, "already_exists", "table already exists: "+ns+"."+req.Name, nil)
			return
		}
		apiErr(w, 500, err.Error())
		return
	}
	// the row readback sits before the snapshot readback: res[idx-1]
	// carries (table_uuid, created_snapshot) of the live row — our new row
	// on a fresh create, the pre-existing row on a retry
	row := res[idx-1].Rows
	if len(row) == 0 {
		apiErr(w, 500, "no live table after insert")
		return
	}
	liveUUID := row[0][0].(string)
	snap := i64(res[idx].Rows[0][0])
	writeJSON(w, 201, map[string]any{
		"name": req.Name, "namespace": ns, "table_uuid": liveUUID,
		"columns": req.Columns, "snapshot_id": snap, "read_snapshot_id": snap,
		"record_count": 0, "file_count": 0, "file_size_bytes": 0,
	})
}

// dropTable marks the live incarnation dropped at a new snapshot.
// Its files and DVs stay for time travel. Head reads exclude them
// because every head read resolves the LIVE incarnation by name.
func dropTable(w http.ResponseWriter, r *http.Request) {
	c, ns, tbl := r.PathValue("c"), r.PathValue("ns"), r.PathValue("t")
	q := r.URL.Query()
	author, message := q.Get("author"), q.Get("message")
	if author == "" || message == "" {
		apiErr(w, 422, "author and message are required for a drop")
		return
	}
	b := newTxb()
	b.expectOne(`SELECT COUNT(*) FROM tables WHERE namespace=? AND name=? AND dropped_snapshot IS NULL`,
		[]any{ns, tbl}, &failure{Status: 404, Code: "not_found", Detail: "table not found: " + ns + "." + tbl})
	if exp := q.Get("expected_table_uuid"); exp != "" {
		b.expectOne(`SELECT COUNT(*) FROM tables WHERE namespace=? AND name=? AND dropped_snapshot IS NULL AND table_uuid=?`,
			[]any{ns, tbl, exp}, recreated(ns, tbl))
	}
	b.affectOne(allocSQL, nil, errNotFound)
	b.affectOne(`UPDATE tables SET dropped_snapshot = `+headSnap+`
		WHERE namespace=? AND name=? AND dropped_snapshot IS NULL`, []any{ns, tbl}, errNotFound)
	b.add(stmt{SQL: `INSERT INTO changes(snapshot_id, kind, object_id)
		SELECT dropped_snapshot, 'table_dropped', id FROM tables
		WHERE namespace=? AND name=? AND dropped_snapshot = ` + headSnap, Args: []any{ns, tbl}})
	b.add(snapshotRow(author, message))
	idx := b.add(readback)
	res, err := hlTx(c, b.stmts)
	if err != nil {
		if f := b.classify(err); f != nil {
			f.write(w)
			return
		}
		apiErr(w, 500, err.Error())
		return
	}
	row := res[idx].Rows[0]
	writeJSON(w, 200, map[string]any{"snapshot_id": i64(row[0]), "schema_version": i64(row[1])})
}

func recreated(ns, tbl string) *failure {
	return &failure{Status: 409, Code: "table_recreated",
		Detail: "expected_table_uuid names an incarnation that is no longer live",
		Extra:  map[string]any{"tables": []string{ns + "." + tbl}, "retry": "re-prepare"}}
}

// ---- reads ----

func getTable(w http.ResponseWriter, r *http.Request) {
	c, ns, tbl := r.PathValue("c"), r.PathValue("ns"), r.PathValue("t")
	snap, f := travel(r, c)
	if f != nil {
		f.write(w)
		return
	}
	totals := true
	switch r.URL.Query().Get("totals") {
	case "", "true":
	case "false":
		totals = false
	default:
		apiErr(w, 400, "totals must be true or false")
		return
	}
	// totals=false reads no file row at all.
	// totals=true at head reads the maintained table_totals row (O(1)).
	// Pinned reads (time travel) still scan: totals track head only.
	// Pre-upgrade catalogs without the table fall back to the scan too.
	agg := `NULL, NULL, NULL`
	useTotals := totals && snap == nil
	if totals && !useTotals {
		agg = `(SELECT COALESCE(SUM(f.record_count),0) FROM files f WHERE f.table_id=t.id AND f.begin_snapshot <= s.id),
			(SELECT COUNT(*) FROM files f WHERE f.table_id=t.id AND f.begin_snapshot <= s.id),
			(SELECT COALESCE(SUM(f.file_size_bytes),0) FROM files f WHERE f.table_id=t.id AND f.begin_snapshot <= s.id)`
	}
	if useTotals {
		agg = `tt.record_count, tt.file_count, tt.file_size_bytes`
	}
	from := `FROM tables t, s`
	if useTotals {
		from = `FROM tables t, s LEFT JOIN table_totals tt ON tt.table_id = t.id`
	}
	res, err := hlQuery(c, atSnap+`SELECT t.table_uuid, t.columns_json, s.id, `+agg+`
		`+from+` WHERE`+tableAt, snapArg(snap), ns, tbl)
	if err != nil && useTotals && strings.Contains(err.Error(), "no such table") {
		// pre-upgrade catalog, no commit since: slow scan once
		agg = `(SELECT COALESCE(SUM(f.record_count),0) FROM files f WHERE f.table_id=t.id AND f.begin_snapshot <= s.id),
			(SELECT COUNT(*) FROM files f WHERE f.table_id=t.id AND f.begin_snapshot <= s.id),
			(SELECT COALESCE(SUM(f.file_size_bytes),0) FROM files f WHERE f.table_id=t.id AND f.begin_snapshot <= s.id)`
		res, err = hlQuery(c, atSnap+`SELECT t.table_uuid, t.columns_json, s.id, `+agg+`
			FROM tables t, s WHERE`+tableAt, snapArg(snap), ns, tbl)
	}
	if err != nil {
		apiErr(w, 500, err.Error())
		return
	}
	if len(res.Rows) == 0 {
		apiErr(w, 404, "table not found")
		return
	}
	row := res.Rows[0]
	var cols []map[string]any
	json.Unmarshal([]byte(fmt.Sprint(row[1])), &cols)
	out := map[string]any{
		"name": tbl, "namespace": ns, "table_uuid": row[0],
		"columns": cols, "read_snapshot_id": i64(row[2]),
	}
	if totals {
		out["record_count"] = i64(row[3])
		out["file_count"] = i64(row[4])
		out["file_size_bytes"] = i64(row[5])
	}
	writeJSON(w, 200, out)
}

func listTables(w http.ResponseWriter, r *http.Request) {
	c, ns := r.PathValue("c"), r.PathValue("ns")
	snap, f := travel(r, c)
	if f != nil {
		f.write(w)
		return
	}
	// One statement: every field answers for the same snapshot.
	res, err := hlQuery(c, atSnap+`SELECT t.name, t.table_uuid,
		(SELECT COALESCE(SUM(f.record_count),0) FROM files f WHERE f.table_id=t.id AND f.begin_snapshot <= s.id),
		(SELECT COUNT(*) FROM files f WHERE f.table_id=t.id AND f.begin_snapshot <= s.id),
		(SELECT COALESCE(SUM(f.file_size_bytes),0) FROM files f WHERE f.table_id=t.id AND f.begin_snapshot <= s.id),
		(SELECT COUNT(DISTINCT c.snapshot_id) FROM changes c
		 WHERE c.object_id=t.id AND c.snapshot_id <= s.id AND c.kind IN `+tableKinds+`),
		(SELECT MIN(c.snapshot_id) FROM changes c
		 WHERE c.object_id=t.id AND c.snapshot_id <= s.id AND c.kind IN `+tableKinds+`)
		FROM tables t, s
		WHERE t.namespace=? AND t.created_snapshot <= s.id
		  AND (t.dropped_snapshot IS NULL OR t.dropped_snapshot > s.id)
		ORDER BY t.name`, snapArg(snap), ns)
	if err != nil {
		apiErr(w, 500, err.Error())
		return
	}
	if len(res.Rows) == 0 {
		// An empty listing must still 404 a missing namespace.
		nr, err := hlQuery(c, `SELECT COUNT(*) FROM namespaces WHERE name=?`, ns)
		if err != nil || len(nr.Rows) == 0 || i64(nr.Rows[0][0]) == 0 {
			apiErr(w, 404, "namespace not found")
			return
		}
	}
	out := []map[string]any{}
	for _, row := range res.Rows {
		m := map[string]any{
			"name": row[0], "table_uuid": row[1], "record_count": i64(row[2]),
			"file_count": i64(row[3]), "file_size_bytes": i64(row[4]),
			"snapshot_count": i64(row[5]),
		}
		if row[6] != nil {
			m["earliest_snapshot_id"] = i64(row[6])
		}
		out = append(out, m)
	}
	writeJSON(w, 200, out)
}

// fileCols are the DataFile columns, in fileJSON order.
const fileCols = `f.id, f.path, f.record_count, f.file_size_bytes, f.row_id_start, f.footer_size,
	f.begin_snapshot, f.stats_json IS NOT NULL AND f.stats_json != ''`

func fileJSON(row []any) map[string]any {
	state := "pending"
	if i64(row[7]) != 0 {
		state = "provided"
	}
	m := map[string]any{
		"data_file_id": i64(row[0]), "path": row[1], "file_format": "parquet",
		"record_count": i64(row[2]), "file_size_bytes": i64(row[3]),
		"row_id_start": i64(row[4]), "stats_state": state,
		"begin_snapshot": i64(row[6]),
	}
	if row[5] != nil {
		m["footer_size"] = i64(row[5])
	}
	return m
}

// dvCols are the DeleteFile columns, in dvJSON order.
const dvCols = `d.delete_file_id, d.data_file_id, d.path, d.delete_count, d.file_size_bytes, d.begin_snapshot`

func dvJSON(row []any) map[string]any {
	return map[string]any{
		"delete_file_id": i64(row[0]), "data_file_id": i64(row[1]), "path": row[2],
		"file_format": "puffin-dv", "delete_count": i64(row[3]),
		"file_size_bytes": i64(row[4]), "begin_snapshot": i64(row[5]),
	}
}

func listFiles(w http.ResponseWriter, r *http.Request) {
	c, ns, tbl := r.PathValue("c"), r.PathValue("ns"), r.PathValue("t")
	snap, f := travel(r, c)
	if f != nil {
		f.write(w)
		return
	}
	if r.URL.Query().Get("limit") == "" && r.URL.Query().Get("after") == "" {
		listAllFiles(w, c, snap, ns, tbl)
		return
	}
	limit := ""
	keyset := ""
	// NOT INDEXED: the planner otherwise picks files_table
	// (table_id, begin_snapshot), which reads every row of the table and
	// sorts them before LIMIT applies — 12s per page at 16M files. id is
	// the rowid and inserts are commit-ordered, so id order IS
	// (begin_snapshot, row_id_start, id) order. Rowid order + LIMIT reads
	// exactly one page; with the after= keyset it is a direct seek.
	filesRef := "files f NOT INDEXED"
	orderBy := "ORDER BY f.id"
	args := []any{}
	if l := r.URL.Query().Get("limit"); l != "" {
		n, err := strconv.Atoi(l)
		if err != nil || n < 1 {
			apiErr(w, 400, "limit must be positive")
			return
		}
		if a := r.URL.Query().Get("after"); a != "" {
			// Keyset pagination: `after` is the last id of the previous
			// page. OFFSET scans every skipped row (18s for one page at
			// 16M files); a keyset seek on the primary key is O(limit).
			aid, err := strconv.ParseInt(a, 10, 64)
			if err != nil || aid < 0 {
				apiErr(w, 400, "after must be a non-negative file id")
				return
			}
			keyset = " AND f.id > ?"
			args = append(args, aid)
			limit = fmt.Sprintf(" LIMIT %d", min(n, 10000))
		} else {
			off, _ := strconv.Atoi(r.URL.Query().Get("offset"))
			limit = fmt.Sprintf(" LIMIT %d OFFSET %d", min(n, 10000), max(off, 0))
		}
	} else if a := r.URL.Query().Get("after"); a != "" {
		// Keyset without an explicit limit: apply the page cap so the
		// seek still happens instead of silently returning page one.
		aid, err := strconv.ParseInt(a, 10, 64)
		if err != nil || aid < 0 {
			apiErr(w, 400, "after must be a non-negative file id")
			return
		}
		keyset = " AND f.id > ?"
		args = append(args, aid)
		limit = " LIMIT 10000"
	}
	// The table is resolved at the snapshot, so a dropped table's files
	// vanish at head and stay visible to time travel before the drop.
	// Placeholder order follows the SQL text: s.id (CTE), then the
	// keyset in the JOIN's ON clause, then namespace+name in the WHERE.
	args = append([]any{snapArg(snap)}, args...)
	args = append(args, ns, tbl)
	res, err := hlQuery(c, atSnap+`SELECT `+fileCols+`, t.id
		FROM s, tables t LEFT JOIN `+filesRef+` ON f.table_id = t.id AND f.begin_snapshot <= s.id`+keyset+`
		WHERE`+tableAt+orderBy+limit, args...)
	if err != nil {
		apiErr(w, 500, err.Error())
		return
	}
	if len(res.Rows) == 0 {
		apiErr(w, 404, "table not found")
		return
	}
	out := []map[string]any{}
	for _, row := range res.Rows {
		if row[0] == nil {
			continue // the LEFT JOIN's no-file row
		}
		out = append(out, fileJSON(row))
	}
	writeJSON(w, 200, out)
}

// scanPlan pairs each data file with its live deletion vector.
// Approximation: dvs keeps only the newest vector per file. A pinned
// read sees that vector when it began at or before the pin, else none.
func scanPlan(w http.ResponseWriter, r *http.Request) {
	c, ns, tbl := r.PathValue("c"), r.PathValue("ns"), r.PathValue("t")
	snap, f := travel(r, c)
	if f != nil {
		f.write(w)
		return
	}
	for _, inc := range r.URL.Query()["include"] {
		for _, v := range splitComma(inc) {
			if v != "column_stats" && v != "split_offsets" {
				apiErr(w, 422, "unknown include value: "+v)
				return
			}
		}
	}
	tid, pin, ok, err := resolveTableAt(c, snap, ns, tbl)
	if err != nil {
		apiErr(w, 500, err.Error())
		return
	}
	if !ok {
		apiErr(w, 404, "table not found")
		return
	}
	// the newest vector of each file at the pin
	rows, err := pageAll(c, filePage(tid, -1, pin, `, `+dvCols,
		` LEFT JOIN dvs d ON d.data_file_id = f.id AND d.begin_snapshot <= ?
			AND d.epoch = (SELECT MAX(d2.epoch) FROM dvs d2
				WHERE d2.data_file_id = f.id AND d2.begin_snapshot <= ?)`, pin, pin))
	if err != nil {
		apiErr(w, 500, err.Error())
		return
	}
	sortRows(rows, fileOrder)
	out := []map[string]any{}
	for _, row := range rows {
		sf := map[string]any{"data_file": fileJSON(row[:8])}
		if row[8] != nil {
			sf["delete_file"] = dvJSON(row[8:])
		}
		out = append(out, sf)
	}
	writeJSON(w, 200, out)
}

// listAllFiles answers listFiles without limit and after: every file of
// the table at snap, in id order, read in internal pages.
func listAllFiles(w http.ResponseWriter, c string, snap *int64, ns, tbl string) {
	tid, pin, ok, err := resolveTableAt(c, snap, ns, tbl)
	if err != nil {
		apiErr(w, 500, err.Error())
		return
	}
	if !ok {
		apiErr(w, 404, "table not found")
		return
	}
	rows, err := pageAll(c, filePage(tid, -1, pin, "", ""))
	if err != nil {
		apiErr(w, 500, err.Error())
		return
	}
	sortRows(rows, func(row []any) [3]int64 { return [3]int64{i64(row[0])} })
	out := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		out = append(out, fileJSON(row))
	}
	writeJSON(w, 200, out)
}

func splitComma(s string) []string {
	var out []string
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == ',' {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	return out
}

// changes is the changefeed plan for (from_snapshot, to_snapshot].
// Files come from the live incarnation. DVs are the live vectors that
// began in the window (superseded vectors are not kept).
func changes(w http.ResponseWriter, r *http.Request) {
	c, ns, tbl := r.PathValue("c"), r.PathValue("ns"), r.PathValue("t")
	q := r.URL.Query()
	from, err := strconv.ParseInt(q.Get("from_snapshot"), 10, 64)
	if err != nil || from < 0 {
		apiErr(w, 422, "from_snapshot must be a non-negative integer")
		return
	}
	var to *int64
	if tv := q.Get("to_snapshot"); tv != "" {
		n, err := strconv.ParseInt(tv, 10, 64)
		if err != nil {
			apiErr(w, 422, "to_snapshot must be an integer")
			return
		}
		to = &n
	}
	// Resolve the table and the window's upper bound in one read.
	head, err := hlQuery(c, `SELECT t.id, t.table_uuid, m.next_snapshot - 1
		FROM tables t, meta m WHERE m.id=1 AND t.namespace=? AND t.name=? AND t.dropped_snapshot IS NULL`, ns, tbl)
	if err != nil {
		apiErr(w, 500, err.Error())
		return
	}
	if len(head.Rows) == 0 {
		apiErr(w, 404, "table not found")
		return
	}
	tid, uuid, hs := i64(head.Rows[0][0]), head.Rows[0][1], i64(head.Rows[0][2])
	upper := hs
	if to != nil {
		if *to > hs {
			apiErr(w, 422, fmt.Sprintf("to_snapshot %d is past head %d", *to, hs))
			return
		}
		upper = *to
	}
	if from > upper {
		apiErr(w, 422, "from_snapshot is after to_snapshot")
		return
	}
	files, err := pageAll(c, filePage(tid, from, upper, "", ""))
	if err != nil {
		apiErr(w, 500, err.Error())
		return
	}
	sortRows(files, fileOrder)
	// dvCols, then epoch and rowid for the order and the keyset
	dvs, err := pageAll(c, pageQuery{
		sel:     `SELECT ` + dvCols + `, d.epoch, d.rowid FROM dvs d WHERE d.table_id = ? AND d.begin_snapshot <= ?`,
		args:    []any{tid, upper},
		snapCol: "d.begin_snapshot", idCol: "d.rowid", lo: from,
		key: func(row []any) (int64, int64) { return i64(row[5]), i64(row[7]) },
	})
	if err != nil {
		apiErr(w, 500, err.Error())
		return
	}
	// (begin_snapshot, data_file_id, epoch)
	sortRows(dvs, func(row []any) [3]int64 { return [3]int64{i64(row[5]), i64(row[1]), i64(row[6])} })
	fo := make([]map[string]any, 0, len(files))
	for _, row := range files {
		fo = append(fo, fileJSON(row))
	}
	do := make([]map[string]any, 0, len(dvs))
	for _, row := range dvs {
		do = append(do, dvJSON(row))
	}
	writeJSON(w, 200, map[string]any{
		"table_uuid": uuid, "from_snapshot": from, "to_snapshot": upper,
		"files": fo, "delete_files": do,
	})
}
