package main

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"
)

// fileReg is one FileRegistration (an append).
type fileReg struct {
	Path          string          `json:"path"`
	RecordCount   int64           `json:"record_count"`
	FileSizeBytes int64           `json:"file_size_bytes"`
	FooterSize    *int64          `json:"footer_size"`
	ColumnStats   json.RawMessage `json:"column_stats"`
}

// dvReg is one DeleteFileRegistration (a deletion vector).
type dvReg struct {
	DataFileID    int64  `json:"data_file_id"`
	Path          string `json:"path"`
	DeleteCount   int64  `json:"delete_count"`
	FileSizeBytes int64  `json:"file_size_bytes"`
}

type tableAppend struct {
	Namespace         string    `json:"namespace"`
	Table             string    `json:"table"`
	ExpectedTableUUID string    `json:"expected_table_uuid,omitempty"`
	Files             []fileReg `json:"files"`
}

type tableDeletes struct {
	Namespace         string  `json:"namespace"`
	Table             string  `json:"table"`
	ExpectedTableUUID string  `json:"expected_table_uuid,omitempty"`
	Files             []dvReg `json:"files"`
}

type commitReq struct {
	Appends        []tableAppend  `json:"appends"`
	Deletes        []tableDeletes `json:"deletes"`
	Author         string         `json:"author"`
	Message        string         `json:"message"`
	ReadSnapshot   *int64         `json:"read_snapshot"`
	IdempotencyKey string         `json:"idempotency_key"`
}

// touched is one table a commit names, in first-seen order.
type touched struct {
	ns, tbl  string
	expected string // expected_table_uuid, if any
	appends  bool
	deletes  bool
	rows     int64 // rows appended by this commit
	nfiles   int64 // files appended by this commit
	bytes    int64 // file bytes appended by this commit
}

func (t *touched) qname() string { return t.ns + "." + t.tbl }

// buildCommit turns a commit request into ONE hoglite transaction.
//
// Order inside the transaction:
//  1. Guards on every touched table: live, incarnation, DDL since
//     read_snapshot.
//  2. Snapshot allocation.
//  3. Appends: file rows with server-assigned row lineage.
//  4. Deletes: per-file guards, then the DV upsert.
//  5. Typed change rows and the snapshot row.
//
// Every guard is a statement with an expectation. A failed guard rolls
// back everything. Its index maps to the typed REST refusal.
func buildCommit(req *commitReq) (*txb, int, *failure) {
	bad := func(msg string) (*txb, int, *failure) {
		return nil, 0, &failure{Status: 422, Code: "validation", Detail: msg}
	}
	if len(req.Appends) == 0 && len(req.Deletes) == 0 {
		return bad("at least one of appends/deletes must be non-empty")
	}
	if len(req.Deletes) > 0 && req.ReadSnapshot == nil {
		return bad("read_snapshot is required when deletes are present")
	}

	var order []*touched
	byName := map[string]*touched{}
	touch := func(ns, tbl, exp string) (*touched, *failure) {
		if ns == "" || tbl == "" {
			return nil, &failure{Status: 422, Code: "validation", Detail: "namespace and table required"}
		}
		t := byName[ns+"\x00"+tbl]
		if t == nil {
			t = &touched{ns: ns, tbl: tbl}
			byName[ns+"\x00"+tbl] = t
			order = append(order, t)
		}
		if exp != "" {
			if t.expected != "" && t.expected != exp {
				return nil, &failure{Status: 422, Code: "validation", Detail: "conflicting expected_table_uuid for " + t.qname()}
			}
			t.expected = exp
		}
		return t, nil
	}
	for _, a := range req.Appends {
		t, f := touch(a.Namespace, a.Table, a.ExpectedTableUUID)
		if f != nil {
			return nil, 0, f
		}
		t.appends = true
		for _, fr := range a.Files {
			if fr.Path == "" || fr.RecordCount < 0 || fr.FileSizeBytes < 0 {
				return bad("file registration needs a path and non-negative counts")
			}
		}
	}
	seenDV := map[int64]bool{}
	for _, d := range req.Deletes {
		t, f := touch(d.Namespace, d.Table, d.ExpectedTableUUID)
		if f != nil {
			return nil, 0, f
		}
		t.deletes = true
		for _, dv := range d.Files {
			if dv.DataFileID <= 0 || dv.Path == "" || dv.DeleteCount < 0 || dv.FileSizeBytes < 0 {
				return bad("delete registration needs data_file_id, path and non-negative counts")
			}
			if seenDV[dv.DataFileID] {
				return bad(fmt.Sprintf("data_file_id %d has two deletion vectors in one commit", dv.DataFileID))
			}
			seenDV[dv.DataFileID] = true
		}
	}

	b := newTxb()
	// Self-heal catalogs created before table_totals existed: bootstrap
	// only runs at catalog create, so older catalogs get the table here.
	// No-op when present (no schema page dirtied, no WAL growth).
	b.add(stmt{SQL: `CREATE TABLE IF NOT EXISTS table_totals(
		table_id INTEGER PRIMARY KEY,
		record_count INTEGER NOT NULL DEFAULT 0,
		file_count INTEGER NOT NULL DEFAULT 0,
		file_size_bytes INTEGER NOT NULL DEFAULT 0)`})
	b.add(stmt{SQL: `CREATE TABLE IF NOT EXISTS hwc_flags(k TEXT PRIMARY KEY, v TEXT)`})
	// One-shot backfill for pre-upgrade catalogs. The flag row gates the
	// files scan: after the first post-upgrade commit this is a cheap
	// PK lookup over the tiny tables table, no files scan.
	b.add(stmt{SQL: `INSERT INTO table_totals(table_id, record_count, file_count, file_size_bytes)
		SELECT t.id, COALESCE(SUM(f.record_count),0), COUNT(f.id), COALESCE(SUM(f.file_size_bytes),0)
		FROM tables t LEFT JOIN files f ON f.table_id = t.id
		WHERE NOT EXISTS(SELECT 1 FROM hwc_flags WHERE k='totals_v1')
		GROUP BY t.id`})
	b.add(stmt{SQL: `INSERT OR IGNORE INTO hwc_flags(k, v) VALUES('totals_v1', '1')`})

	// 1. Table guards.
	for _, t := range order {
		b.expectOne(`SELECT COUNT(*) FROM tables WHERE namespace=? AND name=? AND dropped_snapshot IS NULL`,
			[]any{t.ns, t.tbl}, &failure{Status: 404, Code: "not_found", Detail: "table not found: " + t.qname()})
		if t.expected != "" {
			b.expectOne(`SELECT COUNT(*) FROM tables WHERE namespace=? AND name=? AND dropped_snapshot IS NULL AND table_uuid=?`,
				[]any{t.ns, t.tbl, t.expected}, recreated(t.ns, t.tbl))
		}
	}
	if rs := req.ReadSnapshot; rs != nil {
		b.expectOne(`SELECT COUNT(*) FROM meta WHERE id=1 AND next_snapshot - 1 >= ?`, []any{*rs},
			&failure{Status: 422, Code: "validation", Detail: fmt.Sprintf("read_snapshot %d is past head", *rs)})
		// DDL on a touched table since the read: the payload was planned
		// against a shape that no longer exists. Not retryable. A table
		// created after the read counts too: the writer never saw it.
		for _, t := range order {
			b.expectZero(`SELECT COUNT(*) FROM changes
				WHERE object_id = `+liveTable+` AND snapshot_id > ?
				  AND kind IN ('table_created','table_dropped','table_altered')`,
				[]any{t.ns, t.tbl, *rs},
				&failure{Status: 409, Code: "ddl_since_read_snapshot",
					Detail: fmt.Sprintf("concurrent DDL since snapshot %d on table(s): %s; re-prepare", *rs, t.qname()),
					Extra:  map[string]any{"tables": []string{t.qname()}, "read_snapshot": *rs, "retry": "re-prepare"}})
		}
	}

	// 2. Allocate this commit's snapshot. headSnap now names it.
	b.affectOne(allocSQL, nil, errNotFound)

	// 3. Appends. Row lineage is per table, running within the request.
	for _, a := range req.Appends {
		t := byName[a.Namespace+"\x00"+a.Table]
		for _, fr := range a.Files {
			var stats any
			if len(fr.ColumnStats) > 0 && string(fr.ColumnStats) != "null" {
				stats = string(fr.ColumnStats)
			}
			var footer any
			if fr.FooterSize != nil {
				footer = *fr.FooterSize
			}
			b.affectOne(`INSERT INTO files(table_id, path, record_count, file_size_bytes, footer_size, row_id_start, stats_json, begin_snapshot)
				SELECT t.id, ?, ?, ?, ?, t.next_row_id + ?, ?, `+headSnap+`
				FROM tables t WHERE t.namespace=? AND t.name=? AND t.dropped_snapshot IS NULL`,
				[]any{fr.Path, fr.RecordCount, fr.FileSizeBytes, footer, t.rows, stats, t.ns, t.tbl},
				errNotFound)
			t.rows += fr.RecordCount
			t.nfiles++
			t.bytes += fr.FileSizeBytes
		}
	}
	for _, t := range order {
		if t.appends && t.rows > 0 {
			b.affectOne(`UPDATE tables SET next_row_id = next_row_id + ?
				WHERE namespace=? AND name=? AND dropped_snapshot IS NULL`,
				[]any{t.rows, t.ns, t.tbl}, errNotFound)
		}
		if t.appends && t.nfiles > 0 {
			// Maintain running totals in the same transaction: they can
			// never diverge from the file rows. The upsert keys on the
			// live incarnation's id, so a dropped+recreated table starts
			// its own totals row.
			b.affectOne(`INSERT INTO table_totals(table_id, record_count, file_count, file_size_bytes)
				SELECT t.id, ?, ?, ? FROM tables t
				WHERE t.namespace=? AND t.name=? AND t.dropped_snapshot IS NULL
				ON CONFLICT(table_id) DO UPDATE SET
					record_count = table_totals.record_count + excluded.record_count,
					file_count = table_totals.file_count + excluded.file_count,
					file_size_bytes = table_totals.file_size_bytes + excluded.file_size_bytes`,
				[]any{t.rows, t.nfiles, t.bytes, t.ns, t.tbl}, errNotFound)
		}
	}

	// 4. Deletes. One live vector per data file; vectors only grow.
	rs := int64(0)
	if req.ReadSnapshot != nil {
		rs = *req.ReadSnapshot
	}
	for _, d := range req.Deletes {
		q := d.Namespace + "." + d.Table
		for _, dv := range d.Files {
			id := dv.DataFileID
			b.expectOne(`SELECT COUNT(*) FROM files WHERE id=? AND table_id = `+liveTable,
				[]any{id, d.Namespace, d.Table},
				&failure{Status: 422, Code: "validation", Detail: fmt.Sprintf("delete for %s targets unknown data_file_id %d", q, id)})
			// The target appeared after the read: the vector was planned
			// without it. Retryable.
			b.expectZero(`SELECT COUNT(*) FROM files WHERE id=? AND begin_snapshot > ?`, []any{id, rs},
				&failure{Status: 409, Code: "commit_conflict",
					Detail: fmt.Sprintf("delete for %s targets data_file_id %d created after read snapshot %d", q, id, rs)})
			b.expectZero(`SELECT COUNT(*) FROM files WHERE id=? AND record_count < ?`, []any{id, dv.DeleteCount},
				&failure{Status: 422, Code: "validation",
					Detail: fmt.Sprintf("delete_count %d exceeds record_count of data_file_id %d in %s", dv.DeleteCount, id, q)})
			// Staleness before monotonicity: a vector registered after the
			// read was not seen by this writer. Retryable.
			b.expectZero(`SELECT COUNT(*) FROM dvs WHERE data_file_id=? AND begin_snapshot > ?`, []any{id, rs},
				&failure{Status: 409, Code: "commit_conflict",
					Detail: fmt.Sprintf("deletion vector for data_file_id %d in %s was superseded after read snapshot %d", id, q, rs)})
			b.expectZero(`SELECT COUNT(*) FROM dvs d WHERE d.data_file_id=? AND d.delete_count > ?
				AND d.epoch = (SELECT MAX(d2.epoch) FROM dvs d2 WHERE d2.data_file_id = d.data_file_id)`,
				[]any{id, dv.DeleteCount},
				&failure{Status: 422, Code: "validation",
					Detail: fmt.Sprintf("delete_count %d for data_file_id %d in %s shrinks the live deletion vector — vectors only grow", dv.DeleteCount, id, q)})
			b.affectOne(`UPDATE meta SET next_dv_id = next_dv_id + 1 WHERE id=1`, nil, errNotFound)
			// History-preserving insert: supersession is a new epoch row.
			// Reads pick the newest epoch with begin_snapshot <= the pin,
			// so a pinned scan sees the vector that was live then.
			b.affectOne(`INSERT INTO dvs(data_file_id, table_id, delete_file_id, path, delete_count, file_size_bytes, begin_snapshot, epoch)
				SELECT f.id, f.table_id, m.next_dv_id - 1, ?, ?, ?, m.next_snapshot - 1,
					COALESCE((SELECT MAX(d.epoch) FROM dvs d WHERE d.data_file_id = f.id), 0) + 1
				FROM files f, meta m WHERE f.id=? AND m.id=1`,
				[]any{dv.Path, dv.DeleteCount, dv.FileSizeBytes, id}, errNotFound)
		}
	}

	// 5. Typed change rows, then the snapshot row.
	for _, t := range order {
		for _, k := range []struct {
			on   bool
			kind string
		}{{t.appends, "table_inserted_into"}, {t.deletes, "table_deleted_from"}} {
			if k.on {
				b.affectOne(`INSERT INTO changes(snapshot_id, kind, object_id)
					SELECT `+headSnap+`, ?, `+liveTable,
					[]any{k.kind, t.ns, t.tbl}, errNotFound)
			}
		}
	}
	b.add(snapshotRow(req.Author, req.Message))
	return b, b.add(readback), nil
}

func commit(w http.ResponseWriter, r *http.Request) {
	c := r.PathValue("c")
	var req commitReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		apiErr(w, 400, err.Error())
		return
	}
	b, idx, f := buildCommit(&req)
	if f != nil {
		f.write(w)
		return
	}
	// Exactly-once across CLIENT retries. Prefer the request's own
	// idempotency key. Else key on a hash of the client payload: it is
	// identical across retries. The built statements are not (they
	// would be if they never carried volatile values, but do not rely
	// on it).
	rawReq, _ := json.Marshal(req)
	sum := sha256.Sum256(rawReq)
	fp := fmt.Sprintf("%x", sum[:16])
	reqID := fmt.Sprintf("ch:%s:%s", c, fp)
	if req.IdempotencyKey != "" {
		reqID = "ik:" + req.IdempotencyKey
	}
	res, err := submitCommit(c, reqID, fp, b.stmts)
	if err != nil {
		if errors.Is(err, errOverloaded) {
			apiErr(w, 503, "queue full: retry")
			return
		}
		if f := b.classify(err); f != nil {
			f.write(w)
			return
		}
		if sqlError(err) {
			apiErrDetail(w, 422, "validation", err.Error(), nil)
			return
		}
		apiErr(w, 500, err.Error())
		return
	}
	row := res[idx].Rows[0]
	writeJSON(w, 200, map[string]any{"snapshot_id": i64(row[0]), "schema_version": i64(row[1])})
}

// submitCommit sends one commit through the catalog's pipeline.
// errOverloaded means the global queue cap is reached: map it to 503.
var errOverloaded = errors.New("hoglake-reimpl queue full")

func submitCommit(c, id, fp string, stmts []stmt) ([]qres, error) {
	p := pipeFor(c)
	rq := &pipeReq{id: id, fp: fp, stmts: stmts, enqueued: time.Now(), reply: make(chan pipeRes, 1)}
	if !p.submit(rq) {
		// pipeline was retired between pipeFor and submit: rebuild and retry
		p = pipeFor(c)
		if !p.submit(rq) {
			if globalQueued.Load() > int64(*globalQueueCap) {
				return nil, errOverloaded
			}
			return nil, fmt.Errorf("pipeline for %s is retired", c)
		}
	}
	// Reactive depth: cover observed demand at once (bounded).
	if q := int64(len(p.ch)) + p.inflight.Load(); q > p.depth.Load() {
		for {
			d := p.depth.Load()
			want := min64(int64(maxInFlight), q*2)
			if want <= d || p.depth.CompareAndSwap(d, want) {
				break
			}
		}
	}
	pr := <-rq.reply
	return pr.res, pr.err
}
