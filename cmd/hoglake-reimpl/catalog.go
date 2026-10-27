package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
)

func createCatalog(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name     string `json:"name"`
		DataPath string `json:"data_path"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Name == "" || req.DataPath == "" {
		apiErr(w, 422, "name and data_path required")
		return
	}
	if err := hlCreateTenant(req.Name); err != nil {
		apiErr(w, 500, err.Error())
		return
	}
	stmts := append(append([]stmt{}, bootstrap...),
		stmt{SQL: `INSERT OR IGNORE INTO meta(id, name, data_path) VALUES(1, ?, ?)`, Args: []any{req.Name, req.DataPath}},
		// the guard reads back the STORED row: a parallel create with a
		// different data_path must not return 201
		stmt{SQL: `SELECT COUNT(*) FROM meta WHERE id=1 AND name=? AND data_path=?`, Args: []any{req.Name, req.DataPath}, ExpectScalarInt: i64p(1)})
	// idempotent create: a retried create returns the existing catalog
	// (201 again), transient hoglite errors retry, and a genuine
	// constraint violation means the catalog already exists with different
	// data_path — that 409s.
	// a catalog that already exists with a different data_path must 409:
	// the readback is the source of truth, not the caller's payload
	if res, err := hlQuery(req.Name, `SELECT name, data_path FROM meta WHERE id=1`); err == nil && len(res.Rows) > 0 {
		if res.Rows[0][1] != req.DataPath {
			apiErr(w, 409, "catalog already exists with a different data_path")
			return
		}
		writeJSON(w, 201, map[string]any{
			"name": req.Name, "data_path": req.DataPath, "head_snapshot_id": 0,
			"schema_version": 1, "capabilities": []string{},
		})
		return
	}
	if _, err := hlTx(req.Name, stmts); err != nil {
		log.Printf("createCatalog %s tx failed: %v", req.Name, err)
		// a 409 (guard fired) or 422 (SQL error): the catalog exists or the
		// create never committed. Read back the stored row: match on name
		// AND data_path is a retry (201), mismatch is a real conflict (409),
		// absent is transient (503, caller retries).
		res, qerr := hlQuery(req.Name, `SELECT name, data_path FROM meta WHERE id=1`)
		if qerr == nil && len(res.Rows) > 0 {
			if res.Rows[0][0] == req.Name && res.Rows[0][1] == req.DataPath {
				writeJSON(w, 201, map[string]any{
					"name": req.Name, "data_path": req.DataPath, "head_snapshot_id": 0,
					"schema_version": 1, "capabilities": []string{},
				})
				return
			}
			apiErr(w, 409, "catalog already exists with a different data_path")
			return
		}
		apiErr(w, 503, "create uncommitted; retry")
		return
	}
	writeJSON(w, 201, map[string]any{
		"name": req.Name, "data_path": req.DataPath, "head_snapshot_id": 0,
		"schema_version": 1, "capabilities": []string{},
	})
}

func getCatalog(w http.ResponseWriter, r *http.Request) {
	c := r.PathValue("c")
	res, err := hlQuery(c, `SELECT name, data_path, next_snapshot, schema_version FROM meta WHERE id=1`)
	if err != nil || len(res.Rows) == 0 {
		apiErr(w, 404, "catalog not found")
		return
	}
	row := res.Rows[0]
	writeJSON(w, 200, map[string]any{
		"name": row[0], "data_path": row[1],
		"head_snapshot_id": i64(row[2]) - 1, "schema_version": row[3],
		"capabilities": []string{},
	})
}

func createNamespace(w http.ResponseWriter, r *http.Request) {
	c := r.PathValue("c")
	var req struct {
		Name string `json:"name"`
	}
	json.NewDecoder(r.Body).Decode(&req)
	if req.Name == "" {
		apiErr(w, 422, "name required")
		return
	}
	// if the namespace already exists, return it (idempotent retry).
	// this read goes through the durable gate like every /query.
	if res, err := hlQuery(c, `SELECT id FROM namespaces WHERE name=?`, req.Name); err == nil && len(res.Rows) > 0 {
		writeJSON(w, 201, map[string]any{"name": req.Name, "namespace_id": i64(res.Rows[0][0])})
		return
	}
	b := newTxb()
	// mint a snapshot only when the namespace is genuinely new: the guard
	// fails (and the tx rolls back) if a parallel create landed first
	b.expectZero(`SELECT COUNT(*) FROM namespaces WHERE name=?`, []any{req.Name},
		&failure{Status: 409, Code: "already_exists", Detail: "namespace already exists"})
	b.affectOne(allocSQL, nil, errNotFound)
	b.add(stmt{SQL: `INSERT INTO namespaces(name) VALUES(?)`, Args: []any{req.Name}})
	b.add(stmt{SQL: `INSERT INTO changes(snapshot_id, kind, object_id)
		SELECT ` + headSnap + `, 'namespace_created', id FROM namespaces WHERE name=?`, Args: []any{req.Name}})
	b.add(snapshotRow("", ""))
	idx := b.add(stmt{SQL: `SELECT id FROM namespaces WHERE name=?`, Args: []any{req.Name}})
	res, err := hlTx(c, b.stmts)
	if err != nil {
		// the absence guard failing means a parallel create won: return the
		// existing row (idempotent), not an error
		if f := b.classify(err); f != nil && f.Code == "already_exists" {
			if res2, qerr := hlQuery(c, `SELECT id FROM namespaces WHERE name=?`, req.Name); qerr == nil && len(res2.Rows) > 0 {
				writeJSON(w, 201, map[string]any{"name": req.Name, "namespace_id": i64(res2.Rows[0][0])})
				return
			}
			apiErr(w, 409, "namespace already exists")
			return
		}
		if sqlError(err) {
			// the tx may have committed despite the error: re-read
			res2, qerr := hlQuery(c, `SELECT id FROM namespaces WHERE name=?`, req.Name)
			if qerr == nil && len(res2.Rows) > 0 {
				writeJSON(w, 201, map[string]any{"name": req.Name, "namespace_id": i64(res2.Rows[0][0])})
				return
			}
			apiErr(w, 503, "create uncommitted; retry")
			return
		}
		log.Printf("createNamespace %s failed: %v", req.Name, err)
		apiErr(w, 500, err.Error())
		return
	}
	writeJSON(w, 201, map[string]any{"name": req.Name, "namespace_id": i64(res[idx].Rows[0][0])})
}

func listNamespaces(w http.ResponseWriter, r *http.Request) {
	c := r.PathValue("c")
	res, err := hlQuery(c, `SELECT name, id FROM namespaces ORDER BY id`)
	if err != nil {
		apiErr(w, 500, err.Error())
		return
	}
	out := []map[string]any{}
	for _, row := range res.Rows {
		out = append(out, map[string]any{"name": row[0], "namespace_id": i64(row[1])})
	}
	writeJSON(w, 200, out)
}

func getNamespace(w http.ResponseWriter, r *http.Request) {
	c, ns := r.PathValue("c"), r.PathValue("ns")
	res, err := hlQuery(c, `SELECT id FROM namespaces WHERE name=?`, ns)
	if err != nil || len(res.Rows) == 0 {
		apiErr(w, 404, "namespace not found")
		return
	}
	writeJSON(w, 200, map[string]any{"name": ns, "namespace_id": i64(res.Rows[0][0])})
}

func listSnapshots(w http.ResponseWriter, r *http.Request) {
	c := r.PathValue("c")
	q := r.URL.Query()
	after := q.Get("after")
	before := q.Get("before")
	lim := 1000
	if l := q.Get("limit"); l != "" {
		n, err := strconv.Atoi(l)
		if err != nil || n < 1 {
			apiErr(w, 422, "limit must be a positive integer")
			return
		}
		lim = n
	}
	var res qres
	var err error
	switch {
	case before != "" && after != "" && after != "0":
		apiErr(w, 422, "before and after are mutually exclusive")
		return
	case before != "":
		res, err = hlQuery(c,
			`SELECT id, ts, schema_version, author, message, changes_json FROM snapshots
			 WHERE id < ? ORDER BY id DESC LIMIT ?`, before, lim)
	default:
		if after == "" {
			after = "0"
		}
		res, err = hlQuery(c,
			`SELECT id, ts, schema_version, author, message, changes_json FROM snapshots
			 WHERE id > ? ORDER BY id LIMIT ?`, after, lim)
	}
	if err != nil {
		apiErr(w, 500, err.Error())
		return
	}
	out := []map[string]any{}
	for _, row := range res.Rows {
		var changes []map[string]any
		json.Unmarshal([]byte(fmt.Sprint(row[5])), &changes)
		out = append(out, map[string]any{
			"snapshot_id": row[0], "snapshot_time": row[1], "schema_version": row[2],
			"author": row[3], "message": row[4], "changes": changes,
		})
	}
	writeJSON(w, 200, map[string]any{"snapshots": out, "has_more": len(out) == lim})
}
