package main

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// Consumer offsets. Minimal semantics:
//   - Monotonic per (consumer, table_uuid). A regression is 409.
//   - The table_uuid must name a known incarnation (live or dropped).
//   - The snapshot must exist (at or below head).
//   - No consumer_floor: hoglake-reimpl never expires snapshots.
//   - No replacement-chain cleanup: hoglake-reimpl has no atomic replace.
//
// An offset commit allocates no snapshot. It is one hoglite tx.

const offsetCols = `consumer_id, table_uuid, committed_snapshot, updated_at`

func offsetJSON(row []any) map[string]any {
	return map[string]any{
		"consumer_id": row[0], "table_uuid": row[1],
		"committed_snapshot": i64(row[2]), "updated_at": row[3],
	}
}

func putOffset(w http.ResponseWriter, r *http.Request) {
	c, consumer, uuid := r.PathValue("c"), r.PathValue("consumer"), r.PathValue("uuid")
	var req struct {
		SnapshotID *int64 `json:"snapshot_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.SnapshotID == nil || *req.SnapshotID < 0 {
		apiErr(w, 422, "snapshot_id required")
		return
	}
	snap := *req.SnapshotID
	b := newTxb()
	b.expectOne(`SELECT COUNT(*) FROM tables WHERE table_uuid=?`, []any{uuid},
		&failure{Status: 404, Code: "not_found", Detail: "no table incarnation has uuid " + uuid})
	b.expectOne(`SELECT COUNT(*) FROM meta WHERE id=1 AND next_snapshot - 1 >= ?`, []any{snap},
		&failure{Status: 422, Code: "validation", Detail: fmt.Sprintf("snapshot %d is past head", snap)})
	// The upsert touches no row when the stored offset is ahead.
	// ON CONFLICT WHERE is not a stable no-op across stores; a plain
	// conditional UPDATE keeps the shape identical on both.
	b.add(stmt{SQL: `UPDATE offsets SET committed_snapshot=?, updated_at=` + nowTS + `
		WHERE consumer_id=? AND table_uuid=? AND committed_snapshot <= ?`,
		Args: []any{snap, consumer, uuid, snap}})
	// a regression is the case where the stored offset is strictly ahead
	b.expectZero(`SELECT COUNT(*) FROM offsets WHERE consumer_id=? AND table_uuid=? AND committed_snapshot > ?`,
		[]any{consumer, uuid, snap},
		&failure{Status: 409, Code: "offset_regression",
			Detail: fmt.Sprintf("snapshot %d is below the stored offset; offsets are monotonic", snap)})
	// the row must exist after the upsert (insert if absent); idempotent
	// replays are no-ops, so this is a plain INSERT OR IGNORE
	b.add(stmt{SQL: `INSERT OR IGNORE INTO offsets(consumer_id, table_uuid, committed_snapshot, updated_at)
		SELECT ?, ?, ?, ` + nowTS, Args: []any{consumer, uuid, snap}})
	idx := b.add(stmt{SQL: `SELECT ` + offsetCols + ` FROM offsets WHERE consumer_id=? AND table_uuid=?`,
		Args: []any{consumer, uuid}})
	res, err := hlTx(c, b.stmts)
	if err != nil {
		if f := b.classify(err); f != nil {
			f.write(w)
			return
		}
		apiErr(w, 500, err.Error())
		return
	}
	writeJSON(w, 200, offsetJSON(res[idx].Rows[0]))
}

func listOffsets(w http.ResponseWriter, r *http.Request) {
	c, consumer := r.PathValue("c"), r.PathValue("consumer")
	res, err := hlQuery(c, `SELECT `+offsetCols+` FROM offsets WHERE consumer_id=? ORDER BY table_uuid`, consumer)
	if err != nil {
		apiErr(w, 500, err.Error())
		return
	}
	out := []map[string]any{}
	for _, row := range res.Rows {
		out = append(out, offsetJSON(row))
	}
	writeJSON(w, 200, out)
}

func getOffset(w http.ResponseWriter, r *http.Request) {
	c, consumer, uuid := r.PathValue("c"), r.PathValue("consumer"), r.PathValue("uuid")
	res, err := hlQuery(c, `SELECT `+offsetCols+` FROM offsets WHERE consumer_id=? AND table_uuid=?`, consumer, uuid)
	if err != nil {
		apiErr(w, 500, err.Error())
		return
	}
	if len(res.Rows) == 0 {
		apiErr(w, 404, "no offset stored")
		return
	}
	writeJSON(w, 200, offsetJSON(res.Rows[0]))
}
