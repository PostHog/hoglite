package hoglite

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"mime"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	scopedMutationMaxBodyBytes = 256 << 10
	scopedMutationMaxColumns   = 1024
	scopedMutationMaxFiles     = 1000
	scopedMutationClient       = "scoped-metadata"
)

var (
	scopedIdentifierRe = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,127}$`)
	scopedColumnTypes  = map[string]bool{
		"boolean": true, "int8": true, "int16": true, "int": true, "long": true,
		"uint8": true, "uint16": true, "uint32": true, "uint64": true,
		"float": true, "double": true, "decimal": true, "date": true, "time": true,
		"timestamp_s": true, "timestamp_ms": true, "timestamp": true,
		"timestamp_ns": true, "timestamptz": true, "string": true, "json": true,
		"uuid": true, "binary": true, "variant": true,
	}
)

type createNamespaceReq struct {
	Name string `json:"name"`
}

type dropNamespaceResp struct {
	DroppedSnapshot int64 `json:"dropped_snapshot"`
}

type createTableColumnReq struct {
	Name       string          `json:"name"`
	Type       string          `json:"type"`
	TypeParams json.RawMessage `json:"type_params,omitempty"`
	Nullable   *bool           `json:"nullable,omitempty"`
	Comment    string          `json:"comment,omitempty"`
}

type createTableReq struct {
	Name    string                 `json:"name"`
	Columns []createTableColumnReq `json:"columns"`
	Comment string                 `json:"comment,omitempty"`
}

type dropTableResp struct {
	DroppedSnapshot int64  `json:"dropped_snapshot"`
	TableUUID       string `json:"table_uuid"`
}

type commitAddFileReq struct {
	Path          string `json:"path"`
	RecordCount   int64  `json:"record_count"`
	FileSizeBytes int64  `json:"file_size_bytes"`
	FooterSize    *int64 `json:"footer_size,omitempty"`
}

type commitAddDeleteFileReq struct {
	DataFileID    int64  `json:"data_file_id"`
	Path          string `json:"path"`
	DeleteCount   int64  `json:"delete_count"`
	FileSizeBytes int64  `json:"file_size_bytes"`
}

type commitTableReq struct {
	TableUUID      string                   `json:"table_uuid"`
	BaseSnapshotID int64                    `json:"base_snapshot_id"`
	AddFiles       []commitAddFileReq       `json:"add_files,omitempty"`
	AddDeleteFiles []commitAddDeleteFileReq `json:"add_delete_files,omitempty"`
}

type commitTableResp struct {
	SnapshotID   int64  `json:"snapshot_id"`
	TableUUID    string `json:"table_uuid"`
	FilesAdded   int    `json:"files_added"`
	DeletesAdded int    `json:"deletes_added"`
}

func int64Ptr(v int64) *int64 { return &v }

func decodeScopedJSONBody(w http.ResponseWriter, r *http.Request, dst any) bool {
	if deadline, ok := r.Context().Deadline(); ok {
		_ = http.NewResponseController(w).SetReadDeadline(deadline)
	}
	ct := r.Header.Get("Content-Type")
	if ct != "" {
		mt, _, err := mime.ParseMediaType(ct)
		if err != nil || mt != "application/json" {
			return false
		}
	}
	if r.Body == nil {
		return false
	}
	raw, err := io.ReadAll(io.LimitReader(r.Body, scopedMutationMaxBodyBytes+1))
	if err != nil || len(raw) == 0 || len(raw) > scopedMutationMaxBodyBytes {
		return false
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return false
	}
	var trailing any
	if err := dec.Decode(&trailing); err != io.EOF {
		return false
	}
	return true
}

func (s *Server) ensureMultiContainmentIndex(ctx context.Context, t *Tenant) error {
	// Drop legacy unique namespaces_live_containment index if present so one team can own multiple local schemas.
	res, _, err := t.SubmitTxCtx(ctx, TxRequest{
		Stmts: []Statement{{
			SQL: `SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = 'namespaces_live_containment' AND sql LIKE 'CREATE UNIQUE%'`,
		}},
	})
	if err != nil || len(res) != 1 || len(res[0].Rows) != 1 || len(res[0].Rows[0]) != 1 {
		return err
	}
	cnt, ok := res[0].Rows[0][0].(int64)
	if !ok || cnt == 0 {
		return nil
	}
	_, _, err = t.SubmitTxCtx(ctx, TxRequest{
		Client: scopedMutationClient,
		TxID:   "migrate-containment-idx-" + uuid.NewString(),
		Stmts: []Statement{
			{SQL: `DROP INDEX IF EXISTS namespaces_live_containment`},
			{SQL: `CREATE INDEX IF NOT EXISTS namespaces_live_containment ON namespaces(contained_field, contained_value) WHERE dropped_snapshot IS NULL AND contained_field IS NOT NULL`},
		},
	})
	return err
}

func (s *Server) createScopedNamespace(w http.ResponseWriter, r *http.Request, target scopedMetadataTarget, rsa *readSessionAuthority, t *Tenant, getManifest func(context.Context) (*Manifest, error)) {
	if !rsa.writable {
		scopedNamespaceRefusal(w, http.StatusForbidden)
		return
	}
	predField, predValue, ok := rsa.containmentPredicate()
	if !ok {
		scopedNamespaceRefusal(w, http.StatusNotFound)
		return
	}
	var req createNamespaceReq
	if !decodeScopedJSONBody(w, r, &req) || !scopedIdentifierRe.MatchString(req.Name) {
		scopedNamespaceRefusal(w, http.StatusBadRequest)
		return
	}
	if s.auth.readSession.isProtectedSchemaLifecycle(req.Name) {
		scopedNamespaceRefusal(w, http.StatusForbidden)
		return
	}
	if status := s.scopedNamespaceOwned(r.Context(), target.tenant, t, getManifest); status != 0 {
		scopedNamespaceRefusal(w, status)
		return
	}
	if err := s.ensureMultiContainmentIndex(r.Context(), t); err != nil {
		if isMissingScopedSchemaError(err) {
			scopedNamespaceRefusal(w, http.StatusNotFound)
		} else {
			scopedNamespaceRefusal(w, http.StatusServiceUnavailable)
		}
		return
	}
	nowTS := time.Now().UTC().Format(time.RFC3339Nano)
	results, _, err := t.SubmitTxCtx(r.Context(), TxRequest{
		Client: scopedMutationClient,
		TxID:   "ns-create-" + uuid.NewString(),
		Stmts: []Statement{
			// 0: catalog live
			{
				SQL:             `SELECT COUNT(*) FROM meta WHERE id = 1 AND name = ? COLLATE BINARY AND dropping = 0`,
				Args:            []any{target.catalog},
				ExpectScalarInt: int64Ptr(1),
			},
			// 1: namespace does not already exist
			{
				SQL:             `SELECT COUNT(*) FROM namespaces WHERE name = ? COLLATE BINARY AND dropped_snapshot IS NULL`,
				Args:            []any{req.Name},
				ExpectScalarInt: int64Ptr(0),
			},
			// 2: allocate snapshot, schema_version, namespace_id
			{
				SQL:            `UPDATE meta SET next_snapshot = next_snapshot + 1, schema_version = schema_version + 1, next_namespace_id = next_namespace_id + 1 WHERE id = 1 AND name = ? COLLATE BINARY AND dropping = 0`,
				Args:           []any{target.catalog},
				ExpectAffected: int64Ptr(1),
			},
			// 3: snapshot row
			{
				SQL:  `INSERT INTO snapshots(id, ts, author, message, schema_version) SELECT next_snapshot - 1, ?, NULL, 'create namespace', schema_version FROM meta WHERE id = 1`,
				Args: []any{nowTS},
			},
			// 4: namespace row stamped with team containment
			{
				SQL:  `INSERT INTO namespaces(id, name, created_snapshot, contained_field, contained_value) SELECT next_namespace_id - 1, ?, next_snapshot - 1, ?, ? FROM meta WHERE id = 1`,
				Args: []any{req.Name, predField, predValue},
			},
			// 5: change row
			{
				SQL: `INSERT INTO changes(snapshot_id, kind, object_id) SELECT next_snapshot - 1, 'namespace_created', next_namespace_id - 1 FROM meta WHERE id = 1`,
			},
			// 6: readback
			{
				SQL: `SELECT next_namespace_id - 1 FROM meta WHERE id = 1`,
			},
		},
	})
	if err != nil {
		var expErr *ExpectFailedError
		if errors.As(err, &expErr) {
			if expErr.Index == 1 {
				scopedNamespaceRefusal(w, http.StatusConflict)
				return
			}
			scopedNamespaceRefusal(w, http.StatusNotFound)
			return
		}
		if isMissingScopedSchemaError(err) {
			scopedNamespaceRefusal(w, http.StatusNotFound)
		} else {
			scopedNamespaceRefusal(w, http.StatusServiceUnavailable)
		}
		return
	}
	if len(results) != 7 || len(results[6].Rows) != 1 || len(results[6].Rows[0]) != 1 {
		scopedNamespaceRefusal(w, http.StatusServiceUnavailable)
		return
	}
	nsID, ok := results[6].Rows[0][0].(int64)
	if !ok {
		scopedNamespaceRefusal(w, http.StatusServiceUnavailable)
		return
	}
	scopedNamespaceReply(w, http.StatusCreated, scopedNamespaceDTO{
		ID:          nsID,
		Name:        req.Name,
		Containment: scopedNamespaceContainment{Field: predField, Value: predValue},
	})
}

func (s *Server) dropScopedNamespace(w http.ResponseWriter, r *http.Request, target scopedMetadataTarget, rsa *readSessionAuthority, t *Tenant, getManifest func(context.Context) (*Manifest, error)) {
	if !rsa.writable {
		scopedNamespaceRefusal(w, http.StatusForbidden)
		return
	}
	predField, predValue, ok := rsa.containmentPredicate()
	if !ok {
		scopedNamespaceRefusal(w, http.StatusNotFound)
		return
	}
	if s.auth.readSession.isProtectedSchemaLifecycle(target.namespace) {
		scopedNamespaceRefusal(w, http.StatusForbidden)
		return
	}
	if status := s.scopedNamespaceOwned(r.Context(), target.tenant, t, getManifest); status != 0 {
		scopedNamespaceRefusal(w, status)
		return
	}
	nowTS := time.Now().UTC().Format(time.RFC3339Nano)
	results, _, err := t.SubmitTxCtx(r.Context(), TxRequest{
		Client: scopedMutationClient,
		TxID:   "ns-drop-" + uuid.NewString(),
		Stmts: []Statement{
			// 0: catalog live and namespace live + owned by caller team
			{
				SQL: `SELECT COUNT(*) FROM meta AS m JOIN namespaces AS n ON 1 = 1
WHERE m.id = 1 AND m.name = ? COLLATE BINARY AND m.dropping = 0
  AND n.name = ? COLLATE BINARY AND n.dropped_snapshot IS NULL
  AND n.contained_field = ? COLLATE BINARY AND typeof(n.contained_value) = 'integer' AND n.contained_value = ?`,
				Args:            []any{target.catalog, target.namespace, predField, predValue},
				ExpectScalarInt: int64Ptr(1),
			},
			// 1: if namespace_id fence is supplied, verify it matches the live namespace (409 on recreated object)
			{
				SQL: `SELECT COUNT(*) FROM namespaces AS n
WHERE n.name = ? COLLATE BINARY AND n.dropped_snapshot IS NULL
  AND (? = 0 OR n.id = ?)`,
				Args:            []any{target.namespace, target.namespaceID, target.namespaceID},
				ExpectScalarInt: int64Ptr(1),
			},
			// 2: namespace must have no live tables
			{
				SQL: `SELECT COUNT(*) FROM namespaces AS n JOIN tables AS t ON t.namespace_id = n.id
WHERE n.name = ? COLLATE BINARY AND n.dropped_snapshot IS NULL AND t.dropped_snapshot IS NULL`,
				Args:            []any{target.namespace},
				ExpectScalarInt: int64Ptr(0),
			},
			// 3: namespace must have no live views
			{
				SQL: `SELECT COUNT(*) FROM namespaces AS n JOIN views AS v ON v.namespace_id = n.id
WHERE n.name = ? COLLATE BINARY AND n.dropped_snapshot IS NULL AND v.dropped_snapshot IS NULL`,
				Args:            []any{target.namespace},
				ExpectScalarInt: int64Ptr(0),
			},
			// 4: allocate snapshot
			{
				SQL:            `UPDATE meta SET next_snapshot = next_snapshot + 1, schema_version = schema_version + 1 WHERE id = 1 AND name = ? COLLATE BINARY AND dropping = 0`,
				Args:           []any{target.catalog},
				ExpectAffected: int64Ptr(1),
			},
			// 5: snapshot row
			{
				SQL:  `INSERT INTO snapshots(id, ts, author, message, schema_version) SELECT next_snapshot - 1, ?, NULL, 'drop namespace', schema_version FROM meta WHERE id = 1`,
				Args: []any{nowTS},
			},
			// 6: change row (must read id before drop)
			{
				SQL:  `INSERT INTO changes(snapshot_id, kind, object_id) SELECT m.next_snapshot - 1, 'namespace_dropped', n.id FROM meta AS m JOIN namespaces AS n ON 1 = 1 WHERE m.id = 1 AND n.name = ? COLLATE BINARY AND n.dropped_snapshot IS NULL`,
				Args: []any{target.namespace},
			},
			// 7: mark namespace dropped
			{
				SQL:            `UPDATE namespaces SET dropped_snapshot = (SELECT next_snapshot - 1 FROM meta WHERE id = 1) WHERE name = ? COLLATE BINARY AND dropped_snapshot IS NULL AND contained_field = ? COLLATE BINARY AND contained_value = ?`,
				Args:           []any{target.namespace, predField, predValue},
				ExpectAffected: int64Ptr(1),
			},
			// 8: readback snapshot
			{
				SQL: `SELECT next_snapshot - 1 FROM meta WHERE id = 1`,
			},
		},
	})
	if err != nil {
		var expErr *ExpectFailedError
		if errors.As(err, &expErr) {
			if expErr.Index == 1 || expErr.Index == 2 || expErr.Index == 3 {
				scopedNamespaceRefusal(w, http.StatusConflict)
				return
			}
			scopedNamespaceRefusal(w, http.StatusNotFound)
			return
		}
		if isMissingScopedSchemaError(err) {
			scopedNamespaceRefusal(w, http.StatusNotFound)
		} else {
			scopedNamespaceRefusal(w, http.StatusServiceUnavailable)
		}
		return
	}
	if len(results) != 9 || len(results[8].Rows) != 1 || len(results[8].Rows[0]) != 1 {
		scopedNamespaceRefusal(w, http.StatusServiceUnavailable)
		return
	}
	snap, ok := results[8].Rows[0][0].(int64)
	if !ok {
		scopedNamespaceRefusal(w, http.StatusServiceUnavailable)
		return
	}
	scopedNamespaceReply(w, http.StatusOK, dropNamespaceResp{DroppedSnapshot: snap})
}

func (s *Server) createScopedTable(w http.ResponseWriter, r *http.Request, target scopedMetadataTarget, rsa *readSessionAuthority, t *Tenant, getManifest func(context.Context) (*Manifest, error)) {
	if !rsa.writable {
		scopedNamespaceRefusal(w, http.StatusForbidden)
		return
	}
	predField, predValue, ok := rsa.containmentPredicate()
	if !ok {
		scopedNamespaceRefusal(w, http.StatusNotFound)
		return
	}
	if s.auth.readSession.isReadOnlyNamespace(target.namespace) {
		scopedNamespaceRefusal(w, http.StatusForbidden)
		return
	}
	var req createTableReq
	if !decodeScopedJSONBody(w, r, &req) || !scopedIdentifierRe.MatchString(req.Name) ||
		len(req.Columns) == 0 || len(req.Columns) > scopedMutationMaxColumns {
		scopedNamespaceRefusal(w, http.StatusBadRequest)
		return
	}
	seenCols := make(map[string]bool, len(req.Columns))
	for _, col := range req.Columns {
		if !scopedIdentifierRe.MatchString(col.Name) || strings.HasPrefix(col.Name, "_hog") ||
			seenCols[col.Name] || !scopedColumnTypes[col.Type] {
			scopedNamespaceRefusal(w, http.StatusBadRequest)
			return
		}
		if len(col.TypeParams) > 0 {
			if _, ok := parseScopedTypeParams(string(col.TypeParams)); !ok {
				scopedNamespaceRefusal(w, http.StatusBadRequest)
				return
			}
		}
		seenCols[col.Name] = true
	}
	if status := s.scopedNamespaceOwned(r.Context(), target.tenant, t, getManifest); status != 0 {
		scopedNamespaceRefusal(w, status)
		return
	}
	tableUUID := uuid.NewString()
	nowTS := time.Now().UTC().Format(time.RFC3339Nano)
	var commentArg any
	if req.Comment != "" {
		commentArg = req.Comment
	}
	stmts := []Statement{
		// 0: catalog live and namespace live + owned by caller team
		{
			SQL: `SELECT COUNT(*) FROM meta AS m JOIN namespaces AS n ON 1 = 1
WHERE m.id = 1 AND m.name = ? COLLATE BINARY AND m.dropping = 0
  AND n.name = ? COLLATE BINARY AND n.dropped_snapshot IS NULL
  AND n.contained_field = ? COLLATE BINARY AND typeof(n.contained_value) = 'integer' AND n.contained_value = ?`,
			Args:            []any{target.catalog, target.namespace, predField, predValue},
			ExpectScalarInt: int64Ptr(1),
		},
		// 1: table name does not already exist live in namespace
		{
			SQL:             `SELECT COUNT(*) FROM tables WHERE namespace = ? COLLATE BINARY AND name = ? COLLATE BINARY AND dropped_snapshot IS NULL`,
			Args:            []any{target.namespace, req.Name},
			ExpectScalarInt: int64Ptr(0),
		},
		// 2: allocate snapshot, schema_version, table_id
		{
			SQL:            `UPDATE meta SET next_snapshot = next_snapshot + 1, schema_version = schema_version + 1, next_table_id = next_table_id + 1 WHERE id = 1 AND name = ? COLLATE BINARY AND dropping = 0`,
			Args:           []any{target.catalog},
			ExpectAffected: int64Ptr(1),
		},
		// 3: snapshot row
		{
			SQL:  `INSERT INTO snapshots(id, ts, author, message, schema_version) SELECT next_snapshot - 1, ?, NULL, 'create table', schema_version FROM meta WHERE id = 1`,
			Args: []any{nowTS},
		},
		// 4: tables row
		{
			SQL: `INSERT INTO tables(id, namespace_id, namespace, name, table_uuid, comment, next_row_id, next_field_id, schema_version, created_snapshot)
SELECT m.next_table_id - 1, n.id, n.name, ?, ?, ?, 0, ?, 0, m.next_snapshot - 1
FROM meta AS m JOIN namespaces AS n ON 1 = 1
WHERE m.id = 1 AND n.name = ? COLLATE BINARY AND n.dropped_snapshot IS NULL`,
			Args: []any{req.Name, tableUUID, commentArg, int64(len(req.Columns) + 1), target.namespace},
		},
		// 5: table_versions row
		{
			SQL: `INSERT INTO table_versions(table_id, namespace_id, namespace, name, comment, begin_snapshot)
SELECT m.next_table_id - 1, n.id, n.name, ?, ?, m.next_snapshot - 1
FROM meta AS m JOIN namespaces AS n ON 1 = 1
WHERE m.id = 1 AND n.name = ? COLLATE BINARY AND n.dropped_snapshot IS NULL`,
			Args: []any{req.Name, commentArg, target.namespace},
		},
		// 6: table_totals row
		{
			SQL: `INSERT INTO table_totals(table_id, record_count, file_count, file_size_bytes) SELECT next_table_id - 1, 0, 0, 0 FROM meta WHERE id = 1`,
		},
		// 7: change row
		{
			SQL: `INSERT INTO changes(snapshot_id, kind, object_id) SELECT next_snapshot - 1, 'table_created', next_table_id - 1 FROM meta WHERE id = 1`,
		},
	}
	colDTOs := make([]scopedColumnDTO, 0, len(req.Columns))
	for i, col := range req.Columns {
		fieldID := int64(i + 1)
		ordinal := int64(i)
		nullable := true
		if col.Nullable != nil {
			nullable = *col.Nullable
		}
		nullableInt := int64(0)
		if nullable {
			nullableInt = 1
		}
		var typeParamsArg any
		if len(col.TypeParams) > 0 {
			typeParamsArg = string(col.TypeParams)
		}
		var colCommentArg any
		if col.Comment != "" {
			colCommentArg = col.Comment
		}
		stmts = append(stmts, Statement{
			SQL: `INSERT INTO columns(table_id, field_id, parent_field_id, ordinal, name, type, type_params_json, nullable, comment, begin_snapshot)
SELECT next_table_id - 1, ?, NULL, ?, ?, ?, ?, ?, ?, next_snapshot - 1 FROM meta WHERE id = 1`,
			Args: []any{fieldID, ordinal, col.Name, col.Type, typeParamsArg, nullableInt, colCommentArg},
		})
		colDTOs = append(colDTOs, scopedColumnDTO{
			FieldID:    fieldID,
			Ordinal:    ordinal,
			Name:       col.Name,
			Type:       col.Type,
			TypeParams: col.TypeParams,
			Nullable:   nullable,
			Comment:    col.Comment,
		})
	}
	// Verify encoded response fits within scopedTableResponseLimit before committing.
	preview, err := json.Marshal(scopedTableDTO{
		NamespaceID:    math.MaxInt64,
		Namespace:      target.namespace,
		Containment:    scopedNamespaceContainment{Field: predField, Value: predValue},
		TableID:        math.MaxInt64,
		TableUUID:      tableUUID,
		Name:           req.Name,
		ReadSnapshotID: math.MaxInt64,
		Columns:        colDTOs,
	})
	if err != nil || len(preview) > scopedTableResponseLimit {
		scopedNamespaceRefusal(w, http.StatusBadRequest)
		return
	}
	readbackIdx := len(stmts)
	stmts = append(stmts, Statement{
		SQL: `SELECT n.id, m.next_table_id - 1, m.next_snapshot - 1 FROM meta AS m JOIN namespaces AS n ON 1 = 1
WHERE m.id = 1 AND n.name = ? COLLATE BINARY AND n.dropped_snapshot IS NULL`,
		Args: []any{target.namespace},
	})
	results, _, err := t.SubmitTxCtx(r.Context(), TxRequest{
		Client: scopedMutationClient,
		TxID:   "tbl-create-" + uuid.NewString(),
		Stmts:  stmts,
	})
	if err != nil {
		var expErr *ExpectFailedError
		if errors.As(err, &expErr) {
			if expErr.Index == 1 {
				scopedNamespaceRefusal(w, http.StatusConflict)
				return
			}
			scopedNamespaceRefusal(w, http.StatusNotFound)
			return
		}
		if isMissingScopedSchemaError(err) {
			scopedNamespaceRefusal(w, http.StatusNotFound)
		} else {
			scopedNamespaceRefusal(w, http.StatusServiceUnavailable)
		}
		return
	}
	if len(results) <= readbackIdx || len(results[readbackIdx].Rows) != 1 || len(results[readbackIdx].Rows[0]) != 3 {
		scopedNamespaceRefusal(w, http.StatusServiceUnavailable)
		return
	}
	row := results[readbackIdx].Rows[0]
	nsID, ok1 := row[0].(int64)
	tableID, ok2 := row[1].(int64)
	snap, ok3 := row[2].(int64)
	if !ok1 || !ok2 || !ok3 {
		scopedNamespaceRefusal(w, http.StatusServiceUnavailable)
		return
	}
	scopedMetadataReply(w, http.StatusCreated, scopedTableDTO{
		NamespaceID:    nsID,
		Namespace:      target.namespace,
		Containment:    scopedNamespaceContainment{Field: predField, Value: predValue},
		TableID:        tableID,
		TableUUID:      tableUUID,
		Name:           req.Name,
		ReadSnapshotID: snap,
		Columns:        colDTOs,
	}, scopedTableResponseLimit)
}

func (s *Server) dropScopedTable(w http.ResponseWriter, r *http.Request, target scopedMetadataTarget, rsa *readSessionAuthority, t *Tenant, getManifest func(context.Context) (*Manifest, error)) {
	if !rsa.writable {
		scopedNamespaceRefusal(w, http.StatusForbidden)
		return
	}
	predField, predValue, ok := rsa.containmentPredicate()
	if !ok {
		scopedNamespaceRefusal(w, http.StatusNotFound)
		return
	}
	if s.auth.readSession.isReadOnlyNamespace(target.namespace) {
		scopedNamespaceRefusal(w, http.StatusForbidden)
		return
	}
	if status := s.scopedNamespaceOwned(r.Context(), target.tenant, t, getManifest); status != 0 {
		scopedNamespaceRefusal(w, status)
		return
	}
	nowTS := time.Now().UTC().Format(time.RFC3339Nano)
	results, _, err := t.SubmitTxCtx(r.Context(), TxRequest{
		Client: scopedMutationClient,
		TxID:   "tbl-drop-" + uuid.NewString(),
		Stmts: []Statement{
			// 0: verify live catalog, team-owned namespace, and live table
			{
				SQL: `SELECT COUNT(*) FROM meta AS m
JOIN namespaces AS n ON 1 = 1
JOIN tables AS t ON t.namespace_id = n.id
WHERE m.id = 1 AND m.name = ? COLLATE BINARY AND m.dropping = 0
  AND n.name = ? COLLATE BINARY AND n.dropped_snapshot IS NULL
  AND n.contained_field = ? COLLATE BINARY AND typeof(n.contained_value) = 'integer' AND n.contained_value = ?
  AND t.name = ? COLLATE BINARY AND t.dropped_snapshot IS NULL`,
				Args:            []any{target.catalog, target.namespace, predField, predValue, target.table},
				ExpectScalarInt: int64Ptr(1),
			},
			// 1: if table_uuid fence is supplied, verify it matches the live table (409 on recreated object)
			{
				SQL: `SELECT COUNT(*) FROM namespaces AS n JOIN tables AS t ON t.namespace_id = n.id
WHERE n.name = ? COLLATE BINARY AND n.dropped_snapshot IS NULL
  AND t.name = ? COLLATE BINARY AND t.dropped_snapshot IS NULL
  AND (? = '' OR t.table_uuid = ? COLLATE BINARY)`,
				Args:            []any{target.namespace, target.table, target.tableUUID, target.tableUUID},
				ExpectScalarInt: int64Ptr(1),
			},
			// 2: read table_uuid before drop
			{
				SQL: `SELECT t.table_uuid FROM namespaces AS n JOIN tables AS t ON t.namespace_id = n.id
WHERE n.name = ? COLLATE BINARY AND n.dropped_snapshot IS NULL
  AND t.name = ? COLLATE BINARY AND t.dropped_snapshot IS NULL LIMIT 1`,
				Args: []any{target.namespace, target.table},
			},
			// 3: allocate snapshot
			{
				SQL:            `UPDATE meta SET next_snapshot = next_snapshot + 1, schema_version = schema_version + 1 WHERE id = 1 AND name = ? COLLATE BINARY AND dropping = 0`,
				Args:           []any{target.catalog},
				ExpectAffected: int64Ptr(1),
			},
			// 4: snapshot row
			{
				SQL:  `INSERT INTO snapshots(id, ts, author, message, schema_version) SELECT next_snapshot - 1, ?, NULL, 'drop table', schema_version FROM meta WHERE id = 1`,
				Args: []any{nowTS},
			},
			// 5: change row
			{
				SQL: `INSERT INTO changes(snapshot_id, kind, object_id)
SELECT m.next_snapshot - 1, 'table_dropped', t.id
FROM meta AS m JOIN namespaces AS n ON 1 = 1 JOIN tables AS t ON t.namespace_id = n.id
WHERE m.id = 1 AND n.name = ? COLLATE BINARY AND n.dropped_snapshot IS NULL
  AND t.name = ? COLLATE BINARY AND t.dropped_snapshot IS NULL`,
				Args: []any{target.namespace, target.table},
			},
			// 6: end live columns, table_versions, and mark table dropped (leave file/DV retirement to batched worker)
			{
				SQL: `UPDATE columns SET end_snapshot = (SELECT next_snapshot - 1 FROM meta WHERE id = 1)
WHERE table_id = (SELECT t.id FROM namespaces AS n JOIN tables AS t ON t.namespace_id = n.id WHERE n.name = ? COLLATE BINARY AND n.dropped_snapshot IS NULL AND t.name = ? COLLATE BINARY AND t.dropped_snapshot IS NULL)
  AND end_snapshot IS NULL`,
				Args: []any{target.namespace, target.table},
			},
			{
				SQL: `UPDATE table_versions SET end_snapshot = (SELECT next_snapshot - 1 FROM meta WHERE id = 1)
WHERE table_id = (SELECT t.id FROM namespaces AS n JOIN tables AS t ON t.namespace_id = n.id WHERE n.name = ? COLLATE BINARY AND n.dropped_snapshot IS NULL AND t.name = ? COLLATE BINARY AND t.dropped_snapshot IS NULL)
  AND end_snapshot IS NULL`,
				Args: []any{target.namespace, target.table},
			},
			{
				SQL: `UPDATE tables SET dropped_snapshot = (SELECT next_snapshot - 1 FROM meta WHERE id = 1)
WHERE namespace_id = (SELECT id FROM namespaces WHERE name = ? COLLATE BINARY AND dropped_snapshot IS NULL AND contained_field = ? COLLATE BINARY AND contained_value = ?)
  AND name = ? COLLATE BINARY AND dropped_snapshot IS NULL`,
				Args:           []any{target.namespace, predField, predValue, target.table},
				ExpectAffected: int64Ptr(1),
			},
			// 9: readback snapshot
			{
				SQL: `SELECT next_snapshot - 1 FROM meta WHERE id = 1`,
			},
		},
	})
	if err != nil {
		var expErr *ExpectFailedError
		if errors.As(err, &expErr) {
			if expErr.Index == 1 {
				scopedNamespaceRefusal(w, http.StatusConflict)
				return
			}
			scopedNamespaceRefusal(w, http.StatusNotFound)
			return
		}
		if isMissingScopedSchemaError(err) {
			scopedNamespaceRefusal(w, http.StatusNotFound)
		} else {
			scopedNamespaceRefusal(w, http.StatusServiceUnavailable)
		}
		return
	}
	if len(results) != 10 || len(results[2].Rows) != 1 || len(results[9].Rows) != 1 {
		scopedNamespaceRefusal(w, http.StatusServiceUnavailable)
		return
	}
	tableUUID, ok1 := results[2].Rows[0][0].(string)
	snap, ok2 := results[9].Rows[0][0].(int64)
	if !ok1 || !ok2 {
		scopedNamespaceRefusal(w, http.StatusServiceUnavailable)
		return
	}
	scopedNamespaceReply(w, http.StatusOK, dropTableResp{
		DroppedSnapshot: snap,
		TableUUID:       tableUUID,
	})
}

func (s *Server) commitScopedTable(w http.ResponseWriter, r *http.Request, target scopedMetadataTarget, rsa *readSessionAuthority, t *Tenant, getManifest func(context.Context) (*Manifest, error)) {
	if !rsa.writable {
		scopedNamespaceRefusal(w, http.StatusForbidden)
		return
	}
	predField, predValue, ok := rsa.containmentPredicate()
	if !ok {
		scopedNamespaceRefusal(w, http.StatusNotFound)
		return
	}
	if s.auth.readSession.isReadOnlyNamespace(target.namespace) {
		scopedNamespaceRefusal(w, http.StatusForbidden)
		return
	}
	var req commitTableReq
	if !decodeScopedJSONBody(w, r, &req) || !isCanonicalLowerUUID(req.TableUUID) || req.BaseSnapshotID < 0 ||
		(len(req.AddFiles) == 0 && len(req.AddDeleteFiles) == 0) ||
		len(req.AddFiles)+len(req.AddDeleteFiles) > scopedMutationMaxFiles {
		scopedNamespaceRefusal(w, http.StatusBadRequest)
		return
	}
	// Defense-in-depth: verify every committed data file and deletion vector URI against
	// the verified primary-catalog write grant prefix before opening any SQLite transaction,
	// and guard against int64 overflow in counts and byte totals.
	var totalAddedRecords, totalAddedBytes int64
	for _, f := range req.AddFiles {
		if f.RecordCount < 0 || f.FileSizeBytes < 0 || (f.FooterSize != nil && *f.FooterSize < 0) {
			scopedNamespaceRefusal(w, http.StatusBadRequest)
			return
		}
		if !rsa.allowsPrimaryCatalogWriteLocation(target.catalog, f.Path) {
			scopedNamespaceRefusal(w, http.StatusForbidden)
			return
		}
		if totalAddedRecords > math.MaxInt64-f.RecordCount || totalAddedBytes > math.MaxInt64-f.FileSizeBytes {
			scopedNamespaceRefusal(w, http.StatusBadRequest)
			return
		}
		totalAddedRecords += f.RecordCount
		totalAddedBytes += f.FileSizeBytes
	}
	seenDeleteTargets := make(map[int64]bool, len(req.AddDeleteFiles))
	for _, dv := range req.AddDeleteFiles {
		if dv.DataFileID <= 0 || dv.DeleteCount <= 0 || dv.FileSizeBytes < 0 || seenDeleteTargets[dv.DataFileID] {
			scopedNamespaceRefusal(w, http.StatusBadRequest)
			return
		}
		if !rsa.allowsPrimaryCatalogWriteLocation(target.catalog, dv.Path) {
			scopedNamespaceRefusal(w, http.StatusForbidden)
			return
		}
		seenDeleteTargets[dv.DataFileID] = true
	}
	if status := s.scopedNamespaceOwned(r.Context(), target.tenant, t, getManifest); status != 0 {
		scopedNamespaceRefusal(w, status)
		return
	}

	nowTS := time.Now().UTC().Format(time.RFC3339Nano)
	fileIDIncrement := int64(len(req.AddFiles) + len(req.AddDeleteFiles))
	// 1: verify immutable table_uuid, retention floor, and table-level base_snapshot_id fence
	// (409 Conflict if expired/stale/recreated/concurrently modified).
	// Fast-path: when base_snapshot_id == m.next_snapshot - 1 (head), skip all history probes.
	// Older bases use separate indexable range probes (no OR across begin_snapshot/end_snapshot):
	// - Append-only commits (no add_delete_files) fence only on DDL (columns, table_versions),
	//   so concurrent INSERTs into a busy table succeed without spurious 409s.
	// - Delete commits (add_delete_files non-empty) also fence on files and dvs for that table.
	fenceSQL := `SELECT COUNT(*) FROM meta AS m
JOIN namespaces AS n ON 1 = 1
JOIN tables AS t ON t.namespace_id = n.id
WHERE m.id = 1 AND m.name = ? COLLATE BINARY AND m.dropping = 0
  AND ? >= m.earliest_snapshot_id AND ? >= t.created_snapshot AND ? <= m.next_snapshot - 1
  AND n.name = ? COLLATE BINARY AND n.dropped_snapshot IS NULL
  AND n.contained_field = ? COLLATE BINARY AND typeof(n.contained_value) = 'integer' AND n.contained_value = ?
  AND t.name = ? COLLATE BINARY AND t.dropped_snapshot IS NULL
  AND t.table_uuid = ? COLLATE BINARY
  AND (? = m.next_snapshot - 1 OR (
        NOT EXISTS (SELECT 1 FROM columns AS c WHERE c.table_id = t.id AND c.begin_snapshot > ?)
    AND NOT EXISTS (SELECT 1 FROM columns AS c WHERE c.table_id = t.id AND c.end_snapshot IS NOT NULL AND c.end_snapshot > ?)
    AND NOT EXISTS (SELECT 1 FROM table_versions AS tv WHERE tv.table_id = t.id AND tv.begin_snapshot > ?)
    AND NOT EXISTS (SELECT 1 FROM table_versions AS tv WHERE tv.table_id = t.id AND tv.end_snapshot IS NOT NULL AND tv.end_snapshot > ?)`
	fenceArgs := []any{
		target.catalog,
		req.BaseSnapshotID, req.BaseSnapshotID, req.BaseSnapshotID,
		target.namespace, predField, predValue,
		target.table, req.TableUUID,
		req.BaseSnapshotID,
		req.BaseSnapshotID, req.BaseSnapshotID,
		req.BaseSnapshotID, req.BaseSnapshotID,
	}
	if len(req.AddDeleteFiles) > 0 {
		fenceSQL += `
    AND NOT EXISTS (SELECT 1 FROM files AS f WHERE f.table_id = t.id AND f.begin_snapshot > ?)
    AND NOT EXISTS (SELECT 1 FROM files AS f WHERE f.end_snapshot IS NOT NULL AND f.end_snapshot > ? AND f.table_id = t.id)
    AND NOT EXISTS (SELECT 1 FROM dvs AS d WHERE d.table_id = t.id AND d.begin_snapshot > ?)
    AND NOT EXISTS (SELECT 1 FROM dvs AS d WHERE d.end_snapshot IS NOT NULL AND d.end_snapshot > ? AND d.table_id = t.id)`
		fenceArgs = append(fenceArgs,
			req.BaseSnapshotID, req.BaseSnapshotID,
			req.BaseSnapshotID, req.BaseSnapshotID,
		)
	}
	fenceSQL += `
  ))`

	stmts := []Statement{
		// 0: verify live catalog, team-owned namespace, and live table exist (404 if absent)
		{
			SQL: `SELECT COUNT(*) FROM meta AS m
JOIN namespaces AS n ON 1 = 1
JOIN tables AS t ON t.namespace_id = n.id
WHERE m.id = 1 AND m.name = ? COLLATE BINARY AND m.dropping = 0
  AND n.name = ? COLLATE BINARY AND n.dropped_snapshot IS NULL
  AND n.contained_field = ? COLLATE BINARY AND typeof(n.contained_value) = 'integer' AND n.contained_value = ?
  AND t.name = ? COLLATE BINARY AND t.dropped_snapshot IS NULL`,
			Args:            []any{target.catalog, target.namespace, predField, predValue, target.table},
			ExpectScalarInt: int64Ptr(1),
		},
		// 1: table-level base_snapshot_id & retention fence (409 Conflict)
		{
			SQL:             fenceSQL,
			Args:            fenceArgs,
			ExpectScalarInt: int64Ptr(1),
		},
	}
	// Check table_layouts, undrained file removals, live file cap, row-ID / total overflow bounds, and DV bounds atomically.
	firstConflictCheckIdx := len(stmts)
	// Reject tables with an active object-prefix layout or active partition spec on data-file appends
	// until local commit carries partition values.
	stmts = append(stmts, Statement{
		SQL: `SELECT COUNT(*) FROM table_layouts AS l JOIN tables AS t ON l.table_id = t.id
WHERE t.table_uuid = ? COLLATE BINARY AND t.dropped_snapshot IS NULL AND l.end_snapshot IS NULL`,
		Args:            []any{req.TableUUID},
		ExpectScalarInt: int64Ptr(0),
	})
	if len(req.AddFiles) > 0 {
		stmts = append(stmts, Statement{
			SQL: `SELECT COUNT(*) FROM partition_specs AS ps JOIN tables AS t ON ps.table_id = t.id
WHERE t.table_uuid = ? COLLATE BINARY AND t.dropped_snapshot IS NULL AND ps.end_snapshot IS NULL`,
			Args:            []any{req.TableUUID},
			ExpectScalarInt: int64Ptr(0),
		})
	}
	// Fence every added data file and deletion vector against undrained cleanup removals
	// and duplicate path registration (prevents append replay from double-registering a file).
	for _, f := range req.AddFiles {
		stmts = append(stmts,
			Statement{
				SQL:             `SELECT COUNT(*) FROM removals WHERE path = ? COLLATE BINARY AND drained_at IS NULL`,
				Args:            []any{f.Path},
				ExpectScalarInt: int64Ptr(0),
			},
			Statement{
				SQL:             `SELECT COUNT(*) FROM files WHERE path = ? COLLATE BINARY`,
				Args:            []any{f.Path},
				ExpectScalarInt: int64Ptr(0),
			},
		)
	}
	for _, dv := range req.AddDeleteFiles {
		stmts = append(stmts,
			Statement{
				SQL:             `SELECT COUNT(*) FROM removals WHERE path = ? COLLATE BINARY AND drained_at IS NULL`,
				Args:            []any{dv.Path},
				ExpectScalarInt: int64Ptr(0),
			},
			Statement{
				SQL:             `SELECT COUNT(*) FROM dvs WHERE path = ? COLLATE BINARY`,
				Args:            []any{dv.Path},
				ExpectScalarInt: int64Ptr(0),
			},
		)
	}
	if len(req.AddFiles) > 0 {
		stmts = append(stmts,
			// Keep total live files within scopedScanMaxFiles so head remains readable via scoped scan.
			Statement{
				SQL:             `SELECT CASE WHEN (SELECT COUNT(*) FROM files AS f JOIN tables AS t ON f.table_id = t.id WHERE t.table_uuid = ? COLLATE BINARY AND t.dropped_snapshot IS NULL AND f.end_snapshot IS NULL) + ? <= ? THEN 1 ELSE 0 END`,
				Args:            []any{req.TableUUID, int64(len(req.AddFiles)), int64(scopedScanMaxFiles)},
				ExpectScalarInt: int64Ptr(1),
			},
			// Prevent int64 overflow in tables.next_row_id and table_totals.
			Statement{
				SQL: `SELECT COUNT(*) FROM tables AS t JOIN table_totals AS tot ON tot.table_id = t.id
WHERE t.table_uuid = ? COLLATE BINARY AND t.dropped_snapshot IS NULL
  AND t.next_row_id <= ? - ?
  AND tot.record_count <= ? - ?
  AND tot.file_size_bytes <= ? - ?`,
				Args: []any{
					req.TableUUID,
					int64(math.MaxInt64), totalAddedRecords,
					int64(math.MaxInt64), totalAddedRecords,
					int64(math.MaxInt64), totalAddedBytes,
				},
				ExpectScalarInt: int64Ptr(1),
			},
		)
	}
	for _, dv := range req.AddDeleteFiles {
		// Target file must belong to this table, be live, satisfy delete_count <= record_count,
		// and never shrink below any existing live deletion vector's delete_count (prevents row resurrection).
		stmts = append(stmts, Statement{
			SQL: `SELECT COUNT(*) FROM tables AS t JOIN files AS f ON f.table_id = t.id
WHERE t.table_uuid = ? COLLATE BINARY AND t.dropped_snapshot IS NULL
  AND f.id = ? AND f.end_snapshot IS NULL
  AND ? <= f.record_count
  AND ? >= COALESCE((SELECT delete_count FROM dvs WHERE data_file_id = f.id AND end_snapshot IS NULL), 0)`,
			Args:            []any{req.TableUUID, dv.DataFileID, dv.DeleteCount, dv.DeleteCount},
			ExpectScalarInt: int64Ptr(1),
		})
	}
	lastConflictCheckIdx := len(stmts) - 1

	// Allocate snapshot and file IDs.
	stmts = append(stmts,
		Statement{
			SQL:            `UPDATE meta SET next_snapshot = next_snapshot + 1, next_file_id = next_file_id + ? WHERE id = 1 AND name = ? COLLATE BINARY AND dropping = 0`,
			Args:           []any{fileIDIncrement, target.catalog},
			ExpectAffected: int64Ptr(1),
		},
		Statement{
			SQL:  `INSERT INTO snapshots(id, ts, author, message, schema_version) SELECT next_snapshot - 1, ?, NULL, 'commit', schema_version FROM meta WHERE id = 1`,
			Args: []any{nowTS},
		},
	)

	// Insert added data files with sequential file_ids and row_id_starts.
	var rowOffset int64
	for i, f := range req.AddFiles {
		idOffset := fileIDIncrement - int64(i)
		var footerArg any
		if f.FooterSize != nil {
			footerArg = *f.FooterSize
		}
		stmts = append(stmts, Statement{
			SQL: `INSERT INTO files(id, table_id, begin_snapshot, end_snapshot, record_count, file_size_bytes, footer_size, row_id_start, explicit_row_ids, stats_state, file_format, path)
SELECT m.next_file_id - ?, t.id, m.next_snapshot - 1, NULL, ?, ?, ?, t.next_row_id + ?, 0, 'pending', 'parquet', ?
FROM meta AS m JOIN tables AS t ON 1 = 1
WHERE m.id = 1 AND t.table_uuid = ? COLLATE BINARY AND t.dropped_snapshot IS NULL`,
			Args: []any{idOffset, f.RecordCount, f.FileSizeBytes, footerArg, rowOffset, f.Path, req.TableUUID},
		})
		rowOffset += f.RecordCount
	}
	if len(req.AddFiles) > 0 {
		stmts = append(stmts,
			Statement{
				SQL:  `UPDATE tables SET next_row_id = next_row_id + ? WHERE table_uuid = ? COLLATE BINARY AND dropped_snapshot IS NULL`,
				Args: []any{totalAddedRecords, req.TableUUID},
			},
			Statement{
				SQL: `UPDATE table_totals SET record_count = record_count + ?, file_count = file_count + ?, file_size_bytes = file_size_bytes + ?
WHERE table_id = (SELECT id FROM tables WHERE table_uuid = ? COLLATE BINARY AND dropped_snapshot IS NULL)`,
				Args: []any{totalAddedRecords, int64(len(req.AddFiles)), totalAddedBytes, req.TableUUID},
			},
			Statement{
				SQL: `INSERT INTO changes(snapshot_id, kind, object_id)
SELECT m.next_snapshot - 1, 'table_inserted_into', t.id
FROM meta AS m JOIN tables AS t ON 1 = 1
WHERE m.id = 1 AND t.table_uuid = ? COLLATE BINARY AND t.dropped_snapshot IS NULL`,
				Args: []any{req.TableUUID},
			},
		)
	}

	// Insert added deletion vectors (ending any previous live DV on the same data_file_id).
	for i, dv := range req.AddDeleteFiles {
		idOffset := fileIDIncrement - int64(len(req.AddFiles)+i)
		stmts = append(stmts,
			Statement{
				SQL: `UPDATE dvs SET end_snapshot = (SELECT next_snapshot - 1 FROM meta WHERE id = 1)
WHERE data_file_id = ? AND end_snapshot IS NULL`,
				Args: []any{dv.DataFileID},
			},
			Statement{
				SQL: `INSERT INTO dvs(data_file_id, table_id, delete_file_id, path, delete_count, file_size_bytes, begin_snapshot, end_snapshot, epoch)
SELECT ?, t.id, m.next_file_id - ?, ?, ?, ?, m.next_snapshot - 1, NULL,
       COALESCE((SELECT MAX(epoch) FROM dvs WHERE data_file_id = ?), 0) + 1
FROM meta AS m JOIN tables AS t ON 1 = 1
WHERE m.id = 1 AND t.table_uuid = ? COLLATE BINARY AND t.dropped_snapshot IS NULL`,
				Args: []any{dv.DataFileID, idOffset, dv.Path, dv.DeleteCount, dv.FileSizeBytes, dv.DataFileID, req.TableUUID},
			},
		)
	}
	if len(req.AddDeleteFiles) > 0 {
		stmts = append(stmts, Statement{
			SQL: `INSERT INTO changes(snapshot_id, kind, object_id)
SELECT m.next_snapshot - 1, 'table_deleted_from', t.id
FROM meta AS m JOIN tables AS t ON 1 = 1
WHERE m.id = 1 AND t.table_uuid = ? COLLATE BINARY AND t.dropped_snapshot IS NULL`,
			Args: []any{req.TableUUID},
		})
	}

	readbackIdx := len(stmts)
	stmts = append(stmts, Statement{SQL: `SELECT next_snapshot - 1 FROM meta WHERE id = 1`})

	results, _, err := t.SubmitTxCtx(r.Context(), TxRequest{
		Client: scopedMutationClient,
		TxID:   "tbl-commit-" + uuid.NewString(),
		Stmts:  stmts,
	})
	if err != nil {
		var expErr *ExpectFailedError
		if errors.As(err, &expErr) {
			if expErr.Index == 1 || (expErr.Index >= firstConflictCheckIdx && expErr.Index <= lastConflictCheckIdx) {
				scopedNamespaceRefusal(w, http.StatusConflict)
				return
			}
			scopedNamespaceRefusal(w, http.StatusNotFound)
			return
		}
		if isMissingScopedSchemaError(err) {
			scopedNamespaceRefusal(w, http.StatusNotFound)
		} else {
			scopedNamespaceRefusal(w, http.StatusServiceUnavailable)
		}
		return
	}
	if len(results) <= readbackIdx || len(results[readbackIdx].Rows) != 1 || len(results[readbackIdx].Rows[0]) != 1 {
		scopedNamespaceRefusal(w, http.StatusServiceUnavailable)
		return
	}
	snap, ok := results[readbackIdx].Rows[0][0].(int64)
	if !ok {
		scopedNamespaceRefusal(w, http.StatusServiceUnavailable)
		return
	}
	scopedNamespaceReply(w, http.StatusOK, commitTableResp{
		SnapshotID:   snap,
		TableUUID:    req.TableUUID,
		FilesAdded:   len(req.AddFiles),
		DeletesAdded: len(req.AddDeleteFiles),
	})
}
