package hoglite

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"
)

const (
	scopedTableResponseLimit = 256 << 10
	scopedScanResponseLimit  = 1 << 20
	scopedListResponseLimit  = 256 << 10
	scopedListMaxItems       = 1000
	scopedScanMaxFiles       = 1000
	scopedColumnMaxDepth     = 64
)

type scopedMetadataKind int

const (
	scopedMetadataKindNamespaces scopedMetadataKind = iota + 1
	scopedMetadataKindNamespace
	scopedMetadataKindTables
	scopedMetadataKindTable
	scopedMetadataKindScan
	scopedMetadataKindCreateNamespace
	scopedMetadataKindDropNamespace
	scopedMetadataKindCreateTable
	scopedMetadataKindDropTable
	scopedMetadataKindCommitTable
)

type scopedMetadataTarget struct {
	kind      scopedMetadataKind
	tenant    string
	catalog   string
	namespace string
	table     string
	after     string
	snapshot  int64
	tableUUID string
	path      string
}

func (t scopedMetadataTarget) namespaceTarget() scopedNamespaceTarget {
	return scopedNamespaceTarget{
		tenant:    t.tenant,
		catalog:   t.catalog,
		namespace: t.namespace,
		path:      t.path,
	}
}

type scopedNamespacesDTO struct {
	Namespaces []scopedNamespaceDTO `json:"namespaces"`
	NextAfter  string               `json:"next_after,omitempty"`
}

type scopedTablesDTO struct {
	NamespaceID int64                 `json:"namespace_id"`
	Namespace   string                `json:"namespace"`
	Tables      []scopedTableEntryDTO `json:"tables"`
	NextAfter   string                `json:"next_after,omitempty"`
}

type scopedTableEntryDTO struct {
	TableID   int64  `json:"table_id"`
	TableUUID string `json:"table_uuid"`
	Name      string `json:"name"`
}

type scopedTableDTO struct {
	NamespaceID    int64                      `json:"namespace_id"`
	Namespace      string                     `json:"namespace"`
	Containment    scopedNamespaceContainment `json:"containment"`
	TableID        int64                      `json:"table_id"`
	TableUUID      string                     `json:"table_uuid"`
	Name           string                     `json:"name"`
	ReadSnapshotID int64                      `json:"read_snapshot_id"`
	Columns        []scopedColumnDTO          `json:"columns"`
}

type scopedColumnDTO struct {
	FieldID    int64             `json:"field_id"`
	Ordinal    int64             `json:"ordinal"`
	Name       string            `json:"name"`
	Type       string            `json:"type"`
	TypeParams json.RawMessage   `json:"type_params,omitempty"`
	Nullable   bool              `json:"nullable"`
	Children   []scopedColumnDTO `json:"children,omitempty"`
	Comment    string            `json:"comment,omitempty"`
}

type scopedScanDTO struct {
	NamespaceID    int64               `json:"namespace_id"`
	Namespace      string              `json:"namespace"`
	TableUUID      string              `json:"table_uuid"`
	ReadSnapshotID int64               `json:"read_snapshot_id"`
	Files          []scopedScanFileDTO `json:"files"`
}

type scopedScanFileDTO struct {
	DataFile   scopedDataFileDTO    `json:"data_file"`
	DeleteFile *scopedDeleteFileDTO `json:"delete_file"`
}

type scopedDataFileDTO struct {
	DataFileID     int64  `json:"data_file_id"`
	Path           string `json:"path"`
	FileFormat     string `json:"file_format"`
	RecordCount    int64  `json:"record_count"`
	FileSizeBytes  int64  `json:"file_size_bytes"`
	FooterSize     *int64 `json:"footer_size"`
	RowIDStart     int64  `json:"row_id_start"`
	StatsState     string `json:"stats_state"`
	BeginSnapshot  int64  `json:"begin_snapshot"`
	ExplicitRowIDs bool   `json:"explicit_row_ids"`
}

type scopedDeleteFileDTO struct {
	DeleteFileID  int64  `json:"delete_file_id"`
	Path          string `json:"path"`
	DeleteCount   int64  `json:"delete_count"`
	FileSizeBytes int64  `json:"file_size_bytes"`
	BeginSnapshot int64  `json:"begin_snapshot"`
}

// parseScopedMetadataTarget validates origin-form paths and exact queries for scoped metadata routes.
func parseScopedMetadataTarget(r *http.Request) (scopedMetadataTarget, bool) {
	var target scopedMetadataTarget
	raw := r.RequestURI
	if raw == "" {
		raw = r.URL.RequestURI()
	}
	if len(raw) > scopedNamespaceTargetLimit || r.URL.Fragment != "" {
		return target, false
	}
	switch r.Method {
	case http.MethodGet:
		if r.ContentLength != 0 || len(r.TransferEncoding) != 0 || (r.Body != nil && r.Body != http.NoBody) {
			return target, false
		}
	case http.MethodDelete:
		if r.ContentLength != 0 || len(r.TransferEncoding) != 0 || (r.Body != nil && r.Body != http.NoBody) {
			return target, false
		}
	case http.MethodPost:
		// Body is validated and size-limited inside the mutation handler.
	default:
		return target, false
	}
	path := r.URL.EscapedPath()
	parts := strings.Split(path, "/")
	if len(parts) < 9 || parts[0] != "" || parts[1] != "v1" ||
		parts[2] != "t" || parts[4] != "metadata" || parts[5] != "v1" ||
		parts[6] != "catalogs" || parts[8] != "namespaces" {
		return target, false
	}
	tenant, ok1 := decodeScopedSegment(parts[3])
	catalog, ok2 := decodeScopedSegment(parts[7])
	if !ok1 || !ok2 || !validTID(tenant) {
		return target, false
	}
	if len(parts) == 9 {
		if r.Method == http.MethodPost {
			if r.URL.RawQuery != "" || r.URL.ForceQuery || raw != path {
				return target, false
			}
			return scopedMetadataTarget{
				kind:    scopedMetadataKindCreateNamespace,
				tenant:  tenant,
				catalog: catalog,
				path:    path,
			}, true
		}
		if r.Method != http.MethodGet {
			return target, false
		}
		after, ok := parseScopedListQuery(raw, path, r.URL.RawQuery, r.URL.ForceQuery)
		if !ok {
			return target, false
		}
		return scopedMetadataTarget{
			kind:    scopedMetadataKindNamespaces,
			tenant:  tenant,
			catalog: catalog,
			after:   after,
			path:    path,
		}, true
	}
	namespace, ok3 := decodeScopedSegment(parts[9])
	if !ok3 {
		return target, false
	}
	switch len(parts) {
	case 10:
		if r.URL.RawQuery != "" || r.URL.ForceQuery || raw != path {
			return target, false
		}
		switch r.Method {
		case http.MethodGet:
			return scopedMetadataTarget{
				kind:      scopedMetadataKindNamespace,
				tenant:    tenant,
				catalog:   catalog,
				namespace: namespace,
				path:      path,
			}, true
		case http.MethodDelete:
			return scopedMetadataTarget{
				kind:      scopedMetadataKindDropNamespace,
				tenant:    tenant,
				catalog:   catalog,
				namespace: namespace,
				path:      path,
			}, true
		default:
			return target, false
		}
	case 11:
		if parts[10] != "tables" {
			return target, false
		}
		if r.Method == http.MethodPost {
			if r.URL.RawQuery != "" || r.URL.ForceQuery || raw != path {
				return target, false
			}
			return scopedMetadataTarget{
				kind:      scopedMetadataKindCreateTable,
				tenant:    tenant,
				catalog:   catalog,
				namespace: namespace,
				path:      path,
			}, true
		}
		if r.Method != http.MethodGet {
			return target, false
		}
		after, ok := parseScopedListQuery(raw, path, r.URL.RawQuery, r.URL.ForceQuery)
		if !ok {
			return target, false
		}
		return scopedMetadataTarget{
			kind:      scopedMetadataKindTables,
			tenant:    tenant,
			catalog:   catalog,
			namespace: namespace,
			after:     after,
			path:      path,
		}, true
	case 12:
		if parts[10] != "tables" || r.URL.RawQuery != "" || r.URL.ForceQuery || raw != path {
			return target, false
		}
		table, ok := decodeScopedSegment(parts[11])
		if !ok {
			return target, false
		}
		switch r.Method {
		case http.MethodGet:
			return scopedMetadataTarget{
				kind:      scopedMetadataKindTable,
				tenant:    tenant,
				catalog:   catalog,
				namespace: namespace,
				table:     table,
				path:      path,
			}, true
		case http.MethodDelete:
			return scopedMetadataTarget{
				kind:      scopedMetadataKindDropTable,
				tenant:    tenant,
				catalog:   catalog,
				namespace: namespace,
				table:     table,
				path:      path,
			}, true
		default:
			return target, false
		}
	case 13:
		if parts[10] != "tables" {
			return target, false
		}
		table, ok := decodeScopedSegment(parts[11])
		if !ok {
			return target, false
		}
		if parts[12] == "commit" && r.Method == http.MethodPost {
			if r.URL.RawQuery != "" || r.URL.ForceQuery || raw != path {
				return target, false
			}
			return scopedMetadataTarget{
				kind:      scopedMetadataKindCommitTable,
				tenant:    tenant,
				catalog:   catalog,
				namespace: namespace,
				table:     table,
				path:      path,
			}, true
		}
		if parts[12] != "scan" || r.Method != http.MethodGet || r.URL.RawQuery == "" || raw != path+"?"+r.URL.RawQuery {
			return target, false
		}
		snapshot, tableUUID, ok := parseScopedScanQuery(r.URL.RawQuery)
		if !ok {
			return target, false
		}
		return scopedMetadataTarget{
			kind:      scopedMetadataKindScan,
			tenant:    tenant,
			catalog:   catalog,
			namespace: namespace,
			table:     table,
			snapshot:  snapshot,
			tableUUID: tableUUID,
			path:      path,
		}, true
	default:
		return target, false
	}
}

func parseScopedListQuery(raw, path, rawQuery string, forceQuery bool) (string, bool) {
	if rawQuery == "" {
		if forceQuery || raw != path {
			return "", false
		}
		return "", true
	}
	if raw != path+"?"+rawQuery || strings.Contains(rawQuery, "&") {
		return "", false
	}
	k, v, found := strings.Cut(rawQuery, "=")
	if !found || k != "after" || v == "" {
		return "", false
	}
	decoded, err := url.QueryUnescape(v)
	if err != nil || decoded == "" || len(decoded) > 256 || !utf8.ValidString(decoded) ||
		strings.IndexFunc(decoded, unicode.IsControl) >= 0 {
		return "", false
	}
	return decoded, true
}

func parseScopedScanQuery(rawQuery string) (int64, string, bool) {
	parts := strings.Split(rawQuery, "&")
	if len(parts) != 2 {
		return 0, "", false
	}
	var snapshot int64
	var tableUUID string
	var hasSnapshot, hasUUID bool
	for _, part := range parts {
		k, v, found := strings.Cut(part, "=")
		if !found || k == "" || v == "" {
			return 0, "", false
		}
		switch k {
		case "snapshot":
			if hasSnapshot {
				return 0, "", false
			}
			n, ok := parseCanonicalNonNegativeInt64(v)
			if !ok {
				return 0, "", false
			}
			snapshot = n
			hasSnapshot = true
		case "table_uuid":
			if hasUUID || !isCanonicalLowerUUID(v) {
				return 0, "", false
			}
			tableUUID = v
			hasUUID = true
		default:
			return 0, "", false
		}
	}
	return snapshot, tableUUID, hasSnapshot && hasUUID
}

func parseCanonicalNonNegativeInt64(raw string) (int64, bool) {
	if raw == "" {
		return 0, false
	}
	if raw == "0" {
		return 0, true
	}
	if raw[0] < '1' || raw[0] > '9' {
		return 0, false
	}
	for i := 1; i < len(raw); i++ {
		if raw[i] < '0' || raw[i] > '9' {
			return 0, false
		}
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || n < 0 || strconv.FormatInt(n, 10) != raw {
		return 0, false
	}
	return n, true
}

func isCanonicalLowerUUID(raw string) bool {
	if len(raw) != 36 {
		return false
	}
	id, err := uuid.Parse(raw)
	return err == nil && id.String() == raw
}

func isMissingScopedSchemaError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	for _, table := range []string{"meta", "namespaces", "table_versions", "tables", "columns", "files", "dvs"} {
		if strings.Contains(msg, "no such table: "+table) {
			return true
		}
	}
	return false
}

const scopedCatalogLiveSQL = `SELECT 1 FROM meta AS m
WHERE m.id = 1 AND m.name = ? COLLATE BINARY AND m.dropping = 0
LIMIT 1`

const scopedNamespacesListSQL = `SELECT n.id, n.name, n.contained_field, n.contained_value
FROM meta AS m
JOIN namespaces AS n ON 1 = 1
WHERE m.id = 1 AND m.name = ? COLLATE BINARY AND m.dropping = 0
  AND n.dropped_snapshot IS NULL
  AND n.contained_field = ? COLLATE BINARY
  AND typeof(n.contained_value) = 'integer'
  AND n.contained_value = ?
  AND n.name > ? COLLATE BINARY
ORDER BY n.name COLLATE BINARY
LIMIT 1001`

const scopedTablesListSQL = `SELECT t.id, t.table_uuid, v.name
FROM meta AS m
JOIN namespaces AS n ON 1 = 1
JOIN table_versions AS v ON v.namespace_id = n.id
JOIN tables AS t ON t.id = v.table_id
WHERE m.id = 1 AND m.name = ? COLLATE BINARY AND m.dropping = 0
  AND m.next_snapshot - 1 >= 0
  AND m.next_snapshot - 1 >= m.earliest_snapshot_id
  AND n.name = ? COLLATE BINARY AND n.dropped_snapshot IS NULL
  AND n.contained_field = ? COLLATE BINARY
  AND typeof(n.contained_value) = 'integer'
  AND n.contained_value = ?
  AND v.begin_snapshot <= m.next_snapshot - 1
  AND (v.end_snapshot IS NULL OR v.end_snapshot > m.next_snapshot - 1)
  AND t.created_snapshot <= m.next_snapshot - 1
  AND (t.dropped_snapshot IS NULL OR t.dropped_snapshot > m.next_snapshot - 1)
  AND v.name > ? COLLATE BINARY
ORDER BY v.name COLLATE BINARY
LIMIT 1001`

func (s *Server) readScopedNamespaces(w http.ResponseWriter, r *http.Request, target scopedMetadataTarget, rsa *readSessionAuthority, t *Tenant, getManifest func(context.Context) (*Manifest, error)) {
	predField, predValue, ok := rsa.containmentPredicate()
	if !ok {
		scopedNamespaceRefusal(w, http.StatusNotFound)
		return
	}
	if status := s.scopedNamespaceOwned(r.Context(), target.tenant, t, getManifest); status != 0 {
		scopedNamespaceRefusal(w, status)
		return
	}
	results, _, err := t.SubmitTxCtx(r.Context(), TxRequest{
		Stmts: []Statement{
			{SQL: scopedCatalogLiveSQL, Args: []any{target.catalog}},
			{SQL: scopedNamespacesListSQL, Args: []any{target.catalog, predField, predValue, target.after}},
		},
	})
	if status := s.scopedNamespaceOwned(r.Context(), target.tenant, t, getManifest); status != 0 {
		scopedNamespaceRefusal(w, status)
		return
	}
	if err != nil {
		if isMissingScopedSchemaError(err) {
			scopedNamespaceRefusal(w, http.StatusNotFound)
		} else {
			scopedNamespaceRefusal(w, http.StatusServiceUnavailable)
		}
		return
	}
	if len(results) != 2 {
		scopedNamespaceRefusal(w, http.StatusServiceUnavailable)
		return
	}
	if len(results[0].Rows) != 1 {
		scopedNamespaceRefusal(w, http.StatusNotFound)
		return
	}
	rows := results[1].Rows
	hasMore := len(rows) > scopedListMaxItems
	if hasMore {
		rows = rows[:scopedListMaxItems]
	}
	items := make([]scopedNamespaceDTO, 0, len(rows))
	for _, row := range rows {
		if len(row) != 4 {
			scopedNamespaceRefusal(w, http.StatusServiceUnavailable)
			return
		}
		id, ok1 := row[0].(int64)
		name, ok2 := row[1].(string)
		field, ok3 := row[2].(string)
		val, ok4 := row[3].(int64)
		if !ok1 || !ok2 || !ok3 || !ok4 || name == "" || field != predField || val != predValue {
			scopedNamespaceRefusal(w, http.StatusServiceUnavailable)
			return
		}
		items = append(items, scopedNamespaceDTO{
			ID:          id,
			Name:        name,
			Containment: scopedNamespaceContainment{Field: field, Value: val},
		})
	}
	// Shrink page if encoded payload exceeds scopedListResponseLimit.
	resp := scopedNamespacesDTO{Namespaces: items}
	if hasMore && len(items) > 0 {
		resp.NextAfter = items[len(items)-1].Name
	}
	for len(resp.Namespaces) > 1 {
		b, err := json.Marshal(resp)
		if err == nil && len(b) <= scopedListResponseLimit {
			break
		}
		resp.Namespaces = resp.Namespaces[:len(resp.Namespaces)/2]
		resp.NextAfter = resp.Namespaces[len(resp.Namespaces)-1].Name
	}
	scopedMetadataReply(w, http.StatusOK, resp, scopedListResponseLimit)
}

func (s *Server) readScopedTables(w http.ResponseWriter, r *http.Request, target scopedMetadataTarget, rsa *readSessionAuthority, t *Tenant, getManifest func(context.Context) (*Manifest, error)) {
	predField, predValue, ok := rsa.containmentPredicate()
	if !ok {
		scopedNamespaceRefusal(w, http.StatusNotFound)
		return
	}
	if status := s.scopedNamespaceOwned(r.Context(), target.tenant, t, getManifest); status != 0 {
		scopedNamespaceRefusal(w, status)
		return
	}
	results, _, err := t.SubmitTxCtx(r.Context(), TxRequest{
		Stmts: []Statement{
			{SQL: scopedNamespaceSQL, Args: []any{target.catalog, target.namespace}},
			{SQL: scopedTablesListSQL, Args: []any{target.catalog, target.namespace, predField, predValue, target.after}},
		},
	})
	if status := s.scopedNamespaceOwned(r.Context(), target.tenant, t, getManifest); status != 0 {
		scopedNamespaceRefusal(w, status)
		return
	}
	if err != nil {
		if isMissingScopedSchemaError(err) {
			scopedNamespaceRefusal(w, http.StatusNotFound)
		} else {
			scopedNamespaceRefusal(w, http.StatusServiceUnavailable)
		}
		return
	}
	if len(results) != 2 {
		scopedNamespaceRefusal(w, http.StatusServiceUnavailable)
		return
	}
	if len(results[0].Rows) != 1 || len(results[0].Rows[0]) != 4 {
		scopedNamespaceRefusal(w, http.StatusNotFound)
		return
	}
	nsRow := results[0].Rows[0]
	nsID, ok1 := nsRow[0].(int64)
	nsName, ok2 := nsRow[1].(string)
	nsField, ok3 := nsRow[2].(string)
	nsVal, ok4 := nsRow[3].(int64)
	if !ok1 || !ok2 || !ok3 || !ok4 || nsName != target.namespace || nsField != predField || nsVal != predValue {
		scopedNamespaceRefusal(w, http.StatusNotFound)
		return
	}
	rows := results[1].Rows
	hasMore := len(rows) > scopedListMaxItems
	if hasMore {
		rows = rows[:scopedListMaxItems]
	}
	tables := make([]scopedTableEntryDTO, 0, len(rows))
	for _, row := range rows {
		if len(row) != 3 {
			scopedNamespaceRefusal(w, http.StatusServiceUnavailable)
			return
		}
		tableID, ok1 := row[0].(int64)
		tableUUID, ok2 := row[1].(string)
		name, ok3 := row[2].(string)
		if !ok1 || !ok2 || !ok3 || name == "" || !isCanonicalLowerUUID(tableUUID) {
			scopedNamespaceRefusal(w, http.StatusServiceUnavailable)
			return
		}
		tables = append(tables, scopedTableEntryDTO{
			TableID:   tableID,
			TableUUID: tableUUID,
			Name:      name,
		})
	}
	resp := scopedTablesDTO{
		NamespaceID: nsID,
		Namespace:   nsName,
		Tables:      tables,
	}
	if hasMore && len(tables) > 0 {
		resp.NextAfter = tables[len(tables)-1].Name
	}
	for len(resp.Tables) > 1 {
		b, err := json.Marshal(resp)
		if err == nil && len(b) <= scopedListResponseLimit {
			break
		}
		resp.Tables = resp.Tables[:len(resp.Tables)/2]
		resp.NextAfter = resp.Tables[len(resp.Tables)-1].Name
	}
	scopedMetadataReply(w, http.StatusOK, resp, scopedListResponseLimit)
}

const scopedTableMetaSQL = `SELECT m.next_snapshot - 1, m.earliest_snapshot_id,
       n.id, n.name, n.contained_field, n.contained_value,
       t.id, t.table_uuid, v.name
FROM meta AS m
JOIN namespaces AS n ON 1 = 1
JOIN table_versions AS v ON v.namespace_id = n.id
JOIN tables AS t ON t.id = v.table_id
WHERE m.id = 1 AND m.name = ? COLLATE BINARY AND m.dropping = 0
  AND m.next_snapshot - 1 >= 0
  AND m.next_snapshot - 1 >= m.earliest_snapshot_id
  AND n.name = ? COLLATE BINARY AND n.dropped_snapshot IS NULL
  AND n.contained_field = ? COLLATE BINARY
  AND typeof(n.contained_value) = 'integer'
  AND n.contained_value = ?
  AND v.name = ? COLLATE BINARY
  AND v.begin_snapshot <= m.next_snapshot - 1
  AND (v.end_snapshot IS NULL OR v.end_snapshot > m.next_snapshot - 1)
  AND t.created_snapshot <= m.next_snapshot - 1
  AND (t.dropped_snapshot IS NULL OR t.dropped_snapshot > m.next_snapshot - 1)
LIMIT 2`

const scopedTableColumnsSQL = `SELECT c.field_id, c.parent_field_id, c.ordinal, c.name, c.type,
       c.type_params_json, c.nullable, c.comment
FROM meta AS m
JOIN namespaces AS n ON 1 = 1
JOIN table_versions AS v ON v.namespace_id = n.id
JOIN tables AS t ON t.id = v.table_id
JOIN columns AS c ON c.table_id = t.id
WHERE m.id = 1 AND m.name = ? COLLATE BINARY AND m.dropping = 0
  AND m.next_snapshot - 1 >= 0
  AND m.next_snapshot - 1 >= m.earliest_snapshot_id
  AND n.name = ? COLLATE BINARY AND n.dropped_snapshot IS NULL
  AND n.contained_field = ? COLLATE BINARY
  AND typeof(n.contained_value) = 'integer'
  AND n.contained_value = ?
  AND v.name = ? COLLATE BINARY
  AND v.begin_snapshot <= m.next_snapshot - 1
  AND (v.end_snapshot IS NULL OR v.end_snapshot > m.next_snapshot - 1)
  AND t.created_snapshot <= m.next_snapshot - 1
  AND (t.dropped_snapshot IS NULL OR t.dropped_snapshot > m.next_snapshot - 1)
  AND c.begin_snapshot <= m.next_snapshot - 1
  AND (c.end_snapshot IS NULL OR c.end_snapshot > m.next_snapshot - 1)
ORDER BY c.parent_field_id IS NOT NULL, c.parent_field_id, c.ordinal, c.field_id`

func (s *Server) readScopedTable(w http.ResponseWriter, r *http.Request, target scopedMetadataTarget, rsa *readSessionAuthority, t *Tenant, getManifest func(context.Context) (*Manifest, error)) {
	predField, predValue, ok := rsa.containmentPredicate()
	if !ok {
		scopedNamespaceRefusal(w, http.StatusNotFound)
		return
	}
	if status := s.scopedNamespaceOwned(r.Context(), target.tenant, t, getManifest); status != 0 {
		scopedNamespaceRefusal(w, status)
		return
	}
	args := []any{target.catalog, target.namespace, predField, predValue, target.table}
	results, _, err := t.SubmitTxCtx(r.Context(), TxRequest{
		Stmts: []Statement{
			{SQL: scopedTableMetaSQL, Args: args},
			{SQL: scopedTableColumnsSQL, Args: args},
		},
	})
	if status := s.scopedNamespaceOwned(r.Context(), target.tenant, t, getManifest); status != 0 {
		scopedNamespaceRefusal(w, status)
		return
	}
	if err != nil {
		if isMissingScopedSchemaError(err) {
			scopedNamespaceRefusal(w, http.StatusNotFound)
		} else {
			scopedNamespaceRefusal(w, http.StatusServiceUnavailable)
		}
		return
	}
	if len(results) != 2 {
		scopedNamespaceRefusal(w, http.StatusServiceUnavailable)
		return
	}
	if len(results[0].Rows) == 0 {
		scopedNamespaceRefusal(w, http.StatusNotFound)
		return
	}
	if len(results[0].Rows) != 1 || len(results[0].Rows[0]) != 9 {
		scopedNamespaceRefusal(w, http.StatusServiceUnavailable)
		return
	}
	row := results[0].Rows[0]
	head, headOK := row[0].(int64)
	earliest, earliestOK := row[1].(int64)
	nsID, nsIDOK := row[2].(int64)
	nsName, nsNameOK := row[3].(string)
	field, fieldOK := row[4].(string)
	value, valueOK := row[5].(int64)
	tableID, tableIDOK := row[6].(int64)
	tableUUID, tableUUIDOK := row[7].(string)
	tableName, tableNameOK := row[8].(string)
	if !fieldOK || !valueOK || field == "" || value <= 0 || !rsa.authority.permits(field, value) {
		scopedNamespaceRefusal(w, http.StatusNotFound)
		return
	}
	if !headOK || !earliestOK || head < 0 || head < earliest ||
		!nsIDOK || !nsNameOK || nsID <= 0 || nsName != target.namespace ||
		!tableIDOK || !tableUUIDOK || !tableNameOK || tableID <= 0 ||
		!isCanonicalLowerUUID(tableUUID) || tableName != target.table {
		scopedNamespaceRefusal(w, http.StatusServiceUnavailable)
		return
	}
	columns, ok := assembleScopedColumns(results[1].Rows)
	if !ok {
		scopedNamespaceRefusal(w, http.StatusServiceUnavailable)
		return
	}
	scopedMetadataReply(w, http.StatusOK, scopedTableDTO{
		NamespaceID:    nsID,
		Namespace:      nsName,
		Containment:    scopedNamespaceContainment{Field: field, Value: value},
		TableID:        tableID,
		TableUUID:      tableUUID,
		Name:           tableName,
		ReadSnapshotID: head,
		Columns:        columns,
	}, scopedTableResponseLimit)
}

type scopedColumnRow struct {
	fieldID    int64
	parentID   *int64
	ordinal    int64
	name       string
	colType    string
	typeParams json.RawMessage
	nullable   bool
	comment    string
}

func assembleScopedColumns(rows [][]any) ([]scopedColumnDTO, bool) {
	parsed := make([]scopedColumnRow, 0, len(rows))
	seen := make(map[int64]bool, len(rows))
	byParent := make(map[int64][]scopedColumnRow)
	var roots []scopedColumnRow
	for _, row := range rows {
		if len(row) != 8 {
			return nil, false
		}
		fieldID, ok1 := row[0].(int64)
		var parentID *int64
		if row[1] != nil {
			p, ok := row[1].(int64)
			if !ok || p <= 0 || p == fieldID {
				return nil, false
			}
			parentID = &p
		}
		ordinal, ok2 := row[2].(int64)
		name, ok3 := row[3].(string)
		colType, ok4 := row[4].(string)
		typeParams, ok5 := parseScopedTypeParams(row[5])
		nullableRaw, ok6 := row[6].(int64)
		var comment string
		if row[7] != nil {
			c, ok := row[7].(string)
			if !ok {
				return nil, false
			}
			comment = c
		}
		if !ok1 || !ok2 || !ok3 || !ok4 || !ok5 || !ok6 ||
			fieldID <= 0 || ordinal < 0 || name == "" || colType == "" || seen[fieldID] {
			return nil, false
		}
		seen[fieldID] = true
		item := scopedColumnRow{
			fieldID:    fieldID,
			parentID:   parentID,
			ordinal:    ordinal,
			name:       name,
			colType:    colType,
			typeParams: typeParams,
			nullable:   nullableRaw != 0,
			comment:    comment,
		}
		parsed = append(parsed, item)
		if parentID == nil {
			roots = append(roots, item)
		} else {
			byParent[*parentID] = append(byParent[*parentID], item)
		}
	}
	visited := make(map[int64]bool, len(parsed))
	var build func(items []scopedColumnRow, depth int) ([]scopedColumnDTO, bool)
	build = func(items []scopedColumnRow, depth int) ([]scopedColumnDTO, bool) {
		if depth > scopedColumnMaxDepth {
			return nil, false
		}
		out := make([]scopedColumnDTO, 0, len(items))
		for _, item := range items {
			if visited[item.fieldID] {
				return nil, false
			}
			visited[item.fieldID] = true
			var children []scopedColumnDTO
			if kids := byParent[item.fieldID]; len(kids) > 0 {
				var ok bool
				children, ok = build(kids, depth+1)
				if !ok {
					return nil, false
				}
			}
			out = append(out, scopedColumnDTO{
				FieldID:    item.fieldID,
				Ordinal:    item.ordinal,
				Name:       item.name,
				Type:       item.colType,
				TypeParams: item.typeParams,
				Nullable:   item.nullable,
				Children:   children,
				Comment:    item.comment,
			})
		}
		return out, true
	}
	columns, ok := build(roots, 0)
	if !ok || len(visited) != len(parsed) {
		return nil, false
	}
	return columns, true
}

func parseScopedTypeParams(raw any) (json.RawMessage, bool) {
	if raw == nil {
		return nil, true
	}
	s, ok := raw.(string)
	if !ok {
		return nil, false
	}
	trimmed := bytes.TrimSpace([]byte(s))
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil, true
	}
	if strictJSON(trimmed) != nil {
		return nil, false
	}
	obj, ok := parseJSONObject(trimmed)
	if !ok {
		return nil, false
	}
	if len(obj) == 0 {
		return nil, true
	}
	return json.RawMessage(trimmed), true
}

const scopedScanMetaSQL = `SELECT m.next_snapshot - 1, m.earliest_snapshot_id,
       n.id, n.name, n.contained_field, n.contained_value,
       t.id, t.table_uuid
FROM meta AS m
JOIN namespaces AS n ON 1 = 1
JOIN table_versions AS v ON v.namespace_id = n.id
JOIN tables AS t ON t.id = v.table_id
WHERE m.id = 1 AND m.name = ? COLLATE BINARY AND m.dropping = 0
  AND m.earliest_snapshot_id <= ? AND ? <= m.next_snapshot - 1
  AND n.name = ? COLLATE BINARY
  AND n.contained_field = ? COLLATE BINARY
  AND typeof(n.contained_value) = 'integer'
  AND n.contained_value = ?
  AND n.created_snapshot <= ?
  AND (n.dropped_snapshot IS NULL OR n.dropped_snapshot > ?)
  AND v.name = ? COLLATE BINARY
  AND t.table_uuid = ? COLLATE BINARY
  AND v.begin_snapshot <= ?
  AND (v.end_snapshot IS NULL OR v.end_snapshot > ?)
  AND t.created_snapshot <= ?
  AND (t.dropped_snapshot IS NULL OR t.dropped_snapshot > ?)
LIMIT 2`

const scopedScanFilesSQL = `WITH bounded AS MATERIALIZED (
  SELECT f.id, f.table_id, f.path, f.file_format, f.record_count, f.file_size_bytes,
         f.footer_size, f.row_id_start, f.stats_state, f.begin_snapshot, f.explicit_row_ids
  FROM meta AS m
  JOIN namespaces AS n ON 1 = 1
  JOIN table_versions AS v ON v.namespace_id = n.id
  JOIN tables AS t ON t.id = v.table_id
  JOIN files AS f ON f.table_id = t.id
  WHERE m.id = 1 AND m.name = ? COLLATE BINARY AND m.dropping = 0
    AND m.earliest_snapshot_id <= ? AND ? <= m.next_snapshot - 1
    AND n.name = ? COLLATE BINARY
    AND n.contained_field = ? COLLATE BINARY
    AND typeof(n.contained_value) = 'integer'
    AND n.contained_value = ?
    AND n.created_snapshot <= ?
    AND (n.dropped_snapshot IS NULL OR n.dropped_snapshot > ?)
    AND v.name = ? COLLATE BINARY
    AND t.table_uuid = ? COLLATE BINARY
    AND v.begin_snapshot <= ?
    AND (v.end_snapshot IS NULL OR v.end_snapshot > ?)
    AND t.created_snapshot <= ?
    AND (t.dropped_snapshot IS NULL OR t.dropped_snapshot > ?)
    AND f.begin_snapshot <= ?
    AND (f.end_snapshot IS NULL OR f.end_snapshot > ?)
  LIMIT 1001
)
SELECT b.id, b.path, b.file_format, b.record_count, b.file_size_bytes,
       b.footer_size, b.row_id_start, b.stats_state, b.begin_snapshot, b.explicit_row_ids,
       d.delete_file_id, d.path, d.delete_count, d.file_size_bytes, d.begin_snapshot
FROM bounded AS b
LEFT JOIN dvs AS d ON (SELECT COUNT(*) FROM bounded) <= 1000
  AND d.data_file_id = b.id
  AND d.table_id = b.table_id
  AND d.begin_snapshot <= ?
  AND (d.end_snapshot IS NULL OR d.end_snapshot > ?)
  AND d.epoch = (
    SELECT MAX(d2.epoch) FROM dvs AS d2
    WHERE d2.data_file_id = b.id
      AND d2.table_id = b.table_id
      AND d2.begin_snapshot <= ?
      AND (d2.end_snapshot IS NULL OR d2.end_snapshot > ?)
  )`

func (s *Server) readScopedScan(w http.ResponseWriter, r *http.Request, target scopedMetadataTarget, rsa *readSessionAuthority, t *Tenant, getManifest func(context.Context) (*Manifest, error)) {
	predField, predValue, ok := rsa.containmentPredicate()
	if !ok {
		scopedNamespaceRefusal(w, http.StatusNotFound)
		return
	}
	if status := s.scopedNamespaceOwned(r.Context(), target.tenant, t, getManifest); status != 0 {
		scopedNamespaceRefusal(w, status)
		return
	}
	snap := target.snapshot
	metaArgs := []any{
		target.catalog, snap, snap,
		target.namespace, predField, predValue, snap, snap,
		target.table, target.tableUUID,
		snap, snap, snap, snap,
	}
	fileArgs := make([]any, 0, len(metaArgs)+6)
	fileArgs = append(fileArgs, metaArgs...)
	fileArgs = append(fileArgs, snap, snap, snap, snap, snap, snap)
	results, _, err := t.SubmitTxCtx(r.Context(), TxRequest{
		Stmts: []Statement{
			{SQL: scopedScanMetaSQL, Args: metaArgs},
			{SQL: scopedScanFilesSQL, Args: fileArgs},
		},
	})
	if status := s.scopedNamespaceOwned(r.Context(), target.tenant, t, getManifest); status != 0 {
		scopedNamespaceRefusal(w, status)
		return
	}
	if err != nil {
		if isMissingScopedSchemaError(err) {
			scopedNamespaceRefusal(w, http.StatusNotFound)
		} else {
			scopedNamespaceRefusal(w, http.StatusServiceUnavailable)
		}
		return
	}
	if len(results) != 2 {
		scopedNamespaceRefusal(w, http.StatusServiceUnavailable)
		return
	}
	if len(results[0].Rows) == 0 {
		scopedNamespaceRefusal(w, http.StatusNotFound)
		return
	}
	if len(results[0].Rows) != 1 || len(results[0].Rows[0]) != 8 {
		scopedNamespaceRefusal(w, http.StatusServiceUnavailable)
		return
	}
	row := results[0].Rows[0]
	head, headOK := row[0].(int64)
	earliest, earliestOK := row[1].(int64)
	nsID, nsIDOK := row[2].(int64)
	nsName, nsNameOK := row[3].(string)
	field, fieldOK := row[4].(string)
	value, valueOK := row[5].(int64)
	tableID, tableIDOK := row[6].(int64)
	tableUUID, tableUUIDOK := row[7].(string)
	if !fieldOK || !valueOK || field == "" || value <= 0 || !rsa.authority.permits(field, value) {
		scopedNamespaceRefusal(w, http.StatusNotFound)
		return
	}
	if !headOK || !earliestOK || snap < earliest || snap > head ||
		!nsIDOK || !nsNameOK || nsID <= 0 || nsName != target.namespace ||
		!tableIDOK || !tableUUIDOK || tableID <= 0 || tableUUID != target.tableUUID {
		scopedNamespaceRefusal(w, http.StatusServiceUnavailable)
		return
	}
	fileRows := results[1].Rows
	if len(fileRows) > scopedScanMaxFiles {
		scopedNamespaceRefusal(w, http.StatusServiceUnavailable)
		return
	}
	files := make([]scopedScanFileDTO, 0, len(fileRows))
	seenDataID := make(map[int64]bool, len(fileRows))
	for _, fr := range fileRows {
		if len(fr) != 15 {
			scopedNamespaceRefusal(w, http.StatusServiceUnavailable)
			return
		}
		dataID, ok1 := fr[0].(int64)
		dataPath, ok2 := fr[1].(string)
		fileFormat, ok3 := fr[2].(string)
		recordCount, ok4 := fr[3].(int64)
		fileSize, ok5 := fr[4].(int64)
		var footerSize *int64
		if fr[5] != nil {
			v, ok := fr[5].(int64)
			if !ok || v < 0 {
				scopedNamespaceRefusal(w, http.StatusServiceUnavailable)
				return
			}
			footerSize = &v
		}
		rowIDStart, ok6 := fr[6].(int64)
		statsState, ok7 := fr[7].(string)
		beginSnap, ok8 := fr[8].(int64)
		explicitRowIDsRaw, ok9 := fr[9].(int64)
		if !ok2 {
			scopedNamespaceRefusal(w, http.StatusServiceUnavailable)
			return
		}
		if !rsa.allowsPrimaryCatalogLocation(target.catalog, dataPath) {
			scopedNamespaceRefusal(w, http.StatusForbidden)
			return
		}
		if !ok1 || !ok3 || !ok4 || !ok5 || !ok6 || !ok7 || !ok8 || !ok9 ||
			dataID <= 0 || seenDataID[dataID] || fileFormat == "" || recordCount < 0 || fileSize < 0 ||
			rowIDStart < 0 || statsState == "" || beginSnap < 0 {
			scopedNamespaceRefusal(w, http.StatusServiceUnavailable)
			return
		}
		seenDataID[dataID] = true
		var dv *scopedDeleteFileDTO
		if fr[10] != nil {
			dvID, dok1 := fr[10].(int64)
			dvPath, dok2 := fr[11].(string)
			dvCount, dok3 := fr[12].(int64)
			dvSize, dok4 := fr[13].(int64)
			dvBegin, dok5 := fr[14].(int64)
			if !dok2 {
				scopedNamespaceRefusal(w, http.StatusServiceUnavailable)
				return
			}
			if !rsa.allowsPrimaryCatalogLocation(target.catalog, dvPath) {
				scopedNamespaceRefusal(w, http.StatusForbidden)
				return
			}
			if !dok1 || !dok3 || !dok4 || !dok5 ||
				dvID <= 0 || dvCount <= 0 || dvSize < 0 || dvBegin < 0 {
				scopedNamespaceRefusal(w, http.StatusServiceUnavailable)
				return
			}
			dv = &scopedDeleteFileDTO{
				DeleteFileID:  dvID,
				Path:          dvPath,
				DeleteCount:   dvCount,
				FileSizeBytes: dvSize,
				BeginSnapshot: dvBegin,
			}
		}
		files = append(files, scopedScanFileDTO{
			DataFile: scopedDataFileDTO{
				DataFileID:     dataID,
				Path:           dataPath,
				FileFormat:     fileFormat,
				RecordCount:    recordCount,
				FileSizeBytes:  fileSize,
				FooterSize:     footerSize,
				RowIDStart:     rowIDStart,
				StatsState:     statsState,
				BeginSnapshot:  beginSnap,
				ExplicitRowIDs: explicitRowIDsRaw != 0,
			},
			DeleteFile: dv,
		})
	}
	slices.SortFunc(files, func(a, b scopedScanFileDTO) int {
		if c := cmp.Compare(a.DataFile.RowIDStart, b.DataFile.RowIDStart); c != 0 {
			return c
		}
		return cmp.Compare(a.DataFile.DataFileID, b.DataFile.DataFileID)
	})
	scopedMetadataReply(w, http.StatusOK, scopedScanDTO{
		NamespaceID:    nsID,
		Namespace:      nsName,
		TableUUID:      tableUUID,
		ReadSnapshotID: snap,
		Files:          files,
	}, scopedScanResponseLimit)
}
