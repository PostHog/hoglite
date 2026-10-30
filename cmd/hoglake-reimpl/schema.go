package main

// Per-catalog schema. Applied idempotently on catalog create.
//
// Model notes:
//   - A table row is one incarnation. Drop sets dropped_snapshot.
//     Recreate inserts a new row. Only one live row per name.
//   - changes holds the typed change rows. It is the conflict-scan
//     index: (object_id, snapshot_id) makes the scan O(window).
//   - snapshots.changes_json is a denormalized copy for listings.
//   - dvs keeps the history of deletion vectors: one row per data file
//     and epoch. A supersession inserts a new row with the next epoch.
//     The live vector of a file is the row with the highest epoch.
//   - offsets holds consumer positions. They are monotonic.
var bootstrap = []stmt{
	{SQL: `CREATE TABLE IF NOT EXISTS meta(
		id INTEGER PRIMARY KEY CHECK(id=1), name TEXT NOT NULL, data_path TEXT NOT NULL,
		next_snapshot INTEGER NOT NULL DEFAULT 1, next_table_id INTEGER NOT NULL DEFAULT 1,
		schema_version INTEGER NOT NULL DEFAULT 1, next_dv_id INTEGER NOT NULL DEFAULT 1)`},
	{SQL: `CREATE TABLE IF NOT EXISTS namespaces(id INTEGER PRIMARY KEY, name TEXT UNIQUE NOT NULL)`},
	{SQL: `CREATE TABLE IF NOT EXISTS tables(
		id INTEGER PRIMARY KEY, namespace TEXT NOT NULL, name TEXT NOT NULL,
		table_uuid TEXT NOT NULL UNIQUE, columns_json TEXT NOT NULL,
		next_row_id INTEGER NOT NULL DEFAULT 0,
		created_snapshot INTEGER NOT NULL, dropped_snapshot INTEGER)`},
	// One live incarnation per name. Dropped rows keep their name.
	{SQL: `CREATE UNIQUE INDEX IF NOT EXISTS tables_live ON tables(namespace, name) WHERE dropped_snapshot IS NULL`},
	{SQL: `CREATE INDEX IF NOT EXISTS tables_name ON tables(namespace, name, created_snapshot)`},
	{SQL: `CREATE TABLE IF NOT EXISTS snapshots(
		id INTEGER PRIMARY KEY, ts TEXT NOT NULL, author TEXT, message TEXT,
		schema_version INTEGER NOT NULL, changes_json TEXT NOT NULL)`},
	{SQL: `CREATE TABLE IF NOT EXISTS changes(
		snapshot_id INTEGER NOT NULL, kind TEXT NOT NULL, object_id INTEGER NOT NULL)`},
	{SQL: `CREATE INDEX IF NOT EXISTS changes_obj ON changes(object_id, snapshot_id)`},
	{SQL: `CREATE INDEX IF NOT EXISTS changes_snap ON changes(snapshot_id)`},
	{SQL: `CREATE TABLE IF NOT EXISTS files(
		id INTEGER PRIMARY KEY, table_id INTEGER NOT NULL, path TEXT NOT NULL,
		record_count INTEGER NOT NULL, file_size_bytes INTEGER NOT NULL,
		footer_size INTEGER, row_id_start INTEGER NOT NULL, stats_json TEXT,
		begin_snapshot INTEGER NOT NULL)`},
	{SQL: `CREATE INDEX IF NOT EXISTS files_table ON files(table_id, begin_snapshot)`},
	// table_totals is maintained inside the commit transaction: one row
	// per table incarnation, updated by the same tx that inserts the file
	// rows. getTable reads it instead of scanning files. Without it,
	// totals queries on a 16M-file table take 100s+ (three full index
	// range scans). Stock hoglake does the same (live_totals metrics).
	{SQL: `CREATE TABLE IF NOT EXISTS table_totals(
		table_id INTEGER PRIMARY KEY,
		record_count INTEGER NOT NULL DEFAULT 0,
		file_count INTEGER NOT NULL DEFAULT 0,
		file_size_bytes INTEGER NOT NULL DEFAULT 0)`},
	{SQL: `CREATE TABLE IF NOT EXISTS dvs(
		data_file_id INTEGER NOT NULL, table_id INTEGER NOT NULL,
		delete_file_id INTEGER NOT NULL, path TEXT NOT NULL,
		delete_count INTEGER NOT NULL, file_size_bytes INTEGER NOT NULL,
		begin_snapshot INTEGER NOT NULL, epoch INTEGER NOT NULL,
		PRIMARY KEY(data_file_id, epoch))`},
	{SQL: `CREATE INDEX IF NOT EXISTS dvs_table ON dvs(table_id, begin_snapshot)`},
	{SQL: `CREATE TABLE IF NOT EXISTS offsets(
		consumer_id TEXT NOT NULL, table_uuid TEXT NOT NULL,
		committed_snapshot INTEGER NOT NULL, updated_at TEXT NOT NULL,
		PRIMARY KEY(consumer_id, table_uuid))`},
}

// Shared SQL fragments.
const (
	// headSnap is the newest allocated snapshot id.
	// Inside a commit tx, after the allocator, it is THIS commit's id.
	headSnap = `(SELECT next_snapshot - 1 FROM meta WHERE id=1)`

	// allocSQL advances the snapshot allocator by one.
	allocSQL = `UPDATE meta SET next_snapshot = next_snapshot + 1 WHERE id=1`

	// liveTable resolves a live table id by (namespace, name).
	liveTable = `(SELECT id FROM tables WHERE namespace=? AND name=? AND dropped_snapshot IS NULL)`

	// nowTS is the snapshot timestamp format (sortable text).
	nowTS = `strftime('%Y-%m-%dT%H:%M:%fZ','now')`

	// tableKinds are the table-scoped change kinds.
	tableKinds = `('table_created','table_dropped','table_altered','table_inserted_into','table_deleted_from','table_compacted')`
)

// snapshotRow writes the snapshot row for the current commit.
// Its change list is read back from the typed change rows.
func snapshotRow(author, message string) stmt {
	return stmt{
		SQL: `INSERT OR IGNORE INTO snapshots(id, ts, author, message, schema_version, changes_json)
			SELECT s.id, ` + nowTS + `, ?, ?, m.schema_version,
				(SELECT json_group_array(json_object('kind', c.kind, 'object_id', c.object_id))
				 FROM changes c WHERE c.snapshot_id = s.id)
			FROM (SELECT next_snapshot - 1 AS id FROM meta WHERE id=1) s, meta m WHERE m.id=1`,
		Args: []any{author, message},
	}
}

// readback returns the commit's snapshot id and schema version.
var readback = stmt{SQL: `SELECT next_snapshot - 1, schema_version FROM meta WHERE id=1`}

func i64p(v int64) *int64 { return &v }
