package hoglite

// SQLite authorizer: the second enforcement layer for the statement
// boundary (the first is textual classification). Denies at the engine
// level, so comment prefixes and driver quirks cannot smuggle:
//
//   - PRAGMA outside the allowlist (a WAL/journal pragma would invalidate
//     the byte stream the segment shipper and parser reason about)
//   - ATTACH/DETACH (cross-database file access)
//   - writes to or drops/alters of the internal _hw_dedup table
//
// Transaction control stays on the textual denylist: the driver's own
// BEGIN/COMMIT also surface as AuthTransaction and cannot be told apart
// from a user-supplied one here.

import (
	"errors"
	"strings"
	"sync"
	"sync/atomic"

	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

var allowedPragmas = map[string]bool{
	// DSN bootstrap pragmas
	"journal_mode": true, "wal_autocheckpoint": true, "synchronous": true,
	"busy_timeout": true, "foreign_keys": true,
	// internal operations
	"wal_checkpoint": true, "page_size": true, "table_info": true,
}

var authorizerHookOnce sync.Once

func registerAuthorizerOnce() {
	authorizerHookOnce.Do(func() {
		sqlite.RegisterConnectionHook(func(c sqlite.ExecQuerierContext, dsn string) error {
			// A read can outlive its tenant's close by a short time (see
			// closeReaders). If that read connection is the last one, SQLite
			// would checkpoint and then unlink the WAL and shm files by path.
			// A re-acquire may already have new files there. PERSIST_WAL
			// keeps the close from unlinking anything.
			if strings.Contains(dsn, "query_only(1)") {
				fc, ok := c.(sqlite.FileControl)
				if !ok {
					return errors.New("read-only connection without file control")
				}
				if _, err := fc.FileControlPersistWAL("main", 1); err != nil {
					return err
				}
			}
			reg, ok := c.(sqlite.AuthorizerRegisterer)
			if !ok {
				return nil
			}
			return reg.RegisterAuthorizer(authorizer)
		})
	})
}

// authorizerOffForTest lets tests run statements that users cannot (for
// example VACUUM, which shrinks the file). Tests only.
var authorizerOffForTest atomic.Bool

func authorizer(action sqlite.AuthorizerActionCode, a1, a2, db, tv string) sqlite.AuthorizerReturnCode {
	if authorizerOffForTest.Load() {
		return sqlite3.SQLITE_OK
	}
	switch action {
	case sqlite.AuthPragma:
		if !allowedPragmas[strings.ToLower(a1)] {
			return sqlite3.SQLITE_DENY
		}
	case sqlite.AuthAttach, sqlite.AuthDetach:
		return sqlite3.SQLITE_DENY
	case sqlite.AuthDropTable, sqlite.AuthDropIndex, sqlite.AuthDropView, sqlite.AuthDropTrigger:
		// writes to the dedup table stay textual (the actor's own INSERT
		// would trip an authorizer rule); drops/alters of it are engine-
		// denied so the receipt store cannot be removed
		if strings.EqualFold(a1, "_hw_dedup") || strings.EqualFold(a2, "_hw_dedup") {
			return sqlite3.SQLITE_DENY
		}
	}
	return sqlite3.SQLITE_OK
}
