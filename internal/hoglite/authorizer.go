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
	"strings"
	"sync"

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
		sqlite.RegisterConnectionHook(func(c sqlite.ExecQuerierContext, _ string) error {
			reg, ok := c.(sqlite.AuthorizerRegisterer)
			if !ok {
				return nil
			}
			return reg.RegisterAuthorizer(authorizer)
		})
	})
}

func authorizer(action sqlite.AuthorizerActionCode, a1, a2, db, tv string) sqlite.AuthorizerReturnCode {
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
