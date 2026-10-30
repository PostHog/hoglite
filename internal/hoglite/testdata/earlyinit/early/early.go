// Package early opens SQLite in its init, before hoglite's init runs
// (Go initializes ready packages in import path order). It stands for
// any dependency of a binary that uses SQLite at init time.
package early

import (
	"database/sql"

	_ "modernc.org/sqlite"
)

func init() {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		panic(err)
	}
	if err := db.Ping(); err != nil {
		panic(err)
	}
	db.Close()
}
