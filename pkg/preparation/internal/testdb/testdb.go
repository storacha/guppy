package testdb

import (
	"database/sql"
	"testing"

	"github.com/pressly/goose/v3"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

// CreateTestDB creates a temporary SQLite database for testing. It returns the
// database connection, a cleanup function, and any error encountered.
func CreateTestDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	require.NoError(t, err, "failed to open in-memory SQLite database")

	t.Cleanup(func() {
		db.Close()
	})

	_, err = db.ExecContext(t.Context(), sqlrepo.Schema)
	require.NoError(t, err, "failed to execute schema")

	goose.SetBaseFS(sqlrepo.MigrationsFS)

	require.NoError(t, goose.SetDialect("sqlite3"), "failed to set goose dialect")

	require.NoError(t, goose.Up(db, "migrations"), "failed to apply migrations")

	// Disable foreign key checks to simplify test.
	_, err = db.ExecContext(t.Context(), "PRAGMA foreign_keys = OFF;")
	require.NoError(t, err, "failed to disable foreign keys")

	return db
}
