package testdb

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/pressly/goose/v3"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

// CreateTestDB creates a temporary SQLite database for testing. It returns the
// database connection, a cleanup function, and any error encountered.
func CreateTestDB(t *testing.T) *sql.DB {
	t.Helper()

	// Give each test its own in-memory database to avoid cross-test contention and
	// limit the connection pool to a single connection so modernc SQLite doesn't
	// deadlock waiting on internal locks.
	d := t.TempDir()
	dsn := fmt.Sprintf("file:%s/testdb_%d.db", d, time.Now().UnixNano())

	db, err := sql.Open("sqlite", dsn)
	require.NoError(t, err, "failed to open in-memory SQLite database")
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

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
