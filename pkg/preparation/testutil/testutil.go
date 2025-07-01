package testutil

import (
	"database/sql"
	"testing"

	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

// CreateTestDB creates a temporary SQLite database for testing. It returns the
// database connection, a cleanup function, and any error encountered.
func CreateTestDB(t *testing.T) *sql.DB {
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err, "failed to open in-memory SQLite database")

	t.Cleanup(func() {
		db.Close()
	})

	_, err = db.ExecContext(t.Context(), sqlrepo.Schema)
	require.NoError(t, err, "failed to execute schema")

	// Disable foreign key checks to simplify test.
	_, err = db.ExecContext(t.Context(), "PRAGMA foreign_keys = OFF;")
	require.NoError(t, err, "failed to disable foreign keys")

	return db
}
