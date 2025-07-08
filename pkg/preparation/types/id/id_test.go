package id_test

import (
	"database/sql"
	"testing"

	"github.com/storacha/guppy/pkg/preparation/types/id"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

func TestID(t *testing.T) {
	t.Run("roundtrips with a DB as a BLOB", func(t *testing.T) {
		db, err := sql.Open("sqlite", ":memory:")
		require.NoError(t, err, "failed to open in-memory SQLite database")

		t.Cleanup(func() {
			db.Close()
		})

		_, err = db.ExecContext(t.Context(), `CREATE TABLE data ( id BLOB PRIMARY KEY ) STRICT;`)
		require.NoError(t, err, "failed to execute schema")

		writtenId := id.New()

		_, err = db.ExecContext(t.Context(), `INSERT INTO data ( id ) VALUES (?)`, writtenId)
		require.NoError(t, err, "failed to insert ID into database")

		var readID id.ID
		err = db.QueryRowContext(t.Context(), `SELECT id FROM data WHERE id = ?`, writtenId).Scan(&readID)
		require.NoError(t, err, "failed to read ID from database")
		require.Equal(t, writtenId, readID)
	})
}
