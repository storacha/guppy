package timestamp_test

import (
	"database/sql"
	"testing"

	"github.com/storacha/guppy/pkg/preparation/types/timestamp"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

func TestTimestamp(t *testing.T) {
	t.Run("roundtrips with a DB as an INTEGER of seconds", func(t *testing.T) {
		db, err := sql.Open("sqlite", ":memory:")
		require.NoError(t, err, "failed to open in-memory SQLite database")

		t.Cleanup(func() {
			db.Close()
		})

		_, err = db.ExecContext(t.Context(), `CREATE TABLE data ( ts INTEGER ) STRICT;`)
		require.NoError(t, err, "failed to execute schema")

		ts := timestamp.Now()

		_, err = db.ExecContext(t.Context(), `INSERT INTO data ( ts ) VALUES (?)`, ts)
		require.NoError(t, err, "failed to insert timestamp into database")

		var readTS timestamp.Timestamp
		err = db.QueryRowContext(t.Context(), `SELECT ts FROM data WHERE ts = ?`, ts).Scan(&readTS)
		require.NoError(t, err, "failed to read timestamp from database")
		require.Equal(t, ts, readTS)
	})
}
