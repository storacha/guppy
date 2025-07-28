package testutil

import (
	crand "crypto/rand"
	"database/sql"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
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

	// Disable foreign key checks to simplify test.
	_, err = db.ExecContext(t.Context(), "PRAGMA foreign_keys = OFF;")
	require.NoError(t, err, "failed to disable foreign keys")

	return db
}

func RandomCID(t *testing.T) cid.Cid {
	t.Helper()

	bytes := make([]byte, 10)
	_, err := crand.Read(bytes)
	require.NoError(t, err)

	hash, err := multihash.Sum(bytes, multihash.SHA2_256, -1)
	return cid.NewCidV1(cid.Raw, hash)
}
