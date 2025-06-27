package sqlrepo_test

import (
	"context"
	"database/sql"
	_ "embed"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/storacha/guppy/pkg/preparation/scans/model"
	scanmodel "github.com/storacha/guppy/pkg/preparation/scans/model"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	"github.com/storacha/guppy/pkg/preparation/util"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

//go:embed schema.sql
var schema string

// createTestDB creates a temporary SQLite database for testing. It returns the
// database connection, a cleanup function, and any error encountered.
func createTestDB(t *testing.T) *sql.DB {
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err, "failed to open in-memory SQLite database")

	t.Cleanup(func() {
		db.Close()
	})

	_, err = db.ExecContext(t.Context(), schema)
	require.NoError(t, err, "failed to execute schema")

	// Disable foreign key checks to simplify test.
	_, err = db.ExecContext(t.Context(), "PRAGMA foreign_keys = OFF;")
	require.NoError(t, err, "failed to disable foreign keys")

	return db
}

func TestCreateScan(t *testing.T) {
	t.Run("with an upload ID", func(t *testing.T) {
		repo := sqlrepo.New(createTestDB(t))
		uploadID := uuid.New()

		scan, err := repo.CreateScan(t.Context(), uploadID)
		require.NoError(t, err)

		readScan, err := repo.GetScanByID(t.Context(), scan.ID())

		require.NoError(t, err)
		require.Equal(t, scan, readScan)
	})

	t.Run("with a nil upload ID", func(t *testing.T) {
		repo := sqlrepo.New(createTestDB(t))
		_, err := repo.CreateScan(t.Context(), uuid.Nil)
		require.ErrorContains(t, err, "update id cannot be empty")
	})

	t.Run("when the DB fails", func(t *testing.T) {
		repo := sqlrepo.New(createTestDB(t))
		uploadID := uuid.New()

		// Simulate a DB failure by cancelling the context before the operation.
		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		_, err := repo.CreateScan(ctx, uploadID)
		require.ErrorContains(t, err, "context canceled")
	})
}

func TestUpdateScan(t *testing.T) {
	t.Run("with a persisted scan", func(t *testing.T) {
		util.SetNowFunc(func() time.Time {
			return time.Date(2025, 1, 2, 3, 4, 5, 6, time.UTC)
		})

		repo := sqlrepo.New(createTestDB(t))
		scan, err := repo.CreateScan(t.Context(), uuid.New())
		require.NoError(t, err)

		readScanBefore, err := repo.GetScanByID(t.Context(), scan.ID())
		require.NoError(t, err)

		util.SetNowFunc(func() time.Time {
			return time.Date(2026, 2, 3, 4, 5, 6, 7, time.UTC)
		})
		scan.Fail("oh no!")
		err = repo.UpdateScan(t.Context(), scan)
		require.NoError(t, err)

		readScanAfter, err := repo.GetScanByID(t.Context(), scan.ID())
		require.NoError(t, err)

		require.Equal(t, scan.ID(), readScanAfter.ID())

		require.Equal(t, readScanBefore.ID(), readScanAfter.ID())
		require.Equal(t, readScanBefore.UploadID(), readScanAfter.UploadID())
		require.Equal(t, readScanBefore.RootID(), readScanAfter.RootID())
		require.Equal(t, readScanBefore.CreatedAt(), readScanAfter.CreatedAt())

		require.Equal(t, time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC), readScanBefore.UpdatedAt())
		require.Equal(t, time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC), readScanAfter.UpdatedAt())
		require.Equal(t, model.ScanStatePending, readScanBefore.State())
		require.Equal(t, model.ScanStateFailed, readScanAfter.State())
		require.Nil(t, readScanBefore.Error())
		require.ErrorContains(t, readScanAfter.Error(), "oh no!")
	})

	t.Run("with an unknown scan", func(t *testing.T) {
		repo := sqlrepo.New(createTestDB(t))

		unsavedScan, err := scanmodel.NewScan(uuid.New())
		require.NoError(t, err)

		unsavedScan.Fail("oh no!")
		err = repo.UpdateScan(t.Context(), unsavedScan)
		require.ErrorContains(t, err, "no scan found with ID")
	})
}
