package sqlrepo_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	"github.com/storacha/guppy/pkg/preparation/testutil"
	"github.com/stretchr/testify/require"
)

func TestCreateScan(t *testing.T) {
	t.Run("with an upload ID", func(t *testing.T) {
		db := testutil.CreateTestDB(t)
		repo := sqlrepo.New(db)
		uploadID := uuid.New()

		scan, err := repo.CreateScan(t.Context(), uploadID)
		require.NoError(t, err)

		readScan, err := repo.GetScanByID(t.Context(), scan.ID())

		require.NoError(t, err)
		require.Equal(t, scan, readScan)
	})

	t.Run("with a nil upload ID", func(t *testing.T) {
		repo := sqlrepo.New(testutil.CreateTestDB(t))
		_, err := repo.CreateScan(t.Context(), uuid.Nil)
		require.ErrorContains(t, err, "update id cannot be empty")
	})

	t.Run("when the DB fails", func(t *testing.T) {
		repo := sqlrepo.New(testutil.CreateTestDB(t))
		uploadID := uuid.New()

		// Simulate a DB failure by canceling the context before the operation.
		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		_, err := repo.CreateScan(ctx, uploadID)
		require.ErrorContains(t, err, "context canceled")
	})
}
