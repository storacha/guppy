package sqlrepo_test

import (
	"context"
	"io/fs"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	"github.com/storacha/guppy/pkg/preparation/testutil"
	"github.com/stretchr/testify/require"
)

func TestCreateScan(t *testing.T) {
	t.Run("with an upload ID", func(t *testing.T) {
		repo := sqlrepo.New(testutil.CreateTestDB(t))
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

func TestFindOrCreateFile(t *testing.T) {
	t.Run("finds a matching file entry, or creates a new one", func(t *testing.T) {
		repo := sqlrepo.New(testutil.CreateTestDB(t))
		modTime := time.Now().UTC().Truncate(time.Second)
		sourceId := uuid.New()

		file, created, err := repo.FindOrCreateFile(t.Context(), "some/file.txt", modTime, 0644, 12345, []byte("checksum"), sourceId)
		require.NoError(t, err)
		require.True(t, created)
		require.NotNil(t, file)

		file2, created2, err := repo.FindOrCreateFile(t.Context(), "some/file.txt", modTime, 0644, 12345, []byte("checksum"), sourceId)
		require.NoError(t, err)
		require.False(t, created2)
		require.Equal(t, file, file2)

		file3, created3, err := repo.FindOrCreateFile(t.Context(), "some/file.txt", modTime, 0644, 12345, []byte("different-checksum"), sourceId)
		require.NoError(t, err)
		require.True(t, created3)
		require.NotEqual(t, file.ID(), file3.ID())
	})

	t.Run("refuses to create a file entry for a directory", func(t *testing.T) {
		repo := sqlrepo.New(testutil.CreateTestDB(t))
		modTime := time.Now().UTC().Truncate(time.Second)
		sourceId := uuid.New()
		_, _, err := repo.FindOrCreateFile(t.Context(), "some/directory", modTime, fs.ModeDir|0644, 12345, []byte("checksum"), sourceId)
		require.ErrorContains(t, err, "cannot create a file with directory mode")
	})
}

func TestFindOrCreateDirectory(t *testing.T) {
	t.Run("finds a matching directory entry, or creates a new one", func(t *testing.T) {
		repo := sqlrepo.New(testutil.CreateTestDB(t))
		modTime := time.Now().UTC().Truncate(time.Second)
		sourceId := uuid.New()

		file, created, err := repo.FindOrCreateDirectory(t.Context(), "some/directory", modTime, fs.ModeDir|0644, []byte("checksum"), sourceId)
		require.NoError(t, err)
		require.True(t, created)
		require.NotNil(t, file)

		file2, created2, err := repo.FindOrCreateDirectory(t.Context(), "some/directory", modTime, fs.ModeDir|0644, []byte("checksum"), sourceId)
		require.NoError(t, err)
		require.False(t, created2)
		require.Equal(t, file, file2)

		file3, created3, err := repo.FindOrCreateDirectory(t.Context(), "some/directory", modTime, fs.ModeDir|0644, []byte("different-checksum"), sourceId)
		require.NoError(t, err)
		require.True(t, created3)
		require.NotEqual(t, file.ID(), file3.ID())
	})

	t.Run("refuses to create a directory entry for a file", func(t *testing.T) {
		repo := sqlrepo.New(testutil.CreateTestDB(t))
		modTime := time.Now().UTC().Truncate(time.Second)
		sourceId := uuid.New()
		_, _, err := repo.FindOrCreateDirectory(t.Context(), "some/file.txt", modTime, 0644, []byte("different-checksum"), sourceId)
		require.ErrorContains(t, err, "cannot create a directory with file mode")
	})
}
