package sqlrepo_test

import (
	"context"
	"io/fs"
	"testing"
	"time"

	"github.com/storacha/go-libstoracha/testutil"
	"github.com/storacha/guppy/pkg/preparation/internal/testdb"
	"github.com/storacha/guppy/pkg/preparation/scans/model"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	"github.com/stretchr/testify/require"
)

func TestCreateScan(t *testing.T) {
	t.Run("with an upload ID", func(t *testing.T) {
		repo := sqlrepo.New(testdb.CreateTestDB(t))
		uploadID := id.New()

		scan, err := repo.CreateScan(t.Context(), uploadID)
		require.NoError(t, err)

		readScan, err := repo.GetScanByID(t.Context(), scan.ID())

		require.NoError(t, err)
		require.Equal(t, scan, readScan)
	})

	t.Run("with a nil upload ID", func(t *testing.T) {
		repo := sqlrepo.New(testdb.CreateTestDB(t))
		_, err := repo.CreateScan(t.Context(), id.Nil)
		require.ErrorContains(t, err, "update id cannot be empty")
	})

	t.Run("when the DB fails", func(t *testing.T) {
		repo := sqlrepo.New(testdb.CreateTestDB(t))
		uploadID := id.New()

		// Simulate a DB failure by canceling the context before the operation.
		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		_, err := repo.CreateScan(ctx, uploadID)
		require.ErrorContains(t, err, "context canceled")
	})
}

func TestFindOrCreateFile(t *testing.T) {
	t.Run("finds a matching file entry, or creates a new one", func(t *testing.T) {
		repo := sqlrepo.New(testdb.CreateTestDB(t))
		modTime := time.Now().UTC().Truncate(time.Second)
		sourceId := id.New()
		spaceDID := testutil.RandomDID(t)

		file, created, err := repo.FindOrCreateFile(t.Context(), "some/file.txt", modTime, 0644, 12345, []byte("checksum"), sourceId, spaceDID)
		require.NoError(t, err)
		require.True(t, created)
		require.NotNil(t, file)

		file2, created2, err := repo.FindOrCreateFile(t.Context(), "some/file.txt", modTime, 0644, 12345, []byte("checksum"), sourceId, spaceDID)
		require.NoError(t, err)
		require.False(t, created2)
		require.Equal(t, file, file2)

		file3, created3, err := repo.FindOrCreateFile(t.Context(), "some/file.txt", modTime, 0644, 12345, []byte("different-checksum"), sourceId, spaceDID)
		require.NoError(t, err)
		require.True(t, created3)
		require.NotEqual(t, file.ID(), file3.ID())
	})

	t.Run("refuses to create a file entry for a directory", func(t *testing.T) {
		repo := sqlrepo.New(testdb.CreateTestDB(t))
		modTime := time.Now().UTC().Truncate(time.Second)
		sourceId := id.New()
		spaceDID := testutil.RandomDID(t)
		_, _, err := repo.FindOrCreateFile(t.Context(), "some/directory", modTime, fs.ModeDir|0644, 12345, []byte("checksum"), sourceId, spaceDID)
		require.ErrorContains(t, err, "cannot create a file with directory mode")
	})
}

func TestFindOrCreateDirectory(t *testing.T) {
	t.Run("finds a matching directory entry, or creates a new one", func(t *testing.T) {
		repo := sqlrepo.New(testdb.CreateTestDB(t))
		modTime := time.Now().UTC().Truncate(time.Second)
		sourceId := id.New()
		spaceDID := testutil.RandomDID(t)

		dir, created, err := repo.FindOrCreateDirectory(t.Context(), "some/directory", modTime, fs.ModeDir|0644, []byte("checksum"), sourceId, spaceDID)
		require.NoError(t, err)
		require.True(t, created)
		require.NotNil(t, dir)

		dir2, created2, err := repo.FindOrCreateDirectory(t.Context(), "some/directory", modTime, fs.ModeDir|0644, []byte("checksum"), sourceId, spaceDID)
		require.NoError(t, err)
		require.False(t, created2)
		require.Equal(t, dir, dir2)

		dir3, created3, err := repo.FindOrCreateDirectory(t.Context(), "some/directory", modTime, fs.ModeDir|0644, []byte("different-checksum"), sourceId, spaceDID)
		require.NoError(t, err)
		require.True(t, created3)
		require.NotEqual(t, dir.ID(), dir3.ID())
	})

	t.Run("refuses to create a directory entry for a file", func(t *testing.T) {
		repo := sqlrepo.New(testdb.CreateTestDB(t))
		modTime := time.Now().UTC().Truncate(time.Second)
		sourceId := id.New()
		spaceDID := testutil.RandomDID(t)
		_, _, err := repo.FindOrCreateDirectory(t.Context(), "some/file.txt", modTime, 0644, []byte("different-checksum"), sourceId, spaceDID)
		require.ErrorContains(t, err, "cannot create a directory with file mode")
	})
}

func TestCreateDirectoryChildren(t *testing.T) {
	repo := sqlrepo.New(testdb.CreateTestDB(t))
	modTime := time.Now().UTC().Truncate(time.Second)
	sourceId := id.New()
	spaceDID := testutil.RandomDID(t)

	dir, _, err := repo.FindOrCreateDirectory(t.Context(), "some/directory", modTime, fs.ModeDir|0644, []byte("checksum"), sourceId, spaceDID)
	require.NoError(t, err)

	file, _, err := repo.FindOrCreateFile(t.Context(), "some/file.txt", modTime, 0644, 12345, []byte("checksum"), sourceId, spaceDID)
	require.NoError(t, err)

	file2, _, err := repo.FindOrCreateFile(t.Context(), "some/another_file.txt", modTime, 0644, 67890, []byte("another-checksum"), sourceId, spaceDID)
	require.NoError(t, err)

	err = repo.CreateDirectoryChildren(t.Context(), dir, []model.FSEntry{file, file2})
	require.NoError(t, err)

	children, err := repo.DirectoryChildren(t.Context(), dir)
	require.NoError(t, err)
	require.ElementsMatch(t, []model.FSEntry{file, file2}, children)
}

func TestGetFSEntryByID(t *testing.T) {
	repo := sqlrepo.New(testdb.CreateTestDB(t))
	modTime := time.Now().UTC().Truncate(time.Second)
	sourceId := id.New()
	spaceDID := testutil.RandomDID(t)

	file, _, err := repo.FindOrCreateFile(t.Context(), "some/file.txt", modTime, 0644, 12345, []byte("checksum"), sourceId, spaceDID)
	require.NoError(t, err)

	foundFile, err := repo.GetFileByID(t.Context(), file.ID())
	require.NoError(t, err)
	require.Equal(t, file, foundFile)
}
