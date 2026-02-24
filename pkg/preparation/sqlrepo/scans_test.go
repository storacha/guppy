package sqlrepo_test

import (
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

func TestFindOrCreateFile(t *testing.T) {
	t.Run("finds a matching file entry, or creates a new one", func(t *testing.T) {
		repo := testutil.Must(sqlrepo.New(testdb.CreateTestDB(t)))(t)
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
		repo := testutil.Must(sqlrepo.New(testdb.CreateTestDB(t)))(t)
		modTime := time.Now().UTC().Truncate(time.Second)
		sourceId := id.New()
		spaceDID := testutil.RandomDID(t)
		_, _, err := repo.FindOrCreateFile(t.Context(), "some/directory", modTime, fs.ModeDir|0644, 12345, []byte("checksum"), sourceId, spaceDID)
		require.ErrorContains(t, err, "cannot create a file with directory mode")
	})
}

func TestFindOrCreateDirectory(t *testing.T) {
	t.Run("finds a matching directory entry, or creates a new one", func(t *testing.T) {
		repo := testutil.Must(sqlrepo.New(testdb.CreateTestDB(t)))(t)
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
		repo := testutil.Must(sqlrepo.New(testdb.CreateTestDB(t)))(t)
		modTime := time.Now().UTC().Truncate(time.Second)
		sourceId := id.New()
		spaceDID := testutil.RandomDID(t)
		_, _, err := repo.FindOrCreateDirectory(t.Context(), "some/file.txt", modTime, 0644, []byte("different-checksum"), sourceId, spaceDID)
		require.ErrorContains(t, err, "cannot create a directory with file mode")
	})
}

func TestCreateDirectoryChildren(t *testing.T) {
	repo := testutil.Must(sqlrepo.New(testdb.CreateTestDB(t)))(t)
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

func TestGetFileByID(t *testing.T) {
	repo := testutil.Must(sqlrepo.New(testdb.CreateTestDB(t)))(t)
	modTime := time.Now().UTC().Truncate(time.Second)
	sourceId := id.New()
	spaceDID := testutil.RandomDID(t)

	file, _, err := repo.FindOrCreateFile(t.Context(), "some/file.txt", modTime, 0644, 12345, []byte("checksum"), sourceId, spaceDID)
	require.NoError(t, err)

	foundFile, err := repo.GetFileByID(t.Context(), file.ID())
	require.NoError(t, err)
	require.Equal(t, file, foundFile)
}

func TestGetFSEntryByID(t *testing.T) {
	repo := testutil.Must(sqlrepo.New(testdb.CreateTestDB(t)))(t)
	modTime := time.Now().UTC().Truncate(time.Second)
	sourceId := id.New()
	spaceDID := testutil.RandomDID(t)

	t.Run("returns a file entry", func(t *testing.T) {
		file, _, err := repo.FindOrCreateFile(t.Context(), "a/file.txt", modTime, 0644, 12345, []byte("cs1"), sourceId, spaceDID)
		require.NoError(t, err)

		entry, err := repo.GetFSEntryByID(t.Context(), file.ID())
		require.NoError(t, err)
		require.Equal(t, file.ID(), entry.ID())
		require.Equal(t, file.Path(), entry.Path())
	})

	t.Run("returns a directory entry", func(t *testing.T) {
		dir, _, err := repo.FindOrCreateDirectory(t.Context(), "a/dir", modTime, fs.ModeDir|0755, []byte("cs2"), sourceId, spaceDID)
		require.NoError(t, err)

		entry, err := repo.GetFSEntryByID(t.Context(), dir.ID())
		require.NoError(t, err)
		require.Equal(t, dir.ID(), entry.ID())
		require.Equal(t, dir.Path(), entry.Path())
	})

	t.Run("returns nil for nonexistent ID", func(t *testing.T) {
		entry, err := repo.GetFSEntryByID(t.Context(), id.New())
		require.NoError(t, err)
		require.Nil(t, entry)
	})
}

func TestGetFSEntryByPath(t *testing.T) {
	repo := testutil.Must(sqlrepo.New(testdb.CreateTestDB(t)))(t)
	modTime := time.Now().UTC().Truncate(time.Second)
	sourceId := id.New()
	spaceDID := testutil.RandomDID(t)

	t.Run("finds an entry by path, source, and space", func(t *testing.T) {
		file, _, err := repo.FindOrCreateFile(t.Context(), "find/me.txt", modTime, 0644, 100, []byte("cs-find"), sourceId, spaceDID)
		require.NoError(t, err)

		entry, err := repo.GetFSEntryByPath(t.Context(), "find/me.txt", sourceId, spaceDID)
		require.NoError(t, err)
		require.NotNil(t, entry)
		require.Equal(t, file.ID(), entry.ID())
	})

	t.Run("returns nil for nonexistent path", func(t *testing.T) {
		entry, err := repo.GetFSEntryByPath(t.Context(), "nonexistent/path.txt", sourceId, spaceDID)
		require.NoError(t, err)
		require.Nil(t, entry)
	})

	t.Run("does not match entries from a different source", func(t *testing.T) {
		otherSourceId := id.New()
		_, _, err := repo.FindOrCreateFile(t.Context(), "scoped/file.txt", modTime, 0644, 100, []byte("cs-scoped"), sourceId, spaceDID)
		require.NoError(t, err)

		entry, err := repo.GetFSEntryByPath(t.Context(), "scoped/file.txt", otherSourceId, spaceDID)
		require.NoError(t, err)
		require.Nil(t, entry)
	})

	t.Run("returns one entry even when duplicates exist", func(t *testing.T) {
		_, _, err := repo.FindOrCreateFile(t.Context(), "dup/file.txt", modTime, 0644, 100, []byte("cs-old"), sourceId, spaceDID)
		require.NoError(t, err)
		_, _, err = repo.FindOrCreateFile(t.Context(), "dup/file.txt", modTime, 0644, 100, []byte("cs-new"), sourceId, spaceDID)
		require.NoError(t, err)

		entry, err := repo.GetFSEntryByPath(t.Context(), "dup/file.txt", sourceId, spaceDID)
		require.NoError(t, err)
		require.NotNil(t, entry)
		require.Equal(t, "dup/file.txt", entry.Path())
	})
}

func TestDeleteFSEntriesByPaths(t *testing.T) {
	modTime := time.Now().UTC().Truncate(time.Second)

	t.Run("deletes entries matching the given paths", func(t *testing.T) {
		repo := testutil.Must(sqlrepo.New(testdb.CreateTestDB(t)))(t)
		sourceId := id.New()
		spaceDID := testutil.RandomDID(t)

		_, _, err := repo.FindOrCreateFile(t.Context(), "a/b/c.txt", modTime, 0644, 100, []byte("cs"), sourceId, spaceDID)
		require.NoError(t, err)
		_, _, err = repo.FindOrCreateDirectory(t.Context(), "a/b", modTime, fs.ModeDir|0755, []byte("dir-cs"), sourceId, spaceDID)
		require.NoError(t, err)
		_, _, err = repo.FindOrCreateDirectory(t.Context(), "a", modTime, fs.ModeDir|0755, []byte("dir-cs2"), sourceId, spaceDID)
		require.NoError(t, err)

		sibling, _, err := repo.FindOrCreateFile(t.Context(), "a/b/d.txt", modTime, 0644, 200, []byte("sibling-cs"), sourceId, spaceDID)
		require.NoError(t, err)

		err = repo.DeleteFSEntriesByPaths(t.Context(), []string{"a/b/c.txt", "a/b", "a"}, sourceId, spaceDID)
		require.NoError(t, err)

		for _, p := range []string{"a/b/c.txt", "a/b", "a"} {
			entry, err := repo.GetFSEntryByPath(t.Context(), p, sourceId, spaceDID)
			require.NoError(t, err)
			require.Nil(t, entry, "expected %s to be deleted", p)
		}

		entry, err := repo.GetFSEntryByPath(t.Context(), "a/b/d.txt", sourceId, spaceDID)
		require.NoError(t, err)
		require.NotNil(t, entry)
		require.Equal(t, sibling.ID(), entry.ID())
	})

	t.Run("deletes all duplicates for a path", func(t *testing.T) {
		repo := testutil.Must(sqlrepo.New(testdb.CreateTestDB(t)))(t)
		sourceId := id.New()
		spaceDID := testutil.RandomDID(t)

		_, _, err := repo.FindOrCreateDirectory(t.Context(), "dup/dir", modTime, fs.ModeDir|0755, []byte("cs1"), sourceId, spaceDID)
		require.NoError(t, err)
		_, _, err = repo.FindOrCreateDirectory(t.Context(), "dup/dir", modTime, fs.ModeDir|0755, []byte("cs2"), sourceId, spaceDID)
		require.NoError(t, err)

		err = repo.DeleteFSEntriesByPaths(t.Context(), []string{"dup/dir"}, sourceId, spaceDID)
		require.NoError(t, err)

		entry, err := repo.GetFSEntryByPath(t.Context(), "dup/dir", sourceId, spaceDID)
		require.NoError(t, err)
		require.Nil(t, entry)
	})

	t.Run("does not delete entries from a different source", func(t *testing.T) {
		repo := testutil.Must(sqlrepo.New(testdb.CreateTestDB(t)))(t)
		sourceId := id.New()
		otherSourceId := id.New()
		spaceDID := testutil.RandomDID(t)

		other, _, err := repo.FindOrCreateFile(t.Context(), "shared/path.txt", modTime, 0644, 100, []byte("cs"), otherSourceId, spaceDID)
		require.NoError(t, err)

		err = repo.DeleteFSEntriesByPaths(t.Context(), []string{"shared/path.txt"}, sourceId, spaceDID)
		require.NoError(t, err)

		entry, err := repo.GetFSEntryByPath(t.Context(), "shared/path.txt", otherSourceId, spaceDID)
		require.NoError(t, err)
		require.NotNil(t, entry)
		require.Equal(t, other.ID(), entry.ID())
	})

	t.Run("no-ops for empty paths list", func(t *testing.T) {
		repo := testutil.Must(sqlrepo.New(testdb.CreateTestDB(t)))(t)
		sourceId := id.New()
		spaceDID := testutil.RandomDID(t)

		err := repo.DeleteFSEntriesByPaths(t.Context(), []string{}, sourceId, spaceDID)
		require.NoError(t, err)
	})
}
