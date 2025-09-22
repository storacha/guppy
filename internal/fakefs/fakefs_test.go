package fakefs_test

import (
	"io"
	"io/fs"
	"path"
	"testing"
	"time"

	"github.com/storacha/go-libstoracha/testutil"
	"github.com/storacha/guppy/internal/fakefs"
	"github.com/stretchr/testify/require"
)

func TestFakeFS(t *testing.T) {
	dfs := fakefs.New(0)
	root, err := dfs.Open(".")
	require.NoError(t, err)
	rootDir, ok := root.(*fakefs.Dir)
	require.True(t, ok, "root should be a directory")

	rootStat, err := rootDir.Stat()
	require.NoError(t, err)
	require.True(t, rootStat.IsDir(), "root should be a directory")
	require.Equal(t, ".", rootStat.Name(), "root name should be .")

	entriesReadAtOnce, err := rootDir.ReadDir(-1)
	require.NoError(t, err)
	require.Greater(t, len(entriesReadAtOnce), 1)
	require.NotEqual(t, entriesReadAtOnce[0].Name(), entriesReadAtOnce[1].Name(), "entries should have different names")

	entriesReadInTwos := make([]fs.DirEntry, 0, len(entriesReadAtOnce))
	root, err = dfs.Open(".")
	require.NoError(t, err)
	rootDir = root.(*fakefs.Dir)

	for len(entriesReadInTwos) <= len(entriesReadAtOnce) {
		entries, err := rootDir.ReadDir(2)
		require.NoError(t, err)
		require.LessOrEqual(t, len(entries), 2, "should read at most 2 entries")
		if len(entries) == 0 {
			break
		}
		entriesReadInTwos = append(entriesReadInTwos, entries...)
	}

	require.Equal(t, entriesReadAtOnce, entriesReadInTwos, "should read same entries")

	// ---
	// A file

	var firstFileEntry fs.DirEntry
	for _, entry := range entriesReadAtOnce {
		if !entry.IsDir() {
			firstFileEntry = entry
			break
		}
	}

	firstFile, err := dfs.Open(path.Join(".", firstFileEntry.Name()))
	require.NoError(t, err)
	require.IsType(t, (*fakefs.File)(nil), firstFile, "should be a file")
	firstFileInfo, err := firstFile.Stat()
	require.NoError(t, err)
	require.Equal(t, firstFileInfo, testutil.Must(firstFileEntry.Info())(t), "file info should match dir entry info")
	require.Equal(t, firstFileInfo, testutil.Must(fs.Stat(dfs, firstFileEntry.Name()))(t), "file info should match fs.Stat() info")

	require.False(t, firstFileInfo.IsDir(), "should be a file")
	require.Equal(t, firstFileEntry.Name(), firstFileInfo.Name(), "file name should match dir entry name")
	require.IsType(t, time.Time{}, firstFileInfo.ModTime(), "file should have mod time")
	require.IsType(t, fs.FileMode(0), firstFileInfo.Mode(), "file should have mode")
	require.True(t, firstFileInfo.Mode().IsRegular(), "file should be a regular file")

	data, err := io.ReadAll(firstFile)
	require.NoError(t, err)
	require.Equal(t, testutil.Must(firstFile.Stat())(t).Size(), int64(len(data)))

	// ---
	// A directory

	var firstDirEntry fs.DirEntry
	for _, entry := range entriesReadAtOnce {
		if entry.IsDir() {
			firstDirEntry = entry
			break
		}
	}

	firstDir, err := dfs.Open(path.Join(".", firstDirEntry.Name()))
	require.NoError(t, err)
	require.IsType(t, (*fakefs.Dir)(nil), firstDir, "should be a directory")
	firstDirInfo, err := firstDir.Stat()
	require.NoError(t, err)
	require.Equal(t, firstDirInfo, testutil.Must(firstDirEntry.Info())(t), "directory info should match dir entry info")
	require.Equal(t, firstDirInfo, testutil.Must(fs.Stat(dfs, firstDirEntry.Name()))(t), "directory info should match fs.Stat() info")

	require.True(t, firstDirInfo.IsDir(), "should be a directory")
	require.Equal(t, firstDirEntry.Name(), firstDirInfo.Name(), "directory name should match dir entry name")
	require.IsType(t, time.Time{}, firstDirInfo.ModTime(), "directory should have mod time")
	require.IsType(t, fs.FileMode(0), firstDirInfo.Mode(), "directory should have mode")
	require.True(t, firstDirInfo.Mode().IsDir(), "directory should be a directory")
}
