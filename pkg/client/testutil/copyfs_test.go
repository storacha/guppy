package testutil_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/afero"
	"github.com/storacha/guppy/pkg/client/testutil"
	"github.com/stretchr/testify/require"
)

func TestCopyFSResumable(t *testing.T) {
	// Create a source filesystem
	srcFS := afero.NewMemMapFs()
	afero.WriteFile(srcFS, "file1.txt", []byte("contents of file1"), 0644)
	afero.WriteFile(srcFS, "dir/file2.txt", []byte("contents of file2"), 0644)

	// Create a temporary destination directory
	destDir := t.TempDir()

	// First copy - should succeed
	err := testutil.CopyFSResumable(destDir, afero.NewIOFS(srcFS))
	require.NoError(t, err)

	// Verify files were copied
	content1, err := os.ReadFile(filepath.Join(destDir, "file1.txt"))
	require.NoError(t, err)
	require.Equal(t, "contents of file1", string(content1))

	content2, err := os.ReadFile(filepath.Join(destDir, "dir", "file2.txt"))
	require.NoError(t, err)
	require.Equal(t, "contents of file2", string(content2))

	// Second copy - should succeed (resume, skip identical files)
	err = testutil.CopyFSResumable(destDir, afero.NewIOFS(srcFS))
	require.NoError(t, err)

	// Verify files are still correct
	content1, err = os.ReadFile(filepath.Join(destDir, "file1.txt"))
	require.NoError(t, err)
	require.Equal(t, "contents of file1", string(content1))
}

func TestCopyFSResumable_DifferentContent(t *testing.T) {
	// Create a source filesystem
	srcFS := afero.NewMemMapFs()
	afero.WriteFile(srcFS, "file1.txt", []byte("new content"), 0644)

	// Create a temporary destination directory with different content
	destDir := t.TempDir()
	err := os.WriteFile(filepath.Join(destDir, "file1.txt"), []byte("old content"), 0644)
	require.NoError(t, err)

	// Copy should fail because content differs
	err = testutil.CopyFSResumable(destDir, afero.NewIOFS(srcFS))
	require.Error(t, err)
	require.ErrorIs(t, err, fs.ErrExist)
}

func TestCopyFSResumable_PartialCopy(t *testing.T) {
	// Create a source filesystem with multiple files
	srcFS := afero.NewMemMapFs()
	afero.WriteFile(srcFS, "file1.txt", []byte("contents of file1"), 0644)
	afero.WriteFile(srcFS, "file2.txt", []byte("contents of file2"), 0644)
	afero.WriteFile(srcFS, "file3.txt", []byte("contents of file3"), 0644)

	destDir := t.TempDir()

	// Simulate partial copy by copying some files manually
	err := os.WriteFile(filepath.Join(destDir, "file1.txt"), []byte("contents of file1"), 0644)
	require.NoError(t, err)

	// Resume copy - should copy remaining files
	err = testutil.CopyFSResumable(destDir, afero.NewIOFS(srcFS))
	require.NoError(t, err)

	// Verify all files exist
	content1, err := os.ReadFile(filepath.Join(destDir, "file1.txt"))
	require.NoError(t, err)
	require.Equal(t, "contents of file1", string(content1))

	content2, err := os.ReadFile(filepath.Join(destDir, "file2.txt"))
	require.NoError(t, err)
	require.Equal(t, "contents of file2", string(content2))

	content3, err := os.ReadFile(filepath.Join(destDir, "file3.txt"))
	require.NoError(t, err)
	require.Equal(t, "contents of file3", string(content3))
}
