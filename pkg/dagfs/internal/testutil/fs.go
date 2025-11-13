package testutil

import (
	"errors"
	"fmt"
	"io/fs"
	"testing"

	"github.com/stretchr/testify/require"
)

// RequireFSEqual asserts that two fs.FS instances contain the same files with
// the same contents. It walks both filesystems and compares all files.
func RequireFSEqual(t *testing.T, expected, actual fs.FS) {
	t.Helper()

	expectedFiles := collectFiles(t, expected)
	actualFiles := collectFiles(t, actual)

	require.Equal(t, expectedFiles, actualFiles)
}

func collectFiles(t *testing.T, fsys fs.FS) map[string]string {
	t.Helper()

	files := make(map[string]string)
	err := fs.WalkDir(fsys, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			contents, err := fs.ReadFile(fsys, path)
			if err != nil {
				return fmt.Errorf("failed to read file %s: %T %w", path, errors.Unwrap(err), err)
			}
			files[path] = string(contents)
		}
		return nil
	})
	require.NoError(t, err)
	return files
}
