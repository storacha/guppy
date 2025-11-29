package preparation
import (
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"github.com/stretchr/testify/require"
)

func TestSingleFileFS(t *testing.T) {
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.txt")
	err := os.WriteFile(tmpFile, []byte("content"), 0644)
	require.NoError(t, err)
	sfs := singleFileFS{path: tmpFile}

	// Should be able to open root
	f, err := sfs.Open(".")
	require.NoError(t, err)
	content, err := io.ReadAll(f)
	require.NoError(t, err)
	require.Equal(t, "content", string(content))
	f.Close()

	// Should reject other paths
	_, err = sfs.Open("other")
	require.ErrorIs(t, err, fs.ErrNotExist)
}