package testutil_test

import (
	"context"
	"io"
	"testing"

	mdtest "github.com/ipfs/boxo/ipld/merkledag/test"
	uio "github.com/ipfs/boxo/ipld/unixfs/io"
	"github.com/spf13/afero"
	"github.com/storacha/guppy/pkg/dagfs/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestBuildDAGFromFS(t *testing.T) {
	t.Run("nested directories and files", func(t *testing.T) {
		ctx := context.Background()

		// Create a source filesystem
		srcFS := afero.NewMemMapFs()
		afero.WriteFile(srcFS, "file1.txt", []byte("contents of file1"), 0644)
		afero.WriteFile(srcFS, "dir1/file2.txt", []byte("contents of file2"), 0644)
		afero.WriteFile(srcFS, "dir1/subdir1/file3.txt", []byte("contents of file3"), 0644)

		// Create a DAG service
		dagService := mdtest.Mock()

		// Build DAG from filesystem
		rootNode, err := testutil.BuildDAGFromFS(ctx, dagService, afero.NewIOFS(srcFS), true)
		require.NoError(t, err)
		require.NotNil(t, rootNode)

		// Verify the root is a directory
		rootDir, err := uio.NewDirectoryFromNode(dagService, rootNode)
		require.NoError(t, err)

		// Verify file1.txt exists at root
		file1Node, err := rootDir.Find(ctx, "file1.txt")
		require.NoError(t, err)
		file1Reader, err := uio.NewDagReader(ctx, file1Node, dagService)
		require.NoError(t, err)
		file1Contents, err := io.ReadAll(file1Reader)
		require.NoError(t, err)
		require.Equal(t, "contents of file1", string(file1Contents))

		// Verify dir1 exists
		dir1Node, err := rootDir.Find(ctx, "dir1")
		require.NoError(t, err)
		dir1, err := uio.NewDirectoryFromNode(dagService, dir1Node)
		require.NoError(t, err)

		// Verify dir1/file2.txt exists
		file2Node, err := dir1.Find(ctx, "file2.txt")
		require.NoError(t, err)
		file2Reader, err := uio.NewDagReader(ctx, file2Node, dagService)
		require.NoError(t, err)
		file2Contents, err := io.ReadAll(file2Reader)
		require.NoError(t, err)
		require.Equal(t, "contents of file2", string(file2Contents))

		// Verify dir1/subdir1 exists
		subdir1Node, err := dir1.Find(ctx, "subdir1")
		require.NoError(t, err)
		subdir1, err := uio.NewDirectoryFromNode(dagService, subdir1Node)
		require.NoError(t, err)

		// Verify dir1/subdir1/file3.txt exists
		file3Node, err := subdir1.Find(ctx, "file3.txt")
		require.NoError(t, err)
		file3Reader, err := uio.NewDagReader(ctx, file3Node, dagService)
		require.NoError(t, err)
		file3Contents, err := io.ReadAll(file3Reader)
		require.NoError(t, err)
		require.Equal(t, "contents of file3", string(file3Contents))
	})

	t.Run("empty directory", func(t *testing.T) {
		ctx := context.Background()

		// Create an empty filesystem
		srcFS := afero.NewMemMapFs()
		srcFS.MkdirAll("empty", 0755)

		dagService := mdtest.Mock()

		// Build DAG from filesystem
		rootNode, err := testutil.BuildDAGFromFS(ctx, dagService, afero.NewIOFS(srcFS), true)
		require.NoError(t, err)
		require.NotNil(t, rootNode)

		// Verify the root is a directory
		rootDir, err := uio.NewDirectoryFromNode(dagService, rootNode)
		require.NoError(t, err)

		// Verify empty directory exists
		emptyNode, err := rootDir.Find(ctx, "empty")
		require.NoError(t, err)
		emptyDir, err := uio.NewDirectoryFromNode(dagService, emptyNode)
		require.NoError(t, err)

		// Verify it has no children
		links, err := emptyDir.Links(ctx)
		require.NoError(t, err)
		require.Len(t, links, 0)
	})

	t.Run("single file", func(t *testing.T) {
		ctx := context.Background()

		// Create a filesystem with just one file
		srcFS := afero.NewMemMapFs()
		afero.WriteFile(srcFS, "single.txt", []byte("single file content"), 0644)

		dagService := mdtest.Mock()

		// Build DAG from filesystem
		rootNode, err := testutil.BuildDAGFromFS(ctx, dagService, afero.NewIOFS(srcFS), true)
		require.NoError(t, err)
		require.NotNil(t, rootNode)

		// Verify the root is a directory
		rootDir, err := uio.NewDirectoryFromNode(dagService, rootNode)
		require.NoError(t, err)

		// Verify single.txt exists
		fileNode, err := rootDir.Find(ctx, "single.txt")
		require.NoError(t, err)
		fileReader, err := uio.NewDagReader(ctx, fileNode, dagService)
		require.NoError(t, err)
		fileContents, err := io.ReadAll(fileReader)
		require.NoError(t, err)
		require.Equal(t, "single file content", string(fileContents))
	})
}
