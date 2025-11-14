package dagfs_test

import (
	"testing"

	mdtest "github.com/ipfs/boxo/ipld/merkledag/test"
	"github.com/spf13/afero"
	"github.com/storacha/guppy/pkg/dagfs"
	"github.com/storacha/guppy/pkg/dagfs/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestDagFS(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		rawLeaves bool
	}{
		{"without raw leaves", false},
		{"with raw leaves", true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := t.Context()

			srcFS := afero.NewMemMapFs()
			afero.WriteFile(srcFS, "file1.txt", []byte("contents of file1"), 0644)
			afero.WriteFile(srcFS, "dir1/file2.txt", []byte("contents of file2"), 0644)
			afero.WriteFile(srcFS, "dir1/subdir1/file3.txt", []byte("contents of file3"), 0644)

			dagService := mdtest.Mock()
			rootNode, err := testutil.BuildDAGFromFS(ctx, dagService, afero.NewIOFS(srcFS), testCase.rawLeaves)
			require.NoError(t, err)
			require.NotNil(t, rootNode)

			dfs := dagfs.New(ctx, dagService, rootNode.Cid())

			testutil.RequireFSEqual(t, afero.NewIOFS(srcFS), dfs)
		})
	}
}
