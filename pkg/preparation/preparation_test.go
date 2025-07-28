package preparation_test

import (
	"context"
	"io/fs"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/storacha/guppy/pkg/preparation"
	"github.com/storacha/guppy/pkg/preparation/shards/model"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	"github.com/storacha/guppy/pkg/preparation/testutil"
	"github.com/stretchr/testify/require"
)

func TestExecuteUpload(t *testing.T) {
	// In case something goes wrong. This should never take this long.
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)

	memFS := afero.NewMemMapFs()
	memFS.MkdirAll("dir1/dir2", 0755)
	afero.WriteFile(memFS, "a", []byte("file a"), 0644)
	afero.WriteFile(memFS, "dir1/b", []byte("file b"), 0644)
	afero.WriteFile(memFS, "dir1/c", []byte("file c"), 0644)
	afero.WriteFile(memFS, "dir1/dir2/d", []byte("file d"), 0644)

	// Set the last modified time for the files; Afero's in-memory FS doesn't do
	// that automatically on creation, we expect it to be present.
	for _, path := range []string{".", "a", "dir1", "dir1/b", "dir1/c", "dir1/dir2", "dir1/dir2/d"} {
		err := memFS.Chtimes(path, time.Now(), time.Now())
		require.NoError(t, err)
	}
	repo := sqlrepo.New(testutil.CreateTestDB(t))

	api := preparation.NewAPI(
		repo,
		preparation.WithGetLocalFSForPathFn(func(path string) (fs.FS, error) {
			require.Equal(t, ".", path, "test expects root to be '.'")
			return afero.NewIOFS(memFS), nil
		}),
	)

	configuration, err := api.CreateConfiguration(ctx, "Large Upload Configuration")
	require.NoError(t, err)

	source, err := api.CreateSource(ctx, "Large Upload Source", ".")
	require.NoError(t, err)

	err = repo.AddSourceToConfiguration(ctx, configuration.ID(), source.ID())
	require.NoError(t, err)

	uploads, err := api.CreateUploads(ctx, configuration.ID())
	require.NoError(t, err)

	for _, upload := range uploads {
		err = api.ExecuteUpload(ctx, upload)
		require.NoError(t, err)
	}

	shards, err := repo.ShardsForUploadByStatus(ctx, uploads[0].ID(), model.ShardStateOpen)
	require.NoError(t, err)
	require.Len(t, shards, 1, "expected one shard for the upload")
}
