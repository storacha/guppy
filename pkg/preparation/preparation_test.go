package preparation_test

import (
	"io/fs"
	"testing"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/spf13/afero"
	"github.com/storacha/guppy/pkg/preparation"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	"github.com/storacha/guppy/pkg/preparation/testutil"
	"github.com/stretchr/testify/require"
)

func TestExecuteUpload(t *testing.T) {
	existingConfig := logging.GetConfig()
	logging.SetLogLevelRegex("preparation/", "DEBUG")
	t.Cleanup(func() {
		logging.SetupLogging(existingConfig)
	})

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

	configuration, err := api.CreateConfiguration(t.Context(), "Large Upload Configuration")
	require.NoError(t, err)

	source, err := api.CreateSource(t.Context(), "Large Upload Source", ".")
	require.NoError(t, err)

	err = repo.AddSourceToConfiguration(t.Context(), configuration.ID(), source.ID())
	require.NoError(t, err)

	uploads, err := api.CreateUploads(t.Context(), configuration.ID())
	require.NoError(t, err)

	for _, upload := range uploads {
		err = api.ExecuteUpload(t.Context(), upload)
		require.NoError(t, err)
	}

	// BOOKMARK:
	// a. This is failing, but that's currently correct, because we haven't
	// implemented the actual upload stage yet. Write more useful assertions.
	// b. Let's also iterate on the top-level API, encapsulating the weird way we
	// have to create things (configs).
}
