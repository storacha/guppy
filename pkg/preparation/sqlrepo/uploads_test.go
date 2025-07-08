package sqlrepo_test

import (
	"testing"

	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	"github.com/storacha/guppy/pkg/preparation/testutil"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	"github.com/storacha/guppy/pkg/preparation/uploads/model"
	"github.com/stretchr/testify/require"
)

func TestCreateUploads(t *testing.T) {
	repo := sqlrepo.New(testutil.CreateTestDB(t))
	configuration, err := repo.CreateConfiguration(t.Context(), "config name")
	require.NoError(t, err)
	source1, err := repo.CreateSource(t.Context(), "source1 name", "source1/path")
	require.NoError(t, err)
	source2, err := repo.CreateSource(t.Context(), "source2 name", "source2/path")
	require.NoError(t, err)
	sourceIDs := []id.SourceID{source1.ID(), source2.ID()}

	uploads, err := repo.CreateUploads(t.Context(), configuration.ID(), sourceIDs)
	require.NoError(t, err)

	for i, upload := range uploads {
		readUpload, err := repo.GetUploadByID(t.Context(), upload.ID())
		require.NoError(t, err)
		require.Equal(t, upload, readUpload)

		require.Equal(t, configuration.ID(), upload.ConfigurationID())
		require.Equal(t, sourceIDs[i], upload.SourceID())
		require.NotEmpty(t, upload.CreatedAt())
		require.Equal(t, model.UploadStatePending, upload.State())
		require.Empty(t, upload.RootFSEntryID())
	}
}
