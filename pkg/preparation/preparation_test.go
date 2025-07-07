package preparation_test

import (
	"testing"

	"github.com/storacha/guppy/pkg/preparation"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	"github.com/storacha/guppy/pkg/preparation/testutil"
	"github.com/stretchr/testify/require"
)

func TestExecuteUpload(t *testing.T) {
	repo := sqlrepo.New(testutil.CreateTestDB(t))

	api := preparation.NewAPI(repo)

	configuration, err := api.CreateConfiguration(t.Context(), "Large Upload Configuration")
	require.NoError(t, err)

	source, err := api.CreateSource(t.Context(), "Large Upload Source", ".")
	require.NoError(t, err)

	err = repo.AddSourceToConfiguration(t.Context(), configuration.ID(), source.ID())
	require.NoError(t, err)

	uploads, err := api.CreateUploads(t.Context(), configuration.ID())
	require.NoError(t, err)

	for _, upload := range uploads {
		api.ExecuteUpload(t.Context(), upload)
	}
}
