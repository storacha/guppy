package sqlrepo_test

import (
	"testing"

	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	"github.com/storacha/guppy/pkg/preparation/testutil"
	"github.com/stretchr/testify/require"
)

func TestCreateConfiguration(t *testing.T) {
	repo := sqlrepo.New(testutil.CreateTestDB(t))

	configuration, err := repo.CreateConfiguration(t.Context(), "config name")
	require.NoError(t, err)

	readConfigurationByID, err := repo.GetConfigurationByID(t.Context(), configuration.ID())

	require.NoError(t, err)
	require.Equal(t, configuration, readConfigurationByID)

	readConfigurationByName, err := repo.GetConfigurationByName(t.Context(), "config name")

	require.NoError(t, err)
	require.Equal(t, configuration, readConfigurationByName)
}
