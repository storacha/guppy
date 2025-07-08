package sqlrepo_test

import (
	"testing"

	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	"github.com/storacha/guppy/pkg/preparation/testutil"
	"github.com/storacha/guppy/pkg/preparation/types"
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

func TestAddSourceToConfiguration(t *testing.T) {
	repo := sqlrepo.New(testutil.CreateTestDB(t))

	configuration, err := repo.CreateConfiguration(t.Context(), "config name")
	require.NoError(t, err)

	source1, err := repo.CreateSource(t.Context(), "source1 name", "source/path")
	require.NoError(t, err)

	source2, err := repo.CreateSource(t.Context(), "source2 name", "source/path")
	require.NoError(t, err)

	err = repo.AddSourceToConfiguration(t.Context(), configuration.ID(), source1.ID())
	require.NoError(t, err)

	err = repo.AddSourceToConfiguration(t.Context(), configuration.ID(), source2.ID())
	require.NoError(t, err)

	sources, err := repo.ListConfigurationSources(t.Context(), configuration.ID())

	require.NoError(t, err)
	require.ElementsMatch(t, []types.SourceID{source1.ID(), source2.ID()}, sources)
}
