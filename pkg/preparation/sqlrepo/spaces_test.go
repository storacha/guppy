package sqlrepo_test

import (
	"testing"

	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	"github.com/storacha/guppy/pkg/preparation/testutil"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	"github.com/stretchr/testify/require"
)

func TestCreateSpace(t *testing.T) {
	repo := sqlrepo.New(testutil.CreateTestDB(t))

	space, err := repo.CreateSpace(t.Context(), "space name")
	require.NoError(t, err)

	readSpaceByID, err := repo.GetSpaceByID(t.Context(), space.ID())

	require.NoError(t, err)
	require.Equal(t, space, readSpaceByID)

	readSpaceByName, err := repo.GetSpaceByName(t.Context(), "space name")

	require.NoError(t, err)
	require.Equal(t, space, readSpaceByName)
}

func TestAddSourceToSpace(t *testing.T) {
	repo := sqlrepo.New(testutil.CreateTestDB(t))

	space, err := repo.CreateSpace(t.Context(), "space name")
	require.NoError(t, err)

	source1, err := repo.CreateSource(t.Context(), "source1 name", "source/path")
	require.NoError(t, err)

	source2, err := repo.CreateSource(t.Context(), "source2 name", "source/path")
	require.NoError(t, err)

	err = repo.AddSourceToSpace(t.Context(), space.ID(), source1.ID())
	require.NoError(t, err)

	err = repo.AddSourceToSpace(t.Context(), space.ID(), source2.ID())
	require.NoError(t, err)

	sources, err := repo.ListSpaceSources(t.Context(), space.ID())

	require.NoError(t, err)
	require.ElementsMatch(t, []id.SourceID{source1.ID(), source2.ID()}, sources)
}
