package sqlrepo_test

import (
	"testing"

	"github.com/storacha/go-libstoracha/testutil"
	"github.com/stretchr/testify/require"

	"github.com/storacha/guppy/pkg/preparation/internal/testdb"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	"github.com/storacha/guppy/pkg/preparation/types/id"
)

func TestFindOrCreateSpace(t *testing.T) {
	repo := testutil.Must(sqlrepo.New(testdb.CreateTestDB(t)))(t)

	did := testutil.RandomDID(t)
	space, err := repo.FindOrCreateSpace(t.Context(), did, "space name")
	require.NoError(t, err)

	readSpaceByDID, err := repo.GetSpaceByDID(t.Context(), space.DID())

	require.NoError(t, err)
	require.Equal(t, space, readSpaceByDID)

	readSpaceByName, err := repo.GetSpaceByName(t.Context(), "space name")

	require.NoError(t, err)
	require.Equal(t, space, readSpaceByName)
}

func TestAddSourceToSpace(t *testing.T) {
	repo := testutil.Must(sqlrepo.New(testdb.CreateTestDB(t)))(t)

	did := testutil.RandomDID(t)
	space, err := repo.FindOrCreateSpace(t.Context(), did, "space name")
	require.NoError(t, err)

	source1, err := repo.CreateSource(t.Context(), "source1 name", "source1/path")
	require.NoError(t, err)

	source2, err := repo.CreateSource(t.Context(), "source2 name", "source2/path")
	require.NoError(t, err)

	err = repo.AddSourceToSpace(t.Context(), space.DID(), source1.ID())
	require.NoError(t, err)

	err = repo.AddSourceToSpace(t.Context(), space.DID(), source2.ID())
	require.NoError(t, err)

	// fail to add duplicate source to space
	err = repo.AddSourceToSpace(t.Context(), space.DID(), source1.ID())
	require.Error(t, err)

	sources, err := repo.ListSpaceSources(t.Context(), space.DID())

	require.NoError(t, err)
	require.ElementsMatch(t, []id.SourceID{source1.ID(), source2.ID()}, sources)
}

func TestAddSameSourceToMultipleSpaces(t *testing.T) {
	repo := testutil.Must(sqlrepo.New(testdb.CreateTestDB(t)))(t)

	source, err := repo.CreateSource(t.Context(), "shared source", "shared/path")
	require.NoError(t, err)

	space1, err := repo.FindOrCreateSpace(t.Context(), testutil.RandomDID(t), "space 1")
	require.NoError(t, err)
	space2, err := repo.FindOrCreateSpace(t.Context(), testutil.RandomDID(t), "space 2")
	require.NoError(t, err)

	require.NoError(t, repo.AddSourceToSpace(t.Context(), space1.DID(), source.ID()))
	require.NoError(t, repo.AddSourceToSpace(t.Context(), space2.DID(), source.ID()))

	sources1, err := repo.ListSpaceSources(t.Context(), space1.DID())
	require.NoError(t, err)
	require.Equal(t, []id.SourceID{source.ID()}, sources1)

	sources2, err := repo.ListSpaceSources(t.Context(), space2.DID())
	require.NoError(t, err)
	require.Equal(t, []id.SourceID{source.ID()}, sources2)
}
