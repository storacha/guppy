package sqlrepo_test

import (
	"testing"

	"github.com/storacha/go-libstoracha/testutil"
	"github.com/storacha/guppy/pkg/preparation/internal/testdb"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	"github.com/stretchr/testify/require"
)

func TestFindOrCreateSpace(t *testing.T) {
	repo := sqlrepo.New(testdb.CreateTestDB(t))

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
	repo := sqlrepo.New(testdb.CreateTestDB(t))

	did := testutil.RandomDID(t)
	space, err := repo.FindOrCreateSpace(t.Context(), did, "space name")
	require.NoError(t, err)

	source1, err := repo.CreateSource(t.Context(), "source1 name", "source/path")
	require.NoError(t, err)

	source2, err := repo.CreateSource(t.Context(), "source2 name", "source2/path")
	require.NoError(t, err)

	err = repo.AddSourceToSpace(t.Context(), space.DID(), source1.ID())
	require.NoError(t, err)

	err = repo.AddSourceToSpace(t.Context(), space.DID(), source2.ID())
	require.NoError(t, err)

	sources, err := repo.ListSpaceSources(t.Context(), space.DID())

	require.NoError(t, err)
	require.ElementsMatch(t, []id.SourceID{source1.ID(), source2.ID()}, sources)
}
