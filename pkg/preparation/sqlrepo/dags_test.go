package sqlrepo_test

import (
	"testing"

	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	"github.com/storacha/guppy/pkg/preparation/testutil"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	"github.com/stretchr/testify/require"
)

func TestFindOrCreateRawNode(t *testing.T) {
	t.Run("finds a matching raw node, or creates a new one", func(t *testing.T) {
		repo := sqlrepo.New(testutil.CreateTestDB(t))
		sourceId := id.New()

		cid1 := randomCID(t)
		cid2 := randomCID(t)

		rawNode, created, err := repo.FindOrCreateRawNode(t.Context(), cid1, 16, "some/path1", sourceId, 0)
		require.NoError(t, err)
		require.True(t, created)
		require.NotNil(t, rawNode)

		rawNode2, created2, err := repo.FindOrCreateRawNode(t.Context(), cid1, 16, "some/path1", sourceId, 0)
		require.NoError(t, err)
		require.False(t, created2)
		require.Equal(t, rawNode, rawNode2)

		rawNode3, created3, err := repo.FindOrCreateRawNode(t.Context(), cid2, 16, "some/path2", sourceId, 0)
		require.NoError(t, err)
		require.True(t, created3)
		require.NotEqual(t, rawNode.CID(), rawNode3.CID())
	})
}
