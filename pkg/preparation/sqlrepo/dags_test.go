package sqlrepo_test

import (
	"testing"

	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/storacha/go-libstoracha/testutil"
	"github.com/storacha/guppy/pkg/preparation/dags/model"
	"github.com/storacha/guppy/pkg/preparation/internal/testdb"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	"github.com/stretchr/testify/require"
)

func TestDAGScan(t *testing.T) {
	t.Run("updates the DAG scan state and error message", func(t *testing.T) {
		repo := testutil.Must(sqlrepo.New(testdb.CreateTestDB(t)))(t)
		uploadID := id.New()
		spaceDID := testutil.RandomDID(t)
		dagScan, err := repo.CreateDAGScan(t.Context(), id.New(), false, uploadID, spaceDID)
		require.NoError(t, err)

		foundScans, err := repo.IncompleteDAGScansForUpload(t.Context(), uploadID)
		require.NoError(t, err)
		require.Len(t, foundScans, 1)
		require.Equal(t, dagScan.FsEntryID(), foundScans[0].FsEntryID())
		otherScans, err := repo.CompleteDAGScansForUpload(t.Context(), uploadID)
		require.NoError(t, err)
		require.Len(t, otherScans, 0)

		dagCID := testutil.RandomCID(t)
		dagScan.Complete(dagCID.(cidlink.Link).Cid)
		err = repo.UpdateDAGScan(t.Context(), dagScan)
		require.NoError(t, err)

		foundScans, err = repo.CompleteDAGScansForUpload(t.Context(), uploadID)
		require.NoError(t, err)
		require.Len(t, foundScans, 1)
		require.Equal(t, dagScan.FsEntryID(), foundScans[0].FsEntryID())
		require.Equal(t, dagCID.(cidlink.Link).Cid, foundScans[0].CID())
		otherScans, err = repo.IncompleteDAGScansForUpload(t.Context(), uploadID)
		require.NoError(t, err)
		require.Len(t, otherScans, 0)
	})
}

func TestFindOrCreateRawNode(t *testing.T) {
	t.Run("finds a matching raw node, or creates a new one", func(t *testing.T) {
		repo := testutil.Must(sqlrepo.New(testdb.CreateTestDB(t)))(t)
		sourceId := id.New()
		spaceDID := testutil.RandomDID(t)

		cid1 := testutil.RandomCID(t)
		cid2 := testutil.RandomCID(t)

		rawNode, created, err := repo.FindOrCreateRawNode(t.Context(), cid1.(cidlink.Link).Cid, 16, spaceDID, "some/path1", sourceId, 0)
		require.NoError(t, err)
		require.True(t, created)
		require.NotNil(t, rawNode)

		rawNode2, created2, err := repo.FindOrCreateRawNode(t.Context(), cid1.(cidlink.Link).Cid, 16, spaceDID, "some/path1", sourceId, 0)
		require.NoError(t, err)
		require.False(t, created2)
		require.Equal(t, rawNode, rawNode2)

		rawNode3, created3, err := repo.FindOrCreateRawNode(t.Context(), cid2.(cidlink.Link).Cid, 16, spaceDID, "some/path2", sourceId, 0)
		require.NoError(t, err)
		require.True(t, created3)
		require.NotEqual(t, rawNode.CID(), rawNode3.CID())
	})
}

func TestDirectoryLinks(t *testing.T) {
	t.Run("for a new DAG scan is empty", func(t *testing.T) {
		repo := testutil.Must(sqlrepo.New(testdb.CreateTestDB(t)))(t)
		spaceDID := testutil.RandomDID(t)
		dagScan, err := repo.CreateDAGScan(t.Context(), id.New(), true, id.New(), spaceDID)
		require.NoError(t, err)
		dirScan, ok := dagScan.(*model.DirectoryDAGScan)
		require.True(t, ok, "Expected dagScan to be a DirectoryDAGScan")

		linkParamses, err := repo.DirectoryLinks(t.Context(), dirScan)
		require.NoError(t, err)

		require.Empty(t, linkParamses, "Expected no directory links for a new DAG scan")
	})
}
