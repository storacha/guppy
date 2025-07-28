package sqlrepo_test

import (
	"testing"

	"github.com/storacha/guppy/pkg/preparation/dags/model"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	"github.com/storacha/guppy/pkg/preparation/testutil"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	"github.com/stretchr/testify/require"
)

func TestDAGScan(t *testing.T) {
	t.Run("updates the DAG scan state and error message", func(t *testing.T) {
		repo := sqlrepo.New(testutil.CreateTestDB(t))
		uploadID := id.New()
		dagScan, err := repo.CreateDAGScan(t.Context(), id.New(), false, uploadID)
		require.NoError(t, err)
		require.Equal(t, model.DAGScanStatePending, dagScan.State())

		foundScans, err := repo.DAGScansForUploadByStatus(t.Context(), uploadID, model.DAGScanStatePending)
		require.NoError(t, err)
		require.Len(t, foundScans, 1)
		require.Equal(t, dagScan.FsEntryID(), foundScans[0].FsEntryID())
		require.Equal(t, model.DAGScanStatePending, foundScans[0].State())
		otherScans, err := repo.DAGScansForUploadByStatus(t.Context(), uploadID, model.DAGScanStateRunning, model.DAGScanStateCompleted)
		require.NoError(t, err)
		require.Len(t, otherScans, 0)

		dagScan.Start()
		err = repo.UpdateDAGScan(t.Context(), dagScan)
		require.NoError(t, err)

		foundScans, err = repo.DAGScansForUploadByStatus(t.Context(), uploadID, model.DAGScanStateRunning)
		require.NoError(t, err)
		require.Len(t, foundScans, 1)
		require.Equal(t, dagScan.FsEntryID(), foundScans[0].FsEntryID())
		require.Equal(t, model.DAGScanStateRunning, foundScans[0].State())
		otherScans, err = repo.DAGScansForUploadByStatus(t.Context(), uploadID, model.DAGScanStatePending, model.DAGScanStateCompleted)
		require.NoError(t, err)
		require.Len(t, otherScans, 0)

		dagCid := testutil.RandomCID(t)
		dagScan.Complete(dagCid)
		err = repo.UpdateDAGScan(t.Context(), dagScan)
		require.NoError(t, err)

		foundScans, err = repo.DAGScansForUploadByStatus(t.Context(), uploadID, model.DAGScanStateCompleted)
		require.NoError(t, err)
		require.Len(t, foundScans, 1)
		require.Equal(t, dagScan.FsEntryID(), foundScans[0].FsEntryID())
		require.Equal(t, model.DAGScanStateCompleted, foundScans[0].State())
		require.Equal(t, dagCid, foundScans[0].CID())
		otherScans, err = repo.DAGScansForUploadByStatus(t.Context(), uploadID, model.DAGScanStatePending, model.DAGScanStateRunning)
		require.NoError(t, err)
		require.Len(t, otherScans, 0)
	})
}

func TestFindOrCreateRawNode(t *testing.T) {
	t.Run("finds a matching raw node, or creates a new one", func(t *testing.T) {
		repo := sqlrepo.New(testutil.CreateTestDB(t))
		sourceId := id.New()

		cid1 := testutil.RandomCID(t)
		cid2 := testutil.RandomCID(t)

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

func TestDirectoryLinks(t *testing.T) {
	t.Run("for a new DAG scan is empty", func(t *testing.T) {
		repo := sqlrepo.New(testutil.CreateTestDB(t))
		dagScan, err := repo.CreateDAGScan(t.Context(), id.New(), true, id.New())
		require.NoError(t, err)
		dirScan, ok := dagScan.(*model.DirectoryDAGScan)
		require.True(t, ok, "Expected dagScan to be a DirectoryDAGScan")

		linkParamses, err := repo.DirectoryLinks(t.Context(), dirScan)
		require.NoError(t, err)

		require.Empty(t, linkParamses, "Expected no directory links for a new DAG scan")
	})
}
