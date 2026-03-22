package sqlrepo_test

import (
	"io/fs"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/storacha/go-libstoracha/testutil"
	"github.com/storacha/guppy/pkg/preparation/dags/model"
	"github.com/storacha/guppy/pkg/preparation/internal/testdb"
	scanmodel "github.com/storacha/guppy/pkg/preparation/scans/model"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	"github.com/stretchr/testify/require"
)

func randomDAGPBCID(t *testing.T) cid.Cid {
	return cid.NewCidV1(cid.DagProtobuf, testutil.RandomMultihash(t))
}

func randomRawCID(t *testing.T) cid.Cid {
	return cid.NewCidV1(cid.Raw, testutil.RandomMultihash(t))
}

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

		dagCID := randomRawCID(t)
		dagScan.Complete(dagCID)
		err = repo.UpdateDAGScan(t.Context(), dagScan)
		require.NoError(t, err)

		foundScans, err = repo.CompleteDAGScansForUpload(t.Context(), uploadID)
		require.NoError(t, err)
		require.Len(t, foundScans, 1)
		require.Equal(t, dagScan.FsEntryID(), foundScans[0].FsEntryID())
		require.Equal(t, dagCID, foundScans[0].CID())
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
		uploadID := id.New()

		cid1 := randomRawCID(t)
		cid2 := randomRawCID(t)

		rawNode, created, err := repo.FindOrCreateRawNode(t.Context(), cid1, 16, spaceDID, uploadID, "some/path1", sourceId, 0)
		require.NoError(t, err)
		require.True(t, created)
		require.NotNil(t, rawNode)

		rawNode2, created2, err := repo.FindOrCreateRawNode(t.Context(), cid1, 16, spaceDID, uploadID, "some/path1", sourceId, 0)
		require.NoError(t, err)
		require.False(t, created2)
		require.Equal(t, rawNode, rawNode2)

		rawNode3, created3, err := repo.FindOrCreateRawNode(t.Context(), cid2, 16, spaceDID, uploadID, "some/path2", sourceId, 0)
		require.NoError(t, err)
		require.True(t, created3)
		require.NotEqual(t, rawNode.CID(), rawNode3.CID())

		cids, err := repo.NodesNotInShards(t.Context(), uploadID, spaceDID)
		require.NoError(t, err)
		require.Len(t, cids, 2)
		require.Contains(t, cids, cid1)
		require.Contains(t, cids, cid2)
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

	t.Run("child with a raw node has t_size equal to node size", func(t *testing.T) {
		repo := testutil.Must(sqlrepo.New(testdb.CreateTestDB(t)))(t)
		spaceDID := testutil.RandomDID(t)
		sourceID := id.New()
		modTime := time.Now().UTC().Truncate(time.Second)

		// Create parent directory
		parentDir, _, err := repo.FindOrCreateDirectory(t.Context(), "parent", modTime, fs.ModeDir|0755, []byte("parent-cksum"), sourceID, spaceDID)
		require.NoError(t, err)

		// Create child file
		child, _, err := repo.FindOrCreateFile(t.Context(), "parent/child.txt", modTime, 0644, 100, []byte("child-cksum"), sourceID, spaceDID)
		require.NoError(t, err)

		// Link child to parent
		err = repo.CreateDirectoryChildren(t.Context(), parentDir, []scanmodel.FSEntry{child})
		require.NoError(t, err)

		// Create upload, DAG scan for child, raw node, and complete
		uploadID := id.New()
		childScan, err := repo.CreateDAGScan(t.Context(), child.ID(), false, uploadID, spaceDID)
		require.NoError(t, err)
		childCID := randomRawCID(t)
		_, _, err = repo.FindOrCreateRawNode(t.Context(), childCID, 100, spaceDID, uploadID, "parent/child.txt", sourceID, 0)
		require.NoError(t, err)
		require.NoError(t, childScan.Complete(childCID))
		require.NoError(t, repo.UpdateDAGScan(t.Context(), childScan))

		// Create parent DAG scan and query
		parentScan, err := repo.CreateDAGScan(t.Context(), parentDir.ID(), true, uploadID, spaceDID)
		require.NoError(t, err)
		dirScan, ok := parentScan.(*model.DirectoryDAGScan)
		require.True(t, ok)

		links, err := repo.DirectoryLinks(t.Context(), dirScan)
		require.NoError(t, err)
		require.Len(t, links, 1)
		require.Equal(t, "child.txt", links[0].Name)
		require.Equal(t, uint64(100), links[0].TSize)
		require.Equal(t, childCID, links[0].Hash)
	})

	t.Run("child with a UnixFS node sums its links' t_sizes", func(t *testing.T) {
		repo := testutil.Must(sqlrepo.New(testdb.CreateTestDB(t)))(t)
		spaceDID := testutil.RandomDID(t)
		sourceID := id.New()
		modTime := time.Now().UTC().Truncate(time.Second)

		// Create parent directory
		parentDir, _, err := repo.FindOrCreateDirectory(t.Context(), "parent", modTime, fs.ModeDir|0755, []byte("parent-cksum"), sourceID, spaceDID)
		require.NoError(t, err)

		// Create child directory
		childDir, _, err := repo.FindOrCreateDirectory(t.Context(), "parent/subdir", modTime, fs.ModeDir|0755, []byte("subdir-cksum"), sourceID, spaceDID)
		require.NoError(t, err)

		// Link child to parent
		err = repo.CreateDirectoryChildren(t.Context(), parentDir, []scanmodel.FSEntry{childDir})
		require.NoError(t, err)

		// Create upload, DAG scan for child
		uploadID := id.New()
		childScan, err := repo.CreateDAGScan(t.Context(), childDir.ID(), true, uploadID, spaceDID)
		require.NoError(t, err)

		// Create UnixFS node with links whose t_sizes should be summed
		childCID := randomDAGPBCID(t)
		nodeSize := uint64(42)
		linkParams := []model.LinkParams{
			{Name: "a.txt", TSize: 50, Hash: randomRawCID(t)},
			{Name: "b.txt", TSize: 30, Hash: randomRawCID(t)},
		}
		_, _, err = repo.FindOrCreateUnixFSNode(t.Context(), childCID, nodeSize, spaceDID, uploadID, []byte("ufsdata"), linkParams)
		require.NoError(t, err)
		require.NoError(t, childScan.Complete(childCID))
		require.NoError(t, repo.UpdateDAGScan(t.Context(), childScan))

		// Query directory links
		parentScan, err := repo.CreateDAGScan(t.Context(), parentDir.ID(), true, uploadID, spaceDID)
		require.NoError(t, err)
		dirScan, ok := parentScan.(*model.DirectoryDAGScan)
		require.True(t, ok)

		links, err := repo.DirectoryLinks(t.Context(), dirScan)
		require.NoError(t, err)
		require.Len(t, links, 1)
		require.Equal(t, "subdir", links[0].Name)
		require.Equal(t, nodeSize+50+30, links[0].TSize)
		require.Equal(t, childCID, links[0].Hash)
	})

	t.Run("child with a UnixFS node with no links has t_size equal to node size", func(t *testing.T) {
		repo := testutil.Must(sqlrepo.New(testdb.CreateTestDB(t)))(t)
		spaceDID := testutil.RandomDID(t)
		sourceID := id.New()
		modTime := time.Now().UTC().Truncate(time.Second)

		// Create parent directory
		parentDir, _, err := repo.FindOrCreateDirectory(t.Context(), "parent", modTime, fs.ModeDir|0755, []byte("parent-cksum"), sourceID, spaceDID)
		require.NoError(t, err)

		// Create child directory
		childDir, _, err := repo.FindOrCreateDirectory(t.Context(), "parent/subdir", modTime, fs.ModeDir|0755, []byte("subdir-cksum"), sourceID, spaceDID)
		require.NoError(t, err)

		// Link child to parent
		err = repo.CreateDirectoryChildren(t.Context(), parentDir, []scanmodel.FSEntry{childDir})
		require.NoError(t, err)

		// Create upload, DAG scan for child
		uploadID := id.New()
		childScan, err := repo.CreateDAGScan(t.Context(), childDir.ID(), true, uploadID, spaceDID)
		require.NoError(t, err)

		// Create UnixFS node with no links
		childCID := randomDAGPBCID(t)
		nodeSize := uint64(42)
		_, _, err = repo.FindOrCreateUnixFSNode(t.Context(), childCID, nodeSize, spaceDID, uploadID, []byte("ufsdata"), nil)
		require.NoError(t, err)
		require.NoError(t, childScan.Complete(childCID))
		require.NoError(t, repo.UpdateDAGScan(t.Context(), childScan))

		// Query directory links
		parentScan, err := repo.CreateDAGScan(t.Context(), parentDir.ID(), true, uploadID, spaceDID)
		require.NoError(t, err)
		dirScan, ok := parentScan.(*model.DirectoryDAGScan)
		require.True(t, ok)

		links, err := repo.DirectoryLinks(t.Context(), dirScan)
		require.NoError(t, err)
		require.Len(t, links, 1)
		require.Equal(t, "subdir", links[0].Name)
		require.Equal(t, nodeSize, links[0].TSize)
	})

	t.Run("multiple children each have correct t_sizes", func(t *testing.T) {
		repo := testutil.Must(sqlrepo.New(testdb.CreateTestDB(t)))(t)
		spaceDID := testutil.RandomDID(t)
		sourceID := id.New()
		modTime := time.Now().UTC().Truncate(time.Second)

		// Create parent directory
		parentDir, _, err := repo.FindOrCreateDirectory(t.Context(), "parent", modTime, fs.ModeDir|0755, []byte("parent-cksum"), sourceID, spaceDID)
		require.NoError(t, err)

		// Create a file child and a directory child
		fileChild, _, err := repo.FindOrCreateFile(t.Context(), "parent/file.txt", modTime, 0644, 200, []byte("file-cksum"), sourceID, spaceDID)
		require.NoError(t, err)
		dirChild, _, err := repo.FindOrCreateDirectory(t.Context(), "parent/subdir", modTime, fs.ModeDir|0755, []byte("subdir-cksum"), sourceID, spaceDID)
		require.NoError(t, err)

		// Link children to parent
		err = repo.CreateDirectoryChildren(t.Context(), parentDir, []scanmodel.FSEntry{fileChild, dirChild})
		require.NoError(t, err)

		uploadID := id.New()

		// Set up file child: raw node, size 200
		fileScan, err := repo.CreateDAGScan(t.Context(), fileChild.ID(), false, uploadID, spaceDID)
		require.NoError(t, err)
		fileCID := randomRawCID(t)
		_, _, err = repo.FindOrCreateRawNode(t.Context(), fileCID, 200, spaceDID, uploadID, "parent/file.txt", sourceID, 0)
		require.NoError(t, err)
		require.NoError(t, fileScan.Complete(fileCID))
		require.NoError(t, repo.UpdateDAGScan(t.Context(), fileScan))

		// Set up dir child: UnixFS node size 10, with links totaling 60
		dirScan, err := repo.CreateDAGScan(t.Context(), dirChild.ID(), true, uploadID, spaceDID)
		require.NoError(t, err)
		dirCID := randomDAGPBCID(t)
		dirLinkParams := []model.LinkParams{
			{Name: "x.txt", TSize: 25, Hash: randomRawCID(t)},
			{Name: "y.txt", TSize: 35, Hash: randomRawCID(t)},
		}
		_, _, err = repo.FindOrCreateUnixFSNode(t.Context(), dirCID, 10, spaceDID, uploadID, []byte("ufsdata"), dirLinkParams)
		require.NoError(t, err)
		require.NoError(t, dirScan.Complete(dirCID))
		require.NoError(t, repo.UpdateDAGScan(t.Context(), dirScan))

		// Query directory links
		parentScan, err := repo.CreateDAGScan(t.Context(), parentDir.ID(), true, uploadID, spaceDID)
		require.NoError(t, err)
		parentDirScan, ok := parentScan.(*model.DirectoryDAGScan)
		require.True(t, ok)

		links, err := repo.DirectoryLinks(t.Context(), parentDirScan)
		require.NoError(t, err)
		require.Len(t, links, 2)

		// Build a map by name for order-independent assertions
		linksByName := map[string]model.LinkParams{}
		for _, l := range links {
			linksByName[l.Name] = l
		}

		require.Equal(t, uint64(200), linksByName["file.txt"].TSize)
		require.Equal(t, fileCID, linksByName["file.txt"].Hash)

		require.Equal(t, uint64(10+25+35), linksByName["subdir"].TSize)
		require.Equal(t, dirCID, linksByName["subdir"].Hash)
	})
}
