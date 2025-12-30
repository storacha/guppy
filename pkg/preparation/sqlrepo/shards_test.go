package sqlrepo_test

import (
	"testing"

	"github.com/ipfs/go-cid"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/storacha/go-libstoracha/testutil"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/guppy/pkg/preparation/blobs/model"
	dagsmodel "github.com/storacha/guppy/pkg/preparation/dags/model"
	"github.com/storacha/guppy/pkg/preparation/internal/testdb"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	"github.com/stretchr/testify/require"
)

func TestAddNodeToShard(t *testing.T) {
	repo := testutil.Must(sqlrepo.New(testdb.CreateTestDB(t)))(t)

	spaceDID, err := did.Parse("did:storacha:space:example")
	require.NoError(t, err)

	uploadID := id.New()

	makeRawNodeWithSize := func(size uint64) dagsmodel.Node {
		node, _, err := repo.FindOrCreateRawNode(t.Context(), testutil.RandomCID(t).(cidlink.Link).Cid, size, spaceDID, "dir/file", id.New(), 0)
		require.NoError(t, err)
		// Create node_upload record (required since AddNodeToShard now does UPDATE instead of INSERT)
		_, _, err = repo.FindOrCreateNodeUpload(t.Context(), uploadID, node.CID(), spaceDID)
		require.NoError(t, err)
		return node
	}

	reloadShard := func(shardID id.ShardID) *model.Shard {
		shard, err := repo.GetShardByID(t.Context(), shardID)
		require.NoError(t, err)
		return shard
	}

	shard, err := repo.CreateShard(t.Context(), uploadID, 100, nil, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(100), shard.Size())

	node1 := makeRawNodeWithSize(10)
	err = repo.AddNodeToShard(t.Context(), shard.ID(), node1.CID(), spaceDID, uploadID, 1)
	require.NoError(t, err)
	shard = reloadShard(shard.ID())
	require.Equal(t, uint64(100+1+10), shard.Size())

	node2 := makeRawNodeWithSize(20)
	err = repo.AddNodeToShard(t.Context(), shard.ID(), node2.CID(), spaceDID, uploadID, 2)
	require.NoError(t, err)
	shard = reloadShard(shard.ID())
	require.Equal(t, uint64(100+1+10+2+20), shard.Size())

	node3 := makeRawNodeWithSize(30)
	err = repo.AddNodeToShard(t.Context(), shard.ID(), node3.CID(), spaceDID, uploadID, 3)
	require.NoError(t, err)
	shard = reloadShard(shard.ID())
	require.Equal(t, uint64(100+1+10+2+20+3+30), shard.Size())

	type nodeInfo struct {
		cid    cid.Cid
		offset uint64
	}

	nodeInfos := make([]nodeInfo, 0, 3)
	err = repo.ForEachNode(t.Context(), shard.ID(), func(node dagsmodel.Node, shardOffset uint64) error {
		nodeInfos = append(nodeInfos, nodeInfo{
			cid:    node.CID(),
			offset: shardOffset,
		})
		return nil
	})
	require.NoError(t, err)

	require.Equal(t, []nodeInfo{
		{cid: node1.CID(), offset: 100 + 1},
		{cid: node2.CID(), offset: 100 + 1 + 10 + 2},
		{cid: node3.CID(), offset: 100 + 1 + 10 + 2 + 20 + 3},
	}, nodeInfos)
}

func TestFindOrCreateNodeUpload(t *testing.T) {
	t.Run("creates a new node upload record", func(t *testing.T) {
		repo := testutil.Must(sqlrepo.New(testdb.CreateTestDB(t)))(t)

		spaceDID, err := did.Parse("did:storacha:space:example")
		require.NoError(t, err)

		uploadID := id.New()
		nodeCID := testutil.RandomCID(t).(cidlink.Link).Cid

		// First create the node
		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID, 100, spaceDID, "dir/file", id.New(), 0)
		require.NoError(t, err)

		// FindOrCreate should create a new record
		nodeUpload, created, err := repo.FindOrCreateNodeUpload(t.Context(), uploadID, nodeCID, spaceDID)
		require.NoError(t, err)
		require.True(t, created, "should have created a new record")
		require.Equal(t, nodeCID, nodeUpload.NodeCID())
		require.Equal(t, spaceDID, nodeUpload.SpaceDID())
		require.Equal(t, uploadID, nodeUpload.UploadID())
		require.False(t, nodeUpload.HasShard(), "should not have shard assigned yet")
	})

	t.Run("finds existing node upload record", func(t *testing.T) {
		repo := testutil.Must(sqlrepo.New(testdb.CreateTestDB(t)))(t)

		spaceDID, err := did.Parse("did:storacha:space:example")
		require.NoError(t, err)

		uploadID := id.New()
		nodeCID := testutil.RandomCID(t).(cidlink.Link).Cid

		// First create the node
		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID, 100, spaceDID, "dir/file", id.New(), 0)
		require.NoError(t, err)

		// Create the node upload record
		nodeUpload1, created1, err := repo.FindOrCreateNodeUpload(t.Context(), uploadID, nodeCID, spaceDID)
		require.NoError(t, err)
		require.True(t, created1)

		// Try to create again - should find existing
		nodeUpload2, created2, err := repo.FindOrCreateNodeUpload(t.Context(), uploadID, nodeCID, spaceDID)
		require.NoError(t, err)
		require.False(t, created2, "should have found existing record")
		require.Equal(t, nodeUpload1.NodeCID(), nodeUpload2.NodeCID())
		require.Equal(t, nodeUpload1.SpaceDID(), nodeUpload2.SpaceDID())
		require.Equal(t, nodeUpload1.UploadID(), nodeUpload2.UploadID())
	})

	t.Run("same node can be in multiple uploads", func(t *testing.T) {
		repo := testutil.Must(sqlrepo.New(testdb.CreateTestDB(t)))(t)

		spaceDID, err := did.Parse("did:storacha:space:example")
		require.NoError(t, err)

		uploadID1 := id.New()
		uploadID2 := id.New()
		nodeCID := testutil.RandomCID(t).(cidlink.Link).Cid

		// First create the node
		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID, 100, spaceDID, "dir/file", id.New(), 0)
		require.NoError(t, err)

		// Create node upload for first upload
		nodeUpload1, created1, err := repo.FindOrCreateNodeUpload(t.Context(), uploadID1, nodeCID, spaceDID)
		require.NoError(t, err)
		require.True(t, created1)
		require.Equal(t, uploadID1, nodeUpload1.UploadID())

		// Create node upload for second upload - should create a new record
		nodeUpload2, created2, err := repo.FindOrCreateNodeUpload(t.Context(), uploadID2, nodeCID, spaceDID)
		require.NoError(t, err)
		require.True(t, created2, "should create new record for different upload")
		require.Equal(t, uploadID2, nodeUpload2.UploadID())
	})
}

func TestNodesNotInShards(t *testing.T) {
	t.Run("returns nodes that are not assigned to shards", func(t *testing.T) {
		repo := testutil.Must(sqlrepo.New(testdb.CreateTestDB(t)))(t)

		spaceDID, err := did.Parse("did:storacha:space:example")
		require.NoError(t, err)

		uploadID := id.New()

		// Create nodes
		nodeCID1 := testutil.RandomCID(t).(cidlink.Link).Cid
		nodeCID2 := testutil.RandomCID(t).(cidlink.Link).Cid
		nodeCID3 := testutil.RandomCID(t).(cidlink.Link).Cid

		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID1, 100, spaceDID, "dir/file1", id.New(), 0)
		require.NoError(t, err)
		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID2, 100, spaceDID, "dir/file2", id.New(), 0)
		require.NoError(t, err)
		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID3, 100, spaceDID, "dir/file3", id.New(), 0)
		require.NoError(t, err)

		// Create node_upload records for all three
		_, _, err = repo.FindOrCreateNodeUpload(t.Context(), uploadID, nodeCID1, spaceDID)
		require.NoError(t, err)
		_, _, err = repo.FindOrCreateNodeUpload(t.Context(), uploadID, nodeCID2, spaceDID)
		require.NoError(t, err)
		_, _, err = repo.FindOrCreateNodeUpload(t.Context(), uploadID, nodeCID3, spaceDID)
		require.NoError(t, err)

		// All three should be returned as not in shards
		cids, err := repo.NodesNotInShards(t.Context(), uploadID, spaceDID)
		require.NoError(t, err)
		require.ElementsMatch(t, []cid.Cid{nodeCID1, nodeCID2, nodeCID3}, cids)

		// Create a shard and assign nodeCID1 and nodeCID2 to it
		shard, err := repo.CreateShard(t.Context(), uploadID, 0, nil, nil)
		require.NoError(t, err)
		err = repo.AddNodeToShard(t.Context(), shard.ID(), nodeCID1, spaceDID, uploadID, 0)
		require.NoError(t, err)
		err = repo.AddNodeToShard(t.Context(), shard.ID(), nodeCID2, spaceDID, uploadID, 0)
		require.NoError(t, err)

		// Only nodeCID3 should be returned now
		cids, err = repo.NodesNotInShards(t.Context(), uploadID, spaceDID)
		require.NoError(t, err)
		require.Equal(t, []cid.Cid{nodeCID3}, cids)
	})

	t.Run("returns empty when all nodes are assigned", func(t *testing.T) {
		repo := testutil.Must(sqlrepo.New(testdb.CreateTestDB(t)))(t)

		spaceDID, err := did.Parse("did:storacha:space:example")
		require.NoError(t, err)

		uploadID := id.New()
		nodeCID := testutil.RandomCID(t).(cidlink.Link).Cid

		// Create node and node_upload
		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID, 100, spaceDID, "dir/file", id.New(), 0)
		require.NoError(t, err)
		_, _, err = repo.FindOrCreateNodeUpload(t.Context(), uploadID, nodeCID, spaceDID)
		require.NoError(t, err)

		// Assign to shard
		shard, err := repo.CreateShard(t.Context(), uploadID, 0, nil, nil)
		require.NoError(t, err)
		err = repo.AddNodeToShard(t.Context(), shard.ID(), nodeCID, spaceDID, uploadID, 0)
		require.NoError(t, err)

		// Should return empty
		cids, err := repo.NodesNotInShards(t.Context(), uploadID, spaceDID)
		require.NoError(t, err)
		require.Empty(t, cids)
	})

	t.Run("only returns nodes for the specified upload", func(t *testing.T) {
		repo := testutil.Must(sqlrepo.New(testdb.CreateTestDB(t)))(t)

		spaceDID, err := did.Parse("did:storacha:space:example")
		require.NoError(t, err)

		uploadID1 := id.New()
		uploadID2 := id.New()

		nodeCID1 := testutil.RandomCID(t).(cidlink.Link).Cid
		nodeCID2 := testutil.RandomCID(t).(cidlink.Link).Cid

		// Create nodes
		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID1, 100, spaceDID, "dir/file1", id.New(), 0)
		require.NoError(t, err)
		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID2, 100, spaceDID, "dir/file2", id.New(), 0)
		require.NoError(t, err)

		// Create node_upload records - nodeCID1 for upload1, nodeCID2 for upload2
		_, _, err = repo.FindOrCreateNodeUpload(t.Context(), uploadID1, nodeCID1, spaceDID)
		require.NoError(t, err)
		_, _, err = repo.FindOrCreateNodeUpload(t.Context(), uploadID2, nodeCID2, spaceDID)
		require.NoError(t, err)

		// NodesNotInShards for upload1 should only return nodeCID1
		cids1, err := repo.NodesNotInShards(t.Context(), uploadID1, spaceDID)
		require.NoError(t, err)
		require.Equal(t, []cid.Cid{nodeCID1}, cids1)

		// NodesNotInShards for upload2 should only return nodeCID2
		cids2, err := repo.NodesNotInShards(t.Context(), uploadID2, spaceDID)
		require.NoError(t, err)
		require.Equal(t, []cid.Cid{nodeCID2}, cids2)
	})
}
