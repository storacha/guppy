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
		node, _, err := repo.FindOrCreateRawNode(t.Context(), testutil.RandomCID(t).(cidlink.Link).Cid, size, spaceDID, uploadID, "dir/file", id.New(), 0)
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

		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID1, 100, spaceDID, uploadID, "dir/file1", id.New(), 0)
		require.NoError(t, err)
		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID2, 100, spaceDID, uploadID, "dir/file2", id.New(), 0)
		require.NoError(t, err)
		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID3, 100, spaceDID, uploadID, "dir/file3", id.New(), 0)
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
		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID, 100, spaceDID, uploadID, "dir/file", id.New(), 0)
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
		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID1, 100, spaceDID, uploadID1, "dir/file1", id.New(), 0)
		require.NoError(t, err)
		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID2, 100, spaceDID, uploadID2, "dir/file2", id.New(), 0)
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
