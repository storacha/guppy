package sqlrepo_test

import (
	"testing"

	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multihash"
	"github.com/storacha/go-libstoracha/testutil"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/guppy/pkg/preparation/internal/testdb"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	"github.com/stretchr/testify/require"
)

func TestShardsNotInIndexes(t *testing.T) {
	t.Run("returns shards that are not assigned to indexes", func(t *testing.T) {
		repo := testutil.Must(sqlrepo.New(testdb.CreateTestDB(t)))(t)

		spaceDID, err := did.Parse("did:storacha:space:example")
		require.NoError(t, err)

		uploadID := id.New()

		// Create nodes (required for shards to have content)
		nodeCID1 := testutil.RandomCID(t).(cidlink.Link).Cid
		nodeCID2 := testutil.RandomCID(t).(cidlink.Link).Cid
		nodeCID3 := testutil.RandomCID(t).(cidlink.Link).Cid

		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID1, 100, spaceDID, "dir/file1", id.New(), 0)
		require.NoError(t, err)
		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID2, 100, spaceDID, "dir/file2", id.New(), 0)
		require.NoError(t, err)
		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID3, 100, spaceDID, "dir/file3", id.New(), 0)
		require.NoError(t, err)

		// Create node_upload records
		_, _, err = repo.FindOrCreateNodeUpload(t.Context(), uploadID, nodeCID1, spaceDID)
		require.NoError(t, err)
		_, _, err = repo.FindOrCreateNodeUpload(t.Context(), uploadID, nodeCID2, spaceDID)
		require.NoError(t, err)
		_, _, err = repo.FindOrCreateNodeUpload(t.Context(), uploadID, nodeCID3, spaceDID)
		require.NoError(t, err)

		// Create three shards
		shard1, err := repo.CreateShard(t.Context(), uploadID, 0, nil, nil)
		require.NoError(t, err)
		err = repo.AddNodeToShard(t.Context(), shard1.ID(), nodeCID1, spaceDID, uploadID, 0)
		require.NoError(t, err)

		shard2, err := repo.CreateShard(t.Context(), uploadID, 0, nil, nil)
		require.NoError(t, err)
		err = repo.AddNodeToShard(t.Context(), shard2.ID(), nodeCID2, spaceDID, uploadID, 0)
		require.NoError(t, err)

		shard3, err := repo.CreateShard(t.Context(), uploadID, 0, nil, nil)
		require.NoError(t, err)
		err = repo.AddNodeToShard(t.Context(), shard3.ID(), nodeCID3, spaceDID, uploadID, 0)
		require.NoError(t, err)

		// All three should be returned as not in indexes
		shardIDs, err := repo.ShardsNotInIndexes(t.Context(), uploadID)
		require.NoError(t, err)
		require.ElementsMatch(t, []id.ShardID{shard1.ID(), shard2.ID(), shard3.ID()}, shardIDs)

		// Create an index and add shard1 and shard2 to it
		// First close the shards (required before adding to index)
		digest1, err := multihash.Encode([]byte("shard1"), multihash.IDENTITY)
		require.NoError(t, err)
		err = shard1.Close(digest1, testutil.RandomCID(t).(cidlink.Link).Cid)
		require.NoError(t, err)
		err = repo.UpdateShard(t.Context(), shard1)
		require.NoError(t, err)

		digest2, err := multihash.Encode([]byte("shard2"), multihash.IDENTITY)
		require.NoError(t, err)
		err = shard2.Close(digest2, testutil.RandomCID(t).(cidlink.Link).Cid)
		require.NoError(t, err)
		err = repo.UpdateShard(t.Context(), shard2)
		require.NoError(t, err)

		index, err := repo.CreateIndex(t.Context(), uploadID)
		require.NoError(t, err)
		err = repo.AddShardToIndex(t.Context(), index.ID(), shard1.ID())
		require.NoError(t, err)
		err = repo.AddShardToIndex(t.Context(), index.ID(), shard2.ID())
		require.NoError(t, err)

		// Only shard3 should be returned now
		shardIDs, err = repo.ShardsNotInIndexes(t.Context(), uploadID)
		require.NoError(t, err)
		require.Equal(t, []id.ShardID{shard3.ID()}, shardIDs)
	})

	t.Run("returns empty when all shards are assigned", func(t *testing.T) {
		repo := testutil.Must(sqlrepo.New(testdb.CreateTestDB(t)))(t)

		spaceDID, err := did.Parse("did:storacha:space:example")
		require.NoError(t, err)

		uploadID := id.New()

		// Create a node
		nodeCID := testutil.RandomCID(t).(cidlink.Link).Cid
		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID, 100, spaceDID, "dir/file", id.New(), 0)
		require.NoError(t, err)
		_, _, err = repo.FindOrCreateNodeUpload(t.Context(), uploadID, nodeCID, spaceDID)
		require.NoError(t, err)

		// Create and close a shard
		shard, err := repo.CreateShard(t.Context(), uploadID, 0, nil, nil)
		require.NoError(t, err)
		err = repo.AddNodeToShard(t.Context(), shard.ID(), nodeCID, spaceDID, uploadID, 0)
		require.NoError(t, err)

		digest, err := multihash.Encode([]byte("shard"), multihash.IDENTITY)
		require.NoError(t, err)
		err = shard.Close(digest, testutil.RandomCID(t).(cidlink.Link).Cid)
		require.NoError(t, err)
		err = repo.UpdateShard(t.Context(), shard)
		require.NoError(t, err)

		// Assign to index
		index, err := repo.CreateIndex(t.Context(), uploadID)
		require.NoError(t, err)
		err = repo.AddShardToIndex(t.Context(), index.ID(), shard.ID())
		require.NoError(t, err)

		// Should return empty
		shardIDs, err := repo.ShardsNotInIndexes(t.Context(), uploadID)
		require.NoError(t, err)
		require.Empty(t, shardIDs)
	})

	t.Run("only returns shards for the specified upload", func(t *testing.T) {
		repo := testutil.Must(sqlrepo.New(testdb.CreateTestDB(t)))(t)

		spaceDID, err := did.Parse("did:storacha:space:example")
		require.NoError(t, err)

		uploadID1 := id.New()
		uploadID2 := id.New()

		// Create nodes
		nodeCID1 := testutil.RandomCID(t).(cidlink.Link).Cid
		nodeCID2 := testutil.RandomCID(t).(cidlink.Link).Cid

		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID1, 100, spaceDID, "dir/file1", id.New(), 0)
		require.NoError(t, err)
		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID2, 100, spaceDID, "dir/file2", id.New(), 0)
		require.NoError(t, err)

		// Create node_upload records for different uploads
		_, _, err = repo.FindOrCreateNodeUpload(t.Context(), uploadID1, nodeCID1, spaceDID)
		require.NoError(t, err)
		_, _, err = repo.FindOrCreateNodeUpload(t.Context(), uploadID2, nodeCID2, spaceDID)
		require.NoError(t, err)

		// Create shards for different uploads
		shard1, err := repo.CreateShard(t.Context(), uploadID1, 0, nil, nil)
		require.NoError(t, err)
		err = repo.AddNodeToShard(t.Context(), shard1.ID(), nodeCID1, spaceDID, uploadID1, 0)
		require.NoError(t, err)

		shard2, err := repo.CreateShard(t.Context(), uploadID2, 0, nil, nil)
		require.NoError(t, err)
		err = repo.AddNodeToShard(t.Context(), shard2.ID(), nodeCID2, spaceDID, uploadID2, 0)
		require.NoError(t, err)

		// ShardsNotInIndexes for upload1 should only return shard1
		shardIDs1, err := repo.ShardsNotInIndexes(t.Context(), uploadID1)
		require.NoError(t, err)
		require.Equal(t, []id.ShardID{shard1.ID()}, shardIDs1)

		// ShardsNotInIndexes for upload2 should only return shard2
		shardIDs2, err := repo.ShardsNotInIndexes(t.Context(), uploadID2)
		require.NoError(t, err)
		require.Equal(t, []id.ShardID{shard2.ID()}, shardIDs2)
	})
}
