package sqlrepo_test

import (
	"fmt"
	"testing"

	"github.com/ipfs/go-cid"
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

		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID1, 100, spaceDID, uploadID, "dir/file1", id.New(), 0)
		require.NoError(t, err)
		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID2, 100, spaceDID, uploadID, "dir/file2", id.New(), 0)
		require.NoError(t, err)
		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID3, 100, spaceDID, uploadID, "dir/file3", id.New(), 0)
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

		// Close all three shards (ShardsNotInIndexes only returns closed shards)
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

		digest3, err := multihash.Encode([]byte("shard3"), multihash.IDENTITY)
		require.NoError(t, err)
		err = shard3.Close(digest3, testutil.RandomCID(t).(cidlink.Link).Cid)
		require.NoError(t, err)
		err = repo.UpdateShard(t.Context(), shard3)
		require.NoError(t, err)

		// All three closed shards should be returned as not in indexes
		shardIDs, err := repo.ShardsNotInIndexes(t.Context(), uploadID)
		require.NoError(t, err)
		require.ElementsMatch(t, []id.ShardID{shard1.ID(), shard2.ID(), shard3.ID()}, shardIDs)

		// Create an index and add shard1 and shard2 to it
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

	t.Run("does not return open shards", func(t *testing.T) {
		repo := testutil.Must(sqlrepo.New(testdb.CreateTestDB(t)))(t)

		spaceDID, err := did.Parse("did:storacha:space:example")
		require.NoError(t, err)

		uploadID := id.New()

		// Create nodes
		nodeCID1 := testutil.RandomCID(t).(cidlink.Link).Cid
		nodeCID2 := testutil.RandomCID(t).(cidlink.Link).Cid

		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID1, 100, spaceDID, uploadID, "dir/file1", id.New(), 0)
		require.NoError(t, err)
		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID2, 100, spaceDID, uploadID, "dir/file2", id.New(), 0)
		require.NoError(t, err)

		// Create one closed shard
		shard1, err := repo.CreateShard(t.Context(), uploadID, 0, nil, nil)
		require.NoError(t, err)
		err = repo.AddNodeToShard(t.Context(), shard1.ID(), nodeCID1, spaceDID, uploadID, 0)
		require.NoError(t, err)
		digest1, err := multihash.Encode([]byte("shard1"), multihash.IDENTITY)
		require.NoError(t, err)
		err = shard1.Close(digest1, testutil.RandomCID(t).(cidlink.Link).Cid)
		require.NoError(t, err)
		err = repo.UpdateShard(t.Context(), shard1)
		require.NoError(t, err)

		// Create one open shard
		shard2, err := repo.CreateShard(t.Context(), uploadID, 0, nil, nil)
		require.NoError(t, err)
		err = repo.AddNodeToShard(t.Context(), shard2.ID(), nodeCID2, spaceDID, uploadID, 0)
		require.NoError(t, err)

		// Only the closed shard should be returned
		shardIDs, err := repo.ShardsNotInIndexes(t.Context(), uploadID)
		require.NoError(t, err)
		require.Equal(t, []id.ShardID{shard1.ID()}, shardIDs)
	})

	t.Run("returns empty when all shards are assigned", func(t *testing.T) {
		repo := testutil.Must(sqlrepo.New(testdb.CreateTestDB(t)))(t)

		spaceDID, err := did.Parse("did:storacha:space:example")
		require.NoError(t, err)

		uploadID := id.New()

		// Create a node
		nodeCID := testutil.RandomCID(t).(cidlink.Link).Cid
		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID, 100, spaceDID, uploadID, "dir/file", id.New(), 0)
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

		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID1, 100, spaceDID, uploadID1, "dir/file1", id.New(), 0)
		require.NoError(t, err)
		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID2, 100, spaceDID, uploadID2, "dir/file2", id.New(), 0)
		require.NoError(t, err)

		// Create and close shards for different uploads
		shard1, err := repo.CreateShard(t.Context(), uploadID1, 0, nil, nil)
		require.NoError(t, err)
		err = repo.AddNodeToShard(t.Context(), shard1.ID(), nodeCID1, spaceDID, uploadID1, 0)
		require.NoError(t, err)
		digest1, err := multihash.Encode([]byte("shard1"), multihash.IDENTITY)
		require.NoError(t, err)
		err = shard1.Close(digest1, testutil.RandomCID(t).(cidlink.Link).Cid)
		require.NoError(t, err)
		err = repo.UpdateShard(t.Context(), shard1)
		require.NoError(t, err)

		shard2, err := repo.CreateShard(t.Context(), uploadID2, 0, nil, nil)
		require.NoError(t, err)
		err = repo.AddNodeToShard(t.Context(), shard2.ID(), nodeCID2, spaceDID, uploadID2, 0)
		require.NoError(t, err)
		digest2, err := multihash.Encode([]byte("shard2"), multihash.IDENTITY)
		require.NoError(t, err)
		err = shard2.Close(digest2, testutil.RandomCID(t).(cidlink.Link).Cid)
		require.NoError(t, err)
		err = repo.UpdateShard(t.Context(), shard2)
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

func TestForEachNodeInIndex(t *testing.T) {
	t.Run("yields all nodes across shards in an index", func(t *testing.T) {
		repo := testutil.Must(sqlrepo.New(testdb.CreateTestDB(t)))(t)

		spaceDID, err := did.Parse("did:storacha:space:example")
		require.NoError(t, err)

		uploadID := id.New()

		// Create 4 nodes
		nodeCID1 := testutil.RandomCID(t).(cidlink.Link).Cid
		nodeCID2 := testutil.RandomCID(t).(cidlink.Link).Cid
		nodeCID3 := testutil.RandomCID(t).(cidlink.Link).Cid
		nodeCID4 := testutil.RandomCID(t).(cidlink.Link).Cid

		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID1, 100, spaceDID, uploadID, "dir/file1", id.New(), 0)
		require.NoError(t, err)
		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID2, 200, spaceDID, uploadID, "dir/file2", id.New(), 0)
		require.NoError(t, err)
		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID3, 300, spaceDID, uploadID, "dir/file3", id.New(), 0)
		require.NoError(t, err)
		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID4, 400, spaceDID, uploadID, "dir/file4", id.New(), 0)
		require.NoError(t, err)

		// Create shard1 with nodes 1 and 2 (offset=0 for simplicity)
		shard1, err := repo.CreateShard(t.Context(), uploadID, 0, nil, nil)
		require.NoError(t, err)
		err = repo.AddNodeToShard(t.Context(), shard1.ID(), nodeCID1, spaceDID, uploadID, 0)
		require.NoError(t, err)
		err = repo.AddNodeToShard(t.Context(), shard1.ID(), nodeCID2, spaceDID, uploadID, 0)
		require.NoError(t, err)
		digest1, err := multihash.Encode([]byte("shard1"), multihash.IDENTITY)
		require.NoError(t, err)
		err = shard1.Close(digest1, testutil.RandomCID(t).(cidlink.Link).Cid)
		require.NoError(t, err)
		err = repo.UpdateShard(t.Context(), shard1)
		require.NoError(t, err)

		// Create shard2 with nodes 3 and 4 (offset=0 for simplicity)
		shard2, err := repo.CreateShard(t.Context(), uploadID, 0, nil, nil)
		require.NoError(t, err)
		err = repo.AddNodeToShard(t.Context(), shard2.ID(), nodeCID3, spaceDID, uploadID, 0)
		require.NoError(t, err)
		err = repo.AddNodeToShard(t.Context(), shard2.ID(), nodeCID4, spaceDID, uploadID, 0)
		require.NoError(t, err)
		digest2, err := multihash.Encode([]byte("shard2"), multihash.IDENTITY)
		require.NoError(t, err)
		err = shard2.Close(digest2, testutil.RandomCID(t).(cidlink.Link).Cid)
		require.NoError(t, err)
		err = repo.UpdateShard(t.Context(), shard2)
		require.NoError(t, err)

		// Create index with both shards
		index, err := repo.CreateIndex(t.Context(), uploadID)
		require.NoError(t, err)
		err = repo.AddShardToIndex(t.Context(), index.ID(), shard1.ID())
		require.NoError(t, err)
		err = repo.AddShardToIndex(t.Context(), index.ID(), shard2.ID())
		require.NoError(t, err)

		// Collect all yielded rows
		type row struct {
			shardDigest multihash.Multihash
			nodeCID     cid.Cid
			nodeSize    uint64
			shardOffset uint64
		}
		var rows []row
		err = repo.ForEachNodeInIndex(t.Context(), index.ID(), func(shardDigest multihash.Multihash, nodeCID cid.Cid, nodeSize uint64, shardOffset uint64) error {
			rows = append(rows, row{shardDigest: shardDigest, nodeCID: nodeCID, nodeSize: nodeSize, shardOffset: shardOffset})
			return nil
		})
		require.NoError(t, err)
		require.Len(t, rows, 4)

		// Build a lookup by nodeCID for easier assertions
		byCID := map[string]row{}
		for _, r := range rows {
			byCID[r.nodeCID.String()] = r
		}

		// shard_offset = shard.size + offset at time of AddNodeToShard
		// shard1: node1 offset=0+0=0, size becomes 100; node2 offset=100+0=100
		// shard2: node3 offset=0+0=0, size becomes 300; node4 offset=300+0=300
		r1 := byCID[nodeCID1.String()]
		require.Equal(t, digest1, []byte(r1.shardDigest))
		require.Equal(t, uint64(100), r1.nodeSize)
		require.Equal(t, uint64(0), r1.shardOffset)

		r2 := byCID[nodeCID2.String()]
		require.Equal(t, digest1, []byte(r2.shardDigest))
		require.Equal(t, uint64(200), r2.nodeSize)
		require.Equal(t, uint64(100), r2.shardOffset)

		r3 := byCID[nodeCID3.String()]
		require.Equal(t, digest2, []byte(r3.shardDigest))
		require.Equal(t, uint64(300), r3.nodeSize)
		require.Equal(t, uint64(0), r3.shardOffset)

		r4 := byCID[nodeCID4.String()]
		require.Equal(t, digest2, []byte(r4.shardDigest))
		require.Equal(t, uint64(400), r4.nodeSize)
		require.Equal(t, uint64(300), r4.shardOffset)
	})

	t.Run("yields nothing for an empty index", func(t *testing.T) {
		repo := testutil.Must(sqlrepo.New(testdb.CreateTestDB(t)))(t)

		uploadID := id.New()

		index, err := repo.CreateIndex(t.Context(), uploadID)
		require.NoError(t, err)

		called := false
		err = repo.ForEachNodeInIndex(t.Context(), index.ID(), func(shardDigest multihash.Multihash, nodeCID cid.Cid, nodeSize uint64, shardOffset uint64) error {
			called = true
			return nil
		})
		require.NoError(t, err)
		require.False(t, called)
	})

	t.Run("only yields nodes for shards in the specified index", func(t *testing.T) {
		repo := testutil.Must(sqlrepo.New(testdb.CreateTestDB(t)))(t)

		spaceDID, err := did.Parse("did:storacha:space:example")
		require.NoError(t, err)

		uploadID := id.New()

		// Create 2 nodes
		nodeCID1 := testutil.RandomCID(t).(cidlink.Link).Cid
		nodeCID2 := testutil.RandomCID(t).(cidlink.Link).Cid

		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID1, 100, spaceDID, uploadID, "dir/file1", id.New(), 0)
		require.NoError(t, err)
		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID2, 200, spaceDID, uploadID, "dir/file2", id.New(), 0)
		require.NoError(t, err)

		// Create shard1 with node1
		shard1, err := repo.CreateShard(t.Context(), uploadID, 0, nil, nil)
		require.NoError(t, err)
		err = repo.AddNodeToShard(t.Context(), shard1.ID(), nodeCID1, spaceDID, uploadID, 0)
		require.NoError(t, err)
		digest1, err := multihash.Encode([]byte("shard1"), multihash.IDENTITY)
		require.NoError(t, err)
		err = shard1.Close(digest1, testutil.RandomCID(t).(cidlink.Link).Cid)
		require.NoError(t, err)
		err = repo.UpdateShard(t.Context(), shard1)
		require.NoError(t, err)

		// Create shard2 with node2
		shard2, err := repo.CreateShard(t.Context(), uploadID, 0, nil, nil)
		require.NoError(t, err)
		err = repo.AddNodeToShard(t.Context(), shard2.ID(), nodeCID2, spaceDID, uploadID, 0)
		require.NoError(t, err)
		digest2, err := multihash.Encode([]byte("shard2"), multihash.IDENTITY)
		require.NoError(t, err)
		err = shard2.Close(digest2, testutil.RandomCID(t).(cidlink.Link).Cid)
		require.NoError(t, err)
		err = repo.UpdateShard(t.Context(), shard2)
		require.NoError(t, err)

		// Create two indexes, one with each shard
		index1, err := repo.CreateIndex(t.Context(), uploadID)
		require.NoError(t, err)
		err = repo.AddShardToIndex(t.Context(), index1.ID(), shard1.ID())
		require.NoError(t, err)

		index2, err := repo.CreateIndex(t.Context(), uploadID)
		require.NoError(t, err)
		err = repo.AddShardToIndex(t.Context(), index2.ID(), shard2.ID())
		require.NoError(t, err)

		// ForEachNodeInIndex for index1 should only yield node1
		var nodeCIDs1 []cid.Cid
		err = repo.ForEachNodeInIndex(t.Context(), index1.ID(), func(shardDigest multihash.Multihash, nodeCID cid.Cid, nodeSize uint64, shardOffset uint64) error {
			nodeCIDs1 = append(nodeCIDs1, nodeCID)
			return nil
		})
		require.NoError(t, err)
		require.Len(t, nodeCIDs1, 1)
		require.Equal(t, nodeCID1, nodeCIDs1[0])

		// ForEachNodeInIndex for index2 should only yield node2
		var nodeCIDs2 []cid.Cid
		err = repo.ForEachNodeInIndex(t.Context(), index2.ID(), func(shardDigest multihash.Multihash, nodeCID cid.Cid, nodeSize uint64, shardOffset uint64) error {
			nodeCIDs2 = append(nodeCIDs2, nodeCID)
			return nil
		})
		require.NoError(t, err)
		require.Len(t, nodeCIDs2, 1)
		require.Equal(t, nodeCID2, nodeCIDs2[0])
	})

	t.Run("errors if a shard has no digest set", func(t *testing.T) {
		repo := testutil.Must(sqlrepo.New(testdb.CreateTestDB(t)))(t)

		spaceDID, err := did.Parse("did:storacha:space:example")
		require.NoError(t, err)

		uploadID := id.New()

		// Create 2 nodes
		nodeCID1 := testutil.RandomCID(t).(cidlink.Link).Cid
		nodeCID2 := testutil.RandomCID(t).(cidlink.Link).Cid

		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID1, 100, spaceDID, uploadID, "dir/file1", id.New(), 0)
		require.NoError(t, err)
		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID2, 200, spaceDID, uploadID, "dir/file2", id.New(), 0)
		require.NoError(t, err)

		// Create shard1 with node1
		shard1, err := repo.CreateShard(t.Context(), uploadID, 0, nil, nil)
		require.NoError(t, err)
		err = repo.AddNodeToShard(t.Context(), shard1.ID(), nodeCID1, spaceDID, uploadID, 0)
		require.NoError(t, err)
		digest1, err := multihash.Encode([]byte("shard1"), multihash.IDENTITY)
		require.NoError(t, err)
		err = shard1.Close(digest1, testutil.RandomCID(t).(cidlink.Link).Cid)
		require.NoError(t, err)
		err = repo.UpdateShard(t.Context(), shard1)
		require.NoError(t, err)

		// Create shard2 with node2
		shard2, err := repo.CreateShard(t.Context(), uploadID, 0, nil, nil)
		require.NoError(t, err)
		err = repo.AddNodeToShard(t.Context(), shard2.ID(), nodeCID2, spaceDID, uploadID, 0)
		require.NoError(t, err)

		// Never close shard2.

		// Create index with both shards
		index, err := repo.CreateIndex(t.Context(), uploadID)
		require.NoError(t, err)
		err = repo.AddShardToIndex(t.Context(), index.ID(), shard1.ID())
		require.NoError(t, err)
		err = repo.AddShardToIndex(t.Context(), index.ID(), shard2.ID())
		require.NoError(t, err)

		err = repo.ForEachNodeInIndex(t.Context(), index.ID(), func(shardDigest multihash.Multihash, nodeCID cid.Cid, nodeSize uint64, shardOffset uint64) error {
			return nil
		})
		require.ErrorContains(t, err, fmt.Sprintf("failed to iterate nodes in index %s, because shard with ID %s has no digest set", index.ID(), shard2.ID()))
	})
}
