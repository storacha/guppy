package blobs_test

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"io/fs"
	"slices"
	"testing"

	commcid "github.com/filecoin-project/go-fil-commcid"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2/blockstore"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multihash"
	commp "github.com/storacha/go-fil-commp-hashhash"
	"github.com/storacha/go-libstoracha/blobindex"
	stestutil "github.com/storacha/go-libstoracha/testutil"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/guppy/pkg/preparation/blobs"
	"github.com/storacha/guppy/pkg/preparation/blobs/model"
	dagsmodel "github.com/storacha/guppy/pkg/preparation/dags/model"
	"github.com/storacha/guppy/pkg/preparation/dags/nodereader"
	"github.com/storacha/guppy/pkg/preparation/internal/testdb"
	"github.com/storacha/guppy/pkg/preparation/internal/testutil"
	spacesmodel "github.com/storacha/guppy/pkg/preparation/spaces/model"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo/util"
	"github.com/storacha/guppy/pkg/preparation/types"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	"github.com/stretchr/testify/require"
)

func TestAddNodeToUploadShardsAndCloseUploadShards(t *testing.T) {
	t.Run("adds nodes to shards", func(t *testing.T) {
		db := testdb.CreateTestDB(t)
		repo := stestutil.Must(sqlrepo.New(db))(t)
		api := blobs.API{Repo: repo, ShardEncoder: blobs.NewCAREncoder()}
		spaceDID := stestutil.RandomDID(t)
		upload, source := testutil.CreateUpload(t, repo, spaceDID, spacesmodel.WithShardSize(1<<16))

		nodeCID1 := stestutil.RandomCID(t)

		// with no shards, creates a new shard and adds the node to it

		openShards, err := repo.ShardsForUploadByState(t.Context(), upload.ID(), model.BlobStateOpen)
		require.NoError(t, err)
		require.Len(t, openShards, 0)
		n, _, err := repo.FindOrCreateRawNode(t.Context(), nodeCID1.(cidlink.Link).Cid, 1<<14, spaceDID, "some/path", source.ID(), 0)
		require.NoError(t, err)
		_, err = repo.FindOrCreateNodeUpload(t.Context(), upload.ID(), nodeCID1.(cidlink.Link).Cid, spaceDID)
		require.NoError(t, err)

		data := stestutil.RandomBytes(t, int(n.Size()))
		shardClosed := false
		err = api.AddNodeToUploadShards(t.Context(), upload.ID(), spaceDID, nodeCID1.(cidlink.Link).Cid, data, func(shard *model.Shard) error {
			shardClosed = true
			return nil
		})
		require.NoError(t, err)
		require.False(t, shardClosed)

		openShards, err = repo.ShardsForUploadByState(t.Context(), upload.ID(), model.BlobStateOpen)
		require.NoError(t, err)
		require.Len(t, openShards, 1)
		firstShard := openShards[0]

		// The CAR header is 18 bytes
		// The varint length prefix for a ~1<<14 block is 3 bytes
		// The CIDv1 is 36 bytes
		require.Equal(t, uint64(18+3+36+(1<<14)), firstShard.Size())

		foundNodeCIDs := nodesInShard(t.Context(), t, db, firstShard.ID())
		require.ElementsMatch(t, []cid.Cid{nodeCID1.(cidlink.Link).Cid}, foundNodeCIDs)

		// with an open shard with room, adds the node to the shard

		nodeCID2 := stestutil.RandomCID(t)
		n, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID2.(cidlink.Link).Cid, 1<<14, spaceDID, "some/other/path", source.ID(), 0)
		require.NoError(t, err)
		_, err = repo.FindOrCreateNodeUpload(t.Context(), upload.ID(), nodeCID2.(cidlink.Link).Cid, spaceDID)
		require.NoError(t, err)
		data = stestutil.RandomBytes(t, int(n.Size()))
		shardClosed = false
		err = api.AddNodeToUploadShards(t.Context(), upload.ID(), spaceDID, nodeCID2.(cidlink.Link).Cid, data, func(shard *model.Shard) error {
			shardClosed = true
			return nil
		})
		require.NoError(t, err)
		require.False(t, shardClosed)

		openShards, err = repo.ShardsForUploadByState(t.Context(), upload.ID(), model.BlobStateOpen)
		require.NoError(t, err)
		require.Len(t, openShards, 1)
		require.Equal(t, firstShard.ID(), openShards[0].ID())
		firstShard = openShards[0] // with fresh data from DB

		require.Equal(t, uint64(18+3+36+(1<<14)+3+36+(1<<14)), firstShard.Size())

		foundNodeCIDs = nodesInShard(t.Context(), t, db, firstShard.ID())
		require.ElementsMatch(t, []cid.Cid{nodeCID1.(cidlink.Link).Cid, nodeCID2.(cidlink.Link).Cid}, foundNodeCIDs)

		// with an open shard without room, closes the shard and creates another

		nodeCID3 := stestutil.RandomCID(t)
		n, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID3.(cidlink.Link).Cid, 1<<15, spaceDID, "yet/other/path", source.ID(), 0)
		require.NoError(t, err)
		_, err = repo.FindOrCreateNodeUpload(t.Context(), upload.ID(), nodeCID3.(cidlink.Link).Cid, spaceDID)
		require.NoError(t, err)

		data = stestutil.RandomBytes(t, int(n.Size()))
		shardClosed = false
		err = api.AddNodeToUploadShards(t.Context(), upload.ID(), spaceDID, nodeCID3.(cidlink.Link).Cid, data, func(shard *model.Shard) error {
			shardClosed = true
			return nil
		})
		require.NoError(t, err)
		require.True(t, shardClosed)

		closedShards, err := repo.ShardsForUploadByState(t.Context(), upload.ID(), model.BlobStateClosed)
		require.NoError(t, err)
		require.Len(t, closedShards, 1)
		require.Equal(t, firstShard.ID(), closedShards[0].ID())

		foundNodeCIDs = nodesInShard(t.Context(), t, db, firstShard.ID())
		require.ElementsMatch(t, []cid.Cid{nodeCID1.(cidlink.Link).Cid, nodeCID2.(cidlink.Link).Cid}, foundNodeCIDs)

		openShards, err = repo.ShardsForUploadByState(t.Context(), upload.ID(), model.BlobStateOpen)
		require.NoError(t, err)
		require.Len(t, openShards, 1)
		secondShard := openShards[0]
		require.NotEqual(t, firstShard.ID(), secondShard.ID())

		require.Equal(t, uint64(18+3+36+(1<<15)), secondShard.Size())
		foundNodeCIDs = nodesInShard(t.Context(), t, db, secondShard.ID())
		require.ElementsMatch(t, []cid.Cid{nodeCID3.(cidlink.Link).Cid}, foundNodeCIDs)

		// finally, close the last shard with CloseUploadShards()

		err = api.CloseUploadShards(t.Context(), upload.ID(), func(shard *model.Shard) error {
			shardClosed = true
			return nil
		})
		require.NoError(t, err)
		require.True(t, shardClosed)

		closedShards, err = repo.ShardsForUploadByState(t.Context(), upload.ID(), model.BlobStateClosed)
		require.NoError(t, err)
		require.Len(t, closedShards, 2)
		require.Equal(t, firstShard.ID(), closedShards[0].ID())

		closedShardIDs := make([]id.ShardID, 0, len(closedShards))
		for _, closedShard := range closedShards {
			closedShardIDs = append(closedShardIDs, closedShard.ID())
		}
		require.ElementsMatch(t, closedShardIDs, []id.ShardID{firstShard.ID(), secondShard.ID()})

		openShards, err = repo.ShardsForUploadByState(t.Context(), upload.ID(), model.BlobStateOpen)
		require.NoError(t, err)
		require.Len(t, openShards, 0)
	})

	t.Run("limits shards to index slice count", func(t *testing.T) {
		db := testdb.CreateTestDB(t)
		repo := stestutil.Must(sqlrepo.New(db))(t)
		api := blobs.API{
			Repo:         repo,
			ShardEncoder: blobs.NewCAREncoder(),

			// Limit shards to 2 nodes each for testing
			MaxNodesPerIndex: 2,
		}
		spaceDID := stestutil.RandomDID(t)
		upload, source := testutil.CreateUpload(t, repo, spaceDID, spacesmodel.WithShardSize(1<<20))

		// Adding one node doesn't close the shard

		nodeCID1 := stestutil.RandomCID(t)
		n, _, err := repo.FindOrCreateRawNode(t.Context(), nodeCID1.(cidlink.Link).Cid, 1<<4, spaceDID, "some/path", source.ID(), 0)
		require.NoError(t, err)
		_, err = repo.FindOrCreateNodeUpload(t.Context(), upload.ID(), nodeCID1.(cidlink.Link).Cid, spaceDID)
		require.NoError(t, err)

		data := stestutil.RandomBytes(t, int(n.Size()))
		shardClosed := false
		err = api.AddNodeToUploadShards(t.Context(), upload.ID(), spaceDID, nodeCID1.(cidlink.Link).Cid, data, func(shard *model.Shard) error {
			shardClosed = true
			return nil
		})
		require.NoError(t, err)
		require.False(t, shardClosed)

		// Adding a second node doesn't close the shard

		nodeCID2 := stestutil.RandomCID(t)
		n, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID2.(cidlink.Link).Cid, 1<<4, spaceDID, "some/other/path", source.ID(), 0)
		require.NoError(t, err)
		_, err = repo.FindOrCreateNodeUpload(t.Context(), upload.ID(), nodeCID2.(cidlink.Link).Cid, spaceDID)
		require.NoError(t, err)
		data = stestutil.RandomBytes(t, int(n.Size()))
		shardClosed = false
		err = api.AddNodeToUploadShards(t.Context(), upload.ID(), spaceDID, nodeCID2.(cidlink.Link).Cid, data, func(shard *model.Shard) error {
			shardClosed = true
			return nil
		})
		require.NoError(t, err)
		require.False(t, shardClosed)

		// Adding a third node closes the shard

		nodeCID3 := stestutil.RandomCID(t)
		n, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID3.(cidlink.Link).Cid, 1<<4, spaceDID, "yet/other/path", source.ID(), 0)
		require.NoError(t, err)
		_, err = repo.FindOrCreateNodeUpload(t.Context(), upload.ID(), nodeCID3.(cidlink.Link).Cid, spaceDID)
		require.NoError(t, err)

		data = stestutil.RandomBytes(t, int(n.Size()))
		shardClosed = false
		err = api.AddNodeToUploadShards(t.Context(), upload.ID(), spaceDID, nodeCID3.(cidlink.Link).Cid, data, func(shard *model.Shard) error {
			shardClosed = true
			return nil
		})
		require.NoError(t, err)
		require.True(t, shardClosed)

		// We end up with one closed shard with the first two nodes, and one open
		// shard with the last node

		closedShards, err := repo.ShardsForUploadByState(t.Context(), upload.ID(), model.BlobStateClosed)
		require.NoError(t, err)
		require.Len(t, closedShards, 1)
		foundNodeCIDs := nodesInShard(t.Context(), t, db, closedShards[0].ID())
		require.ElementsMatch(t, []cid.Cid{nodeCID1.(cidlink.Link).Cid, nodeCID2.(cidlink.Link).Cid}, foundNodeCIDs)

		openShards, err := repo.ShardsForUploadByState(t.Context(), upload.ID(), model.BlobStateOpen)
		require.NoError(t, err)
		require.Len(t, openShards, 1)
		foundNodeCIDs = nodesInShard(t.Context(), t, db, openShards[0].ID())
		require.ElementsMatch(t, []cid.Cid{nodeCID3.(cidlink.Link).Cid}, foundNodeCIDs)
	})

	t.Run("with a node too big for a shard", func(t *testing.T) {
		db := testdb.CreateTestDB(t)
		repo := stestutil.Must(sqlrepo.New(db))(t)
		api := blobs.API{Repo: repo, ShardEncoder: blobs.NewCAREncoder()}
		spaceDID := stestutil.RandomDID(t)
		upload, source := testutil.CreateUpload(t, repo, spaceDID, spacesmodel.WithShardSize(128))

		nodeCID1 := stestutil.RandomCID(t)

		_, _, err := repo.FindOrCreateRawNode(t.Context(), nodeCID1.(cidlink.Link).Cid, 120, spaceDID, "some/path", source.ID(), 0)
		require.NoError(t, err)
		_, err = repo.FindOrCreateNodeUpload(t.Context(), upload.ID(), nodeCID1.(cidlink.Link).Cid, spaceDID)
		require.NoError(t, err)

		err = api.AddNodeToUploadShards(t.Context(), upload.ID(), spaceDID, nodeCID1.(cidlink.Link).Cid, nil, nil)
		require.ErrorContains(t, err, "too large to fit in new shard for upload")
	})
}

func TestAddNodesToUploadShards(t *testing.T) {
	t.Run("assigns unsharded nodes to shards", func(t *testing.T) {
		db := testdb.CreateTestDB(t)
		repo := stestutil.Must(sqlrepo.New(db))(t)
		api := blobs.API{Repo: repo, ShardEncoder: blobs.NewCAREncoder()}
		spaceDID := stestutil.RandomDID(t)
		upload, source := testutil.CreateUpload(t, repo, spaceDID, spacesmodel.WithShardSize(1<<20))

		// Create nodes and node_upload records (but don't assign to shards yet)
		nodeCID1 := stestutil.RandomCID(t).(cidlink.Link).Cid
		nodeCID2 := stestutil.RandomCID(t).(cidlink.Link).Cid
		nodeCID3 := stestutil.RandomCID(t).(cidlink.Link).Cid

		_, _, err := repo.FindOrCreateRawNode(t.Context(), nodeCID1, 100, spaceDID, "dir/file1", source.ID(), 0)
		require.NoError(t, err)
		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID2, 100, spaceDID, "dir/file2", source.ID(), 0)
		require.NoError(t, err)
		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID3, 100, spaceDID, "dir/file3", source.ID(), 0)
		require.NoError(t, err)

		// Create node_upload records
		_, err = repo.FindOrCreateNodeUpload(t.Context(), upload.ID(), nodeCID1, spaceDID)
		require.NoError(t, err)
		_, err = repo.FindOrCreateNodeUpload(t.Context(), upload.ID(), nodeCID2, spaceDID)
		require.NoError(t, err)
		_, err = repo.FindOrCreateNodeUpload(t.Context(), upload.ID(), nodeCID3, spaceDID)
		require.NoError(t, err)

		// Verify all three are unsharded
		unshardedCIDs, err := repo.NodesNotInShards(t.Context(), upload.ID(), spaceDID)
		require.NoError(t, err)
		require.Len(t, unshardedCIDs, 3)

		// Call AddNodesToUploadShards
		shardsClosed := 0
		err = api.AddNodesToUploadShards(t.Context(), upload.ID(), spaceDID, func(shard *model.Shard) error {
			shardsClosed++
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 0, shardsClosed, "no shards should have closed since they all fit")

		// Verify all nodes are now assigned to shards
		unshardedCIDs, err = repo.NodesNotInShards(t.Context(), upload.ID(), spaceDID)
		require.NoError(t, err)
		require.Empty(t, unshardedCIDs, "all nodes should be assigned to shards")

		// Verify we have one open shard with all three nodes
		openShards, err := repo.ShardsForUploadByState(t.Context(), upload.ID(), model.BlobStateOpen)
		require.NoError(t, err)
		require.Len(t, openShards, 1)

		foundNodeCIDs := nodesInShard(t.Context(), t, db, openShards[0].ID())
		require.ElementsMatch(t, []cid.Cid{nodeCID1, nodeCID2, nodeCID3}, foundNodeCIDs)
	})

	t.Run("does nothing when no unsharded nodes", func(t *testing.T) {
		db := testdb.CreateTestDB(t)
		repo := stestutil.Must(sqlrepo.New(db))(t)
		api := blobs.API{Repo: repo, ShardEncoder: blobs.NewCAREncoder()}
		spaceDID := stestutil.RandomDID(t)
		upload, _ := testutil.CreateUpload(t, repo, spaceDID, spacesmodel.WithShardSize(1<<20))

		// No nodes created at all
		shardsClosed := 0
		err := api.AddNodesToUploadShards(t.Context(), upload.ID(), spaceDID, func(shard *model.Shard) error {
			shardsClosed++
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 0, shardsClosed)

		// Verify no shards were created
		openShards, err := repo.ShardsForUploadByState(t.Context(), upload.ID(), model.BlobStateOpen)
		require.NoError(t, err)
		require.Empty(t, openShards)
	})

	t.Run("closes shards when they fill up", func(t *testing.T) {
		db := testdb.CreateTestDB(t)
		repo := stestutil.Must(sqlrepo.New(db))(t)
		nodeReader := stubNodeReader{}
		// Use Filepack encoder since it doesn't add headers, making digest calculation simpler for tests
		api := blobs.API{
			Repo:             repo,
			ShardEncoder:     blobs.NewFilepackEncoder(),
			OpenNodeReader:   nodeReader.OpenNodeReader,
			MaxNodesPerIndex: 2,
		}
		spaceDID := stestutil.RandomDID(t)
		upload, source := testutil.CreateUpload(t, repo, spaceDID, spacesmodel.WithShardSize(1<<20))

		// Create 5 nodes - with MaxNodesPerIndex=2, should create 3 shards
		// Use paths that match stubNodeReader's expected format: "BLOCK DATA: <path>"
		for i := 0; i < 5; i++ {
			path := fmt.Sprintf("dir/file%d", i)
			expectedData := fmt.Sprintf("BLOCK DATA: %s", path)
			nodeCID := stestutil.RandomCID(t).(cidlink.Link).Cid
			_, _, err := repo.FindOrCreateRawNode(t.Context(), nodeCID, uint64(len(expectedData)), spaceDID, path, source.ID(), 0)
			require.NoError(t, err)
			_, err = repo.FindOrCreateNodeUpload(t.Context(), upload.ID(), nodeCID, spaceDID)
			require.NoError(t, err)
		}

		// Call AddNodesToUploadShards
		shardsClosed := 0
		err := api.AddNodesToUploadShards(t.Context(), upload.ID(), spaceDID, func(shard *model.Shard) error {
			shardsClosed++
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 2, shardsClosed, "2 shards should have been closed")

		// Verify all nodes are assigned
		unshardedCIDs, err := repo.NodesNotInShards(t.Context(), upload.ID(), spaceDID)
		require.NoError(t, err)
		require.Empty(t, unshardedCIDs)

		// Verify we have 2 closed shards and 1 open shard
		closedShards, err := repo.ShardsForUploadByState(t.Context(), upload.ID(), model.BlobStateClosed)
		require.NoError(t, err)
		require.Len(t, closedShards, 2)

		openShards, err := repo.ShardsForUploadByState(t.Context(), upload.ID(), model.BlobStateOpen)
		require.NoError(t, err)
		require.Len(t, openShards, 1)
	})

	t.Run("only assigns nodes for the specified upload", func(t *testing.T) {
		db := testdb.CreateTestDB(t)
		repo := stestutil.Must(sqlrepo.New(db))(t)
		api := blobs.API{Repo: repo, ShardEncoder: blobs.NewCAREncoder()}
		spaceDID := stestutil.RandomDID(t)

		upload1, source1 := testutil.CreateUpload(t, repo, spaceDID, spacesmodel.WithShardSize(1<<20))
		upload2, source2 := testutil.CreateUpload(t, repo, spaceDID, spacesmodel.WithShardSize(1<<20))

		// Create nodes for each upload
		nodeCID1 := stestutil.RandomCID(t).(cidlink.Link).Cid
		nodeCID2 := stestutil.RandomCID(t).(cidlink.Link).Cid

		_, _, err := repo.FindOrCreateRawNode(t.Context(), nodeCID1, 100, spaceDID, "dir/file1", source1.ID(), 0)
		require.NoError(t, err)
		_, err = repo.FindOrCreateNodeUpload(t.Context(), upload1.ID(), nodeCID1, spaceDID)
		require.NoError(t, err)

		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID2, 100, spaceDID, "dir/file2", source2.ID(), 0)
		require.NoError(t, err)
		_, err = repo.FindOrCreateNodeUpload(t.Context(), upload2.ID(), nodeCID2, spaceDID)
		require.NoError(t, err)

		unshardedCIDs1, err := repo.NodesNotInShards(t.Context(), upload1.ID(), spaceDID)
		require.NoError(t, err)
		require.Len(t, unshardedCIDs1, 1)
		require.Equal(t, nodeCID1, unshardedCIDs1[0])

		unshardedCIDs2, err := repo.NodesNotInShards(t.Context(), upload2.ID(), spaceDID)
		require.NoError(t, err)
		require.Len(t, unshardedCIDs2, 1)
		require.Equal(t, nodeCID2, unshardedCIDs2[0])

		// Only process upload1
		err = api.AddNodesToUploadShards(t.Context(), upload1.ID(), spaceDID, nil)
		require.NoError(t, err)

		// nodeCID1 should be assigned, nodeCID2 should not
		unshardedCIDs1, err = repo.NodesNotInShards(t.Context(), upload1.ID(), spaceDID)
		require.NoError(t, err)
		require.Empty(t, unshardedCIDs1)

		unshardedCIDs2, err = repo.NodesNotInShards(t.Context(), upload2.ID(), spaceDID)
		require.NoError(t, err)
		require.Len(t, unshardedCIDs2, 1)
		require.Equal(t, nodeCID2, unshardedCIDs2[0])
	})
}

func TestAddShardsToUploadIndexes(t *testing.T) {
	t.Run("assigns unindexed shards to indexes", func(t *testing.T) {
		db := testdb.CreateTestDB(t)
		repo := stestutil.Must(sqlrepo.New(db))(t)
		api := blobs.API{Repo: repo, ShardEncoder: blobs.NewCAREncoder()}
		spaceDID := stestutil.RandomDID(t)
		upload, source := testutil.CreateUpload(t, repo, spaceDID, spacesmodel.WithShardSize(1<<20))

		// Create nodes and shards (closed)
		nodeCID1 := stestutil.RandomCID(t).(cidlink.Link).Cid
		nodeCID2 := stestutil.RandomCID(t).(cidlink.Link).Cid
		nodeCID3 := stestutil.RandomCID(t).(cidlink.Link).Cid

		_, _, err := repo.FindOrCreateRawNode(t.Context(), nodeCID1, 100, spaceDID, "dir/file1", source.ID(), 0)
		require.NoError(t, err)
		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID2, 100, spaceDID, "dir/file2", source.ID(), 0)
		require.NoError(t, err)
		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID3, 100, spaceDID, "dir/file3", source.ID(), 0)
		require.NoError(t, err)

		// Create node_upload records
		_, err = repo.FindOrCreateNodeUpload(t.Context(), upload.ID(), nodeCID1, spaceDID)
		require.NoError(t, err)
		_, err = repo.FindOrCreateNodeUpload(t.Context(), upload.ID(), nodeCID2, spaceDID)
		require.NoError(t, err)
		_, err = repo.FindOrCreateNodeUpload(t.Context(), upload.ID(), nodeCID3, spaceDID)
		require.NoError(t, err)

		// Create three closed shards
		shard1, err := repo.CreateShard(t.Context(), upload.ID(), 0, nil, nil)
		require.NoError(t, err)
		err = repo.AddNodeToShard(t.Context(), shard1.ID(), nodeCID1, spaceDID, upload.ID(), 0)
		require.NoError(t, err)
		digest1, err := multihash.Encode([]byte("shard1"), multihash.IDENTITY)
		require.NoError(t, err)
		err = shard1.Close(digest1, stestutil.RandomCID(t).(cidlink.Link).Cid)
		require.NoError(t, err)
		err = repo.UpdateShard(t.Context(), shard1)
		require.NoError(t, err)

		shard2, err := repo.CreateShard(t.Context(), upload.ID(), 0, nil, nil)
		require.NoError(t, err)
		err = repo.AddNodeToShard(t.Context(), shard2.ID(), nodeCID2, spaceDID, upload.ID(), 0)
		require.NoError(t, err)
		digest2, err := multihash.Encode([]byte("shard2"), multihash.IDENTITY)
		require.NoError(t, err)
		err = shard2.Close(digest2, stestutil.RandomCID(t).(cidlink.Link).Cid)
		require.NoError(t, err)
		err = repo.UpdateShard(t.Context(), shard2)
		require.NoError(t, err)

		shard3, err := repo.CreateShard(t.Context(), upload.ID(), 0, nil, nil)
		require.NoError(t, err)
		err = repo.AddNodeToShard(t.Context(), shard3.ID(), nodeCID3, spaceDID, upload.ID(), 0)
		require.NoError(t, err)
		digest3, err := multihash.Encode([]byte("shard3"), multihash.IDENTITY)
		require.NoError(t, err)
		err = shard3.Close(digest3, stestutil.RandomCID(t).(cidlink.Link).Cid)
		require.NoError(t, err)
		err = repo.UpdateShard(t.Context(), shard3)
		require.NoError(t, err)

		// Verify all three are unindexed
		unindexedShardIDs, err := repo.ShardsNotInIndexes(t.Context(), upload.ID())
		require.NoError(t, err)
		require.Len(t, unindexedShardIDs, 3)

		// Call AddShardsToUploadIndexes
		indexesClosed := 0
		err = api.AddShardsToUploadIndexes(t.Context(), upload.ID(), func(index *model.Index) error {
			indexesClosed++
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 0, indexesClosed, "no indexes should have closed since they all fit")

		// Verify all shards are now assigned to indexes
		unindexedShardIDs, err = repo.ShardsNotInIndexes(t.Context(), upload.ID())
		require.NoError(t, err)
		require.Empty(t, unindexedShardIDs, "all shards should be assigned to indexes")

		// Verify we have one open index
		openIndexes, err := repo.IndexesForUploadByState(t.Context(), upload.ID(), model.BlobStateOpen)
		require.NoError(t, err)
		require.Len(t, openIndexes, 1)
	})

	t.Run("does nothing when no unindexed shards", func(t *testing.T) {
		db := testdb.CreateTestDB(t)
		repo := stestutil.Must(sqlrepo.New(db))(t)
		api := blobs.API{Repo: repo, ShardEncoder: blobs.NewCAREncoder()}
		spaceDID := stestutil.RandomDID(t)
		upload, _ := testutil.CreateUpload(t, repo, spaceDID, spacesmodel.WithShardSize(1<<20))

		// No shards created at all
		indexesClosed := 0
		err := api.AddShardsToUploadIndexes(t.Context(), upload.ID(), func(index *model.Index) error {
			indexesClosed++
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 0, indexesClosed)

		// Verify no indexes were created
		openIndexes, err := repo.IndexesForUploadByState(t.Context(), upload.ID(), model.BlobStateOpen)
		require.NoError(t, err)
		require.Empty(t, openIndexes)
	})

	t.Run("closes indexes when they fill up", func(t *testing.T) {
		db := testdb.CreateTestDB(t)
		repo := stestutil.Must(sqlrepo.New(db))(t)
		// MaxNodesPerIndex=2 means each index can hold 2 slices total
		api := blobs.API{
			Repo:             repo,
			ShardEncoder:     blobs.NewCAREncoder(),
			MaxNodesPerIndex: 2,
		}
		spaceDID := stestutil.RandomDID(t)
		upload, source := testutil.CreateUpload(t, repo, spaceDID, spacesmodel.WithShardSize(1<<20))

		// Create 5 shards, each with 1 node (1 slice each)
		// With MaxNodesPerIndex=2, should create 3 indexes (2+2+1)
		for i := 0; i < 5; i++ {
			nodeCID := stestutil.RandomCID(t).(cidlink.Link).Cid
			_, _, err := repo.FindOrCreateRawNode(t.Context(), nodeCID, 100, spaceDID, fmt.Sprintf("dir/file%d", i), source.ID(), 0)
			require.NoError(t, err)
			_, err = repo.FindOrCreateNodeUpload(t.Context(), upload.ID(), nodeCID, spaceDID)
			require.NoError(t, err)

			shard, err := repo.CreateShard(t.Context(), upload.ID(), 0, nil, nil)
			require.NoError(t, err)
			err = repo.AddNodeToShard(t.Context(), shard.ID(), nodeCID, spaceDID, upload.ID(), 0)
			require.NoError(t, err)
			// Re-fetch shard to get updated slice_count after AddNodeToShard
			shard, err = repo.GetShardByID(t.Context(), shard.ID())
			require.NoError(t, err)
			digest, err := multihash.Encode([]byte(fmt.Sprintf("shard%d", i)), multihash.IDENTITY)
			require.NoError(t, err)
			err = shard.Close(digest, stestutil.RandomCID(t).(cidlink.Link).Cid)
			require.NoError(t, err)
			err = repo.UpdateShard(t.Context(), shard)
			require.NoError(t, err)
		}

		// Call AddShardsToUploadIndexes
		indexesClosed := 0
		err := api.AddShardsToUploadIndexes(t.Context(), upload.ID(), func(index *model.Index) error {
			indexesClosed++
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 2, indexesClosed, "2 indexes should have been closed")

		// Verify all shards are assigned
		unindexedShardIDs, err := repo.ShardsNotInIndexes(t.Context(), upload.ID())
		require.NoError(t, err)
		require.Empty(t, unindexedShardIDs)

		// Verify we have 2 closed indexes and 1 open index
		closedIndexes, err := repo.IndexesForUploadByState(t.Context(), upload.ID(), model.BlobStateClosed)
		require.NoError(t, err)
		require.Len(t, closedIndexes, 2)

		openIndexes, err := repo.IndexesForUploadByState(t.Context(), upload.ID(), model.BlobStateOpen)
		require.NoError(t, err)
		require.Len(t, openIndexes, 1)
	})

	t.Run("only assigns shards for the specified upload", func(t *testing.T) {
		db := testdb.CreateTestDB(t)
		repo := stestutil.Must(sqlrepo.New(db))(t)
		api := blobs.API{Repo: repo, ShardEncoder: blobs.NewCAREncoder()}
		spaceDID := stestutil.RandomDID(t)

		upload1, source1 := testutil.CreateUpload(t, repo, spaceDID, spacesmodel.WithShardSize(1<<20))
		upload2, source2 := testutil.CreateUpload(t, repo, spaceDID, spacesmodel.WithShardSize(1<<20))

		// Create a shard for each upload
		nodeCID1 := stestutil.RandomCID(t).(cidlink.Link).Cid
		nodeCID2 := stestutil.RandomCID(t).(cidlink.Link).Cid

		_, _, err := repo.FindOrCreateRawNode(t.Context(), nodeCID1, 100, spaceDID, "dir/file1", source1.ID(), 0)
		require.NoError(t, err)
		_, err = repo.FindOrCreateNodeUpload(t.Context(), upload1.ID(), nodeCID1, spaceDID)
		require.NoError(t, err)

		shard1, err := repo.CreateShard(t.Context(), upload1.ID(), 0, nil, nil)
		require.NoError(t, err)
		err = repo.AddNodeToShard(t.Context(), shard1.ID(), nodeCID1, spaceDID, upload1.ID(), 0)
		require.NoError(t, err)
		digest1, err := multihash.Encode([]byte("shard1"), multihash.IDENTITY)
		require.NoError(t, err)
		err = shard1.Close(digest1, stestutil.RandomCID(t).(cidlink.Link).Cid)
		require.NoError(t, err)
		err = repo.UpdateShard(t.Context(), shard1)
		require.NoError(t, err)

		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID2, 100, spaceDID, "dir/file2", source2.ID(), 0)
		require.NoError(t, err)
		_, err = repo.FindOrCreateNodeUpload(t.Context(), upload2.ID(), nodeCID2, spaceDID)
		require.NoError(t, err)

		shard2, err := repo.CreateShard(t.Context(), upload2.ID(), 0, nil, nil)
		require.NoError(t, err)
		err = repo.AddNodeToShard(t.Context(), shard2.ID(), nodeCID2, spaceDID, upload2.ID(), 0)
		require.NoError(t, err)
		digest2, err := multihash.Encode([]byte("shard2"), multihash.IDENTITY)
		require.NoError(t, err)
		err = shard2.Close(digest2, stestutil.RandomCID(t).(cidlink.Link).Cid)
		require.NoError(t, err)
		err = repo.UpdateShard(t.Context(), shard2)
		require.NoError(t, err)

		// Verify both uploads have unindexed shards
		unindexedShardIDs1, err := repo.ShardsNotInIndexes(t.Context(), upload1.ID())
		require.NoError(t, err)
		require.Len(t, unindexedShardIDs1, 1)
		require.Equal(t, shard1.ID(), unindexedShardIDs1[0])

		unindexedShardIDs2, err := repo.ShardsNotInIndexes(t.Context(), upload2.ID())
		require.NoError(t, err)
		require.Len(t, unindexedShardIDs2, 1)
		require.Equal(t, shard2.ID(), unindexedShardIDs2[0])

		// Only process upload1
		err = api.AddShardsToUploadIndexes(t.Context(), upload1.ID(), nil)
		require.NoError(t, err)

		// shard1 should be assigned, shard2 should not
		unindexedShardIDs1, err = repo.ShardsNotInIndexes(t.Context(), upload1.ID())
		require.NoError(t, err)
		require.Empty(t, unindexedShardIDs1)

		unindexedShardIDs2, err = repo.ShardsNotInIndexes(t.Context(), upload2.ID())
		require.NoError(t, err)
		require.Len(t, unindexedShardIDs2, 1)
		require.Equal(t, shard2.ID(), unindexedShardIDs2[0])
	})
}

// (Until the repo has a way to query for this itself...)
func nodesInShard(ctx context.Context, t *testing.T, db *sql.DB, shardID id.ShardID) []cid.Cid {
	rows, err := db.QueryContext(ctx, `SELECT node_cid FROM node_uploads WHERE shard_id = ?`, shardID)
	require.NoError(t, err)
	defer rows.Close()

	var foundNodeCIDs []cid.Cid
	for rows.Next() {
		var foundNodeCID cid.Cid
		err = rows.Scan(util.DbCID(&foundNodeCID))
		require.NoError(t, err)
		foundNodeCIDs = append(foundNodeCIDs, foundNodeCID)
	}
	return foundNodeCIDs
}

type stubNodeReader struct {
	// If set, always returns an error when trying to read these node.
	errorNodes []cid.Cid
}

func (s stubNodeReader) OpenNodeReader() (nodereader.NodeReader, error) {
	return s, nil
}

func (s stubNodeReader) Close() error {
	return nil
}

func (s stubNodeReader) GetData(ctx context.Context, node dagsmodel.Node) ([]byte, error) {
	if slices.Contains(s.errorNodes, node.CID()) {
		return nil, fmt.Errorf("stub error reading node %s: %w", node.CID(), fs.ErrInvalid)
	}

	rawNode := node.(*dagsmodel.RawNode)
	data := fmt.Appendf(nil, "BLOCK DATA: %s", rawNode.Path())
	if rawNode.Size() != uint64(len(data)) {
		// The size in FindOrCreateRawNode is a bit of a magic number, but at least
		// this can tell us early if we need to change it.
		panic(fmt.Errorf("size for node %s (%s) should be set to %d, not %d", rawNode.CID(), rawNode.Path(), len(data), rawNode.Size()))
	}
	return data, nil
}

func TestReaderForShard(t *testing.T) {
	t.Run("returns a CAR reader for the shard", func(t *testing.T) {
		db := testdb.CreateTestDB(t)
		repo := stestutil.Must(sqlrepo.New(db))(t)
		spaceDID, err := did.Parse("did:storacha:space:example")
		require.NoError(t, err)

		uploadID := id.New()

		nodeCID1 := stestutil.RandomCID(t).(cidlink.Link).Cid
		nodeCID2 := stestutil.RandomCID(t).(cidlink.Link).Cid
		nodeCID3 := stestutil.RandomCID(t).(cidlink.Link).Cid

		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID1, 21, spaceDID, "dir/file1", id.New(), 0)
		require.NoError(t, err)
		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID2, 21, spaceDID, "dir/file2", id.New(), 0)
		require.NoError(t, err)
		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID3, 26, spaceDID, "dir/dir2/file3", id.New(), 0)
		require.NoError(t, err)

		// Create node_upload records
		_, err = repo.FindOrCreateNodeUpload(t.Context(), uploadID, nodeCID1, spaceDID)
		require.NoError(t, err)
		_, err = repo.FindOrCreateNodeUpload(t.Context(), uploadID, nodeCID2, spaceDID)
		require.NoError(t, err)
		_, err = repo.FindOrCreateNodeUpload(t.Context(), uploadID, nodeCID3, spaceDID)
		require.NoError(t, err)

		shard, err := repo.CreateShard(t.Context(), uploadID, 0, nil, nil /* irrelevant */)
		require.NoError(t, err)

		err = repo.AddNodeToShard(t.Context(), shard.ID(), nodeCID1, spaceDID, uploadID, 0 /* irrelevant */)
		require.NoError(t, err)
		err = repo.AddNodeToShard(t.Context(), shard.ID(), nodeCID2, spaceDID, uploadID, 0 /* irrelevant */)
		require.NoError(t, err)
		err = repo.AddNodeToShard(t.Context(), shard.ID(), nodeCID3, spaceDID, uploadID, 0 /* irrelevant */)
		require.NoError(t, err)

		nodeReader := stubNodeReader{}
		api := blobs.API{
			Repo:           repo,
			OpenNodeReader: nodeReader.OpenNodeReader,
			ShardEncoder:   blobs.NewCAREncoder(),
		}

		carReader, err := api.ReaderForShard(t.Context(), shard.ID())
		require.NoError(t, err)

		// Read in the entire CAR, so we can create an [io.ReaderAt] for the
		// blockstore. In the future, the reader we create could be an [io.ReaderAt]
		// itself, as it technically knows enough information to jump around. However,
		// that's complex, and in our use case, we're going to end up reading the
		// whole thing anyway to store it in Storacha.
		carBytes, err := io.ReadAll(carReader)
		require.NoError(t, err)
		bufferedCarReader := bytes.NewReader(carBytes)

		// Try to read the CAR bytes as a CAR.
		bs, err := blockstore.NewReadOnly(bufferedCarReader, nil)
		require.NoError(t, err)

		cidsCh, err := bs.AllKeysChan(t.Context())
		require.NoError(t, err)
		var cids []cid.Cid
		for cid := range cidsCh {
			cids = append(cids, cid)
		}
		require.Equal(t, []cid.Cid{nodeCID1, nodeCID2, nodeCID3}, cids)

		var b blocks.Block

		b, err = bs.Get(t.Context(), nodeCID1)
		require.NoError(t, err)
		require.Equal(t, []byte("BLOCK DATA: dir/file1"), b.RawData())

		b, err = bs.Get(t.Context(), nodeCID2)
		require.NoError(t, err)
		require.Equal(t, []byte("BLOCK DATA: dir/file2"), b.RawData())

		b, err = bs.Get(t.Context(), nodeCID3)
		require.NoError(t, err)
		require.Equal(t, []byte("BLOCK DATA: dir/dir2/file3"), b.RawData())
	})

	t.Run("upon encountering an error reading a node", func(t *testing.T) {
		db := testdb.CreateTestDB(t)
		repo := stestutil.Must(sqlrepo.New(db))(t)
		spaceDID, err := did.Parse("did:storacha:space:example")
		require.NoError(t, err)

		uploadID := id.New()

		nodeCID1 := stestutil.RandomCID(t).(cidlink.Link).Cid
		nodeCID2 := stestutil.RandomCID(t).(cidlink.Link).Cid
		nodeCID3 := stestutil.RandomCID(t).(cidlink.Link).Cid
		nodeCID4 := stestutil.RandomCID(t).(cidlink.Link).Cid

		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID1, 21, spaceDID, "dir/file1", id.New(), 0)
		require.NoError(t, err)
		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID2, 21, spaceDID, "dir/file2", id.New(), 0)
		require.NoError(t, err)
		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID3, 21, spaceDID, "dir/file3", id.New(), 0)
		require.NoError(t, err)
		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID4, 21, spaceDID, "dir/file4", id.New(), 0)
		require.NoError(t, err)

		// Create node_upload records
		for _, nodeCID := range []cid.Cid{nodeCID1, nodeCID2, nodeCID3, nodeCID4} {
			_, err = repo.FindOrCreateNodeUpload(t.Context(), uploadID, nodeCID, spaceDID)
			require.NoError(t, err)
		}

		shard, err := repo.CreateShard(t.Context(), uploadID, 0, nil, nil /* irrelevant */)
		require.NoError(t, err)

		for _, nodeCID := range []cid.Cid{nodeCID1, nodeCID2, nodeCID3, nodeCID4} {
			err = repo.AddNodeToShard(t.Context(), shard.ID(), nodeCID, spaceDID, uploadID, 0 /* irrelevant */)
			require.NoError(t, err)
		}

		nodeReader := stubNodeReader{
			errorNodes: []cid.Cid{nodeCID2, nodeCID4},
		}
		api := blobs.API{
			Repo:           repo,
			OpenNodeReader: nodeReader.OpenNodeReader,
			ShardEncoder:   blobs.NewCAREncoder(),
		}

		carReader, err := api.ReaderForShard(t.Context(), shard.ID())
		require.NoError(t, err)

		_, err = io.ReadAll(carReader)
		var errBadNodes types.BadNodesError
		require.ErrorAs(t, err, &errBadNodes)
		require.Len(t, errBadNodes.Errs(), 2)
		require.Equal(t, nodeCID2, errBadNodes.Errs()[0].CID(), "the first error should be for the first bad node encountered")
		require.Equal(t, nodeCID4, errBadNodes.Errs()[1].CID(), "the second error should be for the second bad node encountered")
	})
}

func TestAddShardToUploadIndexesAndCloseUploadIndexes(t *testing.T) {
	t.Run("adds shards to indexes", func(t *testing.T) {
		db := testdb.CreateTestDB(t)
		repo := stestutil.Must(sqlrepo.New(db))(t)
		api := blobs.API{Repo: repo, ShardEncoder: blobs.NewCAREncoder(), MaxNodesPerIndex: 4}
		spaceDID := stestutil.RandomDID(t)
		upload, source := testutil.CreateUpload(t, repo, spaceDID, spacesmodel.WithShardSize(2500))

		numClosedShards := 0
		var indexClosed bool

		handleCloseIndex := func(index *model.Index) error {
			indexClosed = true
			return nil
		}

		handleClosedShard := func(shard *model.Shard) error {
			numClosedShards++
			err := api.AddShardToUploadIndexes(t.Context(), upload.ID(), shard.ID(), handleCloseIndex)
			require.NoError(t, err)
			return nil
		}

		// with no indexes, creates a new index and adds the shard to it

		openIndexes, err := repo.IndexesForUploadByState(t.Context(), upload.ID(), model.BlobStateOpen)
		require.NoError(t, err)
		require.Len(t, openIndexes, 0)

		indexClosed = false
		testutil.AddNodeToUploadShards(t, repo, api, upload.ID(), source.ID(), spaceDID, handleClosedShard, 1000)
		require.Equal(t, 0, numClosedShards)
		testutil.AddNodeToUploadShards(t, repo, api, upload.ID(), source.ID(), spaceDID, handleClosedShard, 1000)
		require.Equal(t, 0, numClosedShards)
		testutil.AddNodeToUploadShards(t, repo, api, upload.ID(), source.ID(), spaceDID, handleClosedShard, 1000)
		require.Equal(t, 1, numClosedShards)
		require.False(t, indexClosed)

		openIndexes, err = repo.IndexesForUploadByState(t.Context(), upload.ID(), model.BlobStateOpen)
		require.NoError(t, err)
		require.Len(t, openIndexes, 1)
		firstIndex := openIndexes[0]

		// The first shard has closed, adding its two slices to the index. The third
		// slice is in an open shard.
		require.Equal(t, 2, openIndexes[0].SliceCount())

		// with an open index with room, adds the shard to the index

		indexClosed = false
		testutil.AddNodeToUploadShards(t, repo, api, upload.ID(), source.ID(), spaceDID, handleClosedShard, 1000)
		require.Equal(t, 1, numClosedShards)
		testutil.AddNodeToUploadShards(t, repo, api, upload.ID(), source.ID(), spaceDID, handleClosedShard, 1000)
		require.Equal(t, 2, numClosedShards)
		require.False(t, indexClosed)

		openIndexes, err = repo.IndexesForUploadByState(t.Context(), upload.ID(), model.BlobStateOpen)
		require.NoError(t, err)
		require.Len(t, openIndexes, 1)
		require.Equal(t, firstIndex.ID(), openIndexes[0].ID())

		// The second shard has closed, adding its two slices to the index. The
		// fifth slice is in an open shard.
		require.Equal(t, 4, openIndexes[0].SliceCount())

		// with an open index without room, closes the index and creates another

		indexClosed = false
		testutil.AddNodeToUploadShards(t, repo, api, upload.ID(), source.ID(), spaceDID, handleClosedShard, 1000)
		require.Equal(t, 2, numClosedShards)
		testutil.AddNodeToUploadShards(t, repo, api, upload.ID(), source.ID(), spaceDID, handleClosedShard, 1000)
		require.Equal(t, 3, numClosedShards)
		require.True(t, indexClosed)

		closedIndexes, err := repo.IndexesForUploadByState(t.Context(), upload.ID(), model.BlobStateClosed)
		require.NoError(t, err)
		require.Len(t, closedIndexes, 1)
		require.Equal(t, firstIndex.ID(), closedIndexes[0].ID())

		openIndexes, err = repo.IndexesForUploadByState(t.Context(), upload.ID(), model.BlobStateOpen)
		require.NoError(t, err)
		require.Len(t, openIndexes, 1)
		require.NotEqual(t, firstIndex.ID(), openIndexes[0].ID())
		secondIndex := openIndexes[0]

		// The third shard has closed. Its two slices don't fit in the open index,
		// which has 4 slices already, because the max index size is set to 5. Thus,
		// a new index is created, and the third shard's slices are added to it. The
		// seventh slice is in an open shard.
		require.Equal(t, 4, closedIndexes[0].SliceCount())
		require.Equal(t, 2, openIndexes[0].SliceCount())

		// when a shard is manually closed, adds the shard to the indexes

		indexClosed = false
		api.CloseUploadShards(t.Context(), upload.ID(), handleClosedShard)
		require.False(t, indexClosed)

		closedIndexes, err = repo.IndexesForUploadByState(t.Context(), upload.ID(), model.BlobStateClosed)
		require.NoError(t, err)
		require.Len(t, closedIndexes, 1)
		require.Equal(t, firstIndex.ID(), closedIndexes[0].ID())

		openIndexes, err = repo.IndexesForUploadByState(t.Context(), upload.ID(), model.BlobStateOpen)
		require.NoError(t, err)
		require.Len(t, openIndexes, 1)
		require.Equal(t, secondIndex.ID(), openIndexes[0].ID())

		// The fourth shard has closed. Its one slice fits in the open index,
		// which remains open.
		require.Equal(t, 4, closedIndexes[0].SliceCount())
		require.Equal(t, 3, openIndexes[0].SliceCount())

		// when an index is manually closed, closes, but adds nothing new

		indexClosed = false
		api.CloseUploadIndexes(t.Context(), upload.ID(), handleCloseIndex)
		require.True(t, indexClosed)

		closedIndexes, err = repo.IndexesForUploadByState(t.Context(), upload.ID(), model.BlobStateClosed)
		require.NoError(t, err)
		require.Len(t, closedIndexes, 2)
		require.Equal(t, firstIndex.ID(), closedIndexes[0].ID())
		require.Equal(t, secondIndex.ID(), closedIndexes[1].ID())

		openIndexes, err = repo.IndexesForUploadByState(t.Context(), upload.ID(), model.BlobStateOpen)
		require.NoError(t, err)
		require.Len(t, openIndexes, 0)

		// The remaining index has closed. The slice counts remain the same.
		require.Equal(t, 4, closedIndexes[0].SliceCount())
		require.Equal(t, 3, closedIndexes[1].SliceCount())
	})

	t.Run("with a node too big for a shard", func(t *testing.T) {
		db := testdb.CreateTestDB(t)
		repo := stestutil.Must(sqlrepo.New(db))(t)
		api := blobs.API{Repo: repo, ShardEncoder: blobs.NewCAREncoder(), MaxNodesPerIndex: 100}
		spaceDID := stestutil.RandomDID(t)
		upload, source := testutil.CreateUpload(t, repo, spaceDID, spacesmodel.WithShardSize(3000))

		handleClosedShard := func(shard *model.Shard) error {
			fmt.Printf("Shard size: %d\n", shard.Size())
			require.Fail(t, "Shard closed too early; the test is written incorrectly. Check the node sizes in the test.")
			return nil
		}

		testutil.AddNodeToUploadShards(t, repo, api, upload.ID(), source.ID(), spaceDID, handleClosedShard, 500)
		testutil.AddNodeToUploadShards(t, repo, api, upload.ID(), source.ID(), spaceDID, handleClosedShard, 500)
		testutil.AddNodeToUploadShards(t, repo, api, upload.ID(), source.ID(), spaceDID, handleClosedShard, 500)

		err := api.CloseUploadShards(t.Context(), upload.ID(), func(shard *model.Shard) error {
			// Normally, shards can't get bigger than the `MaxShardSize`, so we force
			// it here to test the behavior if something does go wrong.
			api.MaxNodesPerIndex = 2

			return api.AddShardToUploadIndexes(t.Context(), upload.ID(), shard.ID(), nil)
		})
		require.ErrorContains(t, err, "too large to fit in new index for upload")
	})
}

func TestComputedShardCIDs(t *testing.T) {
	db := testdb.CreateTestDB(t)
	repo := stestutil.Must(sqlrepo.New(db))(t)
	// we're going to use fileback here because it doesn't add any bytes, making expected CIDs easier to calculate
	api := blobs.API{Repo: repo, ShardEncoder: blobs.NewFilepackEncoder()}
	spaceDID := stestutil.RandomDID(t)
	upload, source := testutil.CreateUpload(t, repo, spaceDID, spacesmodel.WithShardSize(1<<16))

	totalData := stestutil.RandomBytes(t, 1<<15) // 32 KiB
	expectedDigest, err := multihash.Sum(totalData, multihash.SHA2_256, -1)
	require.NoError(t, err)
	calc := &commp.Calc{}
	calc.Write(totalData)
	commP, _, err := calc.Digest()
	require.NoError(t, err)
	expectedPieceCID, err := commcid.DataCommitmentToPieceCidv2(commP, uint64(len(totalData)))
	require.NoError(t, err)

	// no shards yet
	openShards, err := repo.ShardsForUploadByState(t.Context(), upload.ID(), model.BlobStateOpen)
	require.NoError(t, err)
	require.Len(t, openShards, 0)

	for offset := 0; offset < len(totalData); offset += 1 << 12 {
		data := totalData[offset : offset+(1<<12)]
		nodeCID := cid.NewCidV1(cid.Raw, stestutil.Must(multihash.Sum(data, multihash.SHA2_256, -1))(t))
		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID, 1<<12, spaceDID, fmt.Sprintf("some/path/%d", offset), source.ID(), 0)
		require.NoError(t, err)
		_, err = repo.FindOrCreateNodeUpload(t.Context(), upload.ID(), nodeCID, spaceDID)
		require.NoError(t, err)
		err = api.AddNodeToUploadShards(t.Context(), upload.ID(), spaceDID, nodeCID, data, nil)
		require.NoError(t, err)
	}

	var shardClosed bool
	err = api.CloseUploadShards(t.Context(), upload.ID(), func(shard *model.Shard) error {
		shardClosed = true
		return nil
	})
	require.NoError(t, err)
	require.True(t, shardClosed)

	closedShards, err := repo.ShardsForUploadByState(t.Context(), upload.ID(), model.BlobStateClosed)
	require.NoError(t, err)
	require.Len(t, closedShards, 1)

	// check the shard's digest and piece CID
	shard := closedShards[0]
	require.Equal(t, expectedDigest, shard.Digest())
	require.Equal(t, expectedPieceCID, shard.PieceCID())
}

func TestReaderForIndex(t *testing.T) {
	t.Run("returns a reader for a single-shard index", func(t *testing.T) {
		repo := stestutil.Must(sqlrepo.New(testdb.CreateTestDB(t)))(t)
		api := blobs.API{
			Repo:         repo,
			ShardEncoder: blobs.NewCAREncoder(),
		}

		spaceDID := stestutil.RandomDID(t)
		upload, source := testutil.CreateUpload(t, repo, spaceDID, spacesmodel.WithShardSize(1<<20))

		// Set a root CID on the upload
		rootLink := stestutil.RandomCID(t)
		err := upload.SetRootCID(rootLink.(cidlink.Link).Cid)
		require.NoError(t, err)
		err = repo.UpdateUpload(t.Context(), upload)
		require.NoError(t, err)

		// Create nodes and add them to shards
		node1, _, err := repo.FindOrCreateRawNode(t.Context(), stestutil.RandomCID(t).(cidlink.Link).Cid, 100, spaceDID, "dir/file1", source.ID(), 0)
		require.NoError(t, err)
		node2, _, err := repo.FindOrCreateRawNode(t.Context(), stestutil.RandomCID(t).(cidlink.Link).Cid, 200, spaceDID, "dir/file2", source.ID(), 0)
		require.NoError(t, err)

		// Create node_upload records
		_, err = repo.FindOrCreateNodeUpload(t.Context(), upload.ID(), node1.CID(), spaceDID)
		require.NoError(t, err)
		_, err = repo.FindOrCreateNodeUpload(t.Context(), upload.ID(), node2.CID(), spaceDID)
		require.NoError(t, err)

		// Create a shard, call the header length 10
		shard, err := repo.CreateShard(t.Context(), upload.ID(), 10, nil, nil)
		require.NoError(t, err)
		err = repo.AddNodeToShard(t.Context(), shard.ID(), node1.CID(), spaceDID, upload.ID(), 1)
		require.NoError(t, err)
		err = repo.AddNodeToShard(t.Context(), shard.ID(), node2.CID(), spaceDID, upload.ID(), 2)
		require.NoError(t, err)

		// Close the shard
		digest, err := multihash.Encode([]byte("shard1 digest"), multihash.IDENTITY)
		require.NoError(t, err)
		err = shard.Close(digest, stestutil.RandomCID(t).(cidlink.Link).Cid)
		require.NoError(t, err)
		err = repo.UpdateShard(t.Context(), shard)
		require.NoError(t, err)

		// Create an index and add the shard to it
		index, err := repo.CreateIndex(t.Context(), upload.ID())
		require.NoError(t, err)
		err = repo.AddShardToIndex(t.Context(), index.ID(), shard.ID())
		require.NoError(t, err)

		// Get the reader for the index
		indexReader, err := api.ReaderForIndex(t.Context(), index.ID())
		require.NoError(t, err)
		require.NotNil(t, indexReader)

		// Read and parse the index
		indexView, err := blobindex.Extract(indexReader)
		require.NoError(t, err)

		// Verify the index content
		require.Equal(t, rootLink, indexView.Content())
		require.Equal(t, 1, indexView.Shards().Size(), "index should have one shard")

		shardSlices := indexView.Shards().Get(digest)
		require.NotNil(t, shardSlices)
		require.Equal(t, 2, shardSlices.Size(), "shard should have two slices")
		require.Equal(t, blobindex.Position{Offset: 10 + 1, Length: 100}, shardSlices.Get(node1.CID().Hash()))
		require.Equal(t, blobindex.Position{Offset: 10 + 1 + 100 + 2, Length: 200}, shardSlices.Get(node2.CID().Hash()))
	})

	t.Run("returns a reader for a multi-shard index", func(t *testing.T) {
		repo := stestutil.Must(sqlrepo.New(testdb.CreateTestDB(t)))(t)
		api := blobs.API{
			Repo:         repo,
			ShardEncoder: blobs.NewCAREncoder(),
		}

		spaceDID := stestutil.RandomDID(t)
		upload, source := testutil.CreateUpload(t, repo, spaceDID, spacesmodel.WithShardSize(1<<20))

		// Set a root CID on the upload
		rootLink := stestutil.RandomCID(t)
		err := upload.SetRootCID(rootLink.(cidlink.Link).Cid)
		require.NoError(t, err)
		err = repo.UpdateUpload(t.Context(), upload)
		require.NoError(t, err)

		// Create nodes
		node1, _, err := repo.FindOrCreateRawNode(t.Context(), stestutil.RandomCID(t).(cidlink.Link).Cid, 100, spaceDID, "dir/file1", source.ID(), 0)
		require.NoError(t, err)
		node2, _, err := repo.FindOrCreateRawNode(t.Context(), stestutil.RandomCID(t).(cidlink.Link).Cid, 200, spaceDID, "dir/file2", source.ID(), 0)
		require.NoError(t, err)
		node3, _, err := repo.FindOrCreateRawNode(t.Context(), stestutil.RandomCID(t).(cidlink.Link).Cid, 300, spaceDID, "dir/file3", source.ID(), 0)
		require.NoError(t, err)

		// Create node_upload records
		_, err = repo.FindOrCreateNodeUpload(t.Context(), upload.ID(), node1.CID(), spaceDID)
		require.NoError(t, err)
		_, err = repo.FindOrCreateNodeUpload(t.Context(), upload.ID(), node2.CID(), spaceDID)
		require.NoError(t, err)
		_, err = repo.FindOrCreateNodeUpload(t.Context(), upload.ID(), node3.CID(), spaceDID)
		require.NoError(t, err)

		// Create first shard with two nodes
		shard1, err := repo.CreateShard(t.Context(), upload.ID(), 10, nil, nil)
		require.NoError(t, err)
		err = repo.AddNodeToShard(t.Context(), shard1.ID(), node1.CID(), spaceDID, upload.ID(), 1)
		require.NoError(t, err)
		err = repo.AddNodeToShard(t.Context(), shard1.ID(), node2.CID(), spaceDID, upload.ID(), 2)
		require.NoError(t, err)
		digest1, err := multihash.Encode([]byte("shard1 digest"), multihash.IDENTITY)
		require.NoError(t, err)
		err = shard1.Close(digest1, stestutil.RandomCID(t).(cidlink.Link).Cid)
		require.NoError(t, err)
		err = repo.UpdateShard(t.Context(), shard1)
		require.NoError(t, err)

		// Create second shard with one node
		shard2, err := repo.CreateShard(t.Context(), upload.ID(), 20, nil, nil)
		require.NoError(t, err)
		err = repo.AddNodeToShard(t.Context(), shard2.ID(), node3.CID(), spaceDID, upload.ID(), 3)
		require.NoError(t, err)
		digest2, err := multihash.Encode([]byte("shard2 digest"), multihash.IDENTITY)
		require.NoError(t, err)
		err = shard2.Close(digest2, stestutil.RandomCID(t).(cidlink.Link).Cid)
		require.NoError(t, err)
		err = repo.UpdateShard(t.Context(), shard2)
		require.NoError(t, err)

		// Create an index and add both shards to it
		index, err := repo.CreateIndex(t.Context(), upload.ID())
		require.NoError(t, err)
		err = repo.AddShardToIndex(t.Context(), index.ID(), shard1.ID())
		require.NoError(t, err)
		err = repo.AddShardToIndex(t.Context(), index.ID(), shard2.ID())
		require.NoError(t, err)

		// Get the reader for the index
		indexReader, err := api.ReaderForIndex(t.Context(), index.ID())
		require.NoError(t, err)
		require.NotNil(t, indexReader)

		// Read and parse the index
		indexView, err := blobindex.Extract(indexReader)
		require.NoError(t, err)

		// Verify the index content
		require.Equal(t, rootLink, indexView.Content())
		require.Equal(t, 2, indexView.Shards().Size(), "index should have two shards")

		// Verify first shard
		shard1Slices := indexView.Shards().Get(digest1)
		require.NotNil(t, shard1Slices)
		require.Equal(t, 2, shard1Slices.Size(), "first shard should have two slices")
		require.Equal(t, blobindex.Position{Offset: 10 + 1, Length: 100}, shard1Slices.Get(node1.CID().Hash()))
		require.Equal(t, blobindex.Position{Offset: 10 + 1 + 100 + 2, Length: 200}, shard1Slices.Get(node2.CID().Hash()))

		// Verify second shard
		shard2Slices := indexView.Shards().Get(digest2)
		require.NotNil(t, shard2Slices)
		require.Equal(t, 1, shard2Slices.Size(), "second shard should have one slice")
		require.Equal(t, blobindex.Position{Offset: 20 + 3, Length: 300}, shard2Slices.Get(node3.CID().Hash()))
	})

	t.Run("returns an error when index does not exist", func(t *testing.T) {
		repo := stestutil.Must(sqlrepo.New(testdb.CreateTestDB(t)))(t)
		api := blobs.API{
			Repo:         repo,
			ShardEncoder: blobs.NewCAREncoder(),
		}

		nonExistentIndexID := id.New()
		indexReader, err := api.ReaderForIndex(t.Context(), nonExistentIndexID)
		require.Error(t, err)
		require.ErrorContains(t, err, "getting index")
		require.Nil(t, indexReader)
	})

	t.Run("returns an error when upload has no root CID", func(t *testing.T) {
		repo := stestutil.Must(sqlrepo.New(testdb.CreateTestDB(t)))(t)
		api := blobs.API{
			Repo:         repo,
			ShardEncoder: blobs.NewCAREncoder(),
		}

		spaceDID := stestutil.RandomDID(t)
		upload, _ := testutil.CreateUpload(t, repo, spaceDID)

		// Create an index for this upload (without setting a root CID)
		index, err := repo.CreateIndex(t.Context(), upload.ID())
		require.NoError(t, err)

		// Try to get a reader for the index
		indexReader, err := api.ReaderForIndex(t.Context(), index.ID())
		require.Error(t, err)
		require.ErrorContains(t, err, "no root CID set yet for upload")
		require.Nil(t, indexReader)
	})

	t.Run("returns an error when shard has no digest", func(t *testing.T) {
		repo := stestutil.Must(sqlrepo.New(testdb.CreateTestDB(t)))(t)
		api := blobs.API{
			Repo:         repo,
			ShardEncoder: blobs.NewCAREncoder(),
		}

		spaceDID := stestutil.RandomDID(t)
		upload, source := testutil.CreateUpload(t, repo, spaceDID)

		// Set a root CID on the upload
		rootLink := stestutil.RandomCID(t)
		err := upload.SetRootCID(rootLink.(cidlink.Link).Cid)
		require.NoError(t, err)
		err = repo.UpdateUpload(t.Context(), upload)
		require.NoError(t, err)

		// Create a shard without closing it (no digest)
		shard, err := repo.CreateShard(t.Context(), upload.ID(), 10, nil, nil)
		require.NoError(t, err)

		// Add a node to the shard
		node, _, err := repo.FindOrCreateRawNode(t.Context(), stestutil.RandomCID(t).(cidlink.Link).Cid, 100, spaceDID, "dir/file1", source.ID(), 0)
		require.NoError(t, err)
		_, err = repo.FindOrCreateNodeUpload(t.Context(), upload.ID(), node.CID(), spaceDID)
		require.NoError(t, err)
		err = repo.AddNodeToShard(t.Context(), shard.ID(), node.CID(), spaceDID, upload.ID(), 1)
		require.NoError(t, err)

		// Create an index and add the shard to it
		index, err := repo.CreateIndex(t.Context(), upload.ID())
		require.NoError(t, err)
		err = repo.AddShardToIndex(t.Context(), index.ID(), shard.ID())
		require.NoError(t, err)

		// Try to get a reader for the index
		indexReader, err := api.ReaderForIndex(t.Context(), index.ID())
		require.Error(t, err)
		require.ErrorContains(t, err, "has no digest set")
		require.Nil(t, indexReader)
	})

	t.Run("returns an empty index when index has no shards", func(t *testing.T) {
		repo := stestutil.Must(sqlrepo.New(testdb.CreateTestDB(t)))(t)
		api := blobs.API{
			Repo:         repo,
			ShardEncoder: blobs.NewCAREncoder(),
		}

		spaceDID := stestutil.RandomDID(t)
		upload, _ := testutil.CreateUpload(t, repo, spaceDID)

		// Set a root CID on the upload
		rootLink := stestutil.RandomCID(t)
		err := upload.SetRootCID(rootLink.(cidlink.Link).Cid)
		require.NoError(t, err)
		err = repo.UpdateUpload(t.Context(), upload)
		require.NoError(t, err)

		// Create an index without adding any shards
		index, err := repo.CreateIndex(t.Context(), upload.ID())
		require.NoError(t, err)

		// Get the reader for the index
		indexReader, err := api.ReaderForIndex(t.Context(), index.ID())
		require.NoError(t, err)
		require.NotNil(t, indexReader)

		// Read and parse the index
		indexView, err := blobindex.Extract(indexReader)
		require.NoError(t, err)

		// Verify the index is empty but has the correct root
		require.Equal(t, rootLink, indexView.Content())
		require.Equal(t, 0, indexView.Shards().Size(), "index should have no shards")
	})
}
