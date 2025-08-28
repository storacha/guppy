package shards_test

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"testing"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2/blockstore"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multihash"
	"github.com/storacha/go-libstoracha/blobindex"
	configurationsmodel "github.com/storacha/guppy/pkg/preparation/configurations/model"
	dagsmodel "github.com/storacha/guppy/pkg/preparation/dags/model"
	"github.com/storacha/guppy/pkg/preparation/shards"
	"github.com/storacha/guppy/pkg/preparation/shards/model"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo/util"
	"github.com/storacha/guppy/pkg/preparation/testutil"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	"github.com/stretchr/testify/require"
)

func TestAddNodeToUploadShardsAndCloseUploadShards(t *testing.T) {
	db := testutil.CreateTestDB(t)
	repo := sqlrepo.New(db)
	api := shards.API{Repo: repo}

	configuration, err := repo.CreateConfiguration(t.Context(), "Test Config", configurationsmodel.WithShardSize(1<<16))
	require.NoError(t, err)
	source, err := repo.CreateSource(t.Context(), "Test Source", ".")
	require.NoError(t, err)
	uploads, err := repo.CreateUploads(t.Context(), configuration.ID(), []id.SourceID{source.ID()})
	require.NoError(t, err)
	require.Len(t, uploads, 1)
	upload := uploads[0]
	nodeCid1 := testutil.RandomCID(t)

	// with no shards, creates a new shard and adds the node to it

	openShards, err := repo.ShardsForUploadByStatus(t.Context(), upload.ID(), model.ShardStateOpen)
	require.NoError(t, err)
	require.Len(t, openShards, 0)
	_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCid1, 1<<14, "some/path", source.ID(), 0)
	require.NoError(t, err)

	shardClosed, err := api.AddNodeToUploadShards(t.Context(), upload.ID(), nodeCid1)
	require.NoError(t, err)

	require.False(t, shardClosed)
	openShards, err = repo.ShardsForUploadByStatus(t.Context(), upload.ID(), model.ShardStateOpen)
	require.NoError(t, err)
	require.Len(t, openShards, 1)
	firstShard := openShards[0]

	// The CAR header is 18 bytes
	// The varint length prefix for a ~1<<14 block is 3 bytes
	// The CIDv1 is 36 bytes
	require.Equal(t, uint64(18+3+36+(1<<14)), firstShard.Size())

	foundNodeCids := nodesInShard(t.Context(), t, db, firstShard.ID())
	require.ElementsMatch(t, []cid.Cid{nodeCid1}, foundNodeCids)

	// with an open shard with room, adds the node to the shard

	nodeCid2 := testutil.RandomCID(t)
	_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCid2, 1<<14, "some/other/path", source.ID(), 0)
	require.NoError(t, err)

	shardClosed, err = api.AddNodeToUploadShards(t.Context(), upload.ID(), nodeCid2)
	require.NoError(t, err)

	require.False(t, shardClosed)
	openShards, err = repo.ShardsForUploadByStatus(t.Context(), upload.ID(), model.ShardStateOpen)
	require.NoError(t, err)
	require.Len(t, openShards, 1)
	require.Equal(t, firstShard.ID(), openShards[0].ID())
	firstShard = openShards[0] // with fresh data from DB

	require.Equal(t, uint64(18+3+36+(1<<14)+3+36+(1<<14)), firstShard.Size())

	foundNodeCids = nodesInShard(t.Context(), t, db, firstShard.ID())
	require.ElementsMatch(t, []cid.Cid{nodeCid1, nodeCid2}, foundNodeCids)

	// with an open shard without room, closes the shard and creates another

	nodeCid3 := testutil.RandomCID(t)
	_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCid3, 1<<15, "yet/other/path", source.ID(), 0)
	require.NoError(t, err)

	shardClosed, err = api.AddNodeToUploadShards(t.Context(), upload.ID(), nodeCid3)
	require.NoError(t, err)

	require.True(t, shardClosed)
	closedShards, err := repo.ShardsForUploadByStatus(t.Context(), upload.ID(), model.ShardStateClosed)
	require.NoError(t, err)
	require.Len(t, closedShards, 1)
	require.Equal(t, firstShard.ID(), closedShards[0].ID())

	foundNodeCids = nodesInShard(t.Context(), t, db, firstShard.ID())
	require.ElementsMatch(t, []cid.Cid{nodeCid1, nodeCid2}, foundNodeCids)

	openShards, err = repo.ShardsForUploadByStatus(t.Context(), upload.ID(), model.ShardStateOpen)
	require.NoError(t, err)
	require.Len(t, openShards, 1)
	secondShard := openShards[0]
	require.NotEqual(t, firstShard.ID(), secondShard.ID())

	require.Equal(t, uint64(18+3+36+(1<<15)), secondShard.Size())
	foundNodeCids = nodesInShard(t.Context(), t, db, secondShard.ID())
	require.ElementsMatch(t, []cid.Cid{nodeCid3}, foundNodeCids)

	// finally, close the last shard with CloseUploadShards()

	shardClosed, err = api.CloseUploadShards(t.Context(), upload.ID())
	require.NoError(t, err)
	require.True(t, shardClosed)

	closedShards, err = repo.ShardsForUploadByStatus(t.Context(), upload.ID(), model.ShardStateClosed)
	require.NoError(t, err)
	require.Len(t, closedShards, 2)
	require.Equal(t, firstShard.ID(), closedShards[0].ID())

	closedShardIDs := make([]id.ShardID, 0, len(closedShards))
	for _, closedShard := range closedShards {
		closedShardIDs = append(closedShardIDs, closedShard.ID())
	}
	require.ElementsMatch(t, closedShardIDs, []id.ShardID{firstShard.ID(), secondShard.ID()})

	openShards, err = repo.ShardsForUploadByStatus(t.Context(), upload.ID(), model.ShardStateOpen)
	require.NoError(t, err)
	require.Len(t, openShards, 0)
}

// (Until the repo has a way to query for this itself...)
func nodesInShard(ctx context.Context, t *testing.T, db *sql.DB, shardID id.ShardID) []cid.Cid {
	rows, err := db.QueryContext(ctx, `SELECT node_cid FROM nodes_in_shards WHERE shard_id = ?`, shardID)
	require.NoError(t, err)
	defer rows.Close()

	var foundNodeCids []cid.Cid
	for rows.Next() {
		var foundNodeCid cid.Cid
		err = rows.Scan(util.DbCid(&foundNodeCid))
		require.NoError(t, err)
		foundNodeCids = append(foundNodeCids, foundNodeCid)
	}
	return foundNodeCids
}

type stubNodeReader struct{}

func (s stubNodeReader) GetData(ctx context.Context, node dagsmodel.Node) ([]byte, error) {
	rawNode := node.(*dagsmodel.RawNode)
	data := fmt.Appendf(nil, "BLOCK DATA: %s", rawNode.Path())
	if rawNode.Size() != uint64(len(data)) {
		// The size in FindOrCreateRawNode is a bit of a magic number, but at least
		// this can tell us early if we need to change it.
		panic(fmt.Errorf("size for node %s (%s) should be set to %d, not %d", rawNode.CID(), rawNode.Path(), len(data), rawNode.Size()))
	}
	return data, nil
}

func TestCarForShard(t *testing.T) {
	db := testutil.CreateTestDB(t)
	repo := sqlrepo.New(db)
	api := shards.API{
		Repo:       repo,
		NodeReader: stubNodeReader{},
	}

	node1, _, err := repo.FindOrCreateRawNode(t.Context(), testutil.RandomCID(t), 21, "dir/file1", id.New(), 0)
	require.NoError(t, err)
	node2, _, err := repo.FindOrCreateRawNode(t.Context(), testutil.RandomCID(t), 21, "dir/file2", id.New(), 0)
	require.NoError(t, err)
	node3, _, err := repo.FindOrCreateRawNode(t.Context(), testutil.RandomCID(t), 26, "dir/dir2/file3", id.New(), 0)
	require.NoError(t, err)

	shard, err := repo.CreateShard(t.Context(), id.New(), 0 /* irrelevant */)

	err = repo.AddNodeToShard(t.Context(), shard.ID(), node1.CID(), 0 /* irrelevant */)
	require.NoError(t, err)
	err = repo.AddNodeToShard(t.Context(), shard.ID(), node2.CID(), 0 /* irrelevant */)
	require.NoError(t, err)
	err = repo.AddNodeToShard(t.Context(), shard.ID(), node3.CID(), 0 /* irrelevant */)
	require.NoError(t, err)

	carReader, err := api.CarForShard(t.Context(), shard.ID())
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
	require.Equal(t, []cid.Cid{node1.CID(), node2.CID(), node3.CID()}, cids)

	var b blocks.Block

	b, err = bs.Get(t.Context(), node1.CID())
	require.NoError(t, err)
	require.Equal(t, []byte("BLOCK DATA: dir/file1"), b.RawData())

	b, err = bs.Get(t.Context(), node2.CID())
	require.NoError(t, err)
	require.Equal(t, []byte("BLOCK DATA: dir/file2"), b.RawData())

	b, err = bs.Get(t.Context(), node3.CID())
	require.NoError(t, err)
	require.Equal(t, []byte("BLOCK DATA: dir/dir2/file3"), b.RawData())
}

func TestIndexForUpload(t *testing.T) {
	t.Run("returns a reader of an index of the upload", func(t *testing.T) {
		repo := sqlrepo.New(testutil.CreateTestDB(t))
		api := shards.API{
			Repo:       repo,
			NodeReader: stubNodeReader{},
		}

		uploads, err := repo.CreateUploads(t.Context(), id.New(), []id.SourceID{id.New()})
		require.NoError(t, err)
		require.Len(t, uploads, 1)
		upload := uploads[0]

		node1, _, err := repo.FindOrCreateRawNode(t.Context(), testutil.RandomCID(t), 100, "dir/file1", id.New(), 0)
		require.NoError(t, err)
		node2, _, err := repo.FindOrCreateRawNode(t.Context(), testutil.RandomCID(t), 200, "dir/file2", id.New(), 0)
		require.NoError(t, err)
		node3, _, err := repo.FindOrCreateRawNode(t.Context(), testutil.RandomCID(t), 300, "dir/dir2/file3", id.New(), 0)
		require.NoError(t, err)

		shard1, err := repo.CreateShard(t.Context(), upload.ID(), 10)
		shard2, err := repo.CreateShard(t.Context(), upload.ID(), 20)

		err = repo.AddNodeToShard(t.Context(), shard1.ID(), node1.CID(), 1)
		require.NoError(t, err)
		err = repo.AddNodeToShard(t.Context(), shard1.ID(), node2.CID(), 2)
		require.NoError(t, err)
		err = repo.AddNodeToShard(t.Context(), shard2.ID(), node3.CID(), 3)
		require.NoError(t, err)

		err = shard1.Close()
		digest1, err := multihash.Encode([]byte("shard1 digest"), multihash.IDENTITY)
		err = shard1.Added(digest1)
		require.NoError(t, err)
		err = repo.UpdateShard(t.Context(), shard1)
		require.NoError(t, err)
		err = shard1.Close()

		err = shard2.Close()
		digest2, err := multihash.Encode([]byte("shard2 digest"), multihash.IDENTITY)
		err = shard2.Added(digest2)
		require.NoError(t, err)
		err = repo.UpdateShard(t.Context(), shard2)
		require.NoError(t, err)
		err = shard2.Close()

		rootCID := testutil.RandomCID(t)
		err = upload.SetRootCID(rootCID)
		require.NoError(t, err)
		err = repo.UpdateUpload(t.Context(), upload)
		require.NoError(t, err)

		indexReader, err := api.IndexForUpload(t.Context(), upload)
		index, err := blobindex.Extract(indexReader)
		require.NoError(t, err)

		require.Equal(t, cidlink.Link{Cid: rootCID}, index.Content())
		require.Equal(t, 2, index.Shards().Size(), "index should have two shards")

		require.Equal(t, 2, index.Shards().Get(digest1).Size(), "first shard should have two slices")
		require.Equal(t, blobindex.Position{Offset: 10 + 1, Length: 100}, index.Shards().Get(digest1).Get(node1.CID().Hash()))
		require.Equal(t, blobindex.Position{Offset: 10 + 1 + 100 + 2, Length: 200}, index.Shards().Get(digest1).Get(node2.CID().Hash()))
		require.Equal(t, 1, index.Shards().Get(digest2).Size(), "second shard should have one slice")
		require.Equal(t, blobindex.Position{Offset: 20 + 3, Length: 300}, index.Shards().Get(digest2).Get(node3.CID().Hash()))
	})

	t.Run("for an upload with no root CID, returns an error", func(t *testing.T) {
		repo := sqlrepo.New(testutil.CreateTestDB(t))
		api := shards.API{
			Repo:       repo,
			NodeReader: stubNodeReader{},
		}

		uploads, err := repo.CreateUploads(t.Context(), id.New(), []id.SourceID{id.New()})
		require.NoError(t, err)
		require.Len(t, uploads, 1)
		upload := uploads[0]

		indexReader, err := api.IndexForUpload(t.Context(), upload)
		require.ErrorContains(t, err, "no root CID set yet on upload")
		require.Nil(t, indexReader)
	})
}
