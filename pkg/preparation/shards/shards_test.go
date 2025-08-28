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
	"github.com/storacha/go-libstoracha/testutil"
	"github.com/storacha/go-ucanto/did"
	dagsmodel "github.com/storacha/guppy/pkg/preparation/dags/model"
	"github.com/storacha/guppy/pkg/preparation/internal/testdb"
	"github.com/storacha/guppy/pkg/preparation/shards"
	"github.com/storacha/guppy/pkg/preparation/shards/model"
	spacesmodel "github.com/storacha/guppy/pkg/preparation/spaces/model"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo/util"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	"github.com/stretchr/testify/require"
)

func TestAddNodeToUploadShardsAndCloseUploadShards(t *testing.T) {
	db := testdb.CreateTestDB(t)
	repo := sqlrepo.New(db)
	api := shards.API{Repo: repo}
	space, err := repo.FindOrCreateSpace(t.Context(), testutil.RandomDID(t), "Test Config", spacesmodel.WithShardSize(1<<16))
	require.NoError(t, err)
	source, err := repo.CreateSource(t.Context(), "Test Source", ".")
	require.NoError(t, err)
	uploads, err := repo.CreateUploads(t.Context(), space.DID(), []id.SourceID{source.ID()})
	require.NoError(t, err)
	require.Len(t, uploads, 1)
	upload := uploads[0]
	nodeCid1 := testutil.RandomCID(t)

	// with no shards, creates a new shard and adds the node to it

	openShards, err := repo.ShardsForUploadByStatus(t.Context(), upload.ID(), model.ShardStateOpen)
	require.NoError(t, err)
	require.Len(t, openShards, 0)
	_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCid1.(cidlink.Link).Cid, 1<<14, space.DID(), "some/path", source.ID(), 0)
	require.NoError(t, err)

	shardClosed, err := api.AddNodeToUploadShards(t.Context(), upload.ID(), nodeCid1.(cidlink.Link).Cid)
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
	require.ElementsMatch(t, []cid.Cid{nodeCid1.(cidlink.Link).Cid}, foundNodeCids)

	// with an open shard with room, adds the node to the shard

	nodeCid2 := testutil.RandomCID(t)
	_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCid2.(cidlink.Link).Cid, 1<<14, space.DID(), "some/other/path", source.ID(), 0)
	require.NoError(t, err)

	shardClosed, err = api.AddNodeToUploadShards(t.Context(), upload.ID(), nodeCid2.(cidlink.Link).Cid)
	require.NoError(t, err)

	require.False(t, shardClosed)
	openShards, err = repo.ShardsForUploadByStatus(t.Context(), upload.ID(), model.ShardStateOpen)
	require.NoError(t, err)
	require.Len(t, openShards, 1)
	require.Equal(t, firstShard.ID(), openShards[0].ID())
	firstShard = openShards[0] // with fresh data from DB

	require.Equal(t, uint64(18+3+36+(1<<14)+3+36+(1<<14)), firstShard.Size())

	foundNodeCids = nodesInShard(t.Context(), t, db, firstShard.ID())
	require.ElementsMatch(t, []cid.Cid{nodeCid1.(cidlink.Link).Cid, nodeCid2.(cidlink.Link).Cid}, foundNodeCids)

	// with an open shard without room, closes the shard and creates another

	nodeCid3 := testutil.RandomCID(t)
	_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCid3.(cidlink.Link).Cid, 1<<15, space.DID(), "yet/other/path", source.ID(), 0)
	require.NoError(t, err)

	shardClosed, err = api.AddNodeToUploadShards(t.Context(), upload.ID(), nodeCid3.(cidlink.Link).Cid)
	require.NoError(t, err)

	require.True(t, shardClosed)
	closedShards, err := repo.ShardsForUploadByStatus(t.Context(), upload.ID(), model.ShardStateClosed)
	require.NoError(t, err)
	require.Len(t, closedShards, 1)
	require.Equal(t, firstShard.ID(), closedShards[0].ID())

	foundNodeCids = nodesInShard(t.Context(), t, db, firstShard.ID())
	require.ElementsMatch(t, []cid.Cid{nodeCid1.(cidlink.Link).Cid, nodeCid2.(cidlink.Link).Cid}, foundNodeCids)

	openShards, err = repo.ShardsForUploadByStatus(t.Context(), upload.ID(), model.ShardStateOpen)
	require.NoError(t, err)
	require.Len(t, openShards, 1)
	secondShard := openShards[0]
	require.NotEqual(t, firstShard.ID(), secondShard.ID())

	require.Equal(t, uint64(18+3+36+(1<<15)), secondShard.Size())
	foundNodeCids = nodesInShard(t.Context(), t, db, secondShard.ID())
	require.ElementsMatch(t, []cid.Cid{nodeCid3.(cidlink.Link).Cid}, foundNodeCids)

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
	db := testdb.CreateTestDB(t)
	repo := sqlrepo.New(db)
	api := shards.API{
		Repo:       repo,
		NodeReader: stubNodeReader{},
	}

	spaceDID, err := did.Parse("did:storacha:space:example")
	require.NoError(t, err)

	nodeCid1 := testutil.RandomCID(t).(cidlink.Link).Cid
	nodeCid2 := testutil.RandomCID(t).(cidlink.Link).Cid
	nodeCid3 := testutil.RandomCID(t).(cidlink.Link).Cid

	_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCid1, 21, spaceDID, "dir/file1", id.New(), 0)
	require.NoError(t, err)
	_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCid2, 21, spaceDID, "dir/file2", id.New(), 0)
	require.NoError(t, err)
	_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCid3, 26, spaceDID, "dir/dir2/file3", id.New(), 0)
	require.NoError(t, err)

	shard, err := repo.CreateShard(t.Context(), id.New(), 0 /* irrelevant */)

	err = repo.AddNodeToShard(t.Context(), shard.ID(), nodeCid1, spaceDID, 0 /* irrelevant */)
	require.NoError(t, err)
	err = repo.AddNodeToShard(t.Context(), shard.ID(), nodeCid2, spaceDID, 0 /* irrelevant */)
	require.NoError(t, err)
	err = repo.AddNodeToShard(t.Context(), shard.ID(), nodeCid3, spaceDID, 0 /* irrelevant */)
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
	require.Equal(t, []cid.Cid{nodeCid1, nodeCid2, nodeCid3}, cids)

	var b blocks.Block

	b, err = bs.Get(t.Context(), nodeCid1)
	require.NoError(t, err)
	require.Equal(t, []byte("BLOCK DATA: dir/file1"), b.RawData())

	b, err = bs.Get(t.Context(), nodeCid2)
	require.NoError(t, err)
	require.Equal(t, []byte("BLOCK DATA: dir/file2"), b.RawData())

	b, err = bs.Get(t.Context(), nodeCid3)
	require.NoError(t, err)
	require.Equal(t, []byte("BLOCK DATA: dir/dir2/file3"), b.RawData())
}
