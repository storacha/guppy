package shards_test

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
	"github.com/storacha/go-libstoracha/testutil"
	"github.com/storacha/go-ucanto/did"
	dagsmodel "github.com/storacha/guppy/pkg/preparation/dags/model"
	"github.com/storacha/guppy/pkg/preparation/dags/nodereader"
	"github.com/storacha/guppy/pkg/preparation/internal/testdb"
	"github.com/storacha/guppy/pkg/preparation/shards"
	"github.com/storacha/guppy/pkg/preparation/shards/model"
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
		repo := sqlrepo.New(db)
		api := shards.API{Repo: repo, ShardEncoder: shards.NewCAREncoder()}
		space, err := repo.FindOrCreateSpace(t.Context(), testutil.RandomDID(t), "Test Space", spacesmodel.WithShardSize(1<<16))
		require.NoError(t, err)
		source, err := repo.CreateSource(t.Context(), "Test Source", ".")
		require.NoError(t, err)
		uploads, err := repo.FindOrCreateUploads(t.Context(), space.DID(), []id.SourceID{source.ID()})
		require.NoError(t, err)
		require.Len(t, uploads, 1)
		upload := uploads[0]
		nodeCID1 := testutil.RandomCID(t)

		// with no shards, creates a new shard and adds the node to it

		openShards, err := repo.ShardsForUploadByState(t.Context(), upload.ID(), model.ShardStateOpen)
		require.NoError(t, err)
		require.Len(t, openShards, 0)
		n, _, err := repo.FindOrCreateRawNode(t.Context(), nodeCID1.(cidlink.Link).Cid, 1<<14, space.DID(), "some/path", source.ID(), 0)
		require.NoError(t, err)

		data := testutil.RandomBytes(t, int(n.Size()))
		shardClosed, err := api.AddNodeToUploadShards(t.Context(), upload.ID(), space.DID(), nodeCID1.(cidlink.Link).Cid, data)
		require.NoError(t, err)

		require.False(t, shardClosed)
		openShards, err = repo.ShardsForUploadByState(t.Context(), upload.ID(), model.ShardStateOpen)
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

		nodeCID2 := testutil.RandomCID(t)
		n, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID2.(cidlink.Link).Cid, 1<<14, space.DID(), "some/other/path", source.ID(), 0)
		require.NoError(t, err)
		data = testutil.RandomBytes(t, int(n.Size()))
		shardClosed, err = api.AddNodeToUploadShards(t.Context(), upload.ID(), space.DID(), nodeCID2.(cidlink.Link).Cid, data)
		require.NoError(t, err)

		require.False(t, shardClosed)
		openShards, err = repo.ShardsForUploadByState(t.Context(), upload.ID(), model.ShardStateOpen)
		require.NoError(t, err)
		require.Len(t, openShards, 1)
		require.Equal(t, firstShard.ID(), openShards[0].ID())
		firstShard = openShards[0] // with fresh data from DB

		require.Equal(t, uint64(18+3+36+(1<<14)+3+36+(1<<14)), firstShard.Size())

		foundNodeCIDs = nodesInShard(t.Context(), t, db, firstShard.ID())
		require.ElementsMatch(t, []cid.Cid{nodeCID1.(cidlink.Link).Cid, nodeCID2.(cidlink.Link).Cid}, foundNodeCIDs)

		// with an open shard without room, closes the shard and creates another

		nodeCID3 := testutil.RandomCID(t)
		n, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID3.(cidlink.Link).Cid, 1<<15, space.DID(), "yet/other/path", source.ID(), 0)
		require.NoError(t, err)

		data = testutil.RandomBytes(t, int(n.Size()))
		shardClosed, err = api.AddNodeToUploadShards(t.Context(), upload.ID(), space.DID(), nodeCID3.(cidlink.Link).Cid, data)
		require.NoError(t, err)

		require.True(t, shardClosed)
		closedShards, err := repo.ShardsForUploadByState(t.Context(), upload.ID(), model.ShardStateClosed)
		require.NoError(t, err)
		require.Len(t, closedShards, 1)
		require.Equal(t, firstShard.ID(), closedShards[0].ID())

		foundNodeCIDs = nodesInShard(t.Context(), t, db, firstShard.ID())
		require.ElementsMatch(t, []cid.Cid{nodeCID1.(cidlink.Link).Cid, nodeCID2.(cidlink.Link).Cid}, foundNodeCIDs)

		openShards, err = repo.ShardsForUploadByState(t.Context(), upload.ID(), model.ShardStateOpen)
		require.NoError(t, err)
		require.Len(t, openShards, 1)
		secondShard := openShards[0]
		require.NotEqual(t, firstShard.ID(), secondShard.ID())

		require.Equal(t, uint64(18+3+36+(1<<15)), secondShard.Size())
		foundNodeCIDs = nodesInShard(t.Context(), t, db, secondShard.ID())
		require.ElementsMatch(t, []cid.Cid{nodeCID3.(cidlink.Link).Cid}, foundNodeCIDs)

		// finally, close the last shard with CloseUploadShards()

		shardClosed, err = api.CloseUploadShards(t.Context(), upload.ID())
		require.NoError(t, err)
		require.True(t, shardClosed)

		closedShards, err = repo.ShardsForUploadByState(t.Context(), upload.ID(), model.ShardStateClosed)
		require.NoError(t, err)
		require.Len(t, closedShards, 2)
		require.Equal(t, firstShard.ID(), closedShards[0].ID())

		closedShardIDs := make([]id.ShardID, 0, len(closedShards))
		for _, closedShard := range closedShards {
			closedShardIDs = append(closedShardIDs, closedShard.ID())
		}
		require.ElementsMatch(t, closedShardIDs, []id.ShardID{firstShard.ID(), secondShard.ID()})

		openShards, err = repo.ShardsForUploadByState(t.Context(), upload.ID(), model.ShardStateOpen)
		require.NoError(t, err)
		require.Len(t, openShards, 0)
	})

	t.Run("with a node too big for a shard", func(t *testing.T) {
		db := testdb.CreateTestDB(t)
		repo := sqlrepo.New(db)
		api := shards.API{Repo: repo, ShardEncoder: shards.NewCAREncoder()}
		space, err := repo.FindOrCreateSpace(t.Context(), testutil.RandomDID(t), "Test Space", spacesmodel.WithShardSize(128))
		require.NoError(t, err)
		source, err := repo.CreateSource(t.Context(), "Test Source", ".")
		require.NoError(t, err)
		uploads, err := repo.FindOrCreateUploads(t.Context(), space.DID(), []id.SourceID{source.ID()})
		require.NoError(t, err)
		require.Len(t, uploads, 1)
		upload := uploads[0]
		nodeCID1 := testutil.RandomCID(t)

		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID1.(cidlink.Link).Cid, 120, space.DID(), "some/path", source.ID(), 0)
		require.NoError(t, err)

		_, err = api.AddNodeToUploadShards(t.Context(), upload.ID(), space.DID(), nodeCID1.(cidlink.Link).Cid, nil)
		require.ErrorContains(t, err, "too large to fit in new shard for upload")
	})
}

// (Until the repo has a way to query for this itself...)
func nodesInShard(ctx context.Context, t *testing.T, db *sql.DB, shardID id.ShardID) []cid.Cid {
	rows, err := db.QueryContext(ctx, `SELECT node_cid FROM nodes_in_shards WHERE shard_id = ?`, shardID)
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
		repo := sqlrepo.New(db)
		spaceDID, err := did.Parse("did:storacha:space:example")
		require.NoError(t, err)

		nodeCID1 := testutil.RandomCID(t).(cidlink.Link).Cid
		nodeCID2 := testutil.RandomCID(t).(cidlink.Link).Cid
		nodeCID3 := testutil.RandomCID(t).(cidlink.Link).Cid

		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID1, 21, spaceDID, "dir/file1", id.New(), 0)
		require.NoError(t, err)
		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID2, 21, spaceDID, "dir/file2", id.New(), 0)
		require.NoError(t, err)
		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID3, 26, spaceDID, "dir/dir2/file3", id.New(), 0)
		require.NoError(t, err)

		shard, err := repo.CreateShard(t.Context(), id.New(), 0, nil, nil /* irrelevant */)
		require.NoError(t, err)

		err = repo.AddNodeToShard(t.Context(), shard.ID(), nodeCID1, spaceDID, 0 /* irrelevant */)
		require.NoError(t, err)
		err = repo.AddNodeToShard(t.Context(), shard.ID(), nodeCID2, spaceDID, 0 /* irrelevant */)
		require.NoError(t, err)
		err = repo.AddNodeToShard(t.Context(), shard.ID(), nodeCID3, spaceDID, 0 /* irrelevant */)
		require.NoError(t, err)

		nodeReader := stubNodeReader{}
		api := shards.API{
			Repo:           repo,
			OpenNodeReader: nodeReader.OpenNodeReader,
			ShardEncoder:   shards.NewCAREncoder(),
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
		repo := sqlrepo.New(db)
		spaceDID, err := did.Parse("did:storacha:space:example")
		require.NoError(t, err)

		nodeCID1 := testutil.RandomCID(t).(cidlink.Link).Cid
		nodeCID2 := testutil.RandomCID(t).(cidlink.Link).Cid
		nodeCID3 := testutil.RandomCID(t).(cidlink.Link).Cid
		nodeCID4 := testutil.RandomCID(t).(cidlink.Link).Cid

		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID1, 21, spaceDID, "dir/file1", id.New(), 0)
		require.NoError(t, err)
		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID2, 21, spaceDID, "dir/file2", id.New(), 0)
		require.NoError(t, err)
		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID3, 21, spaceDID, "dir/file3", id.New(), 0)
		require.NoError(t, err)
		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID4, 21, spaceDID, "dir/file4", id.New(), 0)
		require.NoError(t, err)

		shard, err := repo.CreateShard(t.Context(), id.New(), 0, nil, nil /* irrelevant */)
		require.NoError(t, err)

		for _, nodeCID := range []cid.Cid{nodeCID1, nodeCID2, nodeCID3, nodeCID4} {
			err = repo.AddNodeToShard(t.Context(), shard.ID(), nodeCID, spaceDID, 0 /* irrelevant */)
			require.NoError(t, err)
		}

		nodeReader := stubNodeReader{
			errorNodes: []cid.Cid{nodeCID2, nodeCID4},
		}
		api := shards.API{
			Repo:           repo,
			OpenNodeReader: nodeReader.OpenNodeReader,
			ShardEncoder:   shards.NewCAREncoder(),
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

func TestIndexForUpload(t *testing.T) {
	t.Run("returns a reader of an index of the upload", func(t *testing.T) {
		repo := sqlrepo.New(testdb.CreateTestDB(t))
		nodeReader := stubNodeReader{}
		api := shards.API{
			Repo:             repo,
			OpenNodeReader:   nodeReader.OpenNodeReader,
			ShardEncoder:     shards.NewCAREncoder(),
			MaxNodesPerIndex: 5,
		}

		spaceDID, err := did.Parse("did:storacha:space:example")
		require.NoError(t, err)

		uploads, err := repo.FindOrCreateUploads(t.Context(), spaceDID, []id.SourceID{id.New()})
		require.NoError(t, err)
		require.Len(t, uploads, 1)
		upload := uploads[0]

		node1, _, err := repo.FindOrCreateRawNode(t.Context(), testutil.RandomCID(t).(cidlink.Link).Cid, 100, spaceDID, "dir/file1", id.New(), 0)
		require.NoError(t, err)
		node2, _, err := repo.FindOrCreateRawNode(t.Context(), testutil.RandomCID(t).(cidlink.Link).Cid, 200, spaceDID, "dir/file2", id.New(), 0)
		require.NoError(t, err)
		node3, _, err := repo.FindOrCreateRawNode(t.Context(), testutil.RandomCID(t).(cidlink.Link).Cid, 300, spaceDID, "dir/file3", id.New(), 0)
		require.NoError(t, err)
		node4, _, err := repo.FindOrCreateRawNode(t.Context(), testutil.RandomCID(t).(cidlink.Link).Cid, 400, spaceDID, "dir/file4", id.New(), 0)
		require.NoError(t, err)
		node5, _, err := repo.FindOrCreateRawNode(t.Context(), testutil.RandomCID(t).(cidlink.Link).Cid, 500, spaceDID, "dir/file5", id.New(), 0)
		require.NoError(t, err)
		node6, _, err := repo.FindOrCreateRawNode(t.Context(), testutil.RandomCID(t).(cidlink.Link).Cid, 600, spaceDID, "dir/file6", id.New(), 0)
		require.NoError(t, err)

		shard1, err := repo.CreateShard(t.Context(), upload.ID(), 10, nil, nil)
		require.NoError(t, err)
		shard2, err := repo.CreateShard(t.Context(), upload.ID(), 20, nil, nil)
		require.NoError(t, err)
		shard3, err := repo.CreateShard(t.Context(), upload.ID(), 30, nil, nil)
		require.NoError(t, err)

		err = repo.AddNodeToShard(t.Context(), shard1.ID(), node1.CID(), spaceDID, 1)
		require.NoError(t, err)
		err = repo.AddNodeToShard(t.Context(), shard1.ID(), node2.CID(), spaceDID, 2)
		require.NoError(t, err)
		err = repo.AddNodeToShard(t.Context(), shard2.ID(), node3.CID(), spaceDID, 3)
		require.NoError(t, err)
		err = repo.AddNodeToShard(t.Context(), shard3.ID(), node4.CID(), spaceDID, 4)
		require.NoError(t, err)
		err = repo.AddNodeToShard(t.Context(), shard3.ID(), node5.CID(), spaceDID, 5)
		require.NoError(t, err)
		err = repo.AddNodeToShard(t.Context(), shard3.ID(), node6.CID(), spaceDID, 6)
		require.NoError(t, err)

		digest1, err := multihash.Encode([]byte("shard1 digest"), multihash.IDENTITY)
		require.NoError(t, err)
		err = shard1.Close(digest1, testutil.RandomCID(t).(cidlink.Link).Cid)
		require.NoError(t, err)
		err = shard1.Added()
		require.NoError(t, err)
		err = repo.UpdateShard(t.Context(), shard1)
		require.NoError(t, err)

		digest2, err := multihash.Encode([]byte("shard2 digest"), multihash.IDENTITY)
		require.NoError(t, err)
		err = shard2.Close(digest2, testutil.RandomCID(t).(cidlink.Link).Cid)
		require.NoError(t, err)
		err = shard2.Added()
		require.NoError(t, err)
		err = repo.UpdateShard(t.Context(), shard2)
		require.NoError(t, err)

		digest3, err := multihash.Encode([]byte("shard3 digest"), multihash.IDENTITY)
		require.NoError(t, err)
		err = shard3.Close(digest3, testutil.RandomCID(t).(cidlink.Link).Cid)
		require.NoError(t, err)
		err = shard3.Added()
		require.NoError(t, err)
		err = repo.UpdateShard(t.Context(), shard3)
		require.NoError(t, err)

		rootLink := testutil.RandomCID(t)
		err = upload.SetRootCID(rootLink.(cidlink.Link).Cid)
		require.NoError(t, err)
		err = repo.UpdateUpload(t.Context(), upload)
		require.NoError(t, err)

		indexReaders, err := api.IndexesForUpload(t.Context(), upload)
		require.NoError(t, err)
		indexes := make([]blobindex.ShardedDagIndexView, 0, 2)
		for _, indexReader := range indexReaders {
			index, err := blobindex.Extract(indexReader)
			require.NoError(t, err)
			indexes = append(indexes, index)
		}

		require.Len(t, indexes, 2, "should return two indexes")
		index1 := indexes[0]
		index2 := indexes[1]

		require.Equal(t, rootLink, index1.Content())
		require.Equal(t, 2, index1.Shards().Size(), "first index should have two shards")

		require.Equal(t, 2, index1.Shards().Get(digest1).Size(), "first shard should have two slices")
		require.Equal(t, blobindex.Position{Offset: 10 + 1, Length: 100}, index1.Shards().Get(digest1).Get(node1.CID().Hash()))
		require.Equal(t, blobindex.Position{Offset: 10 + 1 + 100 + 2, Length: 200}, index1.Shards().Get(digest1).Get(node2.CID().Hash()))
		require.Equal(t, 1, index1.Shards().Get(digest2).Size(), "second shard should have one slice")
		require.Equal(t, blobindex.Position{Offset: 20 + 3, Length: 300}, index1.Shards().Get(digest2).Get(node3.CID().Hash()))

		require.Equal(t, rootLink, index2.Content())
		require.Equal(t, 1, index2.Shards().Size(), "second index should have one shard")

		require.Equal(t, 3, index2.Shards().Get(digest3).Size(), "shard should have three slices")
		require.Equal(t, blobindex.Position{Offset: 30 + 4, Length: 400}, index2.Shards().Get(digest3).Get(node4.CID().Hash()))
		require.Equal(t, blobindex.Position{Offset: 30 + 4 + 400 + 5, Length: 500}, index2.Shards().Get(digest3).Get(node5.CID().Hash()))
		require.Equal(t, blobindex.Position{Offset: 30 + 4 + 400 + 5 + 500 + 6, Length: 600}, index2.Shards().Get(digest3).Get(node6.CID().Hash()))
	})

	t.Run("for an upload with no root CID, returns an error", func(t *testing.T) {
		repo := sqlrepo.New(testdb.CreateTestDB(t))
		nodeReader := stubNodeReader{}
		api := shards.API{
			Repo:           repo,
			OpenNodeReader: nodeReader.OpenNodeReader,
			ShardEncoder:   shards.NewCAREncoder(),
		}

		spaceDID, err := did.Parse("did:storacha:space:example")
		require.NoError(t, err)

		uploads, err := repo.FindOrCreateUploads(t.Context(), spaceDID, []id.SourceID{id.New()})
		require.NoError(t, err)
		require.Len(t, uploads, 1)
		upload := uploads[0]

		indexReader, err := api.IndexesForUpload(t.Context(), upload)
		require.ErrorContains(t, err, "no root CID set yet on upload")
		require.Nil(t, indexReader)
	})
}

func TestComputedShardCIDs(t *testing.T) {
	db := testdb.CreateTestDB(t)
	repo := sqlrepo.New(db)
	// we're going to use fileback here because it doesn't add any bytes, making expected CIDs easier to calculate
	api := shards.API{Repo: repo, ShardEncoder: shards.NewFilepackEncoder()}
	space, err := repo.FindOrCreateSpace(t.Context(), testutil.RandomDID(t), "Test Space", spacesmodel.WithShardSize(1<<16))
	require.NoError(t, err)
	source, err := repo.CreateSource(t.Context(), "Test Source", ".")
	require.NoError(t, err)
	uploads, err := repo.FindOrCreateUploads(t.Context(), space.DID(), []id.SourceID{source.ID()})
	require.NoError(t, err)
	require.Len(t, uploads, 1)
	upload := uploads[0]

	totalData := testutil.RandomBytes(t, 1<<15) // 32 KiB
	expectedDigest, err := multihash.Sum(totalData, multihash.SHA2_256, -1)
	require.NoError(t, err)
	calc := &commp.Calc{}
	calc.Write(totalData)
	commP, _, err := calc.Digest()
	require.NoError(t, err)
	expectedPieceCID, err := commcid.DataCommitmentToPieceCidv2(commP, uint64(len(totalData)))
	require.NoError(t, err)

	// no shards yet
	openShards, err := repo.ShardsForUploadByState(t.Context(), upload.ID(), model.ShardStateOpen)
	require.NoError(t, err)
	require.Len(t, openShards, 0)

	for offset := 0; offset < len(totalData); offset += 1 << 12 {
		data := totalData[offset : offset+(1<<12)]
		nodeCID := cid.NewCidV1(cid.Raw, testutil.Must(multihash.Sum(data, multihash.SHA2_256, -1))(t))
		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCID, 1<<12, space.DID(), fmt.Sprintf("some/path/%d", offset), source.ID(), 0)
		require.NoError(t, err)
		_, err = api.AddNodeToUploadShards(t.Context(), upload.ID(), space.DID(), nodeCID, data)
		require.NoError(t, err)
	}

	shardClosed, err := api.CloseUploadShards(t.Context(), upload.ID())
	require.NoError(t, err)
	require.True(t, shardClosed)

	closedShards, err := repo.ShardsForUploadByState(t.Context(), upload.ID(), model.ShardStateClosed)
	require.NoError(t, err)
	require.Len(t, closedShards, 1)

	// check the shard's digest and piece CID
	shard := closedShards[0]
	require.Equal(t, expectedDigest, shard.Digest())
	require.Equal(t, expectedPieceCID, shard.PieceCID())
}
