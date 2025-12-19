package storacha_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	commcid "github.com/filecoin-project/go-fil-commcid"
	commp "github.com/filecoin-project/go-fil-commp-hashhash"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	stestutil "github.com/storacha/go-libstoracha/testutil"
	"github.com/storacha/go-ucanto/did"
	"github.com/stretchr/testify/require"

	"github.com/storacha/guppy/pkg/preparation/blobs"
	"github.com/storacha/guppy/pkg/preparation/blobs/model"
	"github.com/storacha/guppy/pkg/preparation/internal/mockclient"
	"github.com/storacha/guppy/pkg/preparation/internal/testdb"
	"github.com/storacha/guppy/pkg/preparation/internal/testutil"
	spacesmodel "github.com/storacha/guppy/pkg/preparation/spaces/model"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	"github.com/storacha/guppy/pkg/preparation/storacha"
	"github.com/storacha/guppy/pkg/preparation/types/id"
)

// commP is not defined for inputs shorter than 127 bytes, so add 127 bytes of
// padding to every "CAR" to make sure it's definitely long enough.
var padding = bytes.Repeat([]byte{0}, 127)

func TestAddShardsForUpload(t *testing.T) {
	t.Run("`space/blob/add`s, `space/blob/replicate`s, and `filecoin/offer`s a CAR for each shard", func(t *testing.T) {
		db := testdb.CreateTestDB(t)
		repo := sqlrepo.New(db)
		spaceDID, err := did.Parse("did:storacha:space:example")
		require.NoError(t, err)
		client := mockclient.MockClient{T: t}

		shardReadersClosed := map[id.ShardID]struct{}{}
		carForShard := func(ctx context.Context, shardID id.ShardID) (io.ReadCloser, error) {
			return &blobReadCloser{
				blobReadersClosed: shardReadersClosed,
				blobID:            shardID,
				Reader:            bytes.NewReader(fmt.Append(nil, "CAR OF SHARD: ", shardID, padding)),
			}, nil
		}

		api := storacha.API{
			Repo:                  repo,
			Client:                &client,
			ReaderForShard:        carForShard,
			BlobUploadParallelism: 1,
		}

		blobsApi := blobs.API{
			Repo:         repo,
			ShardEncoder: blobs.NewCAREncoder(),
		}

		upload, source := testutil.CreateUpload(t, repo, spaceDID, spacesmodel.WithShardSize(1<<16))

		// Add enough nodes to close one shard and create a second one.
		testutil.AddNodeToUploadShards(t, repo, blobsApi, upload.ID(), source.ID(), spaceDID, nil, 1<<14)
		testutil.AddNodeToUploadShards(t, repo, blobsApi, upload.ID(), source.ID(), spaceDID, nil, 1<<14)
		testutil.AddNodeToUploadShards(t, repo, blobsApi, upload.ID(), source.ID(), spaceDID, nil, 1<<15)

		shards, err := repo.ShardsForUploadByState(t.Context(), upload.ID(), model.BlobStateClosed)
		require.NoError(t, err)
		require.Len(t, shards, 1)
		firstShard := shards[0]

		shards, err = repo.ShardsForUploadByState(t.Context(), upload.ID(), model.BlobStateOpen)
		require.NoError(t, err)
		require.Len(t, shards, 1)
		secondShard := shards[0]

		// Upload shards that are ready to go.
		err = api.AddShardsForUpload(t.Context(), upload.ID(), spaceDID)
		require.NoError(t, err)

		// Reload shards
		firstShard, err = repo.GetShardByID(t.Context(), firstShard.ID())
		require.NoError(t, err)
		secondShard, err = repo.GetShardByID(t.Context(), secondShard.ID())
		require.NoError(t, err)
		require.Equal(t, model.BlobStateAdded, firstShard.State(), "expected first shard to be marked as added now")
		require.Equal(t, model.BlobStateOpen, secondShard.State(), "expected second shard to remain open")

		// This run should `space/blob/add` the first, closed shard.
		expectedData := fmt.Append(nil, "CAR OF SHARD: ", firstShard.ID(), padding)
		require.Len(t, client.SpaceBlobAddInvocations, 1)
		require.Equal(t, expectedData, client.SpaceBlobAddInvocations[0].BlobAdded)
		require.Equal(t, spaceDID, client.SpaceBlobAddInvocations[0].Space)

		// Then it should `space/blob/replicate` it.
		require.Len(t, client.SpaceBlobReplicateInvocations, 1)
		require.Equal(t, firstShard.Digest(), client.SpaceBlobReplicateInvocations[0].Blob.Digest)
		require.Equal(t, firstShard.Size(), client.SpaceBlobReplicateInvocations[0].Blob.Size)
		require.Equal(t, spaceDID, client.SpaceBlobReplicateInvocations[0].Space)
		require.Equal(t, uint(3), client.SpaceBlobReplicateInvocations[0].ReplicaCount)
		require.Equal(t, client.SpaceBlobAddInvocations[0].ReturnedLocation, client.SpaceBlobReplicateInvocations[0].LocationCommitment)

		// Then it should `filecoin/offer` it.
		require.Len(t, client.FilecoinOfferInvocations, 1)
		require.Equal(t, spaceDID, client.FilecoinOfferInvocations[0].Space)
		require.Equal(t, cidlink.Link{Cid: firstShard.CID()}, client.FilecoinOfferInvocations[0].Content)
		require.Equal(t, client.SpaceBlobAddInvocations[0].ReturnedPDPAccept, client.FilecoinOfferInvocations[0].Options.PDPAcceptInvocation())

		// It should have closed the first shard's reader.
		require.Contains(t, shardReadersClosed, firstShard.ID(), "expected first shard reader to be closed now")

		// Now close the upload shards and run it again.
		err = blobsApi.CloseUploadShards(t.Context(), upload.ID(), nil)
		require.NoError(t, err)
		err = api.AddShardsForUpload(t.Context(), upload.ID(), spaceDID)
		require.NoError(t, err)

		// Reload second shard
		secondShard, err = repo.GetShardByID(t.Context(), secondShard.ID())
		require.NoError(t, err)
		require.Equal(t, model.BlobStateAdded, secondShard.State(), "expected second shard to be marked as added now")

		// This run should `space/blob/add` the second, newly closed shard.
		require.Len(t, client.SpaceBlobAddInvocations, 2)
		require.Equal(t, fmt.Append(nil, "CAR OF SHARD: ", secondShard.ID(), padding), client.SpaceBlobAddInvocations[1].BlobAdded)
		require.Equal(t, spaceDID, client.SpaceBlobAddInvocations[1].Space)

		// Then it should `space/blob/replicate` it.
		require.Len(t, client.SpaceBlobReplicateInvocations, 2)
		require.Equal(t, secondShard.Digest(), client.SpaceBlobReplicateInvocations[1].Blob.Digest)
		require.Equal(t, secondShard.Size(), client.SpaceBlobReplicateInvocations[1].Blob.Size)
		require.Equal(t, spaceDID, client.SpaceBlobReplicateInvocations[1].Space)
		require.Equal(t, uint(3), client.SpaceBlobReplicateInvocations[1].ReplicaCount)
		require.Equal(t, client.SpaceBlobAddInvocations[1].ReturnedLocation, client.SpaceBlobReplicateInvocations[1].LocationCommitment)

		// It should have closed the second shard's reader.
		require.Contains(t, shardReadersClosed, secondShard.ID(), "expected second shard reader to be closed now")
	})

	t.Run("does not `space/blob/add` again on retry once it succeeds", func(t *testing.T) {
		db := testdb.CreateTestDB(t)
		repo := sqlrepo.New(db)
		spaceDID, err := did.Parse("did:storacha:space:example")
		require.NoError(t, err)
		client := mockclient.MockClient{T: t}

		shardReadersClosed := map[id.ShardID]struct{}{}
		carForShard := func(ctx context.Context, shardID id.ShardID) (io.ReadCloser, error) {
			return &blobReadCloser{
				blobReadersClosed: shardReadersClosed,
				blobID:            shardID,
				Reader:            bytes.NewReader(fmt.Append(nil, "CAR OF SHARD: ", shardID, padding)),
			}, nil
		}

		api := storacha.API{
			Repo:                  repo,
			Client:                &client,
			ReaderForShard:        carForShard,
			BlobUploadParallelism: 1,
		}

		blobsApi := blobs.API{
			Repo:         repo,
			ShardEncoder: blobs.NewCAREncoder(),
		}

		upload, source := testutil.CreateUpload(t, repo, spaceDID, spacesmodel.WithShardSize(1<<16))

		testutil.AddNodeToUploadShards(t, repo, blobsApi, upload.ID(), source.ID(), spaceDID, nil, 1<<14)

		err = blobsApi.CloseUploadShards(t.Context(), upload.ID(), nil)
		require.NoError(t, err)

		client.SpaceBlobAddError = fmt.Errorf("simulated SpaceBlobAdd error")

		err = api.AddShardsForUpload(t.Context(), upload.ID(), spaceDID)
		require.ErrorContains(t, err, "simulated SpaceBlobAdd error")

		// It should have `space/blob/add`ed (and failed)...
		require.Len(t, client.SpaceBlobAddInvocations, 1)

		// ...but not have proceeded to `space/blob/replicate` or `filecoin/offer`.
		require.Len(t, client.SpaceBlobReplicateInvocations, 0)
		require.Len(t, client.FilecoinOfferInvocations, 0)

		// It should have closed the first shard's reader.
		require.Len(t, shardReadersClosed, 1, "expected shard readerto be closed, even though it failed")
		// reset the shard readers closed map
		for shardID := range shardReadersClosed {
			delete(shardReadersClosed, shardID)
		}

		// Now retry: `space/blob/add` succeeds but `space/blob/replicate` fails.
		client.SpaceBlobAddError = nil
		client.SpaceBlobReplicateError = fmt.Errorf("simulated SpaceBlobReplicate error")
		err = api.AddShardsForUpload(t.Context(), upload.ID(), spaceDID)
		require.ErrorContains(t, err, "simulated SpaceBlobReplicate error")

		// It should have `space/blob/add`ed again...
		require.Len(t, client.SpaceBlobAddInvocations, 2)
		// ...and then attempted `space/blob/replicate` and failed...
		require.Len(t, client.SpaceBlobReplicateInvocations, 1)
		// ...but not have proceeded to `filecoin/offer`.
		require.Len(t, client.FilecoinOfferInvocations, 0)

		// It should have closed the first shard's reader.
		require.Len(t, shardReadersClosed, 1, "expected shard reader to be closed, even though blob was not uploaded")
		// reset the shard readers closed map
		for shardID := range shardReadersClosed {
			delete(shardReadersClosed, shardID)
		}

		// Now retry: `space/blob/replicate` succeeds but `filecoin/offer` fails.
		client.SpaceBlobReplicateError = nil
		client.FilecoinOfferError = fmt.Errorf("simulated FilecoinOffer error")
		err = api.AddShardsForUpload(t.Context(), upload.ID(), spaceDID)
		require.ErrorContains(t, err, "simulated FilecoinOffer error")

		// It should NOT `space/blob/add` again...
		require.Len(t, client.SpaceBlobAddInvocations, 2)
		// ...but should have `space/blob/replicate`ed again...
		require.Len(t, client.SpaceBlobReplicateInvocations, 2)
		// ...and then attempted `filecoin/offer` and failed.
		require.Len(t, client.FilecoinOfferInvocations, 1)
		// It should have closed the first shard's reader.
		require.Len(t, shardReadersClosed, 1, "expected shard reader to be closed, even though blob was not uploaded")
		// reset the shard readers closed map
		for shardID := range shardReadersClosed {
			delete(shardReadersClosed, shardID)
		}
	})

	t.Run("with a shard too small for a CommP, avoids `filecoin/offer`ing it", func(t *testing.T) {
		db := testdb.CreateTestDB(t)
		repo := sqlrepo.New(db)
		spaceDID, err := did.Parse("did:storacha:space:example")
		require.NoError(t, err)
		client := mockclient.MockClient{T: t}

		data := []byte("VERY SHORT CAR")
		carForShard := func(ctx context.Context, shardID id.ShardID) (io.ReadCloser, error) {
			// Note that the decision to skip `filecoin/offer` is based on the size
			// listed on the shard, which is based on the size of the nodes added to
			// it, not the actual CAR bytes (which are written lazily). So the
			// behavior is triggered by the node below being 1, while the short CAR
			// is here to cause an error if we *do* try to `filecoin/offer` it.
			return io.NopCloser(bytes.NewReader(data)), nil
		}

		api := storacha.API{
			Repo:                  repo,
			Client:                &client,
			ReaderForShard:        carForShard,
			BlobUploadParallelism: 1,
		}

		blobsApi := blobs.API{
			Repo:         repo,
			ShardEncoder: blobs.NewCAREncoder(),
		}

		upload, source := testutil.CreateUpload(t, repo, spaceDID, spacesmodel.WithShardSize(1<<16))

		testutil.AddNodeToUploadShardsWithData(t, repo, blobsApi, upload.ID(), source.ID(), spaceDID, nil, data)

		err = blobsApi.CloseUploadShards(t.Context(), upload.ID(), nil)
		require.NoError(t, err)

		err = api.AddShardsForUpload(t.Context(), upload.ID(), spaceDID)
		require.NoError(t, err)

		// It should `space/blob/add`...
		require.Len(t, client.SpaceBlobAddInvocations, 1)

		// ...and it should `space/blob/replicate`...
		require.Len(t, client.SpaceBlobReplicateInvocations, 1)

		// ...but it should cleanly avoid `filecoin/offer`ing, which wouldn't work.
		require.Len(t, client.FilecoinOfferInvocations, 0)
	})
}

func TestAddIndexesForUpload(t *testing.T) {
	t.Run("`space/blob/add`s and `space/blob/replicate`s index CARs", func(t *testing.T) {
		logging.SetLogLevel("preparation/storacha", "warn")
		db := testdb.CreateTestDB(t)
		repo := sqlrepo.New(db)
		spaceDID, err := did.Parse("did:storacha:space:example")
		require.NoError(t, err)
		client := mockclient.MockClient{T: t}

		indexReadersClosed := map[id.ShardID]struct{}{}
		carForIndex := func(ctx context.Context, indexID id.IndexID) (io.ReadCloser, error) {
			return &blobReadCloser{
				blobReadersClosed: indexReadersClosed,
				blobID:            indexID,
				Reader:            bytes.NewReader(fmt.Append(nil, "CAR OF INDEX: ", indexID, padding))}, nil
		}

		api := storacha.API{
			Repo:                  repo,
			Client:                &client,
			ReaderForIndex:        carForIndex,
			BlobUploadParallelism: 1,
		}

		blobsApi := blobs.API{
			Repo:             repo,
			ShardEncoder:     blobs.NewCAREncoder(),
			MaxNodesPerIndex: 3,
		}

		upload, source := testutil.CreateUpload(t, repo, spaceDID, spacesmodel.WithShardSize(1<<7))
		rootCID := stestutil.RandomCID(t).(cidlink.Link).Cid
		upload.SetRootCID(rootCID)
		err = repo.UpdateUpload(t.Context(), upload)
		require.NoError(t, err)

		var indexes []*model.Index
		recordClosedIndex := func(index *model.Index) error {
			indexes = append(indexes, index)
			return nil
		}

		var shards []*model.Shard
		recordClosedShard := func(shard *model.Shard) error {
			blobsApi.AddShardToUploadIndexes(t.Context(), upload.ID(), shard.ID(), recordClosedIndex)
			shards = append(shards, shard)
			return nil
		}

		// Add enough nodes to create three shards and two indexes.
		for range 5 {
			testutil.AddNodeToUploadShards(t, repo, blobsApi, upload.ID(), source.ID(), spaceDID, recordClosedShard, 1<<3)
		}
		err = blobsApi.CloseUploadShards(t.Context(), upload.ID(), recordClosedShard)
		require.NoError(t, err)
		require.Len(t, shards, 3)
		require.Len(t, indexes, 1)

		err = api.AddIndexesForUpload(t.Context(), upload.ID(), spaceDID)
		require.NoError(t, err)

		// Reload first shard
		firstIndex, err := repo.GetIndexByID(t.Context(), indexes[0].ID())
		require.NoError(t, err)
		require.Equal(t, model.BlobStateAdded, firstIndex.State(), "expected first index to be marked as added now")

		// This run should `space/blob/add` the first, closed index.
		expectedData := fmt.Append(nil, "CAR OF INDEX: ", firstIndex.ID(), padding)
		require.Len(t, client.SpaceBlobAddInvocations, 1)
		require.Equal(t, expectedData, client.SpaceBlobAddInvocations[0].BlobAdded)
		require.Equal(t, spaceDID, client.SpaceBlobAddInvocations[0].Space)

		// Then it should `space/blob/replicate` it.
		require.Len(t, client.SpaceBlobReplicateInvocations, 1)
		require.Equal(t, firstIndex.Digest(), client.SpaceBlobReplicateInvocations[0].Blob.Digest)
		require.Equal(t, firstIndex.Size(), client.SpaceBlobReplicateInvocations[0].Blob.Size)
		require.Equal(t, spaceDID, client.SpaceBlobReplicateInvocations[0].Space)
		require.Equal(t, uint(3), client.SpaceBlobReplicateInvocations[0].ReplicaCount)
		require.Equal(t, client.SpaceBlobAddInvocations[0].ReturnedLocation, client.SpaceBlobReplicateInvocations[0].LocationCommitment)

		// And it should `space/index/add` it.
		require.Len(t, client.SpaceIndexAddInvocations, 1)
		require.Equal(t, spaceDID, client.SpaceIndexAddInvocations[0].Space)
		require.Equal(t, firstIndex.CID(), client.SpaceIndexAddInvocations[0].IndexCID)
		require.Equal(t, firstIndex.Size(), client.SpaceIndexAddInvocations[0].IndexSize)
		require.Equal(t, rootCID, client.SpaceIndexAddInvocations[0].RootCID)

		// But it should not `filecoin/offer` it, because it's an index
		require.Len(t, client.FilecoinOfferInvocations, 0)

		// It should have closed the first index's reader.
		require.Contains(t, indexReadersClosed, firstIndex.ID(), "expected first index reader to be closed now")

		// Now close the upload indexes and run it again.
		err = blobsApi.CloseUploadIndexes(t.Context(), upload.ID(), recordClosedIndex)
		require.NoError(t, err)
		require.Len(t, indexes, 2)
		err = api.AddIndexesForUpload(t.Context(), upload.ID(), spaceDID)
		require.NoError(t, err)

		// Reload second shard
		secondIndex, err := repo.GetIndexByID(t.Context(), indexes[1].ID())
		require.NoError(t, err)
		require.Equal(t, model.BlobStateAdded, secondIndex.State(), "expected second index to be marked as added now")

		// This run should `space/blob/add` the second, newly closed shard.
		require.Len(t, client.SpaceBlobAddInvocations, 2)
		require.Equal(t, fmt.Append(nil, "CAR OF INDEX: ", secondIndex.ID(), padding), client.SpaceBlobAddInvocations[1].BlobAdded)
		require.Equal(t, spaceDID, client.SpaceBlobAddInvocations[1].Space)

		// Then it should `space/blob/replicate` it.
		require.Len(t, client.SpaceBlobReplicateInvocations, 2)
		require.Equal(t, secondIndex.Digest(), client.SpaceBlobReplicateInvocations[1].Blob.Digest)
		require.Equal(t, secondIndex.Size(), client.SpaceBlobReplicateInvocations[1].Blob.Size)
		require.Equal(t, spaceDID, client.SpaceBlobReplicateInvocations[1].Space)
		require.Equal(t, uint(3), client.SpaceBlobReplicateInvocations[1].ReplicaCount)
		require.Equal(t, client.SpaceBlobAddInvocations[1].ReturnedLocation, client.SpaceBlobReplicateInvocations[1].LocationCommitment)

		// It should have closed the second index's reader.
		require.Contains(t, indexReadersClosed, secondIndex.ID(), "expected second index reader to be closed now")
	})
}

func TestAddStorachaUploadForUpload(t *testing.T) {
	t.Run("`upload/add`s the root and shards", func(t *testing.T) {
		db := testdb.CreateTestDB(t)
		repo := sqlrepo.New(db)
		spaceDID, err := did.Parse("did:storacha:space:example")
		require.NoError(t, err)
		client := mockclient.MockClient{}

		api := storacha.API{
			Repo:                  repo,
			Client:                &client,
			BlobUploadParallelism: 1,
		}

		upload, _ := testutil.CreateUpload(t, repo, spaceDID, spacesmodel.WithShardSize(1<<16))

		rootLink := stestutil.RandomCID(t)

		data1 := append([]byte("CAR OF SHARD 1"), padding...)
		shard1Digest, err := multihash.Sum(data1, multihash.SHA2_256, -1)
		require.NoError(t, err)
		commp := &commp.Calc{}
		commp.Write(data1)
		piece1CID, err := commcid.DataCommitmentToPieceCidv2(commp.Sum(nil), uint64(len(data1)))
		require.NoError(t, err)
		shard1, err := repo.CreateShard(t.Context(), upload.ID(), 10, nil, nil)
		require.NoError(t, err)
		err = shard1.Close(shard1Digest, piece1CID)
		require.NoError(t, err)
		err = shard1.Added()
		require.NoError(t, err)
		err = repo.UpdateShard(t.Context(), shard1)
		require.NoError(t, err)

		data2 := append([]byte("CAR OF SHARD 2"), padding...)
		shard2Digest, err := multihash.Sum(data2, multihash.SHA2_256, -1)
		require.NoError(t, err)
		commp.Write(data2)
		piece2CID, err := commcid.DataCommitmentToPieceCidv2(commp.Sum(nil), uint64(len(data2)))
		require.NoError(t, err)
		shard2, err := repo.CreateShard(t.Context(), upload.ID(), 20, nil, nil)
		require.NoError(t, err)
		err = shard2.Close(shard2Digest, piece2CID)
		require.NoError(t, err)
		err = shard2.Added()
		require.NoError(t, err)
		err = repo.UpdateShard(t.Context(), shard2)
		require.NoError(t, err)

		upload.SetRootCID(rootLink.(cidlink.Link).Cid)
		err = repo.UpdateUpload(t.Context(), upload)
		require.NoError(t, err)

		err = api.AddStorachaUploadForUpload(t.Context(), upload.ID(), spaceDID)
		require.NoError(t, err)

		require.Len(t, client.UploadAddInvocations, 1)
		require.Equal(t, spaceDID, client.UploadAddInvocations[0].Space)
		require.Equal(t, rootLink, client.UploadAddInvocations[0].Root)
		require.ElementsMatch(t, []cidlink.Link{
			{Cid: cid.NewCidV1(uint64(multicodec.Car), shard1Digest)},
			{Cid: cid.NewCidV1(uint64(multicodec.Car), shard2Digest)},
		}, client.UploadAddInvocations[0].Shards)
	})
}

type blobReadCloser struct {
	io.Reader
	blobReadersClosed map[id.ID]struct{}
	blobID            id.ID
}

func (sc *blobReadCloser) Close() error {
	sc.blobReadersClosed[sc.blobID] = struct{}{}
	return nil
}
