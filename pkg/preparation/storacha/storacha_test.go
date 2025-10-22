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
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"github.com/storacha/go-libstoracha/testutil"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/guppy/pkg/preparation/internal/mockclient"
	"github.com/storacha/guppy/pkg/preparation/internal/testdb"
	"github.com/storacha/guppy/pkg/preparation/shards"
	"github.com/storacha/guppy/pkg/preparation/shards/model"
	spacesmodel "github.com/storacha/guppy/pkg/preparation/spaces/model"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	"github.com/storacha/guppy/pkg/preparation/storacha"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	uploadsmodel "github.com/storacha/guppy/pkg/preparation/uploads/model"
	"github.com/stretchr/testify/require"
)

// commP is not defined for inputs shorter than 65 bytes, so add 65 bytes of
// padding to every "CAR" to make sure it's definitely long enough.
var padding = bytes.Repeat([]byte{0}, 65)

func TestAddShardsForUpload(t *testing.T) {
	t.Run("`space/blob/add`s, `space/blob/replicate`s, and `filecoin/offer`s a CAR for each shard", func(t *testing.T) {
		db := testdb.CreateTestDB(t)
		repo := sqlrepo.New(db)
		spaceDID, err := did.Parse("did:storacha:space:example")
		require.NoError(t, err)
		client := mockclient.MockClient{T: t}

		carForShard := func(ctx context.Context, shardID id.ShardID) (io.Reader, error) {
			return bytes.NewReader(fmt.Append(nil, "CAR OF SHARD: ", shardID, padding)), nil
		}

		api := storacha.API{
			Repo:        repo,
			Client:      &client,
			CarForShard: carForShard,
		}

		shardsApi := shards.API{
			Repo: repo,
		}

		_, err = repo.FindOrCreateSpace(t.Context(), spaceDID, "Test Space", spacesmodel.WithShardSize(1<<16))
		require.NoError(t, err)
		source, err := repo.CreateSource(t.Context(), "Test Source", ".")
		require.NoError(t, err)
		uploads, err := repo.FindOrCreateUploads(t.Context(), spaceDID, []id.SourceID{source.ID()})
		require.NoError(t, err)
		require.Len(t, uploads, 1)
		upload := uploads[0]

		nodeCid1 := testutil.RandomCID(t).(cidlink.Link).Cid
		nodeCid2 := testutil.RandomCID(t).(cidlink.Link).Cid
		nodeCid3 := testutil.RandomCID(t).(cidlink.Link).Cid

		// Add enough nodes to close one shard and create a second one.
		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCid1, 1<<14, spaceDID, "some/path", source.ID(), 0)
		require.NoError(t, err)
		_, err = shardsApi.AddNodeToUploadShards(t.Context(), upload.ID(), spaceDID, nodeCid1)
		require.NoError(t, err)
		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCid2, 1<<14, spaceDID, "some/other/path", source.ID(), 0)
		require.NoError(t, err)
		_, err = shardsApi.AddNodeToUploadShards(t.Context(), upload.ID(), spaceDID, nodeCid2)
		require.NoError(t, err)
		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCid3, 1<<15, spaceDID, "yet/other/path", source.ID(), 0)
		require.NoError(t, err)
		_, err = shardsApi.AddNodeToUploadShards(t.Context(), upload.ID(), spaceDID, nodeCid3)
		require.NoError(t, err)

		shards, err := repo.ShardsForUploadByStatus(t.Context(), upload.ID(), model.ShardStateClosed)
		require.NoError(t, err)
		require.Len(t, shards, 1)
		firstShard := shards[0]

		shards, err = repo.ShardsForUploadByStatus(t.Context(), upload.ID(), model.ShardStateOpen)
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
		require.Equal(t, model.ShardStateAdded, firstShard.State(), "expected first shard to be marked as added now")
		require.Equal(t, model.ShardStateOpen, secondShard.State(), "expected second shard to remain open")

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

		cp := &commp.Calc{}
		_, err = cp.Write(expectedData)
		require.NoError(t, err)
		digest, _, err := cp.Digest()
		require.NoError(t, err)
		shardPieceCID, err := commcid.DataCommitmentToPieceCidv2(digest, firstShard.Size())
		require.NoError(t, err)
		shardPieceLink := cidlink.Link{Cid: shardPieceCID}

		require.Equal(t, shardPieceLink, client.FilecoinOfferInvocations[0].Piece)

		// Now close the upload shards and run it again.
		_, err = shardsApi.CloseUploadShards(t.Context(), upload.ID())
		require.NoError(t, err)
		err = api.AddShardsForUpload(t.Context(), upload.ID(), spaceDID)
		require.NoError(t, err)

		// Reload second shard
		secondShard, err = repo.GetShardByID(t.Context(), secondShard.ID())
		require.NoError(t, err)
		require.Equal(t, model.ShardStateAdded, secondShard.State(), "expected second shard to be marked as added now")

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
	})

	t.Run("with a shard too small for a CommP, avoids `filecoin/offer`ing it", func(t *testing.T) {
		db := testdb.CreateTestDB(t)
		repo := sqlrepo.New(db)
		spaceDID, err := did.Parse("did:storacha:space:example")
		require.NoError(t, err)
		client := mockclient.MockClient{T: t}

		carForShard := func(ctx context.Context, shardID id.ShardID) (io.Reader, error) {
			// Note that the decision to skip `filecoin/offer` is based on the size
			// listed on the shard, which is based on the size of the nodes added to
			// it, not the actual CAR bytes (which are written lazily). So the
			// behavior is triggered by the node below being 1, while the short CAR
			// is here to cause an error if we *do* try to `filecoin/offer` it.
			return bytes.NewReader([]byte("VERY SHORT CAR")), nil
		}

		api := storacha.API{
			Repo:        repo,
			Client:      &client,
			CarForShard: carForShard,
		}

		shardsApi := shards.API{
			Repo: repo,
		}

		_, err = repo.FindOrCreateSpace(t.Context(), spaceDID, "Test Space", spacesmodel.WithShardSize(1<<16))
		require.NoError(t, err)
		source, err := repo.CreateSource(t.Context(), "Test Source", ".")
		require.NoError(t, err)
		uploads, err := repo.FindOrCreateUploads(t.Context(), spaceDID, []id.SourceID{source.ID()})
		require.NoError(t, err)
		require.Len(t, uploads, 1)
		upload := uploads[0]

		nodeCid1 := testutil.RandomCID(t).(cidlink.Link).Cid

		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCid1, 1, spaceDID, "some/path", source.ID(), 0)
		require.NoError(t, err)
		_, err = shardsApi.AddNodeToUploadShards(t.Context(), upload.ID(), spaceDID, nodeCid1)
		require.NoError(t, err)
		_, err = shardsApi.CloseUploadShards(t.Context(), upload.ID())
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
		db := testdb.CreateTestDB(t)
		repo := sqlrepo.New(db)
		spaceDID, err := did.Parse("did:storacha:space:example")
		require.NoError(t, err)
		client := mockclient.MockClient{T: t}

		IndexesForUpload := func(ctx context.Context, upload *uploadsmodel.Upload) ([]io.Reader, error) {
			return []io.Reader{
				bytes.NewReader([]byte(fmt.Sprintf("INDEX 1 OF UPLOAD: %s", upload.ID()))),
				bytes.NewReader([]byte(fmt.Sprintf("INDEX 2 OF UPLOAD: %s", upload.ID()))),
			}, nil
		}

		api := storacha.API{
			Repo:             repo,
			Client:           &client,
			IndexesForUpload: IndexesForUpload,
		}

		_, err = repo.FindOrCreateSpace(t.Context(), spaceDID, "Test Space", spacesmodel.WithShardSize(1<<16))
		require.NoError(t, err)
		source, err := repo.CreateSource(t.Context(), "Test Source", ".")
		require.NoError(t, err)
		uploads, err := repo.FindOrCreateUploads(t.Context(), spaceDID, []id.SourceID{source.ID()})
		require.NoError(t, err)
		require.Len(t, uploads, 1)
		upload := uploads[0]

		err = api.AddIndexesForUpload(t.Context(), upload.ID(), spaceDID)
		require.NoError(t, err)

		expectedIndexBlobs := [][]byte{
			fmt.Appendf(nil, "INDEX 1 OF UPLOAD: %s", upload.ID()),
			fmt.Appendf(nil, "INDEX 2 OF UPLOAD: %s", upload.ID()),
		}

		require.Len(t, client.SpaceBlobAddInvocations, len(expectedIndexBlobs))
		require.Len(t, client.SpaceBlobReplicateInvocations, len(expectedIndexBlobs))
		require.Len(t, client.SpaceIndexAddInvocations, len(expectedIndexBlobs))

		for i, blob := range expectedIndexBlobs {
			require.Equal(t, blob, client.SpaceBlobAddInvocations[i].BlobAdded)
			require.Equal(t, spaceDID, client.SpaceBlobAddInvocations[i].Space)

			require.Equal(t, client.SpaceBlobAddInvocations[i].ReturnedLocation, client.SpaceBlobReplicateInvocations[i].LocationCommitment)
			require.Equal(t, spaceDID, client.SpaceBlobReplicateInvocations[i].Space)
			require.Equal(t, uint(3), client.SpaceBlobReplicateInvocations[i].ReplicaCount)

			cid, err := cid.V1Builder{Codec: uint64(multicodec.Car), MhType: multihash.SHA2_256}.Sum(blob)
			require.NoError(t, err)

			require.Equal(t, cidlink.Link{Cid: cid}, client.SpaceIndexAddInvocations[i].IndexLink)
		}
	})
}

func TestAddStorachaUploadForUpload(t *testing.T) {
	t.Run("`upload/add`s the root and shards", func(t *testing.T) {
		db := testdb.CreateTestDB(t)
		repo := sqlrepo.New(db)
		spaceDID, err := did.Parse("did:storacha:space:example")
		require.NoError(t, err)
		client := mockclient.MockClient{}

		indexesForUpload := func(ctx context.Context, upload *uploadsmodel.Upload) ([]io.Reader, error) {
			return []io.Reader{}, nil
		}

		api := storacha.API{
			Repo:             repo,
			Client:           &client,
			IndexesForUpload: indexesForUpload,
		}

		_, err = repo.FindOrCreateSpace(t.Context(), spaceDID, "Test Space", spacesmodel.WithShardSize(1<<16))
		require.NoError(t, err)
		source, err := repo.CreateSource(t.Context(), "Test Source", ".")
		require.NoError(t, err)
		uploads, err := repo.FindOrCreateUploads(t.Context(), spaceDID, []id.SourceID{source.ID()})
		require.NoError(t, err)
		require.Len(t, uploads, 1)
		upload := uploads[0]

		rootLink := testutil.RandomCID(t)

		shard1Digest, err := multihash.Sum(append([]byte("CAR OF SHARD 1"), padding...), multihash.SHA2_256, -1)
		require.NoError(t, err)
		shard1, err := repo.CreateShard(t.Context(), upload.ID(), 10)
		require.NoError(t, err)
		err = shard1.Close()
		require.NoError(t, err)
		err = shard1.Added(shard1Digest)
		require.NoError(t, err)
		err = repo.UpdateShard(t.Context(), shard1)
		require.NoError(t, err)

		shard2Digest, err := multihash.Sum(append([]byte("CAR OF SHARD 2"), padding...), multihash.SHA2_256, -1)
		require.NoError(t, err)
		shard2, err := repo.CreateShard(t.Context(), upload.ID(), 20)
		require.NoError(t, err)
		err = shard2.Close()
		require.NoError(t, err)
		err = shard2.Added(shard2Digest)
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
