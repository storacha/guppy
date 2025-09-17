package storacha_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

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

func TestSpaceBlobAddShardsForUpload(t *testing.T) {
	t.Run("`space/blob/add`s a CAR for each shard", func(t *testing.T) {
		db := testdb.CreateTestDB(t)
		repo := sqlrepo.New(db)
		spaceDID, err := did.Parse("did:storacha:space:example")
		require.NoError(t, err)
		client := mockclient.MockClient{}

		carForShard := func(ctx context.Context, shardID id.ShardID) (io.Reader, error) {
			return bytes.NewReader(fmt.Appendf(nil, "CAR OF SHARD: %s", shardID)), nil
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
		uploads, err := repo.CreateUploads(t.Context(), spaceDID, []id.SourceID{source.ID()})
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
		err = api.SpaceBlobAddShardsForUpload(t.Context(), upload.ID(), spaceDID)
		require.NoError(t, err)

		// This run should `space/blob/add` the first, closed shard.
		require.Len(t, client.SpaceBlobAddInvocations, 1)
		require.Equal(t, fmt.Appendf(nil, "CAR OF SHARD: %s", firstShard.ID()), client.SpaceBlobAddInvocations[0].BlobAdded)
		require.Equal(t, spaceDID, client.SpaceBlobAddInvocations[0].Space)

		// Now close the upload shards and run it again.
		_, err = shardsApi.CloseUploadShards(t.Context(), upload.ID())
		require.NoError(t, err)
		err = api.SpaceBlobAddShardsForUpload(t.Context(), upload.ID(), spaceDID)
		require.NoError(t, err)

		// This run should `space/blob/add` the second, newly closed shard.
		require.Len(t, client.SpaceBlobAddInvocations, 2)
		require.Equal(t, fmt.Appendf(nil, "CAR OF SHARD: %s", secondShard.ID()), client.SpaceBlobAddInvocations[1].BlobAdded)
		require.Equal(t, spaceDID, client.SpaceBlobAddInvocations[1].Space)
	})
}

func TestAddIndexesForUpload(t *testing.T) {
	t.Run("`space/blob/add`s an index CAR", func(t *testing.T) {
		db := testdb.CreateTestDB(t)
		repo := sqlrepo.New(db)
		spaceDID, err := did.Parse("did:storacha:space:example")
		require.NoError(t, err)
		client := mockclient.MockClient{}

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
		uploads, err := repo.CreateUploads(t.Context(), spaceDID, []id.SourceID{source.ID()})
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
		require.Len(t, client.SpaceIndexAddInvocations, len(expectedIndexBlobs))

		for i, blob := range expectedIndexBlobs {
			require.Equal(t, blob, client.SpaceBlobAddInvocations[i].BlobAdded)
			require.Equal(t, spaceDID, client.SpaceBlobAddInvocations[i].Space)

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
		uploads, err := repo.CreateUploads(t.Context(), spaceDID, []id.SourceID{source.ID()})
		require.NoError(t, err)
		require.Len(t, uploads, 1)
		upload := uploads[0]

		rootLink := testutil.RandomCID(t)

		shard1Digest, err := multihash.Sum([]byte("CAR OF SHARD 1"), multihash.SHA2_256, -1)
		require.NoError(t, err)
		shard1, err := repo.CreateShard(t.Context(), upload.ID(), 10)
		require.NoError(t, err)
		err = shard1.Close()
		require.NoError(t, err)
		err = shard1.Added(shard1Digest)
		require.NoError(t, err)
		err = repo.UpdateShard(t.Context(), shard1)
		require.NoError(t, err)

		shard2Digest, err := multihash.Sum([]byte("CAR OF SHARD 2"), multihash.SHA2_256, -1)
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
