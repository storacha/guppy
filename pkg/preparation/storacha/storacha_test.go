package storacha_test

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"testing"

	"github.com/ipfs/go-cid"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multihash"
	"github.com/storacha/go-libstoracha/testutil"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/guppy/pkg/client"
	"github.com/storacha/guppy/pkg/preparation/internal/testdb"
	"github.com/storacha/guppy/pkg/preparation/shards"
	"github.com/storacha/guppy/pkg/preparation/shards/model"
	spacesmodel "github.com/storacha/guppy/pkg/preparation/spaces/model"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo/util"
	"github.com/storacha/guppy/pkg/preparation/storacha"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	"github.com/stretchr/testify/require"
)

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

type mockSpaceBlobAdder struct {
	T           *testing.T
	invocations []spaceBlobAddInvocation
}

type spaceBlobAddInvocation struct {
	contentRead  []byte
	spaceAddedTo did.DID
}

var _ storacha.SpaceBlobAdder = (*mockSpaceBlobAdder)(nil)

func (m *mockSpaceBlobAdder) SpaceBlobAdd(ctx context.Context, content io.Reader, space did.DID, options ...client.SpaceBlobAddOption) (multihash.Multihash, delegation.Delegation, error) {
	contentBytes, err := io.ReadAll(content)
	require.NoError(m.T, err, "reading content for SpaceBlobAdd")

	m.invocations = append(m.invocations, spaceBlobAddInvocation{
		contentRead:  contentBytes,
		spaceAddedTo: space,
	})

	return []byte{}, nil, nil
}

func TestSpaceBlobAddShardsForUpload(t *testing.T) {
	t.Run("`space/blob/add`s a CAR for each shard", func(t *testing.T) {
		db := testdb.CreateTestDB(t)
		repo := sqlrepo.New(db)
		spaceDID, err := did.Parse("did:storacha:space:example")
		require.NoError(t, err)
		spaceBlobAdder := mockSpaceBlobAdder{T: t}

		carForShard := func(ctx context.Context, shard *model.Shard) (io.Reader, error) {
			nodes := nodesInShard(ctx, t, db, shard.ID())
			b := []byte("CAR CONTAINING NODES:")
			for _, n := range nodes {
				b = append(b, ' ')
				b = append(b, []byte(n.String())...)
			}
			return bytes.NewReader(b), nil
		}

		api := storacha.API{
			Repo:        repo,
			Client:      &spaceBlobAdder,
			Space:       spaceDID,
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
		_, err = shardsApi.AddNodeToUploadShards(t.Context(), upload.ID(), nodeCid1)
		require.NoError(t, err)
		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCid2, 1<<14, spaceDID, "some/other/path", source.ID(), 0)
		require.NoError(t, err)
		_, err = shardsApi.AddNodeToUploadShards(t.Context(), upload.ID(), nodeCid2)
		require.NoError(t, err)
		_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCid3, 1<<15, spaceDID, "yet/other/path", source.ID(), 0)
		require.NoError(t, err)
		_, err = shardsApi.AddNodeToUploadShards(t.Context(), upload.ID(), nodeCid3)
		require.NoError(t, err)

		// Upload shards that are ready to go.
		err = api.SpaceBlobAddShardsForUpload(t.Context(), upload.ID())
		require.NoError(t, err)

		// This run should `space/blob/add` the first, closed shard.
		require.Len(t, spaceBlobAdder.invocations, 1)
		require.NotEmpty(t, spaceBlobAdder.invocations[0].contentRead)
		require.Equal(t, fmt.Appendf(nil, "CAR CONTAINING NODES: %s %s", nodeCid1, nodeCid2), spaceBlobAdder.invocations[0].contentRead)
		require.Equal(t, spaceDID, spaceBlobAdder.invocations[0].spaceAddedTo)

		// Now close the upload shards and run it again.
		_, err = shardsApi.CloseUploadShards(t.Context(), upload.ID())
		require.NoError(t, err)
		err = api.SpaceBlobAddShardsForUpload(t.Context(), upload.ID())
		require.NoError(t, err)

		// This run should `space/blob/add` the second, newly closed shard.
		require.Len(t, spaceBlobAdder.invocations, 2)
		require.NotEmpty(t, spaceBlobAdder.invocations[1].contentRead)
		require.Equal(t, fmt.Appendf(nil, "CAR CONTAINING NODES: %s", nodeCid3), spaceBlobAdder.invocations[1].contentRead)
		require.Equal(t, spaceDID, spaceBlobAdder.invocations[1].spaceAddedTo)
	})
}
