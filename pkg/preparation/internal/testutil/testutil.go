package testutil

import (
	"fmt"
	"testing"

	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	stestutil "github.com/storacha/go-libstoracha/testutil"
	"github.com/storacha/go-ucanto/did"
	"github.com/stretchr/testify/require"

	"github.com/storacha/guppy/pkg/preparation/blobs"
	"github.com/storacha/guppy/pkg/preparation/blobs/model"
	sourcesmodel "github.com/storacha/guppy/pkg/preparation/sources/model"
	spacesmodel "github.com/storacha/guppy/pkg/preparation/spaces/model"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	uploadsmodel "github.com/storacha/guppy/pkg/preparation/uploads/model"
)

var sourceCounter = 0

func CreateUpload(t *testing.T, repo *sqlrepo.Repo, spaceDID did.DID, options ...spacesmodel.SpaceOption) (*uploadsmodel.Upload, *sourcesmodel.Source) {
	t.Helper()

	_, err := repo.FindOrCreateSpace(t.Context(), spaceDID, "Test Space", options...)
	require.NoError(t, err)
	source, err := repo.CreateSource(t.Context(), fmt.Sprintf("Test Source %d", sourceCounter), fmt.Sprintf(".%d", sourceCounter))
	require.NoError(t, err)
	sourceCounter++
	uploads, err := repo.FindOrCreateUploads(t.Context(), spaceDID, []id.SourceID{source.ID()})
	require.NoError(t, err)
	require.Len(t, uploads, 1)
	return uploads[0], source
}

func AddNodeToUploadShards(t *testing.T, repo *sqlrepo.Repo, blobsApi blobs.API, uploadID id.UploadID, sourceID id.SourceID, spaceDID did.DID, shardCB func(shard *model.Shard) error, size uint64) {
	t.Helper()

	data := stestutil.RandomBytes(t, int(size))
	AddNodeToUploadShardsWithData(t, repo, blobsApi, uploadID, sourceID, spaceDID, shardCB, data)
}

func AddNodeToUploadShardsWithData(t *testing.T, repo *sqlrepo.Repo, blobsApi blobs.API, uploadID id.UploadID, sourceID id.SourceID, spaceDID did.DID, shardCB func(shard *model.Shard) error, data []byte) {
	t.Helper()

	nodeCID := stestutil.RandomCID(t).(cidlink.Link).Cid
	path := fmt.Sprintf("some/path/%s", nodeCID.String())
	_, _, err := repo.FindOrCreateRawNode(t.Context(), nodeCID, uint64(len(data)), spaceDID, path, sourceID, 0)
	require.NoError(t, err)
	// Create node_upload record first (required since AddNodeToShard now does UPDATE instead of INSERT)
	_, err = repo.FindOrCreateNodeUpload(t.Context(), uploadID, nodeCID, spaceDID)
	require.NoError(t, err)
	err = blobsApi.AddNodeToUploadShards(t.Context(), uploadID, spaceDID, nodeCID, data, shardCB)
	require.NoError(t, err)
}
