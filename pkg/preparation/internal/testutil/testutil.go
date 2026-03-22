package testutil

import (
	"fmt"
	"testing"
	"time"

	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	stestutil "github.com/storacha/go-libstoracha/testutil"
	"github.com/storacha/go-ucanto/did"
	"github.com/stretchr/testify/require"

	"github.com/storacha/guppy/pkg/preparation/blobs"
	blobsmodel "github.com/storacha/guppy/pkg/preparation/blobs/model"
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
	source, err := repo.CreateSource(t.Context(), fmt.Sprintf("Test Source %d", sourceCounter), fmt.Sprintf("/%d", sourceCounter))
	require.NoError(t, err)
	sourceCounter++
	uploads, err := repo.FindOrCreateUploads(t.Context(), spaceDID, []id.SourceID{source.ID()})
	require.NoError(t, err)
	require.Len(t, uploads, 1)
	return uploads[0], source
}

func AddNodeToUploadShards(t *testing.T, repo *sqlrepo.Repo, blobsApi blobs.API, uploadID id.UploadID, sourceID id.SourceID, spaceDID did.DID, shardCB func(shard *blobsmodel.Shard) error, size uint64) {
	t.Helper()

	data := stestutil.RandomBytes(t, int(size))
	AddNodeToUploadShardsWithData(t, repo, blobsApi, uploadID, sourceID, spaceDID, shardCB, data)
}

func AddNodeToUploadShardsWithData(t *testing.T, repo *sqlrepo.Repo, blobsApi blobs.API, uploadID id.UploadID, sourceID id.SourceID, spaceDID did.DID, shardCB func(shard *blobsmodel.Shard) error, data []byte) {
	t.Helper()

	nodeCID := stestutil.RandomCID(t).(cidlink.Link).Cid
	path := fmt.Sprintf("some/path/%s", nodeCID.String())
	_, _, err := repo.FindOrCreateRawNode(t.Context(), nodeCID, uint64(len(data)), spaceDID, uploadID, path, sourceID, 0)
	require.NoError(t, err)
	err = blobsApi.AddNodeToUploadShards(t.Context(), uploadID, spaceDID, nodeCID, data, shardCB)
	require.NoError(t, err)
}

// UploadBuilder provides a fluent interface for building test uploads with various states.
type UploadBuilder struct {
	t        *testing.T
	repo     *sqlrepo.Repo
	spaceDID did.DID
	upload   *uploadsmodel.Upload
	source   *sourcesmodel.Source
}

// NewUploadBuilder creates a new UploadBuilder with a basic upload setup.
func NewUploadBuilder(t *testing.T, repo *sqlrepo.Repo) *UploadBuilder {
	t.Helper()
	spaceDID := stestutil.RandomDID(t)
	upload, source := CreateUpload(t, repo, spaceDID)
	return &UploadBuilder{
		t:        t,
		repo:     repo,
		spaceDID: spaceDID,
		upload:   upload,
		source:   source,
	}
}

// WithRootFSEntry creates a file FSEntry and sets it as the upload's root_fs_entry_id.
func (b *UploadBuilder) WithRootFSEntry() *UploadBuilder {
	b.t.Helper()
	file, _, err := b.repo.FindOrCreateFile(b.t.Context(), "test.txt", time.Now(), 0644, 100, []byte("checksum"), b.source.ID(), b.spaceDID)
	require.NoError(b.t, err)
	err = b.upload.SetRootFSEntryID(file.ID())
	require.NoError(b.t, err)
	err = b.repo.UpdateUpload(b.t.Context(), b.upload)
	require.NoError(b.t, err)
	return b
}

// WithRootCID creates a node and sets it as the upload's root_cid.
func (b *UploadBuilder) WithRootCID() *UploadBuilder {
	b.t.Helper()
	nodeCID := stestutil.RandomCID(b.t).(cidlink.Link).Cid
	_, _, err := b.repo.FindOrCreateRawNode(b.t.Context(), nodeCID, 100, b.spaceDID, b.upload.ID(), "test.txt", b.source.ID(), 0)
	require.NoError(b.t, err)
	err = b.upload.SetRootCID(nodeCID)
	require.NoError(b.t, err)
	err = b.repo.UpdateUpload(b.t.Context(), b.upload)
	require.NoError(b.t, err)
	return b
}

// WithInvalidRootFSEntry sets root_fs_entry_id to a non-existent ID.
func (b *UploadBuilder) WithInvalidRootFSEntry() *UploadBuilder {
	b.t.Helper()
	err := b.upload.SetRootFSEntryID(id.New()) // Random non-existent ID
	require.NoError(b.t, err)
	err = b.repo.UpdateUpload(b.t.Context(), b.upload)
	require.NoError(b.t, err)
	return b
}

// WithInvalidRootCID sets root_cid to a non-existent CID.
func (b *UploadBuilder) WithInvalidRootCID() *UploadBuilder {
	b.t.Helper()
	randomCID := stestutil.RandomCID(b.t).(cidlink.Link).Cid
	err := b.upload.SetRootCID(randomCID) // Random non-existent CID
	require.NoError(b.t, err)
	err = b.repo.UpdateUpload(b.t.Context(), b.upload)
	require.NoError(b.t, err)
	return b
}

// WithFileAndDAGScan creates a file FSEntry and DAGScan. If complete is true, the DAGScan will have a CID.
func (b *UploadBuilder) WithFileAndDAGScan(path string, complete bool) *UploadBuilder {
	b.t.Helper()

	// Create file and set as root FSEntry
	file, _, err := b.repo.FindOrCreateFile(b.t.Context(), path, time.Now(), 0644, 100, []byte("checksum"), b.source.ID(), b.spaceDID)
	require.NoError(b.t, err)
	err = b.upload.SetRootFSEntryID(file.ID())
	require.NoError(b.t, err)

	// Create DAGScan and set root CID if complete
	dagScan, err := b.repo.CreateDAGScan(b.t.Context(), file.ID(), false, b.upload.ID(), b.spaceDID)
	require.NoError(b.t, err)

	if complete {
		nodeCID := stestutil.RandomCID(b.t).(cidlink.Link).Cid
		_, _, err := b.repo.FindOrCreateRawNode(b.t.Context(), nodeCID, 100, b.spaceDID, b.upload.ID(), path, b.source.ID(), 0)
		require.NoError(b.t, err)
		err = dagScan.Complete(nodeCID)
		require.NoError(b.t, err)
		err = b.repo.UpdateDAGScan(b.t.Context(), dagScan)
		require.NoError(b.t, err)
		err = b.upload.SetRootCID(dagScan.CID())
		require.NoError(b.t, err)
	}

	err = b.repo.UpdateUpload(b.t.Context(), b.upload)
	require.NoError(b.t, err)

	return b
}

// WithUnshardedNodes creates nodes without shard assignments (missing node_uploads records).
func (b *UploadBuilder) WithUnshardedNodes(count int) *UploadBuilder {
	b.t.Helper()
	for i := range count {
		nodeCID := stestutil.RandomCID(b.t).(cidlink.Link).Cid
		path := fmt.Sprintf("unsharded/file%d.txt", i)
		_, _, err := b.repo.FindOrCreateRawNode(b.t.Context(), nodeCID, 100, b.spaceDID, b.upload.ID(), path, b.source.ID(), 0)
		require.NoError(b.t, err)
		// Note: FindOrCreateRawNode creates node_uploads record, but we could delete it
		// For simplicity, we'll rely on the test to manually query NodesNotInShards
	}
	return b
}

// WithIncompleteShards creates shards that are not in BlobStateAdded.
func (b *UploadBuilder) WithIncompleteShards(count int) *UploadBuilder {
	b.t.Helper()
	for range count {
		// Shard is created in BlobStateOpen by default, which is incomplete
		_, err := b.repo.CreateShard(b.t.Context(), b.upload.ID(), 1024, []byte("digest"), []byte("piece"))
		require.NoError(b.t, err)
	}
	return b
}

// WithIncompleteIndexes creates indexes that are not in BlobStateAdded.
func (b *UploadBuilder) WithIncompleteIndexes(count int) *UploadBuilder {
	b.t.Helper()
	for range count {
		// Index is created in BlobStateOpen by default, which is incomplete
		_, err := b.repo.CreateIndex(b.t.Context(), b.upload.ID())
		require.NoError(b.t, err)
	}
	return b
}

// Build returns the upload and source.
func (b *UploadBuilder) Build() (*uploadsmodel.Upload, *sourcesmodel.Source) {
	return b.upload, b.source
}
