package preparation_test

import (
	"bytes"
	"context"
	"io"
	"io/fs"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/files"
	"github.com/ipfs/boxo/ipld/merkledag"
	unixfile "github.com/ipfs/boxo/ipld/unixfs/file"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipld/go-car/v2/blockstore"
	"github.com/multiformats/go-multihash"
	"github.com/spf13/afero"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/testing/helpers"
	"github.com/storacha/guppy/pkg/client"
	ctestutil "github.com/storacha/guppy/pkg/client/testutil"
	"github.com/storacha/guppy/pkg/preparation"
	configurationsmodel "github.com/storacha/guppy/pkg/preparation/configurations/model"
	"github.com/storacha/guppy/pkg/preparation/shards"
	"github.com/storacha/guppy/pkg/preparation/shards/model"
	"github.com/storacha/guppy/pkg/preparation/sources"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	"github.com/storacha/guppy/pkg/preparation/testutil"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func randomBytes(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(rand.Intn(256))
	}
	return b
}

// spaceBlobAddClient is a [shards.SpaceBlobAdder] that wraps a [client.Client]
// to use a custom putClient.
type spaceBlobAddClient struct {
	*client.Client
	putClient *http.Client
}

var _ shards.SpaceBlobAdder = (*spaceBlobAddClient)(nil)

func (c *spaceBlobAddClient) SpaceBlobAdd(ctx context.Context, content io.Reader, space did.DID, options ...client.SpaceBlobAddOption) (multihash.Multihash, delegation.Delegation, error) {
	return c.Client.SpaceBlobAdd(ctx, content, space, append(options, client.WithPutClient(c.putClient))...)
}

// compositeBlockstore is a [blockstore.Blockstore] that combines multiple
// blockstores into one. It doesn't actually implement the entire interface, and
// is only suitable for testing purposes.
type compositeBlockstore struct {
	blockstores []blockstore.Blockstore
}

var _ blockstore.Blockstore = (*compositeBlockstore)(nil)

func (c *compositeBlockstore) Get(ctx context.Context, key cid.Cid) (blocks.Block, error) {
	for _, bs := range c.blockstores {
		if b, err := bs.Get(ctx, key); err == nil {
			return b, nil
		}
	}
	return nil, format.ErrNotFound{Cid: key}
}

func (c *compositeBlockstore) DeleteBlock(context.Context, cid.Cid) error {
	panic("not implemented")
}

func (c *compositeBlockstore) Has(context.Context, cid.Cid) (bool, error) {
	panic("not implemented")
}

func (c *compositeBlockstore) GetSize(context.Context, cid.Cid) (int, error) {
	panic("not implemented")
}

func (c *compositeBlockstore) Put(context.Context, blocks.Block) error {
	panic("not implemented")
}

func (c *compositeBlockstore) PutMany(context.Context, []blocks.Block) error {
	panic("not implemented")
}

func (c *compositeBlockstore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	panic("not implemented")
}

func (c *compositeBlockstore) HashOnRead(enabled bool) {
	panic("not implemented")
}

func TestExecuteUpload(t *testing.T) {
	// In case something goes wrong. This should never take this long.
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)

	aData := randomBytes(1 << 16)
	bData := randomBytes(1 << 16)
	cData := randomBytes(1 << 16)
	dData := randomBytes(1 << 16)

	memFS := afero.NewMemMapFs()
	memFS.MkdirAll("dir1/dir2", 0755)
	afero.WriteFile(memFS, "a", aData, 0644)
	afero.WriteFile(memFS, "dir1/b", bData, 0644)
	afero.WriteFile(memFS, "dir1/c", cData, 0644)
	afero.WriteFile(memFS, "dir1/dir2/d", dData, 0644)

	// Set the last modified time for the files; Afero's in-memory FS doesn't do
	// that automatically on creation, we expect it to be present.
	for _, path := range []string{".", "a", "dir1", "dir1/b", "dir1/c", "dir1/dir2", "dir1/dir2/d"} {
		err := memFS.Chtimes(path, time.Now(), time.Now())
		require.NoError(t, err)
	}
	repo := sqlrepo.New(testutil.CreateTestDB(t))

	putClient := ctestutil.NewPutClient()

	c := &spaceBlobAddClient{
		Client:    helpers.Must(ctestutil.SpaceBlobAddClient()),
		putClient: putClient,
	}

	// Use the client's issuer as the space DID to avoid any concerns about
	// authorization.
	spaceDID := c.Issuer().DID()

	api := preparation.NewAPI(
		repo,
		c,
		spaceDID,
		preparation.WithGetLocalFSForPathFn(func(path string) (fs.FS, error) {
			require.Equal(t, ".", path, "test expects root to be '.'")
			return afero.NewIOFS(memFS), nil
		}),
	)

	configuration, err := api.CreateConfiguration(ctx, "Large Upload Configuration", configurationsmodel.WithShardSize(1<<16))
	require.NoError(t, err)
	source, err := api.CreateSource(ctx, "Large Upload Source", ".")
	require.NoError(t, err)
	err = repo.AddSourceToConfiguration(ctx, configuration.ID(), source.ID())
	require.NoError(t, err)
	uploads, err := api.CreateUploads(ctx, configuration.ID())
	require.NoError(t, err)
	require.Len(t, uploads, 1, "expected exactly one upload to be created")
	upload := uploads[0]

	rootCid, err := api.ExecuteUpload(ctx, upload)
	require.NoError(t, err)

	openShards, err := repo.ShardsForUploadByStatus(ctx, uploads[0].ID(), model.ShardStateOpen)
	require.NoError(t, err)
	require.Len(t, openShards, 0, "expected no open shards at end of upload")

	closedShards, err := repo.ShardsForUploadByStatus(ctx, uploads[0].ID(), model.ShardStateClosed)
	require.NoError(t, err)
	require.Len(t, closedShards, 0, "expected no closed shards at end of upload")

	addedShards, err := repo.ShardsForUploadByStatus(ctx, uploads[0].ID(), model.ShardStateAdded)
	require.NoError(t, err)
	require.Len(t, addedShards, 5, "expected all shards to added be for the upload")

	putBlobs := ctestutil.ReceivedBlobs(putClient)
	require.Len(t, putBlobs, 5, "expected exactly 5 blobs to be added")

	blobBlockstores := make([]blockstore.Blockstore, 0, len(putBlobs))
	for _, blob := range putBlobs {
		bs, err := blockstore.NewReadOnly(bytes.NewReader(blob), nil)
		require.NoError(t, err)
		blobBlockstores = append(blobBlockstores, bs)
	}

	bs := &compositeBlockstore{
		blockstores: blobBlockstores,
	}

	blockserv := blockservice.New(bs, nil)
	dagserv := merkledag.NewDAGService(blockserv)
	rootNode, err := dagserv.Get(ctx, rootCid)
	require.NoError(t, err)
	rootFileNode, err := unixfile.NewUnixfsFile(ctx, dagserv, rootNode)
	require.NoError(t, err)

	foundData := make(map[string][]byte)
	files.Walk(rootFileNode, func(fpath string, fnode files.Node) error {
		file, ok := fnode.(files.File)
		if !ok {
			// Skip directories.
			return nil
		}
		data, err := io.ReadAll(file)
		require.NoError(t, err)
		foundData[fpath] = data
		return nil
	})

	// Don't do this directly in the assertion, because if it fails, we don't want
	// to try to print all of that data.
	areEqual := assert.ObjectsAreEqual(
		map[string][]byte{
			"a":           aData,
			"dir1/b":      bData,
			"dir1/c":      cData,
			"dir1/dir2/d": dData,
		},
		foundData,
	)

	require.True(t, areEqual, "expected all files to be present and match")
}

func TestStreamingShardCAR(t *testing.T) {
	db := testutil.CreateTestDB(t)
	repo := sqlrepo.New(db)

	// Setup test data
	config, err := repo.CreateConfiguration(t.Context(), "Test Config")
	require.NoError(t, err)

	tempDir := t.TempDir()

	// Create test files
	testFiles := []struct {
		name string
		data []byte
	}{
		{"file1.txt", []byte("hello world from file 1")},
		{"file2.txt", []byte("this is file 2 with more content")},
		{"file3.txt", []byte("file 3 has different data here")},
	}

	for _, tf := range testFiles {
		filePath := filepath.Join(tempDir, tf.name)
		err := os.WriteFile(filePath, tf.data, 0644)
		require.NoError(t, err)
	}

	source, err := repo.CreateSource(t.Context(), "Test Source", tempDir)
	require.NoError(t, err)

	uploads, err := repo.CreateUploads(t.Context(), config.ID(), []id.SourceID{source.ID()})
	require.NoError(t, err)
	upload := uploads[0]

	shard, err := repo.CreateShard(t.Context(), upload.ID())
	require.NoError(t, err)

	for _, tf := range testFiles {
		nodeCID := testutil.RandomCID(t)
		_, _, err := repo.FindOrCreateRawNode(t.Context(), nodeCID, uint64(len(tf.data)), tf.name, source.ID(), 0)
		require.NoError(t, err)
		err = repo.AddNodeToShard(t.Context(), shard.ID(), nodeCID)
		require.NoError(t, err)
	}

	sourcesAPI := sources.API{
		Repo: repo,
		GetLocalFSForPathFn: func(path string) (fs.FS, error) {
			return os.DirFS(path), nil
		},
	}

	// Test streaming CAR creation
	reader, err := preparation.NewStreamingShardCAR(t.Context(), shard, repo, sourcesAPI)
	require.NoError(t, err)

	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Greater(t, len(data), 100, "Should produce CAR data")

	t.Logf("Successfully streamed %d bytes", len(data))
}
