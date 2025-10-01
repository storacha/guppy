package preparation_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"math/rand"
	"net/http"
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
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-car/v2/blockstore"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multicodec"
	"github.com/spf13/afero"
	"github.com/storacha/go-libstoracha/blobindex"
	spaceindexcap "github.com/storacha/go-libstoracha/capabilities/space/index"
	uploadcap "github.com/storacha/go-libstoracha/capabilities/upload"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/receipt/fx"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/core/result/failure"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal"
	"github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/go-ucanto/server"
	"github.com/storacha/go-ucanto/testing/helpers"
	"github.com/storacha/go-ucanto/ucan"
	ctestutil "github.com/storacha/guppy/pkg/client/testutil"
	"github.com/storacha/guppy/pkg/preparation"
	"github.com/storacha/guppy/pkg/preparation/internal/testdb"
	spacesmodel "github.com/storacha/guppy/pkg/preparation/spaces/model"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	uploadsmodel "github.com/storacha/guppy/pkg/preparation/uploads/model"
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

// prepareTestClient creates a new test [storacha.Client] that uses the given
// [http.Client] as the client for PUT requests, stores the index link from the
// `space/index/add` invocation in the given [indexLink] pointer, and stores the
// root link and shard links from the `upload/add` in the [rootLink] and
// [shardLinks] pointers, respectively. The returned function returns the blobs
// received by the PUT client, and the index link.
func prepareTestClient(
	t *testing.T,
	space principal.Signer,
	putClient *http.Client,
	indexLink *ipld.Link,
	rootLink *ipld.Link,
	shardLinks *[]ipld.Link,
) *ctestutil.ClientWithCustomPut {
	client := &ctestutil.ClientWithCustomPut{
		Client: helpers.Must(ctestutil.Client(
			ctestutil.WithSpaceBlobAdd(),

			ctestutil.WithServerOptions(
				server.WithServiceMethod(
					spaceindexcap.Add.Can(),
					server.Provide(
						spaceindexcap.Add,
						func(
							ctx context.Context,
							cap ucan.Capability[spaceindexcap.AddCaveats],
							inv invocation.Invocation,
							context server.InvocationContext,
						) (result.Result[spaceindexcap.AddOk, failure.IPLDBuilderFailure], fx.Effects, error) {
							assert.Equal(t, space.DID().String(), cap.With(), "expected `space/index/add` invocation to be for the correct space")
							assert.Nil(t, *indexLink, "expected only one `space/index/add` invocation")
							*indexLink = cap.Nb().Index
							return result.Ok[spaceindexcap.AddOk, failure.IPLDBuilderFailure](spaceindexcap.AddOk{}), nil, nil
						},
					),
				),
			),

			ctestutil.WithServerOptions(
				server.WithServiceMethod(
					uploadcap.Add.Can(),
					server.Provide(
						uploadcap.Add,
						func(
							ctx context.Context,
							cap ucan.Capability[uploadcap.AddCaveats],
							inv invocation.Invocation,
							context server.InvocationContext,
						) (result.Result[uploadcap.AddOk, failure.IPLDBuilderFailure], fx.Effects, error) {
							assert.Equal(t, space.DID().String(), cap.With(), "expected `upload/add` invocation to be for the correct space")
							assert.Nil(t, *rootLink, "expected only one `upload/add` invocation")
							*rootLink = cap.Nb().Root
							*shardLinks = cap.Nb().Shards
							return result.Ok[uploadcap.AddOk, failure.IPLDBuilderFailure](uploadcap.AddOk{
								Root:   cap.Nb().Root,
								Shards: cap.Nb().Shards,
							}), nil, nil
						},
					),
				),
			),
		)),
		PutClient: putClient,
	}

	// Delegate * on the space to the client
	cap := ucan.NewCapability("*", space.DID().String(), ucan.NoCaveats{})
	proof, err := delegation.Delegate(space, client.Issuer(), []ucan.Capability[ucan.NoCaveats]{cap}, delegation.WithNoExpiration())
	require.NoError(t, err)
	err = client.AddProofs(proof)
	require.NoError(t, err)

	return client
}

func createUpload(t *testing.T, repo *sqlrepo.Repo, spaceDID did.DID, api preparation.API) *uploadsmodel.Upload {
	_, err := api.FindOrCreateSpace(t.Context(), spaceDID, "Large Upload Space", spacesmodel.WithShardSize(1<<16))
	require.NoError(t, err)
	source, err := api.CreateSource(t.Context(), "Large Upload Source", ".")
	require.NoError(t, err)
	err = repo.AddSourceToSpace(t.Context(), spaceDID, source.ID())
	require.NoError(t, err)
	uploads, err := api.FindOrCreateUploads(t.Context(), spaceDID)
	require.NoError(t, err)
	require.Len(t, uploads, 1, "expected exactly one upload to be created")
	return uploads[0]
}

func TestExecuteUpload(t *testing.T) {
	t.Run("uploads", func(t *testing.T) {
		space, err := signer.Generate()
		require.NoError(t, err)

		db := testdb.CreateTestDB(t)
		// Enable foreign keys for this high-level test.
		_, err = db.ExecContext(t.Context(), "PRAGMA foreign_keys = ON;")
		require.NoError(t, err, "failed to enable foreign keys")
		repo := sqlrepo.New(db)

		fsData := map[string][]byte{
			"a":           randomBytes(1 << 16),
			"dir1/b":      randomBytes(1 << 16),
			"dir1/c":      randomBytes(1 << 16),
			"dir1/dir2/d": randomBytes(1 << 16),
		}

		testFs := prepareFs(t, fsData)

		putClient := ctestutil.NewPutClient()
		var indexLink ipld.Link
		var rootLink ipld.Link
		var shardLinks []ipld.Link
		c := prepareTestClient(t, space, putClient, &indexLink, &rootLink, &shardLinks)

		api := preparation.NewAPI(
			repo,
			c,
			space.DID(),
			preparation.WithGetLocalFSForPathFn(func(path string) (fs.FS, error) {
				require.Equal(t, ".", path, "test expects root to be '.'")
				return testFs, nil
			}),
		)

		upload := createUpload(t, repo, space.DID(), api)

		returnedRootCid, err := api.ExecuteUpload(t.Context(), upload)
		require.NoError(t, err)
		require.NotEmpty(t, returnedRootCid, "expected non-empty root CID")

		putBlobs := ctestutil.ReceivedBlobs(putClient)
		require.Equal(t, 6, putBlobs.Size(), "expected 5 shards + 1 index to be added")
		require.NotNil(t, indexLink, "expected `space/index/add` to be called")
		indexCIDLink, ok := indexLink.(cidlink.Link)
		require.True(t, ok, "expected index link to be a CID link")
		require.NotNil(t, rootLink, "expected `upload/add` to be called")
		rootCIDLink, ok := rootLink.(cidlink.Link)
		require.True(t, ok, "expected root link to be a CID link")
		require.Equal(t, rootLink.(cidlink.Link).Cid, returnedRootCid, "expected returned root CID to match the one in the `upload/add`")

		foundData := filesData(t.Context(), t, rootCIDLink.Cid, indexCIDLink.Cid, putBlobs)

		// Don't do this directly in the assertion, because if it fails, we don't want
		// to try to print all of that data.
		areEqual := assert.ObjectsAreEqual(
			fsData,
			foundData,
		)

		require.True(t, areEqual, "expected all files to be present and match")
	})

	t.Run("after an error, can be retried safely", func(t *testing.T) {
		// Hide the error we're about to cause from the logs.
		logging.SetLogLevel("preparation/uploads", "dpanic")

		space, err := signer.Generate()
		require.NoError(t, err)

		db := testdb.CreateTestDB(t)
		// Enable foreign keys for this high-level test.
		_, err = db.ExecContext(t.Context(), "PRAGMA foreign_keys = ON;")
		require.NoError(t, err, "failed to enable foreign keys")
		repo := sqlrepo.New(db)

		fsData := map[string][]byte{
			"a":           randomBytes(1 << 16),
			"dir1/b":      randomBytes(1 << 16),
			"dir1/c":      randomBytes(1 << 16),
			"dir1/dir2/d": randomBytes(1 << 16),
		}

		testFs := prepareFs(t, fsData)

		putCount := 0
		putClient := ctestutil.NewPutClient()
		putClient.Transport = &errorableTransport{
			wrapped: putClient.Transport,
			errFn: func(req *http.Request) error {
				putCount++
				if putCount == 3 {
					// Simulate an error on the third PUT request.
					return assert.AnError
				}
				return nil
			},
		}

		var indexLink ipld.Link
		var rootLink ipld.Link
		var shardLinks []ipld.Link
		c := prepareTestClient(t, space, putClient, &indexLink, &rootLink, &shardLinks)

		api := preparation.NewAPI(
			repo,
			c,
			space.DID(),
			preparation.WithGetLocalFSForPathFn(func(path string) (fs.FS, error) {
				require.Equal(t, ".", path, "test expects root to be '.'")
				return testFs, nil
			}),
		)

		upload := createUpload(t, repo, space.DID(), api)

		// The first time, it should hit an error (on the third PUT)
		_, err = api.ExecuteUpload(t.Context(), upload)
		require.ErrorIs(t, err, assert.AnError, "expected error on third PUT request")

		putBlobs := ctestutil.ReceivedBlobs(putClient)
		require.Equal(t, 2, putBlobs.Size(), "expected only 2 shards to be added so far")
		require.Nil(t, indexLink, "expected `space/index/add` not to have been called yet")
		require.Nil(t, rootLink, "expected `upload/add` not to have been called yet")
		require.Nil(t, shardLinks, "expected `upload/add` not to have been called yet")

		// Now, retry.

		upload, err = api.GetUploadByID(t.Context(), upload.ID())
		require.NoError(t, err)

		// The second time, it should succeed
		returnedRootCid, err := api.ExecuteUpload(t.Context(), upload)
		require.NoError(t, err, "expected upload to succeed on retry")
		require.NotEmpty(t, returnedRootCid, "expected non-empty root CID")

		putBlobs = ctestutil.ReceivedBlobs(putClient)
		require.Equal(t, 6, putBlobs.Size(), "expected 5 shards + 1 index to be added in the end")
		rootCIDLink, ok := rootLink.(cidlink.Link)
		require.True(t, ok, "expected root link to be a CID link")
		indexCIDLink, ok := indexLink.(cidlink.Link)
		require.True(t, ok, "expected index link to be a CID link")
		require.Equal(t, rootLink.(cidlink.Link).Cid, returnedRootCid, "expected returned root CID to match the one in the `upload/add`")

		foundData := filesData(t.Context(), t, rootCIDLink.Cid, indexCIDLink.Cid, putBlobs)

		// Don't do this directly in the assertion, because if it fails, we don't want
		// to try to print all of that data.
		areEqual := assert.ObjectsAreEqual(
			fsData,
			foundData,
		)

		require.True(t, areEqual, "expected all files to be present and match")
	})
}

func prepareFs(t *testing.T, files map[string][]byte) afero.IOFS {
	t.Helper()

	memFS := afero.NewMemMapFs()

	for path, data := range files {
		err := memFS.MkdirAll(filepath.Dir(path), 0755)
		require.NoError(t, err)

		err = afero.WriteFile(memFS, path, data, 0644)
		require.NoError(t, err, "failed to write file %s", path)
	}

	memIOFS := afero.NewIOFS(memFS)

	fs.WalkDir(memIOFS, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		return memFS.Chtimes(path, time.Now(), time.Now())
	})

	return memIOFS
}

func filesData(ctx context.Context, t *testing.T, rootCid cid.Cid, indexCid cid.Cid, shards ctestutil.BlobMap) map[string][]byte {
	bs, err := newIndexAndShardsBlockstore(indexCid, shards)
	require.NoError(t, err)

	blockserv := blockservice.New(bs, nil)
	dagserv := merkledag.NewDAGService(blockserv)
	rootNode, err := dagserv.Get(ctx, rootCid)
	require.NoError(t, err)
	rootFileNode, err := unixfile.NewUnixfsFile(ctx, dagserv, rootNode)
	require.NoError(t, err)

	foundData := make(map[string][]byte)
	err = files.Walk(rootFileNode, func(fpath string, fnode files.Node) error {
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
	require.NoError(t, err)

	return foundData
}

// indexAndShardsBlockstore is a [blockstore.Blockstore] that combines multiple
// blockstores into one. It doesn't actually implement the entire interface, and
// is only suitable for testing purposes.
type indexAndShardsBlockstore struct {
	index  blobindex.ShardedDagIndex
	shards ctestutil.BlobMap
}

var _ blockstore.Blockstore = (*indexAndShardsBlockstore)(nil)

func newIndexAndShardsBlockstore(indexCid cid.Cid, shards ctestutil.BlobMap) (*indexAndShardsBlockstore, error) {
	if indexCid.Prefix().Codec != uint64(multicodec.Car) {
		return nil, fmt.Errorf("expected index link CID to have codec 0x%x (CAR), got 0x%x", multicodec.Car, indexCid.Prefix().Codec)
	}

	indexDigest := indexCid.Hash()
	if !shards.Has(indexDigest) {
		return nil, fmt.Errorf("index CID %s (digest %s) not found in provided shards", indexCid, indexDigest.B58String())
	}

	index, err := blobindex.Extract(bytes.NewReader(shards.Get(indexDigest)))
	if err != nil {
		return nil, fmt.Errorf("extracting index from CAR: %w", err)
	}

	return &indexAndShardsBlockstore{
		index:  index,
		shards: shards,
	}, nil
}

func (c *indexAndShardsBlockstore) Get(ctx context.Context, key cid.Cid) (blocks.Block, error) {
	for shardDigest, sliceMap := range c.index.Shards().Iterator() {
		for sliceDigest, position := range sliceMap.Iterator() {
			if bytes.Equal(key.Hash(), sliceDigest) {
				if !c.shards.Has(shardDigest) {
					return nil, fmt.Errorf("shard with digest %s not found in provided shards", shardDigest.B58String())
				}
				shardBlob := c.shards.Get(shardDigest)
				return blocks.NewBlockWithCid(shardBlob[position.Offset:position.Offset+position.Length], key)
			}
		}
	}
	return nil, format.ErrNotFound{Cid: key}
}

func (c *indexAndShardsBlockstore) DeleteBlock(context.Context, cid.Cid) error {
	panic("not implemented")
}

func (c *indexAndShardsBlockstore) Has(context.Context, cid.Cid) (bool, error) {
	panic("not implemented")
}

func (c *indexAndShardsBlockstore) GetSize(context.Context, cid.Cid) (int, error) {
	panic("not implemented")
}

func (c *indexAndShardsBlockstore) Put(context.Context, blocks.Block) error {
	panic("not implemented")
}

func (c *indexAndShardsBlockstore) PutMany(context.Context, []blocks.Block) error {
	panic("not implemented")
}

func (c *indexAndShardsBlockstore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	panic("not implemented")
}

func (c *indexAndShardsBlockstore) HashOnRead(enabled bool) {
	panic("not implemented")
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

// errorableTransport wraps an [http.RoundTripper] to provide an opportunity to
// return an error instead of succeeding.
type errorableTransport struct {
	wrapped http.RoundTripper
	errFn   func(req *http.Request) error
}

var _ http.RoundTripper = (*errorableTransport)(nil)
var _ ctestutil.BlobReceiver = (*errorableTransport)(nil)

func (e *errorableTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if e.errFn != nil {
		if err := e.errFn(req); err != nil {
			return nil, err
		}
	}
	return e.wrapped.RoundTrip(req)
}

func (e *errorableTransport) ReceivedBlobs() ctestutil.BlobMap {
	if receiver, ok := e.wrapped.(ctestutil.BlobReceiver); ok {
		return receiver.ReceivedBlobs()
	}
	return nil
}
