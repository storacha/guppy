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
	carblockstore "github.com/ipld/go-car/v2/blockstore"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"github.com/spf13/afero"
	"github.com/storacha/go-libstoracha/blobindex"
	filecoincap "github.com/storacha/go-libstoracha/capabilities/filecoin"
	spaceblobcap "github.com/storacha/go-libstoracha/capabilities/space/blob"
	spaceindexcap "github.com/storacha/go-libstoracha/capabilities/space/index"
	"github.com/storacha/go-libstoracha/capabilities/types"
	uploadcap "github.com/storacha/go-libstoracha/capabilities/upload"
	"github.com/storacha/go-libstoracha/digestutil"
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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ctestutil "github.com/storacha/guppy/pkg/client/testutil"
	"github.com/storacha/guppy/pkg/preparation"
	"github.com/storacha/guppy/pkg/preparation/internal/testdb"
	spacesmodel "github.com/storacha/guppy/pkg/preparation/spaces/model"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	gtypes "github.com/storacha/guppy/pkg/preparation/types"
	uploadsmodel "github.com/storacha/guppy/pkg/preparation/uploads/model"
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
	indexCaps *[]ucan.Capability[spaceindexcap.AddCaveats],
	replicateCaps *[]ucan.Capability[spaceblobcap.ReplicateCaveats],
	offerCaps *[]ucan.Capability[filecoincap.OfferCaveats],
	uploadAddCaps *[]ucan.Capability[uploadcap.AddCaveats],
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
							*indexCaps = append(*indexCaps, cap)
							return result.Ok[spaceindexcap.AddOk, failure.IPLDBuilderFailure](spaceindexcap.AddOk{}), nil, nil
						},
					),
				),

				server.WithServiceMethod(
					spaceblobcap.Replicate.Can(),
					server.Provide(
						spaceblobcap.Replicate,
						func(
							ctx context.Context,
							cap ucan.Capability[spaceblobcap.ReplicateCaveats],
							inv invocation.Invocation,
							context server.InvocationContext,
						) (result.Result[spaceblobcap.ReplicateOk, failure.IPLDBuilderFailure], fx.Effects, error) {
							*replicateCaps = append(*replicateCaps, cap)
							sitePromises := make([]types.Promise, cap.Nb().Replicas)
							for i := range sitePromises {
								siteDigest, err := multihash.Encode(fmt.Appendf(nil, "test-replicated-site-%d", i), multihash.IDENTITY)
								if err != nil {
									return nil, nil, fmt.Errorf("encoding site digest: %w", err)
								}
								sitePromises[i] = types.Promise{
									UcanAwait: types.Await{
										Selector: ".out.ok.site",
										Link:     cidlink.Link{Cid: cid.NewCidV1(cid.Raw, siteDigest)},
									},
								}
							}
							return result.Ok[spaceblobcap.ReplicateOk, failure.IPLDBuilderFailure](
								spaceblobcap.ReplicateOk{
									Site: sitePromises,
								},
							), nil, nil
						},
					),
				),

				server.WithServiceMethod(
					filecoincap.Offer.Can(),
					server.Provide(
						filecoincap.Offer,
						func(
							ctx context.Context,
							cap ucan.Capability[filecoincap.OfferCaveats],
							inv invocation.Invocation,
							context server.InvocationContext,
						) (result.Result[filecoincap.OfferOk, failure.IPLDBuilderFailure], fx.Effects, error) {
							*offerCaps = append(*offerCaps, cap)
							return result.Ok[filecoincap.OfferOk, failure.IPLDBuilderFailure](
								filecoincap.OfferOk{
									Piece: cap.Nb().Piece,
								},
							), nil, nil
						},
					),
				),

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
							*uploadAddCaps = append(*uploadAddCaps, cap)
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

func createUpload(
	t *testing.T,
	sourcePath string,
	repo *sqlrepo.Repo,
	spaceDID did.DID,
	api preparation.API,
) *uploadsmodel.Upload {
	_, err := api.FindOrCreateSpace(t.Context(), spaceDID, "Large Upload Space", spacesmodel.WithShardSize(1<<16))
	require.NoError(t, err)
	source, err := api.CreateSource(t.Context(), "Large Upload Source", sourcePath)
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

		aBytes := randomBytes((1 << 16) - 128)
		fsData := map[string][]byte{
			// These numbers are tuned to create 6 shards at a shard size of 1<<16.
			"a":           aBytes,
			"dir1/b":      randomBytes((1 << 16) - 128),
			"dir1/c":      randomBytes((1 << 16) - 128),
			"dir1/dir2/d": randomBytes((1 << 16) - 128),

			// Make one file identical to another to test deduplication.
			"dir1/dir2/a-again": aBytes,
		}

		testFs := prepareFs(t, fsData)

		putClient := ctestutil.NewPutClient()
		var indexCaps []ucan.Capability[spaceindexcap.AddCaveats]
		var replicateCaps []ucan.Capability[spaceblobcap.ReplicateCaveats]
		var offerCaps []ucan.Capability[filecoincap.OfferCaveats]
		var uploadAddCaps []ucan.Capability[uploadcap.AddCaveats]
		c := prepareTestClient(t, space, putClient, &indexCaps, &replicateCaps, &offerCaps, &uploadAddCaps)

		uploadSourcePath := t.TempDir()
		api := preparation.NewAPI(
			repo,
			c,
			preparation.WithGetLocalFSForPathFn(func(path string) (fs.FS, error) {
				assert.Equal(t, uploadSourcePath, path, "test expects root to be '.'")
				return testFs, nil
			}),
			preparation.WithShardUploadParallelism(1),
		)

		upload := createUpload(t, uploadSourcePath, repo, space.DID(), api)

		returnedRootCID, err := api.ExecuteUpload(t.Context(), upload)
		require.NoError(t, err)
		require.NotEmpty(t, returnedRootCID, "expected non-empty root CID")

		putBlobs := ctestutil.ReceivedBlobs(putClient)

		require.Equal(t, 6, putBlobs.Size(), "expected 5 shards + 1 index to be added")

		require.Len(t, indexCaps, 1, "expected exactly one `space/index/add` invocation")
		require.Equal(t, space.DID().String(), indexCaps[0].With(), "expected `space/index/add` invocation to be for the correct space")
		require.NotNil(t, indexCaps[0].Nb().Index, "expected `space/index/add` to be called")
		indexCIDLink, ok := indexCaps[0].Nb().Index.(cidlink.Link)
		require.True(t, ok, "expected index link to be a CID link")

		require.Len(t, replicateCaps, 6, "expected 5 shards + 1 index to be replicated")
		putBlobDescs := make([]types.Blob, 0, putBlobs.Size())
		for digest, blob := range putBlobs.Iterator() {
			putBlobDescs = append(putBlobDescs, types.Blob{
				Digest: digest,
				Size:   uint64(len(blob)),
			})
		}
		replicatedBlobDescs := make([]types.Blob, 0, len(replicateCaps))

		for _, i := range replicateCaps {
			require.Equal(t, space.DID().String(), i.With(), "expected `space/blob/replicate` invocation to be for the correct space")
			require.Equal(t, uint(3), i.Nb().Replicas, "expected `space/blob/replicate` to request 3 replicas")

			// Verifying the correct site is left to lower level tests.
			require.NotNil(t, uint(3), i.Nb().Site, "expected `space/blob/replicate` to provide a site")

			replicatedBlobDescs = append(replicatedBlobDescs, i.Nb().Blob)
		}
		require.ElementsMatch(t, putBlobDescs, replicatedBlobDescs, "expected all PUT blobs to be replicated")

		require.Len(t, offerCaps, 5, "expected the 5 shards to be `filecoin/offer`ed")
		putLinks := make([]ipld.Link, 0, putBlobs.Size())
		for digest := range putBlobs.Iterator() {
			putLink := cidlink.Link{Cid: cid.NewCidV1(uint64(multicodec.Car), digest)}
			if putLink.Cid != indexCIDLink.Cid {
				putLinks = append(putLinks, putLink)
			}
		}
		offeredLinks := make([]ipld.Link, 0, len(offerCaps))
		for _, i := range offerCaps {
			offeredLinks = append(offeredLinks, i.Nb().Content)
		}
		require.ElementsMatch(t, putLinks, offeredLinks, "expected all PUT shards to be `filecoin/offer`ed")

		require.Len(t, uploadAddCaps, 1, "expected only one `upload/add` invocation")
		require.Equal(t, space.DID().String(), uploadAddCaps[0].With(), "expected `upload/add` invocation to be for the correct space")
		rootCIDLink, ok := uploadAddCaps[0].Nb().Root.(cidlink.Link)
		require.True(t, ok, "expected root link to be a CID link")
		require.Equal(t, rootCIDLink.Cid, returnedRootCID, "expected returned root CID to match the one in the `upload/add`")

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
			// These numbers are tuned to create 5 shards at a shard size of 1<<16.
			"dir1/b":      randomBytes((1 << 16) - 128),
			"a":           randomBytes((1 << 16) - 128),
			"dir1/c":      randomBytes((1 << 16) - 128),
			"dir1/dir2/d": randomBytes((1 << 16) - 128),
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

		var indexCaps []ucan.Capability[spaceindexcap.AddCaveats]
		var replicateCaps []ucan.Capability[spaceblobcap.ReplicateCaveats]
		var offerCaps []ucan.Capability[filecoincap.OfferCaveats]
		var uploadAddCaps []ucan.Capability[uploadcap.AddCaveats]
		c := prepareTestClient(t, space, putClient, &indexCaps, &replicateCaps, &offerCaps, &uploadAddCaps)

		uploadSourcePath := t.TempDir()
		api := preparation.NewAPI(
			repo,
			c,
			preparation.WithGetLocalFSForPathFn(func(path string) (fs.FS, error) {
				assert.Equal(t, uploadSourcePath, path, "test expects root to be '.'")
				return testFs, nil
			}),
			preparation.WithShardUploadParallelism(1),
		)

		upload := createUpload(t, uploadSourcePath, repo, space.DID(), api)

		// The first time, it should hit an error (on the third PUT)
		_, err = api.ExecuteUpload(t.Context(), upload)

		var shardUploadErrors gtypes.BlobUploadErrors
		require.ErrorAs(t, err, &shardUploadErrors, "expected a ShardUploadErrors error")

		underlying := shardUploadErrors.Unwrap()
		require.Len(t, underlying, 1, "expected exactly one underlying error")
		require.ErrorIs(t, underlying[0], assert.AnError, "expected error on third PUT request")

		putBlobs := ctestutil.ReceivedBlobs(putClient)
		// We don't know exactly how many successful PUTs there were, but we know it
		// should be at least 2 and at most 4.
		require.GreaterOrEqual(t, putBlobs.Size(), 3, "expected at least 2/5 shards + 1 index to be added so far")
		require.LessOrEqual(t, putBlobs.Size(), 5, "expected at most 4/5 shards + 1 index to be added so far")
		require.GreaterOrEqual(t, len(replicateCaps), 3, "expected at least 2/5 shards + 1 index to be replicated so far")
		require.LessOrEqual(t, len(replicateCaps), 5, "expected at most 4/5 shards + 1 index to be replicated so far")
		require.GreaterOrEqual(t, len(offerCaps), 2, "expected at least 2/5 shards to be `filecoin/offer`ed so far")
		require.LessOrEqual(t, len(offerCaps), 4, "expected at most 4/5 shards to be `filecoin/offer`ed so far")

		require.Len(t, indexCaps, 1, "expected one `space/index/add` invocation")
		require.Equal(t, space.DID().String(), indexCaps[0].With(), "expected `space/index/add` invocation to be for the correct space")
		require.NotNil(t, indexCaps[0].Nb().Index, "expected `space/index/add` to be called")
		indexCIDLink, ok := indexCaps[0].Nb().Index.(cidlink.Link)
		require.True(t, ok, "expected index link to be a CID link")

		require.Len(t, uploadAddCaps, 0, "expected `upload/add` not to have been called yet")

		// Now, retry.

		upload, err = api.GetUploadByID(t.Context(), upload.ID())
		require.NoError(t, err)

		// The second time, it should succeed
		returnedRootCID, err := api.ExecuteUpload(t.Context(), upload)
		require.NoError(t, err, "expected upload to succeed on retry")
		require.NotEmpty(t, returnedRootCID, "expected non-empty root CID")

		putBlobs = ctestutil.ReceivedBlobs(putClient)
		require.Equal(t, 6, putBlobs.Size(), "expected 5 shards + 1 index to be added in the end")
		require.Len(t, replicateCaps, 6, "expected 5 shards + 1 index to be replicated")
		require.Len(t, offerCaps, 5, "expected the 5 shards to be `filecoin/offer`ed")

		require.Len(t, indexCaps, 1, "expected `space/index/add` not to have been invoked again")
		require.Len(t, uploadAddCaps, 1, "expected  one `upload/add` invocation")
		require.Equal(t, space.DID().String(), uploadAddCaps[0].With(), "expected `upload/add` invocation to be for the correct space")
		rootCIDLink, ok := uploadAddCaps[0].Nb().Root.(cidlink.Link)
		require.True(t, ok, "expected root link to be a CID link")
		require.Equal(t, rootCIDLink.Cid, returnedRootCID, "expected returned root CID to match the one in the `upload/add`")

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

func filesData(ctx context.Context, t *testing.T, rootCID cid.Cid, indexCID cid.Cid, shards ctestutil.BlobMap) map[string][]byte {
	bs, err := newIndexAndShardsBlockstore(indexCID, shards)
	require.NoError(t, err)

	blockserv := blockservice.New(bs, nil)
	dagserv := merkledag.NewDAGService(blockserv)
	rootNode, err := dagserv.Get(ctx, rootCID)
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

// indexAndShardsBlockstore is a [carblockstore.Blockstore] that combines multiple
// blockstores into one. It doesn't actually implement the entire interface, and
// is only suitable for testing purposes.
type indexAndShardsBlockstore struct {
	index  blobindex.ShardedDagIndex
	shards ctestutil.BlobMap
}

var _ carblockstore.Blockstore = (*indexAndShardsBlockstore)(nil)

func newIndexAndShardsBlockstore(indexCID cid.Cid, shards ctestutil.BlobMap) (*indexAndShardsBlockstore, error) {
	if indexCID.Prefix().Codec != uint64(multicodec.Car) {
		return nil, fmt.Errorf("expected index link CID to have codec 0x%x (CAR), got 0x%x", multicodec.Car, indexCID.Prefix().Codec)
	}

	indexDigest := indexCID.Hash()
	if !shards.Has(indexDigest) {
		return nil, fmt.Errorf("index CID %s (digest %s) not found in provided shards", indexCID, digestutil.Format(indexDigest))
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
					return nil, fmt.Errorf("shard with digest %s not found in provided shards", digestutil.Format(shardDigest))
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

// compositeBlockstore is a [carblockstore.Blockstore] that combines multiple
// blockstores into one. It doesn't actually implement the entire interface, and
// is only suitable for testing purposes.
type compositeBlockstore struct {
	blockstores []carblockstore.Blockstore
}

var _ carblockstore.Blockstore = (*compositeBlockstore)(nil)

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
