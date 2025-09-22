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
	"github.com/ipld/go-car/v2/blockstore"
	"github.com/multiformats/go-multihash"
	"github.com/spf13/afero"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/testing/helpers"
	"github.com/storacha/guppy/pkg/client"
	ctestutil "github.com/storacha/guppy/pkg/client/testutil"
	"github.com/storacha/guppy/pkg/preparation"
	"github.com/storacha/guppy/pkg/preparation/internal/testdb"
	spacesmodel "github.com/storacha/guppy/pkg/preparation/spaces/model"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	"github.com/storacha/guppy/pkg/preparation/storacha"
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

// spaceBlobAddClient is a [storacha.SpaceBlobAdder] that wraps a
// [client.Client] to use a custom putClient.
type spaceBlobAddClient struct {
	*client.Client
	putClient *http.Client
}

var _ storacha.SpaceBlobAdder = (*spaceBlobAddClient)(nil)

func (c *spaceBlobAddClient) SpaceBlobAdd(ctx context.Context, content io.Reader, space did.DID, options ...client.SpaceBlobAddOption) (multihash.Multihash, delegation.Delegation, error) {
	return c.Client.SpaceBlobAdd(ctx, content, space, append(options, client.WithPutClient(c.putClient))...)
}

func TestExecuteUpload(t *testing.T) {
	t.Run("uploads", func(t *testing.T) {
		// In case something goes wrong. This should never take this long.
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		t.Cleanup(cancel)

		repo := sqlrepo.New(testdb.CreateTestDB(t))

		fsData := map[string][]byte{
			"a":           randomBytes(1 << 16),
			"dir1/b":      randomBytes(1 << 16),
			"dir1/c":      randomBytes(1 << 16),
			"dir1/dir2/d": randomBytes(1 << 16),
		}

		testFs := prepareFs(t, fsData)

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
				return testFs, nil
			}),
		)

		_, err := api.FindOrCreateSpace(ctx, spaceDID, "Large Upload Space", spacesmodel.WithShardSize(1<<16))
		require.NoError(t, err)
		source, err := api.CreateSource(ctx, "Large Upload Source", ".")
		require.NoError(t, err)
		err = repo.AddSourceToSpace(ctx, spaceDID, source.ID())
		require.NoError(t, err)
		uploads, err := api.CreateUploads(ctx, spaceDID)
		require.NoError(t, err)
		require.Len(t, uploads, 1, "expected exactly one upload to be created")
		upload := uploads[0]

		rootCid, err := api.ExecuteUpload(ctx, upload)
		require.NoError(t, err)

		putBlobs := ctestutil.ReceivedBlobs(putClient)
		fmt.Printf("Received %d blobs\n", len(putBlobs))
		require.Len(t, putBlobs, 5, "expected exactly 5 blobs to be added")

		foundData := filesData(ctx, t, rootCid, putBlobs)

		// Don't do this directly in the assertion, because if it fails, we don't want
		// to try to print all of that data.
		areEqual := assert.ObjectsAreEqual(
			fsData,
			foundData,
		)

		require.True(t, areEqual, "expected all files to be present and match")
	})

	t.Run("after an error, can be retried safely", func(t *testing.T) {
		// In case something goes wrong. This should never take this long.
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		t.Cleanup(cancel)
		// ctx := t.Context()

		repo := sqlrepo.New(testdb.CreateTestDB(t))

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
				return testFs, nil
			}),
		)

		_, err := api.FindOrCreateSpace(ctx, spaceDID, "Large Upload Space", spacesmodel.WithShardSize(1<<16))
		require.NoError(t, err)
		source, err := api.CreateSource(ctx, "Large Upload Source", ".")
		require.NoError(t, err)
		err = repo.AddSourceToSpace(ctx, spaceDID, source.ID())
		require.NoError(t, err)
		uploads, err := api.CreateUploads(ctx, spaceDID)
		require.NoError(t, err)
		require.Len(t, uploads, 1, "expected exactly one upload to be created")
		upload := uploads[0]

		// The first time, it should hit an error (on the third PUT)
		_, err = api.ExecuteUpload(ctx, upload)
		require.ErrorIs(t, err, assert.AnError, "expected error on third PUT request")

		putBlobs := ctestutil.ReceivedBlobs(putClient)
		require.Len(t, putBlobs, 2, "expected only 2 blobs to be added so far")

		// The second time, it should succeedfs
		rootCid, err := api.ExecuteUpload(ctx, upload)
		require.NoError(t, err, "expected upload to succeed on retry")

		putBlobs = ctestutil.ReceivedBlobs(putClient)
		require.Len(t, putBlobs, 5, "expected only 5 blobs to be added in the end")

		foundData := filesData(ctx, t, rootCid, putBlobs)

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

func filesData(ctx context.Context, t *testing.T, rootCid cid.Cid, putBlobs [][]byte) map[string][]byte {
	t.Helper()

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

func (e *errorableTransport) ReceivedBlobs() [][]byte {
	if receiver, ok := e.wrapped.(ctestutil.BlobReceiver); ok {
		return receiver.ReceivedBlobs()
	}
	return nil
}
