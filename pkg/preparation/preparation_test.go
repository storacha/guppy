package preparation_test

import (
	"context"
	"io"
	"io/fs"
	"math/rand"
	"net/http"
	"testing"
	"time"

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
	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	"github.com/storacha/guppy/pkg/preparation/testutil"
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

func TestExecuteUpload(t *testing.T) {
	// In case something goes wrong. This should never take this long.
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)

	memFS := afero.NewMemMapFs()
	memFS.MkdirAll("dir1/dir2", 0755)
	afero.WriteFile(memFS, "a", randomBytes(1<<16), 0644)
	afero.WriteFile(memFS, "dir1/b", randomBytes(1<<16), 0644)
	afero.WriteFile(memFS, "dir1/c", randomBytes(1<<16), 0644)
	afero.WriteFile(memFS, "dir1/dir2/d", randomBytes(1<<16), 0644)

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

	err = api.ExecuteUpload(ctx, upload)
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
}
