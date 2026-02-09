package client_test

import (
	"context"
	"testing"
	"time"

	spaceblobcap "github.com/storacha/go-libstoracha/capabilities/space/blob"
	"github.com/storacha/go-libstoracha/capabilities/types"
	"github.com/storacha/go-libstoracha/testutil"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/receipt/fx"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/core/result/failure"
	ed25519 "github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/go-ucanto/server"
	"github.com/storacha/go-ucanto/ucan"
	ctestutil "github.com/storacha/guppy/pkg/client/testutil"
	"github.com/stretchr/testify/require"
)

func TestSpaceBlobList(t *testing.T) {
	t.Run("lists blobs in a space", func(t *testing.T) {
		space, err := ed25519.Generate()
		require.NoError(t, err)

		blobBytes := testutil.RandomBytes(t, 32)
		results := []spaceblobcap.ListBlobItem{
			{
				Blob: types.Blob{
					Digest: testutil.MultihashFromBytes(t, blobBytes),
					Size:   uint64(len(blobBytes)),
				},
				InsertedAt: time.Unix(time.Now().Unix(), 0).UTC(),
			},
		}

		c, err := ctestutil.Client(
			ctestutil.WithServerOptions(
				server.WithServiceMethod(
					spaceblobcap.ListAbility,
					server.Provide(
						spaceblobcap.List,
						func(
							ctx context.Context,
							cap ucan.Capability[spaceblobcap.ListCaveats],
							inv invocation.Invocation,
							context server.InvocationContext,
						) (result.Result[spaceblobcap.ListOk, failure.IPLDBuilderFailure], fx.Effects, error) {
							return result.Ok[spaceblobcap.ListOk, failure.IPLDBuilderFailure](
								spaceblobcap.ListOk{
									Size:    uint64(len(results)),
									Results: results,
								},
							), nil, nil
						},
					),
				),
			),
		)
		require.NoError(t, err)

		cap := ucan.NewCapability("*", space.DID().String(), ucan.NoCaveats{})
		proof, err := delegation.Delegate(space, c.Issuer(), []ucan.Capability[ucan.NoCaveats]{cap}, delegation.WithNoExpiration())
		require.NoError(t, err)

		err = c.AddProofs(proof)
		require.NoError(t, err)

		page, err := c.SpaceBlobList(t.Context(), space.DID(), spaceblobcap.ListCaveats{})
		require.NoError(t, err)
		require.Equal(t, uint64(len(results)), page.Size)
		require.Equal(t, results, page.Results)
	})
}
