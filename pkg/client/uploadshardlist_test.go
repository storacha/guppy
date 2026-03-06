package client_test

import (
	"context"
	"testing"

	shardcap "github.com/storacha/go-libstoracha/capabilities/upload/shard"
	"github.com/storacha/go-libstoracha/testutil"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/ipld"
	"github.com/storacha/go-ucanto/core/receipt/fx"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/core/result/failure"
	ed25519 "github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/go-ucanto/server"
	"github.com/storacha/go-ucanto/ucan"
	ctestutil "github.com/storacha/guppy/pkg/client/testutil"
	"github.com/stretchr/testify/require"
)

func TestUploadShardList(t *testing.T) {
	t.Run("lists shards for an upload", func(t *testing.T) {
		space, err := ed25519.Generate()
		require.NoError(t, err)

		results := []ipld.Link{testutil.RandomCID(t), testutil.RandomCID(t)}

		c, err := ctestutil.Client(
			ctestutil.WithServerOptions(
				server.WithServiceMethod(
					shardcap.ListAbility,
					server.Provide(
						shardcap.List,
						func(
							ctx context.Context,
							cap ucan.Capability[shardcap.ListCaveats],
							inv invocation.Invocation,
							context server.InvocationContext,
						) (result.Result[shardcap.ListOk, failure.IPLDBuilderFailure], fx.Effects, error) {
							return result.Ok[shardcap.ListOk, failure.IPLDBuilderFailure](
								shardcap.ListOk{
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

		page, err := c.UploadShardList(t.Context(), space.DID(), shardcap.ListCaveats{
			Root: testutil.RandomCID(t),
		})
		require.NoError(t, err)
		require.Equal(t, uint64(len(results)), page.Size)
		require.Equal(t, results, page.Results)
	})
}
