package client_test

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/storacha/go-libstoracha/capabilities/access"
	uploadcap "github.com/storacha/go-libstoracha/capabilities/upload"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/receipt/fx"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/core/result/failure"
	"github.com/storacha/go-ucanto/server"
	uhelpers "github.com/storacha/go-ucanto/testing/helpers"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/storacha/guppy/pkg/client"
	"github.com/storacha/guppy/pkg/client/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func buildDelegationsModel(dels ...delegation.Delegation) access.DelegationsModel {
	keys := make([]string, 0, len(dels))
	values := make(map[string][]byte, len(dels))

	for _, del := range dels {
		keys = append(keys, del.Link().String())
		values[del.Link().String()] = uhelpers.Must(io.ReadAll(del.Archive()))
	}

	return access.DelegationsModel{
		Keys:   keys,
		Values: values,
	}
}

func TestClaimAccess(t *testing.T) {
	t.Run("returns the delegations from `access/claim`'s receipt", func(t *testing.T) {
		// Declare these up front to refer to them in the service method.
		var storedDels access.DelegationsModel
		var c *client.Client

		connection := testutil.NewTestServerConnection(
			server.WithServiceMethod(
				access.Claim.Can(),
				server.Provide(
					access.Claim,
					func(
						ctx context.Context,
						cap ucan.Capability[access.ClaimCaveats],
						inv invocation.Invocation,
						context server.InvocationContext,
					) (result.Result[access.ClaimOk, failure.IPLDBuilderFailure], fx.Effects, error) {
						assert.Equal(t, c.Issuer().DID().String(), cap.With(), "expected to claim access for the agent")

						return result.Ok[access.ClaimOk, failure.IPLDBuilderFailure](access.ClaimOk{Delegations: storedDels}), nil, nil
					},
				),
			),
		)

		c = uhelpers.Must(client.NewClient(client.WithConnection(connection)))

		// Some arbitrary delegation which has been stored to be claimed.
		del := uhelpers.Must(uploadcap.Get.Delegate(
			c.Issuer(),
			c.Issuer(),
			c.Issuer().DID().String(),
			uploadcap.GetCaveats{Root: uhelpers.RandomCID()},
		))
		storedDels = buildDelegationsModel(del)

		claimedDels, err := c.ClaimAccess(testContext(t))

		require.NoError(t, err)
		require.Len(t, claimedDels, 1, "expected exactly one delegation to be claimed")
		require.Equal(t, del.Link().String(), claimedDels[0].Link().String(), "expected the claimed delegation to match the stored one")
	})

	t.Run("returns any handler error", func(t *testing.T) {
		connection := testutil.NewTestServerConnection(
			server.WithServiceMethod(
				access.Claim.Can(),
				server.Provide(
					access.Claim,
					func(
						ctx context.Context,
						cap ucan.Capability[access.ClaimCaveats],
						inv invocation.Invocation,
						context server.InvocationContext,
					) (result.Result[access.ClaimOk, failure.IPLDBuilderFailure], fx.Effects, error) {
						return nil, nil, fmt.Errorf("Something went wrong!")
					},
				),
			),
		)

		c := uhelpers.Must(client.NewClient(client.WithConnection(connection)))
		claimedDels, err := c.ClaimAccess(testContext(t))

		require.ErrorContains(t, err, "`access/claim` failed: Something went wrong!")
		require.Len(t, claimedDels, 0)
	})

	t.Run("returns a useful error on any other UCAN failure", func(t *testing.T) {
		// In this case, we test the server not implementing the `access/claim`
		// capability.
		connection := testutil.NewTestServerConnection()

		c := uhelpers.Must(client.NewClient(client.WithConnection(connection)))
		claimedDels, err := c.ClaimAccess(testContext(t))

		require.ErrorContains(t, err, "`access/claim` failed with unexpected error:")
		require.ErrorContains(t, err, "HandlerNotFoundError")
		require.Len(t, claimedDels, 0)
	})
}
