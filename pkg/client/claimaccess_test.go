package client_test

import (
	"fmt"
	"io"
	"testing"

	"github.com/storacha/go-libstoracha/capabilities/access"
	"github.com/storacha/go-libstoracha/capabilities/upload"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/receipt/fx"
	"github.com/storacha/go-ucanto/server"
	uhelpers "github.com/storacha/go-ucanto/testing/helpers"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/storacha/guppy/pkg/client"
	"github.com/storacha/guppy/pkg/testing/helpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClaimAccess(t *testing.T) {
	t.Run("returns the delegations from `access/claim`'s receipt", func(t *testing.T) {
		// Declare these up front to refer to them in the service method.
		var storedDel delegation.Delegation
		var c *client.Client

		connection := helpers.NewServerConnection(
			server.WithServiceMethod(
				access.Claim.Can(),
				server.Provide(
					access.Claim,
					func(
						cap ucan.Capability[access.ClaimCaveats],
						inv invocation.Invocation,
						ctx server.InvocationContext,
					) (access.ClaimOk, fx.Effects, error) {
						assert.Equal(t, c.Issuer().DID().String(), cap.With(), "expected to claim access for the agent")

						return access.ClaimOk{
							Delegations: access.DelegationsModel{
								Keys: []string{storedDel.Link().String()},
								Values: map[string][]byte{
									storedDel.Link().String(): uhelpers.Must(io.ReadAll(storedDel.Archive())),
								},
							},
						}, nil, nil
					},
				),
			),
		)

		c = uhelpers.Must(client.NewClient(connection))

		// Some arbitrary delegation which has been stored to be claimed.
		storedDel = uhelpers.Must(upload.Get.Delegate(
			c.Issuer(),
			c.Issuer(),
			c.Issuer().DID().String(),
			upload.GetCaveats{Root: uhelpers.RandomCID()},
		))

		claimedDels, err := c.ClaimAccess()

		require.NoError(t, err)
		require.Len(t, claimedDels, 1, "expected exactly one delegation to be claimed")
		require.Equal(t, storedDel.Link().String(), claimedDels[0].Link().String(), "expected the claimed delegation to match the stored one")
	})

	t.Run("returns any handler error", func(t *testing.T) {
		connection := helpers.NewServerConnection(
			server.WithServiceMethod(
				access.Claim.Can(),
				server.Provide(
					access.Claim,
					func(
						cap ucan.Capability[access.ClaimCaveats],
						inv invocation.Invocation,
						ctx server.InvocationContext,
					) (access.ClaimOk, fx.Effects, error) {
						return access.ClaimOk{}, nil, fmt.Errorf("Something went wrong!")
					},
				),
			),
		)

		c := uhelpers.Must(client.NewClient(connection))
		claimedDels, err := c.ClaimAccess()

		require.ErrorContains(t, err, "`access/claim` failed: Something went wrong!")
		require.Len(t, claimedDels, 0)
	})

	t.Run("returns a useful error on any other UCAN failure", func(t *testing.T) {
		// In this case, we test the server not implementing the `access/claim`
		// capability.
		connection := helpers.NewServerConnection()

		c := uhelpers.Must(client.NewClient(connection))
		claimedDels, err := c.ClaimAccess()

		require.ErrorContains(t, err, "`access/claim` failed with unexpected error:")
		require.ErrorContains(t, err, "HandlerNotFoundError")
		require.Len(t, claimedDels, 0)
	})
}
