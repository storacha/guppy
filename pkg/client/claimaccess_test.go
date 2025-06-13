package client_test

import (
	"fmt"
	"io"
	"testing"

	"github.com/storacha/go-libstoracha/capabilities/access"
	"github.com/storacha/go-libstoracha/capabilities/upload"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/receipt/fx"
	ed25519 "github.com/storacha/go-ucanto/principal/ed25519/signer"
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
		agentPrincipal := uhelpers.Must(ed25519.Generate())

		// Some arbitrary delegation which has been stored to be claimed.
		storedDel := uhelpers.Must(upload.Get.Delegate(
			agentPrincipal,
			agentPrincipal,
			agentPrincipal.DID().String(),
			upload.GetCaveats{Root: uhelpers.RandomCID()},
		))

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
						assert.Equal(t, agentPrincipal.DID().String(), cap.With(), "expected to claim access for the agent")

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

		claimedDels, err := client.ClaimAccess(agentPrincipal, client.WithConnection(connection))

		require.NoError(t, err)
		require.Len(t, claimedDels, 1, "expected exactly one delegation to be claimed")
		require.Equal(t, storedDel.Link().String(), claimedDels[0].Link().String(), "expected the claimed delegation to match the stored one")
	})

	t.Run("returns any handler error", func(t *testing.T) {
		agent := uhelpers.Must(ed25519.Generate())

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

		claimedDels, err := client.ClaimAccess(agent, client.WithConnection(connection))

		require.Error(t, err)
		require.Equal(t, "`access/claim` failed: Something went wrong!", err.Error())
		require.Len(t, claimedDels, 0)
	})
}
