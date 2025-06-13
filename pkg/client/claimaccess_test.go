package client_test

import (
	"fmt"
	"io"
	"testing"

	"github.com/storacha/go-libstoracha/capabilities/access"
	"github.com/storacha/go-libstoracha/capabilities/upload"
	uclient "github.com/storacha/go-ucanto/client"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/receipt/fx"
	"github.com/storacha/go-ucanto/did"
	ed25519 "github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/go-ucanto/principal/signer"
	"github.com/storacha/go-ucanto/server"
	"github.com/storacha/go-ucanto/testing/helpers"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/storacha/guppy/pkg/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClaimAccess(t *testing.T) {
	t.Run("returns the delegations from `access/claim`'s receipt", func(t *testing.T) {
		agent := helpers.Must(ed25519.Generate())

		service := helpers.Must(signer.Wrap(
			helpers.Must(ed25519.Generate()),
			helpers.Must(did.Parse("did:web:storage.example.com")),
		))

		// Some arbitrary delegation which has been stored to be claimed.
		storedDel := helpers.Must(upload.Get.Delegate(
			service,
			agent,
			service.DID().String(),
			upload.GetCaveats{Root: helpers.RandomCID()},
		))

		server := helpers.Must(server.NewServer(
			service,
			server.WithServiceMethod(
				access.Claim.Can(),
				server.Provide(
					access.Claim,
					func(
						cap ucan.Capability[access.ClaimCaveats],
						inv invocation.Invocation,
						ctx server.InvocationContext,
					) (access.ClaimOk, fx.Effects, error) {
						assert.Equal(t, agent.DID().String(), cap.With(), "expected to claim access for the agent")

						return access.ClaimOk{
							Delegations: access.DelegationsModel{
								Keys: []string{storedDel.Link().String()},
								Values: map[string][]byte{
									storedDel.Link().String(): helpers.Must(io.ReadAll(storedDel.Archive())),
								},
							},
						}, nil, nil
					},
				),
			),
		))

		connection := helpers.Must(uclient.NewConnection(server.ID(), server))

		claimedDels, err := client.ClaimAccess(agent, client.WithConnection(connection))

		require.NoError(t, err)
		require.Len(t, claimedDels, 1, "expected exactly one delegation to be claimed")
		require.Equal(t, storedDel.Link().String(), claimedDels[0].Link().String(), "expected the claimed delegation to match the stored one")
	})

	t.Run("returns any handler error", func(t *testing.T) {
		agent := helpers.Must(ed25519.Generate())

		service := helpers.Must(signer.Wrap(
			helpers.Must(ed25519.Generate()),
			helpers.Must(did.Parse("did:web:storage.example.com")),
		))

		server := helpers.Must(server.NewServer(
			service,
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
		))

		connection := helpers.Must(uclient.NewConnection(server.ID(), server))

		claimedDels, err := client.ClaimAccess(agent, client.WithConnection(connection))

		require.Error(t, err)
		require.Equal(t, "`access/claim` failed: Something went wrong!", err.Error())
		require.Len(t, claimedDels, 0)
	})
}
