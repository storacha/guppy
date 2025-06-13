package client_test

import (
	"testing"

	"github.com/storacha/go-libstoracha/capabilities/access"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/receipt/fx"
	"github.com/storacha/go-ucanto/did"
	ed25519 "github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/go-ucanto/server"
	uhelpers "github.com/storacha/go-ucanto/testing/helpers"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/storacha/guppy/pkg/client"
	"github.com/storacha/guppy/pkg/testing/helpers"
	"github.com/stretchr/testify/require"
)

func TestRequestAccess(t *testing.T) {
	t.Run("invokes `access/authorize`", func(t *testing.T) {
		agentPrincipal := uhelpers.Must(ed25519.Generate())

		account := uhelpers.Must(did.Parse("did:mailto:example.com:alice"))

		invokedCapabilities := make([]ucan.Capability[access.AuthorizeCaveats], 0)

		connection := helpers.NewServerConnection(
			server.WithServiceMethod(
				access.Authorize.Can(),
				server.Provide(
					access.Authorize,
					func(
						cap ucan.Capability[access.AuthorizeCaveats],
						inv invocation.Invocation,
						ctx server.InvocationContext,
					) (access.AuthorizeOk, fx.Effects, error) {
						invokedCapabilities = append(invokedCapabilities, cap)
						return access.AuthorizeOk{}, nil, nil
					},
				),
			),
		)

		client.RequestAccess(agentPrincipal, account, client.WithConnection(connection))

		require.Len(t, invokedCapabilities, 1, "expected exactly one capability to be invoked")
		capability := invokedCapabilities[0]

		nb := uhelpers.Must(access.AuthorizeCaveatsReader.Read(capability.Nb()))
		require.Equal(t, "did:mailto:example.com:alice", nb.Iss.String(), "expected to authorize the correct issuer")

		requestedCapabilities := make([]string, 0, len(nb.Att))
		for _, att := range nb.Att {
			requestedCapabilities = append(requestedCapabilities, att.Can)
		}

		require.ElementsMatch(
			t,
			[]string{
				"space/*",
				"blob/*",
				"index/*",
				"store/*",
				"upload/*",
				"access/*",
				"filecoin/*",
				"usage/*",
			}, requestedCapabilities,
			"expected to authorize the capabilities required to manage a space")
	})
}
