package client_test

import (
	"context"
	"testing"

	"github.com/storacha/go-libstoracha/capabilities/access"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/receipt/fx"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/core/result/failure"
	"github.com/storacha/go-ucanto/server"
	uhelpers "github.com/storacha/go-ucanto/testing/helpers"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/storacha/guppy/pkg/client"
	"github.com/storacha/guppy/pkg/client/testutil"
	"github.com/stretchr/testify/require"
)

func TestRequestAccess(t *testing.T) {
	t.Run("invokes `access/authorize`", func(t *testing.T) {
		invokedInvocations := []invocation.Invocation{}
		invokedCapabilities := []ucan.Capability[access.AuthorizeCaveats]{}

		connection := testutil.NewTestServerConnection(
			server.WithServiceMethod(
				access.Authorize.Can(),
				server.Provide(
					access.Authorize,
					func(
						ctx context.Context,
						cap ucan.Capability[access.AuthorizeCaveats],
						inv invocation.Invocation,
						context server.InvocationContext,
					) (result.Result[access.AuthorizeOk, failure.IPLDBuilderFailure], fx.Effects, error) {
						invokedInvocations = append(invokedInvocations, inv)
						invokedCapabilities = append(invokedCapabilities, cap)
						return result.Ok[access.AuthorizeOk, failure.IPLDBuilderFailure](
							access.AuthorizeOk{
								Request:    inv.Link(),
								Expiration: 123,
							},
						), nil, nil
					},
				),
			),
		)

		c := uhelpers.Must(client.NewClient(connection, nil))

		authOk, err := c.RequestAccess(testContext(t), "did:mailto:example.com:alice")

		require.Len(t, invokedInvocations, 1, "expected exactly one invocation to be invoked")
		invocation := invokedInvocations[0]
		require.Len(t, invokedCapabilities, 1, "expected exactly one capability to be invoked")
		capability := invokedCapabilities[0]

		nb := uhelpers.Must(access.AuthorizeCaveatsReader.Read(capability.Nb()))
		require.Equal(t, "did:mailto:example.com:alice", *nb.Iss, "expected to authorize the correct issuer")

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
			"expected to authorize the capabilities required to manage a space",
		)

		require.NoError(t, err, "expected to successfully request access")
		require.Equal(t, invocation.Link().String(), authOk.Request.String(), "expected to return the request link")
		require.Equal(t, 123, authOk.Expiration, "expected to return the expiration")
	})
}
