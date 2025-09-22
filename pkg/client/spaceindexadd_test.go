package client_test

import (
	"context"
	"testing"

	spaceindexcap "github.com/storacha/go-libstoracha/capabilities/space/index"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/receipt/fx"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/core/result/failure"
	ed25519signer "github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/go-ucanto/server"
	"github.com/storacha/go-ucanto/testing/helpers"
	uhelpers "github.com/storacha/go-ucanto/testing/helpers"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/storacha/guppy/pkg/client"
	"github.com/storacha/guppy/pkg/client/testutil"
	"github.com/stretchr/testify/require"
)

func TestSpaceIndexAdd(t *testing.T) {
	space, err := ed25519signer.Generate()
	require.NoError(t, err)

	invokedCapabilities := []ucan.Capability[spaceindexcap.AddCaveats]{}

	connection := testutil.NewTestServerConnection(
		server.WithServiceMethod(
			spaceindexcap.Add.Can(),
			server.Provide(
				spaceindexcap.Add,
				func(
					ctx context.Context,
					cap ucan.Capability[spaceindexcap.AddCaveats],
					inv invocation.Invocation,
					context server.InvocationContext,
				) (result.Result[spaceindexcap.AddOk, failure.IPLDBuilderFailure], fx.Effects, error) {
					invokedCapabilities = append(invokedCapabilities, cap)
					return result.Ok[spaceindexcap.AddOk, failure.IPLDBuilderFailure](
						spaceindexcap.AddOk{},
					), nil, nil
				},
			),
		),
	)

	c := uhelpers.Must(client.NewClient(client.WithConnection(connection)))

	// Delegate * on the space to the client
	cap := ucan.NewCapability("*", space.DID().String(), ucan.NoCaveats{})
	proof, err := delegation.Delegate(space, c.Issuer(), []ucan.Capability[ucan.NoCaveats]{cap}, delegation.WithNoExpiration())
	require.NoError(t, err)
	err = c.AddProofs(proof)
	require.NoError(t, err)

	indexLink := helpers.RandomCID()
	err = c.SpaceIndexAdd(t.Context(), indexLink, space.DID())
	require.NoError(t, err)

	require.Len(t, invokedCapabilities, 1, "expected exactly one capability to be invoked")
	capability := invokedCapabilities[0]

	nb := uhelpers.Must(spaceindexcap.AddCaveatsReader.Read(capability.Nb()))
	require.Equal(t, indexLink, nb.Index, "expected to add the correct index")
}
