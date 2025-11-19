package client_test

import (
	"context"
	"testing"

	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	contentcap "github.com/storacha/go-libstoracha/capabilities/space/content"
	spaceindexcap "github.com/storacha/go-libstoracha/capabilities/space/index"
	"github.com/storacha/go-ucanto/core/dag/blockstore"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/ipld"
	"github.com/storacha/go-ucanto/core/receipt/fx"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/core/result/failure"
	ed25519signer "github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/go-ucanto/server"
	"github.com/storacha/go-ucanto/testing/helpers"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/storacha/guppy/pkg/client"
	"github.com/storacha/guppy/pkg/client/testutil"
	"github.com/stretchr/testify/require"
)

func TestSpaceIndexAdd(t *testing.T) {
	space, err := ed25519signer.Generate()
	require.NoError(t, err)

	invokedCapabilities := []ucan.Capability[spaceindexcap.AddCaveats]{}
	invokedInvocations := []invocation.Invocation{}

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
					invokedInvocations = append(invokedInvocations, inv)
					return result.Ok[spaceindexcap.AddOk, failure.IPLDBuilderFailure](
						spaceindexcap.AddOk{},
					), nil, nil
				},
			),
		),
	)

	c := helpers.Must(client.NewClient(client.WithConnection(connection)))

	// Delegate * on the space to the client
	cap := ucan.NewCapability("*", space.DID().String(), ucan.NoCaveats{})
	proof, err := delegation.Delegate(space, c.Issuer(), []ucan.Capability[ucan.NoCaveats]{cap}, delegation.WithNoExpiration())
	require.NoError(t, err)
	err = c.AddProofs(proof)
	require.NoError(t, err)

	indexLink := helpers.RandomCID()
	rootLink := helpers.RandomCID()
	err = c.SpaceIndexAdd(t.Context(), indexLink.(cidlink.Link).Cid, 123, rootLink.(cidlink.Link).Cid, space.DID())
	require.NoError(t, err)

	require.Len(t, invokedCapabilities, 1, "expected exactly one capability to be invoked")
	capability := invokedCapabilities[0]

	require.Equal(t, space.DID().String(), capability.With(), "expected capability to be invoked for the correct space")
	nb := helpers.Must(spaceindexcap.AddCaveatsReader.Read(capability.Nb()))
	require.Equal(t, indexLink, nb.Index, "expected to add the correct index")
	require.Equal(t, rootLink, nb.Content, "expected to add the correct content root")

	require.Len(t, invokedInvocations, 1, "expected exactly one invocation to be made")
	invocation := invokedInvocations[0]

	require.Equal(t, 1, len(invocation.Facts()), "expected exactly one fact to be present")
	require.Equal(t, 1, len(invocation.Facts()[0]), "expected exactly one fact to be present")
	authFactValue, ok := invocation.Facts()[0]["retrievalAuth"]
	require.True(t, ok, "expected 'retrievalAuth' fact to be present in invocation")
	authFactLink, err := authFactValue.(ipld.Node).AsLink()
	require.NoError(t, err, "expected 'retrievalAuth' fact to be a link")

	invocationBlocks, err := blockstore.NewBlockStore(blockstore.WithBlocksIterator(invocation.Blocks()))
	require.NoError(t, err, "creating blockstore from invocation blocks")

	authDelegation, err := delegation.NewDelegationView(authFactLink, invocationBlocks)
	require.NoError(t, err, "loading retrievalAuth delegation from invocation blocks")

	require.Equal(t, c.Issuer().DID(), authDelegation.Issuer())
	require.Equal(t, connection.ID().DID(), authDelegation.Audience())
	require.Equal(t, 1, len(authDelegation.Capabilities()))
	authCapability := authDelegation.Capabilities()[0]
	require.Equal(t, space.DID().String(), authCapability.With())
	retrievalCaveats := helpers.Must(contentcap.RetrieveCaveatsReader.Read(authCapability.Nb()))
	require.Equal(t, indexLink.(cidlink.Link).Cid.Hash(), retrievalCaveats.Blob.Digest, "expected retrieval auth to be for the correct blob")
	require.Equal(t, uint64(0), retrievalCaveats.Range.Start, "expected retrieval auth to have correct range start")
	require.Equal(t, uint64(122), retrievalCaveats.Range.End, "expected retrieval auth to have correct range end")
}
