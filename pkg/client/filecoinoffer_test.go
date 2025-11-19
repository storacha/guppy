package client_test

import (
	"context"
	crand "crypto/rand"
	"io"
	"testing"

	commcid "github.com/filecoin-project/go-fil-commcid"
	commp "github.com/filecoin-project/go-fil-commp-hashhash"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	filecoincap "github.com/storacha/go-libstoracha/capabilities/filecoin"
	pdpcap "github.com/storacha/go-libstoracha/capabilities/pdp"
	"github.com/storacha/go-ucanto/core/dag/blockstore"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/invocation"
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

type recordedInvocation struct {
	capability ucan.Capability[filecoincap.OfferCaveats]
	invocation invocation.Invocation
}

func TestFilecoinOffer(t *testing.T) {
	space, err := ed25519signer.Generate()
	require.NoError(t, err)

	blobLink := helpers.RandomCID()

	dummyPrincipal, err := ed25519signer.Generate()
	require.NoError(t, err)

	pdpAcceptInv, err := pdpcap.Accept.Invoke(
		dummyPrincipal,
		dummyPrincipal.DID(),
		space.DID().String(),
		pdpcap.AcceptCaveats{
			Blob: blobLink.(cidlink.Link).Cid.Hash(),
		},
	)
	require.NoError(t, err)

	cp := &commp.Calc{}
	_, err = io.CopyN(cp, crand.Reader, 128)
	require.NoError(t, err)
	digest, _, err := cp.Digest()
	require.NoError(t, err)
	blobPieceCID, err := commcid.DataCommitmentToPieceCidv2(digest, 128)
	require.NoError(t, err)
	blobPieceLink := cidlink.Link{Cid: blobPieceCID}

	testCases := []struct {
		name           string
		opts           []client.FilecoinOfferOption
		testCapability func(t *testing.T, rc recordedInvocation, offerOk filecoincap.OfferOk)
	}{
		{
			name: "without PDP offer invocation",
			opts: nil,
			testCapability: func(t *testing.T, rc recordedInvocation, offerOk filecoincap.OfferOk) {
				capability := rc.capability
				require.Equal(t, space.DID().String(), capability.With(), "expected to have the space as the resource")
				require.Equal(t, blobLink, capability.Nb().Content, "expected to have the correct content link")
				require.Equal(t, blobPieceLink, capability.Nb().Piece, "expected to have the correct piece link")
				require.Equal(t, blobPieceLink, offerOk.Piece, "expected to get the correct piece link in the response")
			},
		},
		{
			name: "with PDP offer invocation",
			opts: []client.FilecoinOfferOption{
				client.WithPDPAcceptInvocation(pdpAcceptInv),
			},
			testCapability: func(t *testing.T, rc recordedInvocation, offerOk filecoincap.OfferOk) {
				capability := rc.capability
				require.Equal(t, space.DID().String(), capability.With(), "expected to have the space as the resource")
				require.Equal(t, blobLink, capability.Nb().Content, "expected to have the correct content link")
				require.Equal(t, blobPieceLink, capability.Nb().Piece, "expected to have the correct piece link")
				require.Equal(t, blobPieceLink, offerOk.Piece, "expected to get the correct piece link in the response")
				require.Equal(t, *capability.Nb().PDP, pdpAcceptInv.Link(), "expected to have the PDP accept invocation link in the capability")
				bs := helpers.Must(blockstore.NewBlockReader(blockstore.WithBlocksIterator(rc.invocation.Blocks())))
				del, err := delegation.NewDelegationView(pdpAcceptInv.Link(), bs)
				require.NoError(t, err)
				require.Len(t, del.Capabilities(), 1, "expected the PDP accept delegation to have exactly one capability")
				require.Equal(t, del.Capabilities()[0].With(), space.DID().String(), "expected the PDP accept delegation to have the space as the resource")
				require.Equal(t, del.Capabilities()[0].Can(), pdpcap.AcceptAbility)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			invokedCapabilities := []recordedInvocation{}

			connection := testutil.NewTestServerConnection(
				server.WithServiceMethod(
					filecoincap.Offer.Can(),
					server.Provide(
						filecoincap.Offer,
						func(
							ctx context.Context,
							cap ucan.Capability[filecoincap.OfferCaveats],
							inv invocation.Invocation,
							context server.InvocationContext,
						) (result.Result[filecoincap.OfferOk, failure.IPLDBuilderFailure], fx.Effects, error) {
							invokedCapabilities = append(invokedCapabilities, recordedInvocation{
								capability: cap,
								invocation: inv,
							})
							return result.Ok[filecoincap.OfferOk, failure.IPLDBuilderFailure](
								filecoincap.OfferOk{
									Piece: cap.Nb().Piece,
								},
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

			offerOk, err := c.FilecoinOffer(t.Context(), space.DID(), blobLink, blobPieceLink, tc.opts...)
			require.NoError(t, err)

			require.Len(t, invokedCapabilities, 1, "expected exactly one capability to be invoked")
			capability := invokedCapabilities[0]

			tc.testCapability(t, capability, offerOk)
		})
	}
}
