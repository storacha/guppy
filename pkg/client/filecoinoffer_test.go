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

func TestFilecoinOffer(t *testing.T) {
	space, err := ed25519signer.Generate()
	require.NoError(t, err)

	invokedCapabilities := []ucan.Capability[filecoincap.OfferCaveats]{}

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
					invokedCapabilities = append(invokedCapabilities, cap)
					return result.Ok[filecoincap.OfferOk, failure.IPLDBuilderFailure](
						filecoincap.OfferOk{
							Piece: cap.Nb().Piece,
						},
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

	blobLink := helpers.RandomCID()

	cp := &commp.Calc{}
	_, err = io.CopyN(cp, crand.Reader, 128)
	require.NoError(t, err)
	digest, _, err := cp.Digest()
	require.NoError(t, err)
	blobPieceCID, err := commcid.DataCommitmentToPieceCidv2(digest, 128)
	require.NoError(t, err)
	blobPieceLink := cidlink.Link{Cid: blobPieceCID}

	offerOk, err := c.FilecoinOffer(t.Context(), space.DID(), blobLink, blobPieceLink)
	require.NoError(t, err)

	require.Len(t, invokedCapabilities, 1, "expected exactly one capability to be invoked")
	capability := invokedCapabilities[0]

	require.Equal(t, space.DID().String(), capability.With(), "expected to have the space as the resource")
	require.Equal(t, blobLink, capability.Nb().Content, "expected to have the correct content link")
	require.Equal(t, blobPieceLink, capability.Nb().Piece, "expected to have the correct piece link")
	require.Equal(t, blobPieceLink, offerOk.Piece, "expected to get the correct piece link in the response")
}
