package client_test

import (
	"bytes"
	"testing"

	"github.com/multiformats/go-multihash"
	"github.com/storacha/go-ucanto/core/delegation"
	ed25519signer "github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/storacha/guppy/pkg/client"
	"github.com/storacha/guppy/pkg/client/testutil"
	"github.com/stretchr/testify/require"
)

func TestSpaceBlobAdd(t *testing.T) {
	space, err := ed25519signer.Generate()
	require.NoError(t, err)

	putClient := testutil.NewPutClient()

	c, err := testutil.Client(testutil.WithSpaceBlobAdd())
	require.NoError(t, err)

	// Delegate * on the space to the client
	cap := ucan.NewCapability("*", space.DID().String(), ucan.NoCaveats{})
	proof, err := delegation.Delegate(space, c.Issuer(), []ucan.Capability[ucan.NoCaveats]{cap}, delegation.WithNoExpiration())
	require.NoError(t, err)
	err = c.AddProofs(proof)
	require.NoError(t, err)

	testBlob := bytes.NewReader([]byte("test"))

	returnedDigest, _, err := c.SpaceBlobAdd(testContext(t), testBlob, space.DID(), client.WithPutClient(putClient))
	require.NoError(t, err)

	digest, err := multihash.Sum([]byte("test"), multihash.SHA2_256, -1)
	require.NoError(t, err)

	require.Equal(t, digest, returnedDigest)
	require.Equal(t, []byte("test"), testutil.ReceivedBlobs(putClient).Get(digest))
	require.Equal(t, 1, testutil.ReceivedBlobs(putClient).Size())
}
