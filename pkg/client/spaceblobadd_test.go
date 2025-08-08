package client_test

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/storacha/go-ucanto/core/delegation"
	ed25519signer "github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/storacha/guppy/pkg/client"
	"github.com/storacha/guppy/pkg/client/testutil"
	"github.com/stretchr/testify/require"
)

// blobPutTransport is an [http.RoundTripper] (an [http.Client] transport) that
// accepts blob PUTs and remembers what was received.
type blobPutTransport struct {
	receivedBlobs [][]byte
}

var _ http.RoundTripper = (*blobPutTransport)(nil)

func (r *blobPutTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	blob, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, fmt.Errorf("reading blob from request: %w", err)
	}
	r.receivedBlobs = append(r.receivedBlobs, blob)

	return &http.Response{
		StatusCode: 200,
	}, nil
}

func TestSpaceBlobAdd(t *testing.T) {
	space, err := ed25519signer.Generate()
	require.NoError(t, err)

	blobPutTransport := blobPutTransport{}

	c, err := testutil.SpaceBlobAddClient()
	require.NoError(t, err)

	// Delegate * on the space to the client
	cap := ucan.NewCapability("*", space.DID().String(), ucan.NoCaveats{})
	proof, err := delegation.Delegate(space, c.Issuer(), []ucan.Capability[ucan.NoCaveats]{cap}, delegation.WithNoExpiration())
	require.NoError(t, err)
	err = c.AddProofs(proof)
	require.NoError(t, err)

	testBlob := bytes.NewReader([]byte("test"))

	_, _, err = c.SpaceBlobAdd(testContext(t), testBlob, space.DID(), client.WithPutClient(&http.Client{
		Transport: &blobPutTransport,
	}))
	require.NoError(t, err)

	require.ElementsMatch(t, [][]byte{[]byte("test")}, blobPutTransport.receivedBlobs)
}
