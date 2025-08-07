package client_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"testing"

	spaceblobcap "github.com/storacha/go-libstoracha/capabilities/space/blob"
	ucancap "github.com/storacha/go-libstoracha/capabilities/ucan"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/message"
	"github.com/storacha/go-ucanto/core/receipt"
	"github.com/storacha/go-ucanto/core/receipt/fx"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/core/result/failure"
	ed25519signer "github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/go-ucanto/server"
	"github.com/storacha/go-ucanto/testing/helpers"
	uhelpers "github.com/storacha/go-ucanto/testing/helpers"
	carresp "github.com/storacha/go-ucanto/transport/car/response"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/storacha/guppy/pkg/client"
	"github.com/storacha/guppy/pkg/client/testutil"
	receiptclient "github.com/storacha/guppy/pkg/receipt"
	"github.com/stretchr/testify/require"
)

// receiptsTransport is an [http.RoundTripper] (an [http.Client] transport) that
// serves known receipts directly rather than using the network.
type receiptsTransport struct {
	receipts map[string]receipt.AnyReceipt
}

var _ http.RoundTripper = (*receiptsTransport)(nil)

func (r *receiptsTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	path := req.URL.Path
	invCid := path[10:]
	rcpt, ok := r.receipts[invCid]
	if !ok {
		return nil, fmt.Errorf("no receipt for invocation %s", invCid)
	}

	msg, err := message.Build(nil, []receipt.AnyReceipt{rcpt})
	if err != nil {
		return nil, fmt.Errorf("building message: %w", err)
	}

	resp, err := carresp.Encode(msg)
	if err != nil {
		return nil, fmt.Errorf("encoding message %w", err)
	}

	return &http.Response{
		StatusCode: 200,
		Body:       io.NopCloser(resp.Body()),
		Header:     resp.Headers(),
	}, nil
}

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

	receiptsTrans := receiptsTransport{
		receipts: make(map[string]receipt.AnyReceipt),
	}

	blobPutTransport := blobPutTransport{}

	var c *client.Client

	connection := testutil.NewTestServerConnection(
		server.WithServiceMethod(
			spaceblobcap.Add.Can(),
			server.Provide(
				spaceblobcap.Add,
				uhelpers.Must(testutil.SpaceBlobAddHandler(
					func(rcpt receipt.AnyReceipt) {
						receiptsTrans.receipts[rcpt.Ran().Root().Link().String()] = rcpt
					},
				)),
			),
		),
		server.WithServiceMethod(
			ucancap.Conclude.Can(),
			server.Provide(
				ucancap.Conclude,
				func(
					ctx context.Context,
					cap ucan.Capability[ucancap.ConcludeCaveats],
					inv invocation.Invocation,
					context server.InvocationContext,
				) (result.Result[ucancap.ConcludeOk, failure.IPLDBuilderFailure], fx.Effects, error) {
					return result.Ok[ucancap.ConcludeOk, failure.IPLDBuilderFailure](ucancap.ConcludeOk{}), nil, nil
				},
			),
		),
	)

	c, err = client.NewClient(
		client.WithConnection(connection),
		client.WithReceiptsClient(
			receiptclient.New(
				helpers.Must(url.Parse("https://receipts.example/receipts")),
				receiptclient.WithHTTPClient(
					&http.Client{
						Transport: &receiptsTrans,
					},
				),
			),
		),
	)

	require.NoError(t, err)

	// delegate * to the space
	cap := ucan.NewCapability("*", space.DID().String(), ucan.NoCaveats{})
	proof, err := delegation.Delegate(space, c.Issuer(), []ucan.Capability[ucan.NoCaveats]{cap}, delegation.WithNoExpiration())
	require.NoError(t, err)

	c.AddProofs(proof)

	testBlob := bytes.NewReader([]byte("test"))

	_, _, err = c.SpaceBlobAdd(testContext(t), testBlob, space.DID(), client.WithPutClient(&http.Client{
		Transport: &blobPutTransport,
	}))
	require.NoError(t, err)

	require.ElementsMatch(t, [][]byte{[]byte("test")}, blobPutTransport.receivedBlobs)
}
