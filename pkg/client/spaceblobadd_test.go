package client_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/fluent/qp"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/multiformats/go-multihash"
	assertcap "github.com/storacha/go-libstoracha/capabilities/assert"
	blobcap "github.com/storacha/go-libstoracha/capabilities/blob"
	httpcap "github.com/storacha/go-libstoracha/capabilities/http"
	spaceblobcap "github.com/storacha/go-libstoracha/capabilities/space/blob"
	captypes "github.com/storacha/go-libstoracha/capabilities/types"
	ucancap "github.com/storacha/go-libstoracha/capabilities/ucan"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/invocation/ran"
	"github.com/storacha/go-ucanto/core/message"
	"github.com/storacha/go-ucanto/core/receipt"
	"github.com/storacha/go-ucanto/core/receipt/fx"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/core/result/failure"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal"
	ed25519signer "github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/go-ucanto/server"
	"github.com/storacha/go-ucanto/testing/helpers"
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

func invokeAllocate(
	service ucan.Signer,
	storageProvider ucan.Principal,
	spaceDID did.DID,
	blobDigest multihash.Multihash,
	blobSize uint64,
	addInv invocation.Invocation,
) (invocation.IssuedInvocation, error) {
	return blobcap.Allocate.Invoke(
		service,
		storageProvider,
		spaceDID.String(),
		blobcap.AllocateCaveats{
			Space: spaceDID,
			Blob: captypes.Blob{
				Digest: blobDigest,
				Size:   blobSize,
			},
			Cause: addInv.Link(),
		},
	)
}

func executeAllocate(
	allocateInv invocation.IssuedInvocation,
	storageProvider ucan.Signer,
	blobSize uint64,
) (receipt.AnyReceipt, error) {
	putBlobURL, err := url.Parse("https://storage.example/store/" + allocateInv.Root().Link().String())
	if err != nil {
		return nil, fmt.Errorf("parsing put blob URL: %w", err)
	}

	allocateResult := result.Ok[blobcap.AllocateOk, failure.IPLDBuilderFailure](blobcap.AllocateOk{
		Size: blobSize,
		Address: &blobcap.Address{
			URL:     *putBlobURL,
			Headers: http.Header{"some-header": []string{"some-value"}},
			Expires: uint64(time.Now().Add(1 * time.Minute).Unix()),
		},
	})

	return receipt.Issue(storageProvider, allocateResult, ran.FromInvocation(allocateInv))
}

type httpPutFact struct {
	id  string
	key []byte
}

func (hpf httpPutFact) ToIPLD() (map[string]datamodel.Node, error) {
	n, err := qp.BuildMap(basicnode.Prototype.Any, 2, func(ma datamodel.MapAssembler) {
		qp.MapEntry(ma, "id", qp.String(hpf.id))
		qp.MapEntry(ma, "keys", qp.Map(2, func(ma datamodel.MapAssembler) {
			qp.MapEntry(ma, hpf.id, qp.Bytes(hpf.key))
		}))
	})
	if err != nil {
		return nil, err
	}

	return map[string]datamodel.Node{
		"keys": n,
	}, nil
}

func invokePut(
	blobProvider principal.Signer,
	blobDigest multihash.Multihash,
	blobSize uint64,
	allocateRcptLink ucan.Link,
) (invocation.IssuedInvocation, error) {
	fct := httpPutFact{
		id:  blobProvider.DID().String(),
		key: blobProvider.Encode(),
	}

	facts := []ucan.FactBuilder{fct}
	return httpcap.Put.Invoke(
		blobProvider,
		blobProvider,
		blobProvider.DID().String(),
		httpcap.PutCaveats{
			URL: captypes.Promise{
				UcanAwait: captypes.Await{
					Selector: ".out.ok.address.url",
					Link:     allocateRcptLink,
				},
			},
			Headers: captypes.Promise{
				UcanAwait: captypes.Await{
					Selector: ".out.ok.address.headers",
					Link:     allocateRcptLink,
				},
			},
			Body: httpcap.Body{
				Digest: blobDigest,
				Size:   blobSize,
			},
		},
		delegation.WithFacts(facts),
	)
}

func invokeAccept(
	service ucan.Signer,
	storageProvider ucan.Principal,
	spaceDID did.DID,
	blobDigest multihash.Multihash,
	blobSize uint64,
	httpPutInvLink ucan.Link,
) (invocation.IssuedInvocation, error) {
	return blobcap.Accept.Invoke(
		service,
		storageProvider,
		storageProvider.DID().String(),
		blobcap.AcceptCaveats{
			Space: spaceDID,
			Blob: captypes.Blob{
				Digest: blobDigest,
				Size:   blobSize,
			},
			Put: blobcap.Promise{
				UcanAwait: blobcap.Await{
					Selector: ".out.ok",
					Link:     httpPutInvLink,
				},
			},
		},
	)
}

func executeAccept(
	acceptInv invocation.IssuedInvocation,
	storageProvider ucan.Signer,
	spaceDID did.DID,
	blobDigest multihash.Multihash,
) (receipt.AnyReceipt, error) {
	locationClaim, err := assertcap.Location.Delegate(
		storageProvider,
		spaceDID,
		spaceDID.String(),
		assertcap.LocationCaveats{
			Space:    spaceDID,
			Content:  captypes.FromHash(blobDigest),
			Location: []url.URL{*helpers.Must(url.Parse("https://storage.example/fetch/" + blobDigest.HexString()))},
		},
		delegation.WithNoExpiration(),
	)
	if err != nil {
		return nil, fmt.Errorf("creating location claim delegation: %w", err)
	}

	acceptOk := result.Ok[blobcap.AcceptOk, failure.IPLDBuilderFailure](blobcap.AcceptOk{
		Site: locationClaim.Link(),
	})

	acceptRcpt, err := receipt.Issue(
		storageProvider,
		acceptOk,
		ran.FromInvocation(acceptInv),
		receipt.WithFork(fx.FromInvocation(locationClaim)),
	)
	if err != nil {
		return nil, fmt.Errorf("issuing receipt: %w", err)
	}
	return acceptRcpt, err
}

func TestSpaceBlobAdd(t *testing.T) {
	space, err := ed25519signer.Generate()
	require.NoError(t, err)

	storageProvider, err := ed25519signer.Generate()
	require.NoError(t, err)

	// TK: why?
	// random signer rather than the proper derived one
	//blobProvider, err := ed25519signer.FromSeed([]byte(blobDigest)[len(blobDigest)-32:])
	blobProvider, err := ed25519signer.Generate()
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
				func(
					ctx context.Context,
					cap ucan.Capability[spaceblobcap.AddCaveats],
					inv invocation.Invocation,
					context server.InvocationContext,
				) (result.Result[spaceblobcap.AddOk, failure.IPLDBuilderFailure], fx.Effects, error) {
					spaceDID, err := did.Parse(cap.With())
					if err != nil {
						return nil, nil, fmt.Errorf("parsing space DID: %w", err)
					}
					blobDigest := cap.Nb().Blob.Digest
					blobSize := cap.Nb().Blob.Size

					allocateInv, err := invokeAllocate(
						context.ID(),
						storageProvider,
						spaceDID,
						blobDigest,
						blobSize,
						inv)
					// TK: allocateInv.Attach(inv.Root())
					require.NoError(t, err)

					allocateRcpt, err := executeAllocate(allocateInv, storageProvider, blobSize)
					require.NoError(t, err)
					receiptsTrans.receipts[allocateInv.Root().Link().String()] = allocateRcpt

					httpPutInv, err := invokePut(
						blobProvider,
						blobDigest,
						blobSize,
						allocateRcpt.Root().Link(),
					)
					require.NoError(t, err)
					// TK: httpPutInv.Attach(allocateRcpt.Root())

					acceptInv, err := invokeAccept(
						context.ID(),
						storageProvider,
						spaceDID,
						blobDigest,
						blobSize,
						httpPutInv.Root().Link(),
					)
					require.NoError(t, err)

					acceptRcpt, err := executeAccept(
						acceptInv,
						storageProvider,
						spaceDID,
						blobDigest,
					)
					require.NoError(t, err)

					receiptsTrans.receipts[acceptInv.Root().Link().String()] = acceptRcpt

					concludeInv, err := ucancap.Conclude.Invoke(
						context.ID(),
						storageProvider,
						cap.With(),
						ucancap.ConcludeCaveats{
							Receipt: allocateRcpt.Root().Link(),
						},
					)
					concludeInv.Attach(allocateRcpt.Root())

					forks := []fx.Effect{
						fx.FromInvocation(allocateInv),
						fx.FromInvocation(concludeInv),
						fx.FromInvocation(httpPutInv),
						fx.FromInvocation(acceptInv),
					}
					fxs := fx.NewEffects(fx.WithFork(forks...))

					ok := spaceblobcap.AddOk{
						Site: captypes.Promise{
							UcanAwait: captypes.Await{
								Selector: ".out.ok.site",
								// TK:
								// Link:     acceptInv.Root().Link(),
								Link: helpers.RandomCID(),
							},
						},
					}
					return result.Ok[spaceblobcap.AddOk, failure.IPLDBuilderFailure](ok), fxs, nil
				},
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
