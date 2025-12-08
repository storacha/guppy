package client

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"iter"
	"net/http"
	"net/url"

	"github.com/multiformats/go-multihash"
	blobcap "github.com/storacha/go-libstoracha/capabilities/blob"
	httpcap "github.com/storacha/go-libstoracha/capabilities/http"
	spaceblobcap "github.com/storacha/go-libstoracha/capabilities/space/blob"
	captypes "github.com/storacha/go-libstoracha/capabilities/types"
	ucancap "github.com/storacha/go-libstoracha/capabilities/ucan"
	w3sblobcap "github.com/storacha/go-libstoracha/capabilities/web3.storage/blob"
	"github.com/storacha/go-libstoracha/failure"
	uclient "github.com/storacha/go-ucanto/client"
	"github.com/storacha/go-ucanto/core/dag/blockstore"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/ipld"
	"github.com/storacha/go-ucanto/core/receipt"
	"github.com/storacha/go-ucanto/core/receipt/ran"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/go-ucanto/ucan"
	receiptclient "github.com/storacha/guppy/pkg/receipt"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// SpaceBlobAddOption configures options for SpaceBlobAdd.
type SpaceBlobAddOption func(*SpaceBlobAddConfig)

// SpaceBlobAddConfig holds configuration for SpaceBlobAdd.
type SpaceBlobAddConfig struct {
	putClient          *http.Client
	precomputedDigest  multihash.Multihash
	precomputedSizePtr *uint64
}

func (s *SpaceBlobAddConfig) PutClient() *http.Client {
	return s.putClient
}

func (s *SpaceBlobAddConfig) PrecomputedDigest() multihash.Multihash {
	return s.precomputedDigest
}

func (s *SpaceBlobAddConfig) PrecomputedSizePtr() *uint64 {
	return s.precomputedSizePtr
}

// NewSpaceBlobAddConfig creates a new SpaceBlobAddConfig with the given options.
func NewSpaceBlobAddConfig(options ...SpaceBlobAddOption) *SpaceBlobAddConfig {
	cfg := &SpaceBlobAddConfig{
		putClient: &http.Client{
			Transport: otelhttp.NewTransport(http.DefaultTransport),
		},
	}
	for _, opt := range options {
		opt(cfg)
	}
	return cfg
}

// WithPutClient configures the HTTP client to use for uploading blobs.
func WithPutClient(client *http.Client) SpaceBlobAddOption {
	return func(cfg *SpaceBlobAddConfig) {
		cfg.putClient = client
	}
}

// WithPrecomputedDigest supplies a previously computed digest/size so we can skip re-hashing.
func WithPrecomputedDigest(d multihash.Multihash, size uint64) SpaceBlobAddOption {
	return func(cfg *SpaceBlobAddConfig) {
		cfg.precomputedDigest = d
		cfg.precomputedSizePtr = &size
	}
}

type AddedBlob struct {
	Digest    multihash.Multihash
	Location  invocation.Invocation
	PDPAccept invocation.Invocation
}

// SpaceBlobAdd adds a blob to the service. The issuer needs proof of
// `space/blob/add` delegated capability.
//
// Required delegated capability proofs: `space/blob/add`
//
// The `space` is the resource the invocation applies to. It is typically the
// DID of a space.
//
// The `content` is the blob content to be added.
//
// The `proofs` are delegation proofs to use in addition to those in the client.
// They won't be saved in the client, only used for this invocation.
//
// Returns the multihash of the added blob and the location commitment that contains details about where the
// blob can be located, or an error if something went wrong.
func (c *Client) SpaceBlobAdd(ctx context.Context, content io.Reader, space did.DID, options ...SpaceBlobAddOption) (AddedBlob, error) {
	ctx, span := tracer.Start(ctx, "space-blob-add", trace.WithAttributes(
		attribute.String("space", space.String()),
	))
	defer span.End()

	// Configure options
	cfg := NewSpaceBlobAddConfig(options...)

	putClient := cfg.PutClient()

	contentReader := content

	contentHash := cfg.PrecomputedDigest()
	contentSizePtr := cfg.PrecomputedSizePtr()
	if contentSizePtr == nil || contentHash == nil || len(contentHash) == 0 {

		_, readSpan := tracer.Start(ctx, "read-content")
		contentBytes, err := io.ReadAll(content)
		if err != nil {
			readSpan.End()
			return AddedBlob{}, fmt.Errorf("reading content: %w", err)
		}
		readSpan.SetAttributes(attribute.Int("content-size", len(contentBytes)))
		readSpan.End()
		_, hashSpan := tracer.Start(ctx, "hash-content", trace.WithAttributes(
			attribute.Int("content-size", len(contentBytes)),
		))
		contentHash, err = multihash.Sum(contentBytes, multihash.SHA2_256, -1)
		if err != nil {
			hashSpan.End()
			return AddedBlob{}, fmt.Errorf("computing content multihash: %w", err)
		}
		hashSpan.End()
		contentReader = bytes.NewReader(contentBytes)
		contentSize := uint64(len(contentBytes))
		contentSizePtr = &contentSize
	}
	caveats := spaceblobcap.AddCaveats{
		Blob: captypes.Blob{
			Digest: contentHash,
			Size:   uint64(*contentSizePtr),
		},
	}

	res, fx, err := invokeAndExecute[spaceblobcap.AddCaveats, spaceblobcap.AddOk](
		ctx,
		c,
		spaceblobcap.Add,
		space.String(),
		caveats,
		spaceblobcap.AddOkType(),
	)
	if err != nil {
		return AddedBlob{}, fmt.Errorf("invoking and executing `space/blob/add`: %w", err)
	}

	_, failErr := result.Unwrap(res)
	if failErr != nil {
		return AddedBlob{}, fmt.Errorf("`space/blob/add` failed: %w", failErr)
	}

	var allocateTask, putTask, acceptTask invocation.Invocation
	legacyAccept := false
	var concludeFxs []invocation.Invocation
	for _, task := range fx.Fork() {
		inv, ok := task.Invocation()
		if ok {
			switch inv.Capabilities()[0].Can() {
			case blobcap.AllocateAbility:
				allocateTask = inv
			case w3sblobcap.AllocateAbility:
				if allocateTask == nil {
					allocateTask = inv
				}
			case ucancap.ConcludeAbility:
				concludeFxs = append(concludeFxs, inv)
			case httpcap.PutAbility:
				putTask = inv
			case blobcap.AcceptAbility:
				acceptTask = inv
			case w3sblobcap.AcceptAbility:
				if acceptTask == nil {
					acceptTask = inv
					legacyAccept = true
				}
			default:
				log.Warnf("ignoring unexpected task: %s", inv.Capabilities()[0].Can())
			}
		}
	}

	switch {
	case allocateTask == nil:
		return AddedBlob{}, fmt.Errorf("mandatory blob/allocate task not received in space/blob/add receipt")
	case putTask == nil:
		return AddedBlob{}, fmt.Errorf("mandatory http/put task not received in space/blob/add receipt")
	case acceptTask == nil:
		return AddedBlob{}, fmt.Errorf("mandatory blob/accept task not received in space/blob/add receipt")
	case len(concludeFxs) == 0:
		return AddedBlob{}, fmt.Errorf("mandatory ucan/conclude tasks not received in space/blob/add receipt")
	}

	var allocateRcpt receipt.Receipt[blobcap.AllocateOk, failure.FailureModel]
	var legacyAllocateRcpt receipt.Receipt[w3sblobcap.AllocateOk, failure.FailureModel]
	// TK: Why not use receipt.Rebind here?
	var putRcpt receipt.AnyReceipt
	var acceptRcpt receipt.Receipt[blobcap.AcceptOk, failure.FailureModel]
	var legacyAcceptRcpt receipt.Receipt[w3sblobcap.AcceptOk, failure.FailureModel]
	for _, concludeFx := range concludeFxs {
		concludeRcpt, err := getConcludeReceipt(concludeFx)
		if err != nil {
			return AddedBlob{}, fmt.Errorf("reading ucan/conclude receipt: %w", err)
		}

		switch concludeRcpt.Ran().Link() {
		case allocateTask.Link():
			ability := allocateTask.Capabilities()[0].Can()
			switch ability {
			case blobcap.AllocateAbility:
				allocateRcpt, err = receipt.Rebind[blobcap.AllocateOk, failure.FailureModel](concludeRcpt, blobcap.AllocateOkType(), failure.FailureType(), captypes.Converters...)
				if err != nil {
					return AddedBlob{}, fmt.Errorf("bad allocate receipt in conclude fx: %w", err)
				}
			case w3sblobcap.AllocateAbility:
				legacyAllocateRcpt, err = receipt.Rebind[w3sblobcap.AllocateOk, failure.FailureModel](concludeRcpt, w3sblobcap.AllocateOkType(), failure.FailureType(), captypes.Converters...)
				if err != nil {
					return AddedBlob{}, fmt.Errorf("bad (legacy) allocate receipt in conclude fx: %w", err)
				}
			default:
				return AddedBlob{}, fmt.Errorf("unexpected capability in allocate task: %s", ability)
			}
		case putTask.Link():
			putRcpt = concludeRcpt
		case acceptTask.Link():
			ability := acceptTask.Capabilities()[0].Can()
			switch ability {
			case blobcap.AcceptAbility:
				acceptRcpt, err = receipt.Rebind[blobcap.AcceptOk, failure.FailureModel](concludeRcpt, blobcap.AcceptOkType(), failure.FailureType(), captypes.Converters...)
				if err != nil {
					return AddedBlob{}, fmt.Errorf("bad accept receipt in conclude fx: %w", err)
				}
			case w3sblobcap.AcceptAbility:
				legacyAcceptRcpt, err = receipt.Rebind[w3sblobcap.AcceptOk, failure.FailureModel](concludeRcpt, w3sblobcap.AcceptOkType(), failure.FailureType(), captypes.Converters...)
				if err != nil {
					return AddedBlob{}, fmt.Errorf("bad (legacy) accept receipt in conclude fx: %w", err)
				}
			default:
				return AddedBlob{}, fmt.Errorf("unexpected capability in accept task: %s", ability)
			}
		default:
			inv, ok := concludeRcpt.Ran().Invocation()
			if !ok {
				log.Debugf("ignoring receipt for unknown task: %s", concludeRcpt.Ran().Link())
			}
			ability := inv.Capabilities()[0].Can()
			switch ability {
			case w3sblobcap.AllocateAbility, w3sblobcap.AcceptAbility:
				log.Debugf("ignoring receipt for additional `%s` legacy task: %s", ability, concludeRcpt.Ran().Link())
			default:
				log.Warnf("ignoring receipt for unexpected `%s` task: %s", ability, concludeRcpt.Ran().Link())
			}
		}
	}

	var url *url.URL
	var headers http.Header
	switch {
	case allocateRcpt != nil:
		allocateOk, err := result.Unwrap(result.MapError(allocateRcpt.Out(), failure.FromFailureModel))
		if err != nil {
			return AddedBlob{}, fmt.Errorf("blob allocation failed: %w", err)
		}

		address := allocateOk.Address
		if address != nil {
			url = &address.URL
			headers = address.Headers
		}

	case legacyAllocateRcpt != nil:
		allocateOk, err := result.Unwrap(result.MapError(legacyAllocateRcpt.Out(), failure.FromFailureModel))
		if err != nil {
			return AddedBlob{}, fmt.Errorf("blob allocation failed: %w", err)
		}

		address := allocateOk.Address
		if address != nil {
			url = &address.URL
			headers = address.Headers
		}

	default:
		return AddedBlob{}, fmt.Errorf("mandatory receipts not received in space/blob/add receipt")
	}

	if url != nil && headers != nil {
		if err := putBlob(ctx, putClient, url, headers, contentReader); err != nil {
			return AddedBlob{}, fmt.Errorf("putting blob: %w", err)
		}
	}

	// invoke `ucan/conclude` with `http/put` receipt
	if putRcpt == nil {
		if err := c.sendPutReceipt(ctx, putTask); err != nil {
			return AddedBlob{}, fmt.Errorf("sending put receipt: %w", err)
		}
	} else {
		putOk, _ := result.Unwrap(putRcpt.Out())
		if putOk == nil {
			if err := c.sendPutReceipt(ctx, putTask); err != nil {
				return AddedBlob{}, fmt.Errorf("sending put receipt: %w", err)
			}
		}
	}

	// ensure the blob has been accepted
	var anyAcceptRcpt receipt.AnyReceipt
	var site ucan.Link
	var pdpAcceptLink *ucan.Link
	var rcptBlocks iter.Seq2[ipld.Block, error]
	if acceptRcpt == nil && legacyAcceptRcpt == nil {
		anyAcceptRcpt, err = c.receiptsClient.Poll(ctx, acceptTask.Link(), receiptclient.WithRetries(5))
		if err != nil {
			return AddedBlob{}, fmt.Errorf("polling accept: %w", err)
		}
	} else if acceptRcpt != nil {
		acceptOk, failErr := result.Unwrap(result.MapError(acceptRcpt.Out(), failure.FromFailureModel))
		if failErr != nil {
			anyAcceptRcpt, err = c.receiptsClient.Poll(ctx, acceptTask.Link(), receiptclient.WithRetries(5))
			if err != nil {
				return AddedBlob{}, fmt.Errorf("polling accept: %w", err)
			}
		} else {
			site = acceptOk.Site
			pdpAcceptLink = acceptOk.PDP
			rcptBlocks = acceptRcpt.Blocks()
		}
	} else if legacyAcceptRcpt != nil {
		acceptOk, failErr := result.Unwrap(result.MapError(legacyAcceptRcpt.Out(), failure.FromFailureModel))
		if failErr != nil {
			anyAcceptRcpt, err = c.receiptsClient.Poll(ctx, acceptTask.Link(), receiptclient.WithRetries(5))
			if err != nil {
				return AddedBlob{}, fmt.Errorf("polling accept: %w", err)
			}
		} else {
			site = acceptOk.Site
			rcptBlocks = legacyAcceptRcpt.Blocks()
		}
	}

	if site == nil {
		if !legacyAccept {
			acceptRcpt, err = receipt.Rebind[blobcap.AcceptOk, failure.FailureModel](anyAcceptRcpt, blobcap.AcceptOkType(), failure.FailureType(), captypes.Converters...)
			if err != nil {
				return AddedBlob{}, fmt.Errorf("fetching accept receipt: %w", err)
			}

			acceptOk, err := result.Unwrap(result.MapError(acceptRcpt.Out(), failure.FromFailureModel))
			if err != nil {
				return AddedBlob{}, fmt.Errorf("blob/accept failed: %w", err)
			}

			site = acceptOk.Site
			pdpAcceptLink = acceptOk.PDP
			rcptBlocks = acceptRcpt.Blocks()
		} else {
			legacyAcceptRcpt, err = receipt.Rebind[w3sblobcap.AcceptOk, failure.FailureModel](anyAcceptRcpt, w3sblobcap.AcceptOkType(), failure.FailureType(), captypes.Converters...)
			if err != nil {
				return AddedBlob{}, fmt.Errorf("fetching legacy accept receipt: %w", err)
			}

			acceptOk, err := result.Unwrap(result.MapError(legacyAcceptRcpt.Out(), failure.FromFailureModel))
			if err != nil {
				return AddedBlob{}, fmt.Errorf("web3.storage/blob/accept failed: %w", err)
			}

			site = acceptOk.Site
			rcptBlocks = legacyAcceptRcpt.Blocks()
		}
	}

	blksReader, err := blockstore.NewBlockStore(blockstore.WithBlocksIterator(rcptBlocks))
	if err != nil {
		return AddedBlob{}, fmt.Errorf("reading location commitment blocks: %w", err)
	}

	location, err := invocation.NewInvocationView(site, blksReader)
	if err != nil {
		return AddedBlob{}, fmt.Errorf("creating location delegation: %w", err)
	}

	var pdpAccept invocation.Invocation
	if pdpAcceptLink != nil {
		pdpAccept, err = invocation.NewInvocationView(*pdpAcceptLink, blksReader)
		if err != nil {
			return AddedBlob{}, fmt.Errorf("creating `pdp/accept` delegation: %w", err)
		}
	}
	return AddedBlob{
		Digest:    contentHash,
		Location:  location,
		PDPAccept: pdpAccept,
	}, nil
}

func getConcludeReceipt(concludeFx invocation.Invocation) (receipt.AnyReceipt, error) {
	concludeNb, fail := ucancap.ConcludeCaveatsReader.Read(concludeFx.Capabilities()[0].Nb())
	if fail != nil {
		return nil, fmt.Errorf("invalid conclude receipt: %w", fail)
	}

	reader := receipt.NewAnyReceiptReader(captypes.Converters...)
	rcpt, err := reader.Read(concludeNb.Receipt, concludeFx.Blocks())
	if err != nil {
		return nil, fmt.Errorf("reading receipt: %w", err)
	}

	return rcpt, nil
}

func putBlob(ctx context.Context, client *http.Client, url *url.URL, headers http.Header, body io.Reader) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url.String(), body)
	if err != nil {
		return fmt.Errorf("creating upload request: %w", err)
	}

	for k, v := range headers {
		req.Header.Set(k, v[0])
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("uploading blob: %w", err)
	}
	io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("uploading blob: %s", resp.Status)
	}

	return nil
}

func (c *Client) sendPutReceipt(ctx context.Context, putTask invocation.Invocation) error {
	if len(putTask.Facts()) != 1 {
		return fmt.Errorf("invalid put facts, wanted 1 fact but got %d", len(putTask.Facts()))
	}

	if _, ok := putTask.Facts()[0]["keys"]; !ok {
		return fmt.Errorf("invalid put facts, missing 'keys' field")
	}

	putKeysNode, ok := putTask.Facts()[0]["keys"].(ipld.Node)
	if !ok {
		return fmt.Errorf("invalid put facts, 'keys' field is not a node")
	}

	// TODO: define a schema and use bindnode.Rebind rather than doing this manually
	var id did.DID
	keys := map[string][]byte{}
	it := putKeysNode.MapIterator()
	for !it.Done() {
		k, v, err := it.Next()
		if err != nil {
			return fmt.Errorf("invalid put facts: %w", err)
		}

		kStr, err := k.AsString()
		if err != nil {
			return fmt.Errorf("invalid put facts: %w", err)
		}

		switch kStr {
		case "id":
			// v is a did string
			vStr, err := v.AsString()
			if err != nil {
				return fmt.Errorf("invalid put facts: %w", err)
			}

			id, err = did.Parse(vStr)
			if err != nil {
				return fmt.Errorf("invalid put facts: %w", err)
			}
		case "keys":
			// v is a did to key map
			it2 := v.MapIterator()
			for !it2.Done() {
				k2, v2, err := it2.Next()
				if err != nil {
					return fmt.Errorf("invalid put facts: %w", err)
				}

				k2Str, err := k2.AsString()
				if err != nil {
					return fmt.Errorf("invalid put facts: %w", err)
				}

				v2Bytes, err := v2.AsBytes()
				if err != nil {
					return fmt.Errorf("invalid put facts: %w", err)
				}

				keys[k2Str] = v2Bytes
			}
		}
	}

	derivedKey, ok := keys[id.String()]
	if !ok {
		return fmt.Errorf("invalid put facts: missing key for %s", id.String())
	}

	derivedSigner, err := signer.Decode(derivedKey)
	if err != nil {
		return fmt.Errorf("deriving signer: %w", err)
	}

	putRcpt, err := receipt.Issue(derivedSigner, result.Ok[httpcap.PutOk, ipld.Builder](httpcap.PutOk{}), ran.FromInvocation(putTask))
	if err != nil {
		return fmt.Errorf("generating receipt: %w", err)
	}

	// var concludeFacts []ucan.FactBuilder
	// for rcptBlock, err := range putRcpt.Blocks() {
	// 	if err != nil {
	// 		return nil, nil, fmt.Errorf("getting receipt block: %w", err)
	// 	}

	// 	concludeFacts = append(concludeFacts, rcptBlock.Link())
	// }

	httpPutConcludeInvocation, err := ucancap.Conclude.Invoke(
		c.Issuer(),
		c.Connection().ID(),
		c.Issuer().DID().String(),
		ucancap.ConcludeCaveats{
			Receipt: putRcpt.Root().Link(),
		},
		// delegation.WithFacts(concludeFacts),
		delegation.WithNoExpiration(),
	)
	if err != nil {
		return fmt.Errorf("generating invocation: %w", err)
	}

	// attach the receipt to the conclude invocation
	for rcptBlock, err := range putRcpt.Blocks() {
		if err != nil {
			return fmt.Errorf("getting receipt block: %w", err)
		}

		httpPutConcludeInvocation.Attach(rcptBlock)
	}

	resp, err := uclient.Execute(ctx, []invocation.Invocation{httpPutConcludeInvocation}, c.Connection())
	if err != nil {
		return fmt.Errorf("executing conclude invocation: %w", err)
	}

	rcptlnk, ok := resp.Get(httpPutConcludeInvocation.Link())
	if !ok {
		return fmt.Errorf("receipt not found: %s", httpPutConcludeInvocation.Link())
	}

	reader, err := receipt.NewReceiptReaderFromTypes[ucancap.ConcludeOk, failure.FailureModel](ucancap.ConcludeOkType(), failure.FailureType(), captypes.Converters...)
	if err != nil {
		return fmt.Errorf("generating receipt reader: %w", err)
	}

	rcpt, err := reader.Read(rcptlnk, resp.Blocks())
	if err != nil {
		return fmt.Errorf("reading receipt: %w", err)
	}

	_, err = result.Unwrap(result.MapError(rcpt.Out(), failure.FromFailureModel))
	if err != nil {
		return fmt.Errorf("ucan/conclude failed: %w", err)
	}

	return nil
}
