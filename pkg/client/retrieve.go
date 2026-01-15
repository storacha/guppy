package client

import (
	"context"
	"fmt"
	"io"
	"math/rand"

	contentcap "github.com/storacha/go-libstoracha/capabilities/space/content"
	captypes "github.com/storacha/go-libstoracha/capabilities/types"
	"github.com/storacha/go-libstoracha/digestutil"
	"github.com/storacha/go-libstoracha/failure"
	rclient "github.com/storacha/go-ucanto/client/retrieval"
	"github.com/storacha/go-ucanto/core/dag/blockstore"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/receipt"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/guppy/pkg/client/locator"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func (c *Client) Retrieve(ctx context.Context, location locator.Location) (io.ReadCloser, error) {
	space := location.Commitment.Nb().Space

	nodeID, err := did.Parse(location.Commitment.With())
	if err != nil {
		return nil, fmt.Errorf("parsing DID of storage provider node `%s`: %w", location.Commitment.With(), err)
	}

	// Select a random URL from the list of available URLs
	urls := location.Commitment.Nb().Location
	url := urls[rand.Intn(len(urls))]

	shardDigest := location.Commitment.Nb().Content.Hash()

	ctx, span := tracer.Start(ctx, "retrieve", trace.WithAttributes(
		attribute.String("retrieve.space", space.DID().String()),
		attribute.String("retrieve.storage-provider", nodeID.String()),
		attribute.String("retrieve.url", url.String()),
		attribute.String("retrieve.shard.digest", digestutil.Format(shardDigest)),
		attribute.Int64("retrieve.offset", int64(location.Position.Offset)),
		attribute.Int64("retrieve.length", int64(location.Position.Length)),
	))
	defer span.End()

	storageProvider, err := did.Parse(location.Commitment.With())
	if err != nil {
		return nil, fmt.Errorf("parsing DID of storage provider `%s`: %w", location.Commitment.With(), err)
	}

	delegations := c.Proofs(CapabilityQuery{
		Can:  contentcap.Retrieve.Can(),
		With: space.String(),
	})
	prfs := make([]delegation.Proof, 0, len(delegations))
	for _, del := range delegations {
		prfs = append(prfs, delegation.FromDelegation(del))
	}

	start := location.Position.Offset
	end := start + location.Position.Length - 1

	inv, err := contentcap.Retrieve.Invoke(
		c.Issuer(),
		storageProvider,
		space.String(),
		contentcap.RetrieveCaveats{
			Blob: contentcap.BlobDigest{Digest: location.Commitment.Nb().Content.Hash()},
			Range: contentcap.Range{
				Start: start,
				End:   end,
			},
		},
		delegation.WithProof(prfs...),
	)
	if err != nil {
		return nil, fmt.Errorf("invoking `space/content/retrieve`: %w", err)
	}

	conn, err := rclient.NewConnection(nodeID, &url, c.retrievalOpts...)
	if err != nil {
		return nil, fmt.Errorf("creating connection: %w", err)
	}

	xres, hres, err := rclient.Execute(ctx, inv, conn)
	if err != nil {
		return nil, fmt.Errorf("executing `space/content/retrieve` invocation: %w", err)
	}

	rcptLink, ok := xres.Get(inv.Link())
	if !ok {
		return nil, fmt.Errorf("execution response did not contain receipt for invocation")
	}

	bs, err := blockstore.NewBlockReader(blockstore.WithBlocksIterator(xres.Blocks()))
	if err != nil {
		return nil, fmt.Errorf("adding blocks to reader: %w", err)
	}

	anyRcpt, err := receipt.NewAnyReceipt(rcptLink, bs)
	if err != nil {
		return nil, fmt.Errorf("creating receipt: %w", err)
	}

	rcpt, err := receipt.Rebind[contentcap.RetrieveOk, failure.FailureModel](anyRcpt, contentcap.RetrieveOkType(), failure.FailureType(), captypes.Converters...)
	if err != nil {
		return nil, fmt.Errorf("binding receipt to types: %w", err)
	}

	_, err = result.Unwrap(result.MapError(rcpt.Out(), failure.FromFailureModel))
	if err != nil {
		return nil, fmt.Errorf("execution failure: %w", err)
	}

	return hres.Body(), nil
}
