package client

import (
	"context"
	"fmt"
	"io"

	contentcap "github.com/storacha/go-libstoracha/capabilities/space/content"
	captypes "github.com/storacha/go-libstoracha/capabilities/types"
	"github.com/storacha/go-libstoracha/failure"
	rclient "github.com/storacha/go-ucanto/client/retrieval"
	"github.com/storacha/go-ucanto/core/dag/blockstore"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/receipt"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/did"

	"github.com/storacha/guppy/pkg/agentstore"
	"github.com/storacha/guppy/pkg/client/locator"
)

func (c *Client) Retrieve(ctx context.Context, location locator.Location) (io.ReadCloser, error) {
	locationCommitment := location.Commitment

	space := locationCommitment.Nb().Space

	nodeID, err := did.Parse(locationCommitment.With())
	if err != nil {
		return nil, fmt.Errorf("parsing DID of storage provider node `%s`: %w", locationCommitment.With(), err)
	}

	urls := locationCommitment.Nb().Location

	var reqErr error
	for _, url := range urls {
		storageProvider, err := did.Parse(locationCommitment.With())
		if err != nil {
			reqErr = fmt.Errorf("parsing DID of storage provider `%s`: %w", locationCommitment.With(), err)
			continue
		}

		delegations, err := c.Proofs(agentstore.CapabilityQuery{
			Can:  contentcap.Retrieve.Can(),
			With: space.String(),
		})
		if err != nil {
			reqErr = err
			continue
		}
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
				Blob: contentcap.BlobDigest{Digest: locationCommitment.Nb().Content.Hash()},
				Range: contentcap.Range{
					Start: start,
					End:   end,
				},
			},
			delegation.WithProof(prfs...),
		)
		if err != nil {
			reqErr = fmt.Errorf("invoking `space/content/retrieve`: %w", err)
			continue
		}

		conn, err := rclient.NewConnection(nodeID, &url, c.retrievalOpts...)
		if err != nil {
			reqErr = fmt.Errorf("creating connection: %w", err)
			continue
		}

		xres, hres, err := rclient.Execute(ctx, inv, conn)
		if err != nil {
			reqErr = fmt.Errorf("executing `space/content/retrieve` invocation: %w", err)
			continue
		}

		rcptLink, ok := xres.Get(inv.Link())
		if !ok {
			reqErr = fmt.Errorf("execution response did not contain receipt for invocation")
			continue
		}

		bs, err := blockstore.NewBlockReader(blockstore.WithBlocksIterator(xres.Blocks()))
		if err != nil {
			reqErr = fmt.Errorf("adding blocks to reader: %w", err)
			continue
		}

		anyRcpt, err := receipt.NewAnyReceipt(rcptLink, bs)
		if err != nil {
			reqErr = fmt.Errorf("creating receipt: %w", err)
			continue
		}

		rcpt, err := receipt.Rebind[contentcap.RetrieveOk, failure.FailureModel](anyRcpt, contentcap.RetrieveOkType(), failure.FailureType(), captypes.Converters...)
		if err != nil {
			reqErr = fmt.Errorf("binding receipt to types: %w", err)
			continue
		}

		_, err = result.Unwrap(result.MapError(rcpt.Out(), failure.FromFailureModel))
		if err != nil {
			reqErr = fmt.Errorf("execution failure: %w", err)
			continue
		}
		return hres.Body(), nil
	}

	return nil, reqErr
}
