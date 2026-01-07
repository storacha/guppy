package client

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"

	mh "github.com/multiformats/go-multihash"
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
)

func (c *Client) Retrieve(ctx context.Context, space did.DID, locations []locator.Location, retrievalOpts ...rclient.Option) ([]byte, error) {
	// Randomly pick one of the available locations
	location := locations[rand.Intn(len(locations))]

	locationCommitment := location.Commitment

	nodeID, err := did.Parse(locationCommitment.With())
	if err != nil {
		return nil, fmt.Errorf("parsing DID of storage provider node `%s`: %w", locationCommitment.With(), err)
	}

	urls := locationCommitment.Nb().Location
	url := urls[rand.Intn(len(urls))]

	storageProvider, err := did.Parse(locationCommitment.With())
	if err != nil {
		return nil, fmt.Errorf("parsing DID of storage provider `%s`: %w", locationCommitment.With(), err)
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
			Blob: contentcap.BlobDigest{Digest: locationCommitment.Nb().Content.Hash()},
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

	conn, err := rclient.NewConnection(nodeID, &url, retrievalOpts...)
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

	body := hres.Body()
	defer body.Close()

	expectedHash := location.Digest

	decHash, err := mh.Decode(expectedHash)
	if err != nil {
		return nil, fmt.Errorf("decoding content multihash %s: %w", expectedHash, err)
	}

	data, err := io.ReadAll(body)
	if err != nil {
		return nil, fmt.Errorf("reading content %s: %w", expectedHash, err)
	}

	dataDigest, err := mh.Sum(data, decHash.Code, -1)
	if err != nil {
		return nil, fmt.Errorf("hashing content %s: %w", expectedHash, err)
	}

	if !bytes.Equal(expectedHash, dataDigest) {
		return nil, fmt.Errorf("content hash mismatch for content %s; got %s", digestutil.Format(expectedHash), digestutil.Format(dataDigest))
	}

	return data, nil
}
