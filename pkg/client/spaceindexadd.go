package client

import (
	"context"
	"fmt"

	"github.com/ipfs/go-cid"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	contentcap "github.com/storacha/go-libstoracha/capabilities/space/content"
	spaceindexcap "github.com/storacha/go-libstoracha/capabilities/space/index"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/ipld"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/ucan"
)

func (c *Client) SpaceIndexAdd(ctx context.Context, indexCID cid.Cid, indexSize uint64, rootCID cid.Cid, space did.DID) error {
	indexLink := cidlink.Link{Cid: indexCID}
	rootLink := cidlink.Link{Cid: rootCID}
	proofs, err := c.Proofs()
	if err != nil {
		return err
	}
	pfs := make([]delegation.Proof, 0, len(proofs))
	for _, del := range proofs {
		pfs = append(pfs, delegation.FromDelegation(del))
	}
	retrievalAuth, err := contentcap.Retrieve.Delegate(
		c.Issuer(),
		c.Connection().ID(),
		space.DID().String(),
		contentcap.RetrieveCaveats{
			Blob:  contentcap.BlobDigest{Digest: indexCID.Hash()},
			Range: contentcap.Range{Start: 0, End: indexSize - 1},
		},
		delegation.WithProof(pfs...),
	)

	if err != nil {
		return fmt.Errorf("creating retrieval auth delegation: %w", err)
	}

	inv, err := invoke[spaceindexcap.AddCaveats, spaceindexcap.AddOk](
		c,
		spaceindexcap.Add,
		space.String(),
		spaceindexcap.AddCaveats{
			Index:   indexLink,
			Content: rootLink,
		},
		delegation.WithFacts([]ucan.FactBuilder{authFact{auth: retrievalAuth.Link()}}),
	)
	if err != nil {
		return fmt.Errorf("invoking `space/index/add`: %w", err)
	}

	for b, err := range retrievalAuth.Blocks() {
		if err != nil {
			return fmt.Errorf("getting block from retrieval auth: %w", err)
		}
		inv.Attach(b)
	}

	res, _, err := execute[spaceindexcap.AddCaveats, spaceindexcap.AddOk](
		ctx,
		c,
		spaceindexcap.Add,
		inv,
		spaceindexcap.AddOkType(),
	)
	if err != nil {
		return fmt.Errorf("executing `space/index/add`: %w", err)
	}

	_, failErr := result.Unwrap(res)
	if failErr != nil {
		return fmt.Errorf("`space/index/add` failed: %w\n\n%#v", failErr, failErr)
	}

	return nil
}

type authFact struct {
	auth ipld.Link
}

func (a authFact) ToIPLD() (map[string]ipld.Node, error) {
	np := basicnode.Prototype.Link
	nb := np.NewBuilder()
	if err := nb.AssignLink(a.auth); err != nil {
		return nil, err
	}
	return map[string]ipld.Node{"retrievalAuth": nb.Build()}, nil
}
