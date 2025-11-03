package client

import (
	"context"
	"fmt"

	filecoincap "github.com/storacha/go-libstoracha/capabilities/filecoin"
	"github.com/storacha/go-ucanto/core/ipld"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/did"
)

func (c *Client) FilecoinOffer(ctx context.Context, space did.DID, content ipld.Link, piece ipld.Link) (filecoincap.OfferOk, error) {
	caveats := filecoincap.OfferCaveats{
		Content: content,
		Piece:   piece,
	}

	res, _, err := invokeAndExecute[filecoincap.OfferCaveats, filecoincap.OfferOk](
		ctx,
		c,
		filecoincap.Offer,
		space.String(),
		caveats,
		filecoincap.OfferOkType(),
	)
	if err != nil {
		return filecoincap.OfferOk{}, fmt.Errorf("invoking and executing `filecoin/offer`: %w", err)
	}

	offerOk, failErr := result.Unwrap(res)
	if failErr != nil {
		return filecoincap.OfferOk{}, fmt.Errorf("`filecoin/offer` failed: %w", failErr)
	}

	return offerOk, nil
}
