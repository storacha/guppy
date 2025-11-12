package client

import (
	"context"
	"fmt"

	filecoincap "github.com/storacha/go-libstoracha/capabilities/filecoin"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/ipld"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/did"
)

type FilecoinOfferOptions struct {
	pdpAcceptInvocation invocation.Invocation
}

func (foo *FilecoinOfferOptions) PDPAcceptInvocation() invocation.Invocation {
	return foo.pdpAcceptInvocation
}

type FilecoinOfferOption func(opts *FilecoinOfferOptions)

func WithPDPAcceptInvocation(inv invocation.Invocation) FilecoinOfferOption {
	return func(opts *FilecoinOfferOptions) {
		opts.pdpAcceptInvocation = inv
	}
}

func NewFilecoinOfferOptions(opts []FilecoinOfferOption) *FilecoinOfferOptions {
	config := &FilecoinOfferOptions{}
	for _, o := range opts {
		o(config)
	}
	return config
}

func (c *Client) FilecoinOffer(ctx context.Context, space did.DID, content ipld.Link, piece ipld.Link, opts ...FilecoinOfferOption) (filecoincap.OfferOk, error) {
	config := NewFilecoinOfferOptions(opts)

	caveats := filecoincap.OfferCaveats{
		Content: content,
		Piece:   piece,
	}

	if config.pdpAcceptInvocation != nil {
		pdpAcceptLink := config.pdpAcceptInvocation.Link()
		caveats.PDP = &pdpAcceptLink
	}
	inv, err := invoke[filecoincap.OfferCaveats, filecoincap.OfferOk](
		c,
		filecoincap.Offer,
		space.String(),
		caveats,
	)
	if err != nil {
		return filecoincap.OfferOk{}, fmt.Errorf("invoking `filecoin/offer`: %w", err)
	}

	if config.pdpAcceptInvocation != nil {
		for b, err := range config.pdpAcceptInvocation.Export() {
			if err != nil {
				return filecoincap.OfferOk{}, fmt.Errorf("getting block from pdp offer invocation: %w", err)
			}
			err = inv.Attach(b)
			if err != nil {
				return filecoincap.OfferOk{}, fmt.Errorf("attaching pdp offer invocation block to filecoin offer invocation: %w", err)
			}
		}
	}

	res, _, err := execute[filecoincap.OfferCaveats, filecoincap.OfferOk](
		ctx,
		c,
		filecoincap.Offer,
		inv,
		filecoincap.OfferOkType(),
	)
	if err != nil {
		return filecoincap.OfferOk{}, fmt.Errorf("executing `filecoin/offer`: %w", err)
	}

	offerOk, failErr := result.Unwrap(res)
	if failErr != nil {
		return filecoincap.OfferOk{}, fmt.Errorf("`filecoin/offer` failed: %w", failErr)
	}

	return offerOk, nil
}
