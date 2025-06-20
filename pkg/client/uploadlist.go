package client

import (
	"context"
	"fmt"

	uclient "github.com/storacha/go-ucanto/client"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/receipt"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/guppy/pkg/capability/uploadlist"
)

// UploadList returns a paginated list of uploads in a space.
//
// Required delegated capability proofs: `upload/list`
//
// The `space` is the resource the invocation applies to. It is typically the
// DID of a space.
//
// The `params` are caveats required to perform an `upload/list` invocation.
//
// The `proofs` are delegation proofs to use in addition to those in the client.
// They won't be saved in the client, only used for this invocation.
func (c *Client) UploadList(ctx context.Context, space did.DID, params uploadlist.Caveat) (receipt.Receipt[*uploadlist.Success, *uploadlist.Failure], error) {
	inv, err := invocation.Invoke(
		c.Issuer(),
		c.Connection().ID(),
		uploadlist.NewCapability(space, params),
	)
	if err != nil {
		return nil, fmt.Errorf("generating invocation: %w", err)
	}

	resp, err := uclient.Execute(ctx, []invocation.Invocation{inv}, c.Connection())
	if err != nil {
		return nil, fmt.Errorf("executing invocation: %w", err)
	}

	rcptlnk, ok := resp.Get(inv.Link())
	if !ok {
		return nil, fmt.Errorf("receipt not found: %s", inv.Link())
	}

	reader, err := uploadlist.NewReceiptReader()
	if err != nil {
		return nil, fmt.Errorf("generating receipt reader: %w", err)
	}

	return reader.Read(rcptlnk, resp.Blocks())
}
