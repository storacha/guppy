package client

import (
	"context"
	"fmt"

	spaceblobcap "github.com/storacha/go-libstoracha/capabilities/space/blob"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/did"
)

// BlobList returns a paginated list of blobs stored in a space.
//
// Required delegated capability proofs: `space/blob/list`
//
// The `space` is the resource the invocation applies to. It is typically the
// DID of a space.
//
// The `params` are caveats required to perform an `space/blob/list` invocation.
//
// The `proofs` are delegation proofs to use in addition to those in the client.
// They won't be saved in the client, only used for this invocation.
func (c *Client) SpaceBlobList(ctx context.Context, space did.DID, params spaceblobcap.ListCaveats) (spaceblobcap.ListOk, error) {
	res, _, err := invokeAndExecute[spaceblobcap.ListCaveats, spaceblobcap.ListOk](
		ctx,
		c,
		spaceblobcap.List,
		space.String(),
		params,
		spaceblobcap.ListOkType(),
	)

	if err != nil {
		return spaceblobcap.ListOk{}, fmt.Errorf("invoking and executing `space/blob/list`: %w", err)
	}

	listOk, failErr := result.Unwrap(res)
	if failErr != nil {
		return spaceblobcap.ListOk{}, fmt.Errorf("`space/blob/list` failed: %w", failErr)
	}

	return listOk, nil
}
