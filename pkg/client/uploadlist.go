package client

import (
	"context"
	"fmt"

	uploadcap "github.com/storacha/go-libstoracha/capabilities/upload"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/did"
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
func (c *Client) UploadList(ctx context.Context, space did.DID, params uploadcap.ListCaveats) (uploadcap.ListOk, error) {
	res, _, err := invokeAndExecute[uploadcap.ListCaveats, uploadcap.ListOk](
		ctx,
		c,
		uploadcap.List,
		space.String(),
		params,
		uploadcap.ListOkType(),
	)

	if err != nil {
		return uploadcap.ListOk{}, fmt.Errorf("invoking and executing %q: %w", uploadcap.ListAbility, err)
	}

	listOk, failErr := result.Unwrap(res)
	if failErr != nil {
		return uploadcap.ListOk{}, fmt.Errorf("%q failed: %w", uploadcap.ListAbility, failErr)
	}

	return listOk, nil
}
