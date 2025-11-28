package client

import (
	"context"
	"fmt"

	"github.com/ipld/go-ipld-prime"
	uploadcap "github.com/storacha/go-libstoracha/capabilities/upload"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/did"
)

// UploadRemove removes an upload from the service's listing for a space.
//
// Required delegated capability proofs: `upload/remove` (depends on service)
//
// The `space` is the resource the invocation applies to. It is typically the
// DID of a space.
func (c *Client) UploadRemove(ctx context.Context, space did.DID, root ipld.Link) (uploadcap.RemoveOk, error) {
	res, _, err := invokeAndExecute[uploadcap.RemoveCaveats, uploadcap.RemoveOk](
		ctx,
		c,
		uploadcap.Remove,
		space.String(),
		uploadcap.RemoveCaveats{Root: root},
		uploadcap.RemoveOkType(),
	)

	if err != nil {
		return uploadcap.RemoveOk{}, fmt.Errorf("invoking and executing `upload/remove`: %w", err)
	}

	remOk, failErr := result.Unwrap(res)
	if failErr != nil {
		return uploadcap.RemoveOk{}, fmt.Errorf("`upload/remove` failed: %w", failErr)
	}

	return remOk, nil
}
