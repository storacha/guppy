package client

import (
	"context"
	"fmt"

	spaceblobcap "github.com/storacha/go-libstoracha/capabilities/space/blob"
	"github.com/storacha/go-libstoracha/capabilities/types"
	"github.com/storacha/go-ucanto/core/ipld"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/did"
)

func (c *Client) SpaceBlobReplicate(ctx context.Context, space did.DID, blob types.Blob, replicaCount uint, site ipld.Link) (spaceblobcap.ReplicateOk, error) {
	caveats := spaceblobcap.ReplicateCaveats{
		Blob:     blob,
		Replicas: replicaCount,
		Site:     site,
	}

	res, _, err := invokeAndExecute[spaceblobcap.ReplicateCaveats, spaceblobcap.ReplicateOk](
		ctx,
		c,
		spaceblobcap.Replicate,
		c.Issuer().DID().String(),
		caveats,
		spaceblobcap.ReplicateOkType(),
	)
	if err != nil {
		return spaceblobcap.ReplicateOk{}, fmt.Errorf("invoking and executing `space/blob/replicate`: %w", err)
	}

	replicateOk, failErr := result.Unwrap(res)
	if failErr != nil {
		return spaceblobcap.ReplicateOk{}, fmt.Errorf("`space/blob/replicate` failed: %w", failErr)
	}

	return replicateOk, nil
}
