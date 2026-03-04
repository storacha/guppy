package client

import (
	"context"
	"fmt"

	shardcap "github.com/storacha/go-libstoracha/capabilities/upload/shard"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/did"
)

// UploadShardList returns a paginated list of shards for an upload.
//
// Required delegated capability proofs: `upload/shard/list`
//
// The `space` is the resource the invocation applies to. It is typically the
// DID of a space.
//
// The `params` are caveats required to perform an `upload/shard/list` invocation.
//
// The `proofs` are delegation proofs to use in addition to those in the client.
// They won't be saved in the client, only used for this invocation.
func (c *Client) UploadShardList(ctx context.Context, space did.DID, params shardcap.ListCaveats) (shardcap.ListOk, error) {
	res, _, err := invokeAndExecute[shardcap.ListCaveats, shardcap.ListOk](
		ctx,
		c,
		shardcap.List,
		space.String(),
		params,
		shardcap.ListOkType(),
	)

	if err != nil {
		return shardcap.ListOk{}, fmt.Errorf("invoking and executing %q: %w", shardcap.ListAbility, err)
	}

	listOk, failErr := result.Unwrap(res)
	if failErr != nil {
		return shardcap.ListOk{}, fmt.Errorf("%q failed: %w", shardcap.ListAbility, failErr)
	}

	return listOk, nil
}
