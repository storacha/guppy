package client

import (
	"context"
	"fmt"

	spaceindexcap "github.com/storacha/go-libstoracha/capabilities/space/index"
	"github.com/storacha/go-ucanto/core/ipld"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/did"
)

func (c *Client) SpaceIndexAdd(ctx context.Context, indexLink ipld.Link, space did.DID) error {
	res, _, err := invokeAndExecute[spaceindexcap.AddCaveats, spaceindexcap.AddOk](
		ctx,
		c,
		spaceindexcap.Add,
		space.String(),
		spaceindexcap.AddCaveats{
			Index: indexLink,
		},
		spaceindexcap.AddOkType(),
	)
	if err != nil {
		return fmt.Errorf("invoking and executing `space/index/add`: %w", err)
	}

	_, failErr := result.Unwrap(res)
	if failErr != nil {
		return fmt.Errorf("`space/index/add` failed: %w", failErr)
	}

	return nil
}
