package client

import (
	"context"
	"fmt"

	spacecap "github.com/storacha/go-libstoracha/capabilities/space"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/did"
)

// SpaceInfo invokes the space/info capability to get information about a space,
// including which providers are associated with it.
func (c *Client) SpaceInfo(ctx context.Context, space did.DID) (spacecap.InfoOk, error) {
	res, _, err := invokeAndExecute[spacecap.InfoCaveats, spacecap.InfoOk](
		ctx,
		c,
		spacecap.Info,
		space.String(),
		spacecap.InfoCaveats{},
		spacecap.InfoOkType(),
	)
	if err != nil {
		return spacecap.InfoOk{}, fmt.Errorf("invoking and executing `space/info`: %w", err)
	}

	infoOk, failErr := result.Unwrap(res)
	if failErr != nil {
		return spacecap.InfoOk{}, fmt.Errorf("`space/info` failed: %w", failErr)
	}

	return infoOk, nil
}
