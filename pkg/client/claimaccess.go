package client

import (
	"context"
	"fmt"

	"github.com/storacha/go-libstoracha/capabilities/access"
	udelegation "github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/guppy/pkg/delegation"
)

// ClaimAccess fetches any stored delegations from the service. This is the
// second step of the Agent authorization process, from the Agent's point of
// view. After the Agent has [RequestAccess]ed, the service will instruct the
// user to confirm the access request out of band, e.g. via email. Once
// confirmed, a delegation will be available on the service for the Agent to
// claim.
func (c *Client) ClaimAccess(ctx context.Context) ([]udelegation.Delegation, error) {
	caveats := access.ClaimCaveats{}

	res, _, err := invokeAndExecute[access.ClaimCaveats, access.ClaimOk](
		ctx,
		c,
		access.Claim,
		caveats,
		access.ClaimOkType(),
	)
	if err != nil {
		return nil, fmt.Errorf("invoking and executing `access/claim`: %w", err)
	}

	claimOk, failErr := result.Unwrap(res)
	if failErr != nil {
		return nil, fmt.Errorf("`access/claim` failed: %w", failErr)
	}

	dels := make([]udelegation.Delegation, 0, len(claimOk.Delegations.Values))
	for _, delBytes := range claimOk.Delegations.Values {
		del, err := delegation.ExtractProof(delBytes)
		if err != nil {
			return nil, fmt.Errorf("extracting delegation: %w", err)
		}
		dels = append(dels, del)
	}

	return dels, nil
}
