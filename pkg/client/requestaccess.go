package client

import (
	"context"
	"fmt"

	"github.com/storacha/go-libstoracha/capabilities/access"
	"github.com/storacha/go-ucanto/core/result"
)

// RequestAccess requests access to the service as an Account. This is the first
// step of the Agent authorization process.
//
// The [issuer] is the Agent which would like to act as the Account.
//
// The [account] is the Account the Agent would like to act as.
func (c *Client) RequestAccess(ctx context.Context, account string) (access.AuthorizeOk, error) {
	caveats := access.AuthorizeCaveats{
		Iss: &account,
		Att: []access.CapabilityRequest{
			// Request capability to act as the account fully
			{Can: "*"},
		},
	}

	res, _, err := invokeAndExecute[access.AuthorizeCaveats, access.AuthorizeOk](
		ctx,
		c,
		access.Authorize,
		c.Issuer().DID().String(),
		caveats,
		access.AuthorizeOkType(),
	)
	if err != nil {
		return access.AuthorizeOk{}, fmt.Errorf("invoking and executing `access/authorize`: %w", err)
	}

	authorizeOk, failErr := result.Unwrap(res)
	if failErr != nil {
		return access.AuthorizeOk{}, fmt.Errorf("`access/authorize` failed: %w", failErr)
	}

	return authorizeOk, nil
}
