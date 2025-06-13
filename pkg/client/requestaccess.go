package client

import (
	"fmt"

	"github.com/storacha/go-libstoracha/capabilities/access"
	uclient "github.com/storacha/go-ucanto/client"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/did"
)

// spaceAccess is the set of capabilities required by the agent to manage a
// space.
var spaceAccess = []access.CapabilityRequest{
	{Can: "space/*"},
	{Can: "blob/*"},
	{Can: "index/*"},
	{Can: "store/*"},
	{Can: "upload/*"},
	{Can: "access/*"},
	{Can: "filecoin/*"},
	{Can: "usage/*"},
}

// RequestAccess requests access to the service as an Account. This is the first
// step of the Agent authorization process.
//
// The [issuer] is the Agent which would like to act as the Account.
//
// The [account] is the Account the Agent would like to act as.
func (c *Client) RequestAccess(account did.DID) error {
	caveats := access.AuthorizeCaveats{
		Iss: &account,
		Att: spaceAccess,
	}

	inv, err := access.Authorize.Invoke(c.issuer, c.connection.ID(), c.issuer.DID().String(), caveats)
	if err != nil {
		return fmt.Errorf("generating invocation: %w", err)
	}

	_, err = uclient.Execute([]invocation.Invocation{inv}, c.connection)
	if err != nil {
		return fmt.Errorf("sending invocation: %w", err)
	}

	return nil
}
