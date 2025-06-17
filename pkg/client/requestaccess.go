package client

import (
	_ "embed"
	"fmt"

	"github.com/storacha/guppy/pkg/client/nodevalue"

	"github.com/storacha/go-libstoracha/capabilities/access"
	captypes "github.com/storacha/go-libstoracha/capabilities/types"
	uclient "github.com/storacha/go-ucanto/client"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/receipt"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/core/result/failure"
	"github.com/storacha/go-ucanto/core/result/failure/datamodel"
	serverdatamodel "github.com/storacha/go-ucanto/server/datamodel"
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
func (c *Client) RequestAccess(account string) (access.AuthorizeOk, error) {
	caveats := access.AuthorizeCaveats{
		Iss: &account,
		Att: spaceAccess,
	}

	inv, err := access.Authorize.Invoke(c.issuer, c.connection.ID(), c.issuer.DID().String(), caveats)
	if err != nil {
		return access.AuthorizeOk{}, fmt.Errorf("generating invocation: %w", err)
	}

	resp, err := uclient.Execute([]invocation.Invocation{inv}, c.connection)
	if err != nil {
		return access.AuthorizeOk{}, fmt.Errorf("sending invocation: %w", err)
	}

	rcptlnk, ok := resp.Get(inv.Link())
	if !ok {
		return access.AuthorizeOk{}, fmt.Errorf("receipt not found: %s", inv.Link())
	}

	reader, err := receipt.NewReceiptReaderFromTypes[access.AuthorizeOk, serverdatamodel.HandlerExecutionErrorModel](access.AuthorizeOkType(), serverdatamodel.HandlerExecutionErrorType(), captypes.Converters...)
	if err != nil {
		return access.AuthorizeOk{}, fmt.Errorf("generating receipt reader: %w", err)
	}

	rcpt, err := reader.Read(rcptlnk, resp.Blocks())
	if err != nil {
		anyRcpt, err := receipt.NewAnyReceiptReader().Read(rcptlnk, resp.Blocks())
		if err != nil {
			return access.AuthorizeOk{}, fmt.Errorf("reading receipt as any: %w", err)
		}
		okNode, errorNode := result.Unwrap(anyRcpt.Out())

		if okNode != nil {
			okValue, err := nodevalue.NodeValue(okNode)
			if err != nil {
				return access.AuthorizeOk{}, fmt.Errorf("reading `access/authorize` ok output: %w", err)
			}
			return access.AuthorizeOk{}, fmt.Errorf("`access/authorize` succeeded with unexpected output: %#v", okValue)
		}

		errorValue, err := nodevalue.NodeValue(errorNode)
		if err != nil {
			return access.AuthorizeOk{}, fmt.Errorf("reading `access/authorize` error output: %w", err)
		}
		return access.AuthorizeOk{}, fmt.Errorf("`access/authorize` failed with unexpected error: %#v", errorValue)
	}

	authorizeOk, failErr := result.Unwrap(
		result.MapError(
			result.MapError(
				rcpt.Out(),
				func(errorModel serverdatamodel.HandlerExecutionErrorModel) datamodel.FailureModel {
					return datamodel.FailureModel(errorModel.Cause)
				},
			),
			failure.FromFailureModel,
		),
	)
	if failErr != nil {
		return access.AuthorizeOk{}, fmt.Errorf("`access/authorize` failed: %w", failErr)
	}

	fmt.Printf("Access granted: %+v\n", authorizeOk)

	return authorizeOk, nil
}
