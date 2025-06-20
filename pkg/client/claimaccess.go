package client

import (
	"context"
	"fmt"

	"github.com/storacha/go-libstoracha/capabilities/access"
	captypes "github.com/storacha/go-libstoracha/capabilities/types"
	uclient "github.com/storacha/go-ucanto/client"
	udelegation "github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/receipt"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/core/result/failure"
	"github.com/storacha/go-ucanto/core/result/failure/datamodel"
	serverdatamodel "github.com/storacha/go-ucanto/server/datamodel"
	"github.com/storacha/guppy/pkg/client/nodevalue"
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

	inv, err := access.Claim.Invoke(c.Issuer(), c.Connection().ID(), c.Issuer().DID().String(), caveats)
	if err != nil {
		return nil, fmt.Errorf("generating invocation: %w", err)
	}

	resp, err := uclient.Execute(ctx, []invocation.Invocation{inv}, c.Connection())
	if err != nil {
		return nil, fmt.Errorf("sending invocation: %w", err)
	}

	rcptlnk, ok := resp.Get(inv.Link())
	if !ok {
		return nil, fmt.Errorf("receipt not found: %s", inv.Link())
	}

	reader, err := receipt.NewReceiptReaderFromTypes[access.ClaimOk, serverdatamodel.HandlerExecutionErrorModel](access.ClaimOkType(), serverdatamodel.HandlerExecutionErrorType(), captypes.Converters...)
	if err != nil {
		return nil, fmt.Errorf("generating receipt reader: %w", err)
	}

	rcpt, err := reader.Read(rcptlnk, resp.Blocks())
	if err != nil {
		anyRcpt, err := receipt.NewAnyReceiptReader().Read(rcptlnk, resp.Blocks())
		if err != nil {
			return nil, fmt.Errorf("reading receipt as any: %w", err)
		}
		okNode, errorNode := result.Unwrap(anyRcpt.Out())

		if okNode != nil {
			okValue, err := nodevalue.NodeValue(okNode)
			if err != nil {
				return nil, fmt.Errorf("reading `access/claim` ok output: %w", err)
			}
			return nil, fmt.Errorf("`access/claim` succeeded with unexpected output: %#v", okValue)
		}

		errorValue, err := nodevalue.NodeValue(errorNode)
		if err != nil {
			return nil, fmt.Errorf("reading `access/claim` error output: %w", err)
		}
		return nil, fmt.Errorf("`access/claim` failed with unexpected error: %#v", errorValue)
	}

	claimOk, failErr := result.Unwrap(
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
