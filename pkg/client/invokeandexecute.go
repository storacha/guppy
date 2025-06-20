package client

import (
	"context"
	"fmt"

	"github.com/ipld/go-ipld-prime/schema"
	captypes "github.com/storacha/go-libstoracha/capabilities/types"
	uclient "github.com/storacha/go-ucanto/client"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/receipt"
	"github.com/storacha/go-ucanto/core/receipt/fx"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/core/result/failure"
	"github.com/storacha/go-ucanto/core/result/failure/datamodel"
	serverdatamodel "github.com/storacha/go-ucanto/server/datamodel"
	"github.com/storacha/go-ucanto/validator"
	"github.com/storacha/guppy/pkg/client/nodevalue"
)

func invokeAndExecute[Caveats, Out any](
	ctx context.Context,
	c *Client,
	capParser validator.CapabilityParser[Caveats],
	caveats Caveats,
	successType schema.Type,
	options ...delegation.Option,
) (result.Result[Out, failure.IPLDBuilderFailure], fx.Effects, error) {
	inv, err := capParser.Invoke(c.Issuer(), c.Connection().ID(), c.Issuer().DID().String(), caveats)
	if err != nil {
		return nil, nil, fmt.Errorf("generating invocation: %w", err)
	}

	resp, err := uclient.Execute(ctx, []invocation.Invocation{inv}, c.Connection())
	if err != nil {
		return nil, nil, fmt.Errorf("sending invocation: %w", err)
	}

	rcptlnk, ok := resp.Get(inv.Link())
	if !ok {
		return nil, nil, fmt.Errorf("receipt not found: %s", inv.Link())
	}

	reader, err := receipt.NewReceiptReaderFromTypes[Out, serverdatamodel.HandlerExecutionErrorModel](successType, serverdatamodel.HandlerExecutionErrorType(), captypes.Converters...)
	if err != nil {
		return nil, nil, fmt.Errorf("generating receipt reader: %w", err)
	}

	rcpt, err := reader.Read(rcptlnk, resp.Blocks())
	if err != nil {
		anyRcpt, err := receipt.NewAnyReceiptReader().Read(rcptlnk, resp.Blocks())
		if err != nil {
			return nil, nil, fmt.Errorf("reading receipt as any: %w", err)
		}
		okNode, errorNode := result.Unwrap(anyRcpt.Out())

		if okNode != nil {
			okValue, err := nodevalue.NodeValue(okNode)
			if err != nil {
				return nil, nil, fmt.Errorf("reading `%s` ok output: %w", capParser.Can(), err)
			}
			return nil, nil, fmt.Errorf("`%s` succeeded with unexpected output: %#v", capParser.Can(), okValue)
		}

		errorValue, err := nodevalue.NodeValue(errorNode)
		if err != nil {
			return nil, nil, fmt.Errorf("reading `%s` error output: %w", capParser.Can(), err)
		}
		return nil, nil, fmt.Errorf("`%s` failed with unexpected error: %#v", capParser.Can(), errorValue)
	}

	return result.MapError(
		result.MapError(
			rcpt.Out(),
			func(errorModel serverdatamodel.HandlerExecutionErrorModel) datamodel.FailureModel {
				return datamodel.FailureModel(errorModel.Cause)
			},
		),
		failure.FromFailureModel,
	), rcpt.Fx(), nil
}
