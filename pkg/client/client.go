package client

import (
	"context"
	"fmt"
	"net/url"

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
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal"
	ed25519 "github.com/storacha/go-ucanto/principal/ed25519/signer"
	serverdatamodel "github.com/storacha/go-ucanto/server/datamodel"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/storacha/go-ucanto/validator"
	"github.com/storacha/guppy/pkg/agentdata"
	"github.com/storacha/guppy/pkg/client/nodevalue"
	receiptclient "github.com/storacha/guppy/pkg/receipt"
)

type Client struct {
	connection     uclient.Connection
	receiptsURL    *url.URL
	receiptsClient *receiptclient.Client
	data           agentdata.AgentData
	saveFn         func(agentdata.AgentData) error
}

// NewClient creates a new client.
func NewClient(options ...Option) (*Client, error) {
	c := Client{
		connection:     DefaultConnection,
		receiptsClient: DefaultReceiptsClient,
	}

	for _, opt := range options {
		if err := opt(&c); err != nil {
			return nil, err
		}
	}

	if c.data.Principal == nil {
		newPrincipal, err := ed25519.Generate()
		if err != nil {
			return nil, err
		}
		c.data.Principal = newPrincipal
	}

	err := c.save()
	if err != nil {
		return nil, err
	}

	return &c, nil
}

// DID returns the DID of the client.
func (c *Client) DID() did.DID {
	if c.data.Principal == nil {
		return did.DID{}
	}
	return c.data.Principal.DID()
}

// Connection returns the connection used by the client.
func (c *Client) Connection() uclient.Connection {
	return c.connection
}

// Issuer returns the issuing signer of the client.
func (c *Client) Issuer() principal.Signer {
	return c.data.Principal
}

// Proofs returns the delegations of the client.
func (c *Client) Proofs() []delegation.Delegation {
	return c.data.Delegations
}

// AddProofs adds the given delegations to the client's data and saves it.
func (c *Client) AddProofs(delegations ...delegation.Delegation) error {
	c.data.Delegations = append(c.data.Delegations, delegations...)
	return c.save()
}

func (c *Client) save() error {
	if c.saveFn == nil {
		return nil
	}

	err := c.saveFn(c.data)
	if err != nil {
		return fmt.Errorf("saving client data: %w", err)
	}
	return nil
}

func (c *Client) Reset() error {
	c.data = agentdata.AgentData{
		Principal: c.Issuer(),
	}
	return c.save()
}

func invokeAndExecute[Caveats, Out any](
	ctx context.Context,
	c *Client,
	capParser validator.CapabilityParser[Caveats],
	with ucan.Resource,
	caveats Caveats,
	successType schema.Type,
	options ...delegation.Option,
) (result.Result[Out, failure.IPLDBuilderFailure], fx.Effects, error) {
	pfs := make([]delegation.Proof, 0, len(c.Proofs()))
	for _, del := range c.Proofs() {
		pfs = append(pfs, delegation.FromDelegation(del))
	}

	inv, err := capParser.Invoke(c.Issuer(), c.Connection().ID(), with, caveats, append(options, delegation.WithProof(pfs...))...)
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

	// Note that this currently only treats handler execution errors nicely
	// (errors returned from the invocation handler itself). Other standard errors
	// (like authorization errors) go through the fallback error reporting below
	// along with anything we can't forsee.
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
