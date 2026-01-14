package client

import (
	"context"
	"fmt"
	"strings"

	logging "github.com/ipfs/go-log/v2"
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
	serverdatamodel "github.com/storacha/go-ucanto/server/datamodel"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/storacha/go-ucanto/validator"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/storacha/guppy/pkg/agentstore"
	"github.com/storacha/guppy/pkg/client/nodevalue"
	receiptclient "github.com/storacha/guppy/pkg/receipt"
)

var (
	log    = logging.Logger("github.com/storacha/guppy/pkg/client")
	tracer = otel.Tracer("github.com/storacha/guppy/pkg/client")
)

type Client struct {
	connection       uclient.Connection
	receiptsClient   *receiptclient.Client
	store            agentstore.Store
	additionalProofs []delegation.Delegation

	signer principal.Signer
}

// NewClient creates a new client.
func NewClient(options ...Option) (*Client, error) {
	defaultStore, err := agentstore.NewMemory()
	if err != nil {
		return nil, err
	}
	c := Client{
		connection:     DefaultConnection,
		receiptsClient: DefaultReceiptsClient,
		store:          defaultStore,
	}

	for _, opt := range options {
		if err := opt(&c); err != nil {
			return nil, err
		}
	}

	signer, err := c.store.Principal()
	if err != nil {
		return nil, err
	}
	c.signer = signer

	return &c, nil
}

// DID returns the DID of the client.
func (c *Client) DID() did.DID {
	if c.signer == nil {
		return did.DID{}
	}
	return c.signer.DID()
}

// Connection returns the connection used by the client.
func (c *Client) Connection() uclient.Connection {
	return c.connection
}

// Issuer returns the issuing signer of the client.
func (c *Client) Issuer() principal.Signer {
	return c.signer
}

// Proofs returns delegations that match the given capability queries.
// If no queries are provided, returns all non-expired delegations.
// Delegations are filtered by:
//   - Expiration: excludes expired delegations
//   - NotBefore: excludes delegations that are not yet valid
//   - Capability matching: if queries are provided, only returns delegations
//     whose capabilities match at least one of the queries
//
// Additionally, this method includes relevant session proofs (ucan/attest delegations)
// that attest to the returned authorizations.
func (c *Client) Proofs(queries ...agentstore.CapabilityQuery) []delegation.Delegation {
	res, err := c.store.Query(queries...)
	if err != nil {
		// TODO may need to break interface
		panic(err)
	}
	return res
}

// AddProofs adds the given delegations to the client's data and saves it.
func (c *Client) AddProofs(delegations ...delegation.Delegation) error {
	return c.store.AddDelegations(delegations...)
}

func (c *Client) Reset() error {
	return c.store.Reset()
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
	inv, err := invoke[Caveats, Out](c, capParser, with, caveats, options...)
	if err != nil {
		return nil, nil, fmt.Errorf("invoking `%s`: %w", capParser.Can(), err)
	}
	return execute[Caveats, Out](ctx, c, capParser, inv, successType)
}

func invoke[Caveats, Out any](
	c *Client,
	capParser validator.CapabilityParser[Caveats],
	with ucan.Resource,
	caveats Caveats,
	options ...delegation.Option,
) (invocation.IssuedInvocation, error) {
	var err error
	pfs := make([]delegation.Proof, 0, len(c.Proofs(agentstore.CapabilityQuery{
		Can:  capParser.Can(),
		With: with,
	})))

	var inv invocation.IssuedInvocation
	for _, del := range c.Proofs() {
		pfs = append(pfs, delegation.FromDelegation(del))
	}

	inv, err = capParser.Invoke(c.Issuer(), c.Connection().ID(), with, caveats, append(options, delegation.WithProof(pfs...))...)
	if err != nil {
		return nil, err
	}

	return inv, nil
}

func execute[Caveats, Out any](
	ctx context.Context,
	c *Client,
	capParser validator.CapabilityParser[Caveats],
	inv invocation.IssuedInvocation,
	successType schema.Type,
) (result.Result[Out, failure.IPLDBuilderFailure], fx.Effects, error) {
	ctx, span := tracer.Start(ctx, "UCAN "+capParser.Can(), trace.WithAttributes(
		attribute.String("invocation.ability", capParser.Can()),
		attribute.String("invocation.audience", inv.Audience().DID().String()),
	))
	defer span.End()

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

		// Try to extract error message if possible
		if errorMap, ok := errorValue.(map[string]any); ok {
			if msg, exists := errorMap["message"]; exists {
				// Add a newline if the message contains multiple lines, for readability
				if msgStr, ok := msg.(string); ok {
					if strings.Contains(msgStr, "\n") {
						msg = "\n" + msgStr
					}
				}

				name := "unnamed"
				if n, exists := errorMap["name"]; exists {
					if nStr, ok := n.(string); ok {
						name = nStr
					}
				}

				return nil, nil, fmt.Errorf("`%s` failed with %s error: %s", capParser.Can(), name, msg)
			}
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
