package client

import (
	"context"
	"fmt"
	"strings"

	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime/schema"
	captypes "github.com/storacha/go-libstoracha/capabilities/types"
	ucancap "github.com/storacha/go-libstoracha/capabilities/ucan"
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
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

var (
	log    = logging.Logger("github.com/storacha/guppy/pkg/client")
	tracer = otel.Tracer("github.com/storacha/guppy/pkg/client")
)

type Client struct {
	connection       uclient.Connection
	receiptsClient   *receiptclient.Client
	data             agentdata.AgentData
	saveFn           func(agentdata.AgentData) error
	additionalProofs []delegation.Delegation
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

// CapabilityQuery represents a query to filter proofs by capability.
type CapabilityQuery struct {
	// Can is the ability to match (e.g., "store/add"). Use "*" to match all abilities.
	Can ucan.Ability
	// With is the resource to match. Use "ucan:*" to match all resources.
	With ucan.Resource
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
//
// Returns both stored delegations (from c.data.Delegations) and additional proofs
// (from c.additionalProofs).
func (c *Client) Proofs(queries ...CapabilityQuery) []delegation.Delegation {
	now := ucan.Now()

	// Map to track which delegations to include (by CID string)
	authorizations := make(map[string]delegation.Delegation)

	// Combine stored delegations and additional proofs
	allDelegations := make([]delegation.Delegation, 0, len(c.data.Delegations)+len(c.additionalProofs))
	allDelegations = append(allDelegations, c.data.Delegations...)
	allDelegations = append(allDelegations, c.additionalProofs...)

	// First pass: collect matching authorizations (non-session-proof delegations)
	for _, del := range allDelegations {
		// Filter out expired delegations
		if exp := del.Expiration(); exp != nil && *exp < now {
			continue
		}

		// Filter out delegations that are not yet valid
		if del.NotBefore() > now {
			continue
		}

		// Skip session proofs in the first pass
		if isSessionProof(del) {
			continue
		}

		// If no queries, include all non-expired delegations
		if len(queries) == 0 {
			authorizations[del.Link().String()] = del
			continue
		}

		// Check if delegation matches any query
		if matchesAnyQuery(del, queries) {
			authorizations[del.Link().String()] = del
		}
	}

	// Second pass: collect session proofs that attest to the authorizations
	sessionProofs := getSessionProofs(allDelegations, now)

	for authCID := range authorizations {
		if proofsForAuth, exists := sessionProofs[authCID]; exists {
			// Add all session proofs for this authorization
			for _, sessionProof := range proofsForAuth {
				authorizations[sessionProof.Link().String()] = sessionProof
			}
		}
	}

	// Convert map to slice
	result := make([]delegation.Delegation, 0, len(authorizations))
	for _, del := range authorizations {
		result = append(result, del)
	}

	return result
}

// matchesAnyQuery checks if a delegation's capabilities match any of the provided queries.
func matchesAnyQuery(del delegation.Delegation, queries []CapabilityQuery) bool {
	caps := del.Capabilities()

	for _, query := range queries {
		for _, cap := range caps {
			if matchesCapability(cap, query) {
				return true
			}
		}
	}
	return false
}

// matchesCapability checks if a capability matches a query using the resolution logic
// from go-ucanto's validator package.
func matchesCapability(cap ucan.Capability[any], query CapabilityQuery) bool {
	// Match ability
	if !matchesAbility(cap.Can(), query.Can) {
		return false
	}

	// Match resource
	if !matchesResource(cap.With(), query.With) {
		return false
	}

	return true
}

// matchesAbility checks if a delegation's ability can authorize the query ability.
// A delegation matches if it has the same ability or a broader wildcard.
// For example, searching for "upload/add" matches delegations with:
//   - "upload/add" (exact match)
//   - "upload/*" (namespace wildcard)
//   - "*" (global wildcard)
func matchesAbility(capAbility, queryAbility ucan.Ability) bool {
	// Exact match
	if capAbility == queryAbility {
		return true
	}

	// Global wildcard in capability
	if capAbility == "*" {
		return true
	}

	// Namespace wildcard in capability (e.g., capability has "upload/*", query is "upload/add")
	if len(capAbility) > 2 && capAbility[len(capAbility)-2:] == "/*" {
		prefix := capAbility[:len(capAbility)-1] // "upload/"
		return len(queryAbility) >= len(prefix) && queryAbility[:len(prefix)] == prefix
	}

	return false
}

// matchesResource checks if a capability's resource matches the query resource.
// A capability matches if:
//   - The resources match exactly
//   - The capability has "ucan:*" (matches any query resource)
func matchesResource(capResource, queryResource ucan.Resource) bool {
	return capResource == "ucan:*" || queryResource == capResource
}

// isSessionProof checks if a delegation is a session proof (ucan/attest capability).
func isSessionProof(del delegation.Delegation) bool {
	caps := del.Capabilities()
	if len(caps) == 0 {
		return false
	}
	// A session proof has a ucan/attest capability
	return caps[0].Can() == ucancap.AttestAbility
}

// getSessionProofs organizes session proofs by the CID of the authorization they attest to.
// Returns a map from authorization CID string to list of session proof delegations.
func getSessionProofs(delegations []delegation.Delegation, now ucan.UTCUnixTimestamp) map[string][]delegation.Delegation {
	proofs := make(map[string][]delegation.Delegation)

	for _, del := range delegations {
		if !isSessionProof(del) {
			continue
		}

		// Filter out expired session proofs
		if exp := del.Expiration(); exp != nil && *exp < now {
			continue
		}

		// Filter out session proofs that are not yet valid
		if del.NotBefore() > now {
			continue
		}

		caps := del.Capabilities()
		if len(caps) == 0 {
			continue
		}

		// Parse the capability using the ucan/attest parser to get typed access
		source := validator.NewSource(caps[0], del)
		match, err := ucancap.Attest.Match(source)
		if err != nil {
			// If we can't parse it, skip it
			continue
		}

		// Get the proof link from the typed capability
		attestCap := match.Value()
		proofCID := attestCap.Nb().Proof.String()
		proofs[proofCID] = append(proofs[proofCID], del)
	}

	return proofs
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
	inv, err := invoke[Caveats, Out](ctx, c, capParser, with, caveats, options...)
	if err != nil {
		return nil, nil, fmt.Errorf("invoking `%s`: %w", capParser.Can(), err)
	}
	return execute[Caveats, Out](ctx, c, capParser, inv, successType)
}

func invoke[Caveats, Out any](
	ctx context.Context,
	c *Client,
	capParser validator.CapabilityParser[Caveats],
	with ucan.Resource,
	caveats Caveats,
	options ...delegation.Option,
) (invocation.IssuedInvocation, error) {

	var err error
	pfs := make([]delegation.Proof, 0, len(c.Proofs(CapabilityQuery{
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
