package client

import (
	"context"
	"fmt"
	"sort"

	"github.com/ipld/go-ipld-prime"
	ucancap "github.com/storacha/go-libstoracha/capabilities/ucan"
	uclient "github.com/storacha/go-ucanto/client"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/receipt"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/storacha/go-ucanto/validator"

	"github.com/storacha/guppy/pkg/agentstore"
	"github.com/storacha/guppy/pkg/client/nodevalue"
)

const (
	PlanGetCan = "plan/get"
	PlanSetCan = "plan/set"
)

type PlanResult struct {
	Limit     string `json:"limit"`
	Product   string `json:"product"`
	UpdatedAt string `json:"updatedAt"`
}

type MapCaveat map[string]any

func (m MapCaveat) ToIPLD() (ipld.Node, error) {
	return nodevalue.FromAny(m)
}

func (c *Client) PlanGet(ctx context.Context, account did.DID) (*PlanResult, error) {
	return executePlanRequest(ctx, c, PlanGetCan, account, MapCaveat{})
}

func (c *Client) PlanSet(ctx context.Context, account did.DID, product did.DID) (*PlanResult, error) {
	return executePlanRequest(ctx, c, PlanSetCan, account, MapCaveat{"product": product.String()})
}

func executePlanRequest(
	ctx context.Context,
	c *Client,
	can string,
	account did.DID,
	caveat MapCaveat,
) (*PlanResult, error) {
	cap := ucan.NewCapability(can, account.String(), caveat)

	delegations, err := c.Proofs(
		agentstore.CapabilityQuery{Can: can, With: account.String()},
		agentstore.CapabilityQuery{Can: "*", With: account.String()},
	)
	if err != nil {
		return nil, fmt.Errorf("finding proofs for %s: %w", can, err)
	}

	if len(delegations) == 0 {
		return nil, fmt.Errorf("no authorizations found for account %s", account)
	}

	sort.SliceStable(delegations, func(i, j int) bool {
		return isAttestation(delegations[i]) && !isAttestation(delegations[j])
	})

	var proofs []delegation.Proof
	for _, d := range delegations {
		proofs = append(proofs, delegation.FromDelegation(d))
	}

	inv, err := invocation.Invoke(
		c.Issuer(),
		c.Connection().ID(),
		cap,
		delegation.WithProof(proofs...),
	)
	if err != nil {
		return nil, fmt.Errorf("creating invocation: %w", err)
	}

	resp, err := uclient.Execute(ctx, []invocation.Invocation{inv}, c.Connection())
	if err != nil {
		return nil, fmt.Errorf("executing invocation: %w", err)
	}

	rcptLink, ok := resp.Get(inv.Link())
	if !ok {
		return nil, fmt.Errorf("receipt not found")
	}

	anyRcpt, err := receipt.NewAnyReceiptReader().Read(rcptLink, resp.Blocks())
	if err != nil {
		return nil, fmt.Errorf("reading receipt: %w", err)
	}

	okNode, errNode := result.Unwrap(anyRcpt.Out())

	if errNode != nil {
		val, _ := nodevalue.NodeValue(errNode)
		if errMap, ok := val.(map[string]any); ok {
			msg, _ := errMap["message"].(string)
			name, _ := errMap["name"].(string)

			if msg == "billing profile not found" || msg == "record not found" || name == "PlanNotFound" {
				return nil, fmt.Errorf("billing profile not found")
			}

			if msg != "" {
				return nil, fmt.Errorf("server error: %s", msg)
			}
		}
		return nil, fmt.Errorf("server error: %v", val)
	}

	val, err := nodevalue.NodeValue(okNode)
	if err != nil {
		return nil, fmt.Errorf("decoding result: %w", err)
	}

	res := &PlanResult{}
	if m, ok := val.(map[string]any); ok {
		if innerOk, hasOk := m["ok"].(map[string]any); hasOk {
			m = innerOk
		}
		if p, ok := m["product"].(string); ok {
			res.Product = p
		}
		if l, ok := m["limit"]; ok {
			res.Limit = fmt.Sprintf("%v", l)
		}
		if u, ok := m["updatedAt"]; ok {
			res.UpdatedAt = fmt.Sprintf("%v", u)
		}
	}

	return res, nil
}

func isAttestation(d delegation.Delegation) bool {
	for _, rawCap := range d.Capabilities() {
		source := validator.NewSource(rawCap, d)
		if _, err := ucancap.Attest.Match(source); err == nil {
			return true
		}
	}
	return false
}
