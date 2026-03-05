package client

import (
	"context"
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	uclient "github.com/storacha/go-ucanto/client"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/receipt"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/ucan"

	"github.com/storacha/guppy/pkg/client/nodevalue"
)

const RevokeCan = "ucan/revoke"

type RevokeCaveat map[string]any

func (r RevokeCaveat) ToIPLD() (ipld.Node, error) {
	return nodevalue.FromAny(r)
}

func (c *Client) Revoke(ctx context.Context, targetCid cid.Cid) error {
	proofs, err := c.Proofs()
	if err != nil {
		return fmt.Errorf("querying local proofs: %w", err)
	}

	var targetProof delegation.Delegation
	for _, p := range proofs {
		if p.Link().String() == targetCid.String() {
			targetProof = p
			break
		}
	}

	if targetProof == nil {
		return fmt.Errorf("delegation %s not found locally. You must possess the delegation to revoke it", targetCid.String())
	}

	caveats := RevokeCaveat{
		"ucan": cidlink.Link{Cid: targetCid},
	}

	cap := ucan.NewCapability(RevokeCan, c.DID().String(), caveats)

	inv, err := invocation.Invoke(
		c.Issuer(),
		c.Connection().ID(),
		cap,
		delegation.WithProof(delegation.FromDelegation(targetProof)),
	)
	if err != nil {
		return fmt.Errorf("creating invocation: %w", err)
	}

	resp, err := uclient.Execute(ctx, []invocation.Invocation{inv}, c.Connection())
	if err != nil {
		return fmt.Errorf("executing invocation: %w", err)
	}

	rcptLink, ok := resp.Get(inv.Link())
	if !ok {
		return fmt.Errorf("receipt not found")
	}

	anyRcpt, err := receipt.NewAnyReceiptReader().Read(rcptLink, resp.Blocks())
	if err != nil {
		return fmt.Errorf("reading receipt: %w", err)
	}

	_, errNode := result.Unwrap(anyRcpt.Out())

	if errNode != nil {
		val, _ := nodevalue.NodeValue(errNode)
		if errMap, ok := val.(map[string]any); ok {
			if msg, ok := errMap["message"].(string); ok && msg != "" {
				return fmt.Errorf("server error: %s", msg)
			}
		}
		return fmt.Errorf("server error: %v", val)
	}

	if err := c.RemoveProof(targetCid); err != nil {
		return fmt.Errorf("network revocation succeeded, but failed to remove from local store: %w", err)
	}

	return nil
}