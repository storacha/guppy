package client

import (
	"context"
	"fmt"

	accesscap "github.com/storacha/go-libstoracha/capabilities/access"
	"github.com/storacha/go-ucanto/core/dag/blockstore"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/ucan"
)

// AccessDelegate invokes the access/delegate capability to store delegations on the service.
// This allows an agent to store delegations (like space grants) so they can be retrieved later.
func (c *Client) AccessDelegate(ctx context.Context, space did.DID, delegations ...delegation.Delegation) (accesscap.DelegateOk, error) {
	// Build the delegations map (CID -> Link)
	delegationsMap := make(map[string]ucan.Link)
	keys := make([]string, 0, len(delegations))

	for _, del := range delegations {
		cidStr := del.Link().String()
		delegationsMap[cidStr] = del.Link()
		keys = append(keys, cidStr)
	}

	caveats := accesscap.DelegateCaveats{
		Delegations: accesscap.DelegationLinksModel{
			Keys:   keys,
			Values: delegationsMap,
		},
	}

	// Include the delegations themselves as proofs
	// Include the delegations themselves as proofs
	delOptions := make([]delegation.Option, 0, len(delegations))
	for _, del := range delegations {
		// Prune the delegation to remove nested proofs (blocks) to reduce header size.
		// We use d.Root() because d.Block() does not exist on the interface.
		pruned, err := pruneDelegation(del)
		if err != nil {
			return accesscap.DelegateOk{}, fmt.Errorf("pruning delegation: %w", err)
		}
		delOptions = append(delOptions, delegation.WithProof(delegation.FromDelegation(pruned)))
	}

	res, _, err := invokeAndExecute[accesscap.DelegateCaveats, accesscap.DelegateOk](
		ctx,
		c,
		accesscap.Delegate,
		space.String(),
		caveats,
		accesscap.DelegateOkType(),
		delOptions...,
	)
	if err != nil {
		return accesscap.DelegateOk{}, fmt.Errorf("invoking and executing `access/delegate`: %w", err)
	}

	delegateOk, failErr := result.Unwrap(res)
	if failErr != nil {
		return accesscap.DelegateOk{}, fmt.Errorf("`access/delegate` failed: %w", failErr)
	}

	return delegateOk, nil
}

// pruneDelegation creates a shallow copy of the delegation that only contains
// the root block in its blockstore. This prevents the entire proof chain (nested blocks)
// from being serialized into the invocation header.
func pruneDelegation(d delegation.Delegation) (delegation.Delegation, error) {
	blk := d.Root()
	bs, err := blockstore.NewBlockStore()
	if err != nil {
		return nil, fmt.Errorf("creating blockstore: %w", err)
	}
	if err := bs.Put(blk); err != nil {
		return nil, fmt.Errorf("putting block: %w", err)
	}
	return delegation.NewDelegation(blk, bs)
}
