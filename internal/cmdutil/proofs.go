package cmdutil

import (
	ucan_bs "github.com/storacha/go-ucanto/core/dag/blockstore"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/storacha/go-ucanto/validator"
)

// proofResource finds the resource for a proof, handling the case where the
// delegated resource is "ucan:*" by recursively checking its proofs to find a
// delegation for the specific resource.
func ProofResource(proof delegation.Delegation, ability ucan.Ability) (ucan.Resource, bool) {
	for _, cap := range proof.Capabilities() {
		if validator.ResolveAbility(cap.Can(), ability) == "" {
			continue
		}
		if cap.With() != "ucan:*" {
			return cap.With(), true
		}
		proofs := proof.Proofs()
		if len(proofs) == 0 {
			continue
		}
		bs, err := ucan_bs.NewBlockReader(ucan_bs.WithBlocksIterator(proof.Blocks()))
		if err != nil {
			return "", false
		}
		for _, plink := range proofs {
			p, err := delegation.NewDelegationView(plink, bs)
			if err != nil {
				return "", false
			}
			if r, ok := ProofResource(p, ability); ok {
				return r, true
			}
		}
	}
	return "", false
}
