package client

import (
	"fmt"

	"github.com/storacha/go-ucanto/core/dag/blockstore"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/did"
)

// Spaces returns all spaces we can act as.
func (c *Client) Spaces() ([]did.DID, error) {
	return spacesFromDelegations(c.Proofs(CapabilityQuery{Can: "space/*", With: "ucan:*"}))
}

func spacesFromDelegations(dels []delegation.Delegation) ([]did.DID, error) {
	var spaces []did.DID
	for _, d := range dels {
		for _, cap := range d.Capabilities() {
			if cap.Can() == "ucan/attest" {
				continue
			} else if cap.With() == "ucan:*" {
				proofDels := make([]delegation.Delegation, 0, len(d.Proofs()))
				bs, err := blockstore.NewBlockReader(blockstore.WithBlocksIterator(d.Blocks()))
				if err != nil {
					return nil, fmt.Errorf("creating blockstore reader: %w", err)
				}
				for _, plink := range d.Proofs() {
					proofDel, err := delegation.NewDelegationView(plink, bs)
					if err != nil {
						return nil, fmt.Errorf("opening proof delegation %s: %w", plink, err)
					}
					proofDels = append(proofDels, proofDel)
				}
				spacesFromProofs, err := spacesFromDelegations(proofDels)
				if err != nil {
					return nil, fmt.Errorf("getting spaces from proofs: %w", err)
				}
				spaces = append(spaces, spacesFromProofs...)
			} else {
				space, err := did.Parse(cap.With())
				if err != nil {
					return nil, fmt.Errorf("parsing space DID %s: %w", cap.With(), err)
				}
				spaces = append(spaces, space)
			}
		}
	}
	return spaces, nil
}
