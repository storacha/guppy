package client

import (
	"fmt"
	"maps"
	"slices"

	"github.com/storacha/go-ucanto/core/dag/blockstore"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/did"

	"github.com/storacha/guppy/pkg/agentstore"
)

// Space represents a Storacha space.
type Space struct {
	DID  did.DID
	Name string
}

// Spaces returns all spaces we can act as.
func (c *Client) Spaces() ([]Space, error) {
	res, err := c.Proofs(agentstore.CapabilityQuery{Can: "space/*"})
	if err != nil {
		return nil, err
	}
	return spacesFromDelegations(res)
}

func spacesFromDelegations(dels []delegation.Delegation) ([]Space, error) {
	spaces := map[did.DID]Space{}
	for _, d := range dels {
		// Extract name from facts
		name := ""
		for _, fact := range d.Facts() {
			if f, ok := fact["name"]; ok {
				if n, ok := f.(string); ok {
					name = n
					break
				}
				// Use a generic interface check for AsString to be more robust
				if node, ok := f.(interface{ AsString() (string, error) }); ok {
					if n, err := node.AsString(); err == nil {
						name = n
						break
					}
				}
			}
		}

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
				for _, s := range spacesFromProofs {
					if existing, ok := spaces[s.DID]; !ok || existing.Name == "" {
						spaces[s.DID] = s
					}
				}
			} else {
				spaceDID, err := did.Parse(cap.With())
				if err != nil {
					return nil, fmt.Errorf("parsing space DID %s: %w", cap.With(), err)
				}
				if existing, ok := spaces[spaceDID]; !ok || existing.Name == "" {
					spaces[spaceDID] = Space{DID: spaceDID, Name: name}
				}
			}
		}
	}
	res := slices.Collect(maps.Values(spaces))
	slices.SortFunc(res, func(a, b Space) int {
		return slices.Compare([]byte(a.DID.String()), []byte(b.DID.String()))
	})
	return res, nil
}
