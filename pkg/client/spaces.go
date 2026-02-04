package client

import (
	"fmt"
	"slices"

	"github.com/ipld/go-ipld-prime/datamodel"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/storacha/go-ucanto/core/dag/blockstore"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/did"

	"github.com/storacha/guppy/pkg/agentstore"
)

// Space represents a space we can act as, along with the proofs that grant access.
type Space struct {
	did          did.DID
	accessProofs []delegation.Delegation
}

// DID returns the DID of this space.
func (s Space) DID() did.DID {
	return s.did
}

// AccessProofs returns the delegations that grant access to this space.
func (s Space) AccessProofs() []delegation.Delegation {
	return s.accessProofs
}

// Names returns the names found in the facts of this space's access proofs.
// Typically, a space has at most one name, but multiple names are possible.
func (s Space) Names() []string {
	var names []string
	for _, proof := range s.accessProofs {
		for _, fact := range proof.Facts() {
			nameValue, ok := fact["name"]
			if !ok {
				continue
			}
			nameNode, ok := nameValue.(datamodel.Node)
			if !ok {
				continue
			}
			name, err := nameNode.AsString()
			if err != nil {
				continue
			}
			names = append(names, name)
		}
	}
	return names
}

// SpaceFact is a UCAN fact that gives a space a name.
type SpaceFact struct {
	name string
}

// NewSpaceFact creates a new SpaceFact with the given name.
func NewSpaceFact(name string) SpaceFact {
	return SpaceFact{name: name}
}

// ToIPLD implements ucan.FactBuilder.
func (sf SpaceFact) ToIPLD() (map[string]datamodel.Node, error) {
	nb := basicnode.Prototype.String.NewBuilder()
	nb.AssignString(sf.name)
	return map[string]datamodel.Node{
		"name": nb.Build(),
	}, nil
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
	// Map from space DID to the delegations that grant access to it
	spaceProofs := map[string][]delegation.Delegation{}
	spaceDIDs := map[string]did.DID{}

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
				// For ucan:* delegations, collect proofs from nested delegations
				// and also add the current delegation as a proof
				for _, s := range spacesFromProofs {
					didStr := s.DID().String()
					spaceDIDs[didStr] = s.DID()
					// Add all proofs from the nested space, plus the current delegation
					spaceProofs[didStr] = append(spaceProofs[didStr], s.AccessProofs()...)
					spaceProofs[didStr] = append(spaceProofs[didStr], d)
				}
			} else {
				spaceDID, err := did.Parse(cap.With())
				if err != nil {
					return nil, fmt.Errorf("parsing space DID %s: %w", cap.With(), err)
				}
				didStr := spaceDID.String()
				spaceDIDs[didStr] = spaceDID
				spaceProofs[didStr] = append(spaceProofs[didStr], d)
			}
		}
	}

	// Convert map to slice of Space structs
	spaces := make([]Space, 0, len(spaceDIDs))
	for didStr, spaceDID := range spaceDIDs {
		// Deduplicate proofs by CID
		proofs := deduplicateDelegations(spaceProofs[didStr])
		spaces = append(spaces, Space{
			did:          spaceDID,
			accessProofs: proofs,
		})
	}

	return spaces, nil
}

// deduplicateDelegations removes duplicate delegations by CID.
func deduplicateDelegations(dels []delegation.Delegation) []delegation.Delegation {
	seen := make(map[string]struct{})
	result := make([]delegation.Delegation, 0, len(dels))
	for _, d := range dels {
		cidStr := d.Link().String()
		if _, exists := seen[cidStr]; !exists {
			seen[cidStr] = struct{}{}
			result = append(result, d)
		}
	}
	return slices.Clip(result)
}
