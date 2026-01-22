package client

import (
	"cmp"
	"maps"
	"slices"

	"github.com/storacha/go-ucanto/did"

	"github.com/storacha/guppy/pkg/agentstore"
)

func (c *Client) Accounts() ([]did.DID, error) {
	accounts := make(map[did.DID]struct{})
	res, err := c.Proofs(agentstore.CapabilityQuery{
		Can:  "*",
		With: "ucan:*",
	})
	if err != nil {
		return nil, err
	}
	for _, p := range res {
		if p.Audience().DID() == c.Issuer().DID() {
			for _, cap := range p.Capabilities() {
				// `Proofs()` also gives us attestations, so filter those out.
				if cap.Can() != "ucan/attest" {
					accounts[p.Issuer().DID()] = struct{}{}
					break
				}
			}
		}
	}
	result := slices.Collect(maps.Keys(accounts))
	slices.SortFunc(result, func(a, b did.DID) int {
		return cmp.Compare(a.String(), b.String())
	})
	return result, nil
}
