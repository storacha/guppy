package client

import (
	"cmp"
	"maps"
	"slices"

	"github.com/storacha/go-ucanto/did"

	"github.com/storacha/guppy/pkg/agentstore"
)

func (c *Client) Accounts() []did.DID {
	accounts := make(map[did.DID]struct{})
	for _, p := range c.Proofs(agentstore.CapabilityQuery{
		Can:  "*",
		With: "ucan:*",
	}) {
		if p.Audience().DID() == c.Issuer().DID() {
			for _, c := range p.Capabilities() {
				// `Proofs()` also gives us attestations, so filter those out.
				if c.Can() != "ucan/attest" {
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
	return result
}
