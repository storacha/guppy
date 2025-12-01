package client

import (
	"cmp"
	"maps"
	"slices"

	"github.com/storacha/go-ucanto/did"
)

func (c *Client) Accounts() []did.DID {
	accounts := make(map[did.DID]struct{})
	for _, p := range c.Proofs(CapabilityQuery{
		Can:  "*",
		With: "ucan:*",
	}) {
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
	return result
}
