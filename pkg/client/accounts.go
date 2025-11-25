package client

import "github.com/storacha/go-ucanto/did"

func (c *Client) Accounts() []did.DID {
	var accounts []did.DID
	for _, p := range c.Proofs(CapabilityQuery{
		Can:  "*",
		With: "ucan:*",
	}) {
		if p.Audience().DID() == c.Issuer().DID() {
			for _, cap := range p.Capabilities() {
				// `Proofs()` also gives us attestations, so filter those out.
				if cap.Can() == "*" {
					accounts = append(accounts, p.Issuer().DID())
					break
				}
			}
		}
	}
	return accounts
}
