package client_test

import (
	"testing"

	ucancap "github.com/storacha/go-libstoracha/capabilities/ucan"
	"github.com/storacha/go-libstoracha/testutil"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/stretchr/testify/require"

	"github.com/storacha/guppy/pkg/client"
)

func TestSpaces(t *testing.T) {
	t.Run("returns empty slice when no space proofs exist", func(t *testing.T) {
		c := testutil.Must(client.NewClient())(t)

		spaces, err := c.Spaces()
		require.NoError(t, err)
		require.Empty(t, spaces)
	})

	t.Run("returns space DIDs from space/* delegations", func(t *testing.T) {
		c := testutil.Must(client.NewClient())(t)
		space := testutil.Must(signer.Generate())(t)

		spaceWildcardCap := ucan.NewCapability("space/*", space.DID().String(), ucan.NoCaveats{})
		spaceDel, err := delegation.Delegate(
			c.Issuer(),
			c.Issuer(),
			[]ucan.Capability[ucan.NoCaveats]{spaceWildcardCap},
		)
		require.NoError(t, err)

		err = c.AddProofs(spaceDel)
		require.NoError(t, err)

		spaces, err := c.Spaces()
		require.NoError(t, err)
		require.Len(t, spaces, 1)
		require.Equal(t, space.DID(), spaces[0].DID())
	})

	t.Run("returns multiple unique spaces", func(t *testing.T) {
		c := testutil.Must(client.NewClient())(t)
		space1 := testutil.Must(signer.Generate())(t)
		space2 := testutil.Must(signer.Generate())(t)

		// Create space/* delegations for two different spaces
		cap1 := ucan.NewCapability("space/*", space1.DID().String(), ucan.NoCaveats{})
		del1, err := delegation.Delegate(c.Issuer(), c.Issuer(), []ucan.Capability[ucan.NoCaveats]{cap1})
		require.NoError(t, err)

		cap2 := ucan.NewCapability("space/*", space2.DID().String(), ucan.NoCaveats{})
		del2, err := delegation.Delegate(c.Issuer(), c.Issuer(), []ucan.Capability[ucan.NoCaveats]{cap2})
		require.NoError(t, err)

		err = c.AddProofs(del1, del2)
		require.NoError(t, err)

		spaces, err := c.Spaces()
		require.NoError(t, err)
		require.Len(t, spaces, 2)

		spaceDIDs := []string{spaces[0].DID().String(), spaces[1].DID().String()}
		require.ElementsMatch(t, []string{space1.DID().String(), space2.DID().String()}, spaceDIDs)
	})

	t.Run("deduplicates spaces from multiple delegations", func(t *testing.T) {
		c := testutil.Must(client.NewClient())(t)
		space := testutil.Must(signer.Generate())(t)

		// Create two different delegations for the same space
		cap1 := ucan.NewCapability("space/*", space.DID().String(), ucan.NoCaveats{})
		del1, err := delegation.Delegate(c.Issuer(), c.Issuer(), []ucan.Capability[ucan.NoCaveats]{cap1})
		require.NoError(t, err)

		// Create another delegation with a different space/* capability for the same space
		cap2 := ucan.NewCapability("space/*", space.DID().String(), ucan.NoCaveats{})
		del2, err := delegation.Delegate(c.Issuer(), c.Issuer(), []ucan.Capability[ucan.NoCaveats]{cap2})
		require.NoError(t, err)

		err = c.AddProofs(del1, del2)
		require.NoError(t, err)

		spaces, err := c.Spaces()
		require.NoError(t, err)
		require.Len(t, spaces, 1)
		require.Equal(t, space.DID(), spaces[0].DID())
	})

	t.Run("skips ucan/attest capabilities", func(t *testing.T) {
		c := testutil.Must(client.NewClient())(t)
		space := testutil.Must(signer.Generate())(t)
		issuer := testutil.Must(signer.Generate())(t)

		// Create a space/* delegation
		spaceCap := ucan.NewCapability("space/*", space.DID().String(), ucan.NoCaveats{})
		spaceDel, err := delegation.Delegate(issuer, c.Issuer(), []ucan.Capability[ucan.NoCaveats]{spaceCap})
		require.NoError(t, err)

		// Create a ucan/attest session proof
		attestDel := testutil.Must(ucancap.Attest.Delegate(
			issuer,
			c.Issuer(),
			issuer.DID().String(),
			ucancap.AttestCaveats{Proof: spaceDel.Link()},
		))(t)

		err = c.AddProofs(spaceDel, attestDel)
		require.NoError(t, err)

		spaces, err := c.Spaces()
		require.NoError(t, err)
		// Should only return the space from the space/* delegation, not try to parse
		// the issuer DID from the attest delegation
		require.Len(t, spaces, 1)
		require.Equal(t, space.DID(), spaces[0].DID())
	})

	t.Run("extracts spaces from ucan:* delegations via proofs", func(t *testing.T) {
		c := testutil.Must(client.NewClient())(t)
		space := testutil.Must(signer.Generate())(t)
		issuer := testutil.Must(signer.Generate())(t)

		// Create a delegation with a specific space
		specificCap := ucan.NewCapability("space/*", space.DID().String(), ucan.NoCaveats{})
		specificDel, err := delegation.Delegate(issuer, c.Issuer(), []ucan.Capability[ucan.NoCaveats]{specificCap})
		require.NoError(t, err)

		// Create a delegation with ucan:* that includes the specific delegation as a proof
		wildcardCap := ucan.NewCapability("space/*", "ucan:*", ucan.NoCaveats{})
		wildcardDel, err := delegation.Delegate(
			c.Issuer(),
			c.Issuer(),
			[]ucan.Capability[ucan.NoCaveats]{wildcardCap},
			delegation.WithProof(delegation.FromDelegation(specificDel)),
		)
		require.NoError(t, err)

		err = c.AddProofs(wildcardDel)
		require.NoError(t, err)

		spaces, err := c.Spaces()
		require.NoError(t, err)
		require.Len(t, spaces, 1)
		require.Equal(t, space.DID(), spaces[0].DID())
	})

	t.Run("excludes expired delegations", func(t *testing.T) {
		c := testutil.Must(client.NewClient())(t)
		space := testutil.Must(signer.Generate())(t)

		// Create an expired space/* delegation
		spaceCap := ucan.NewCapability("space/*", space.DID().String(), ucan.NoCaveats{})
		expiredDel, err := delegation.Delegate(
			c.Issuer(),
			c.Issuer(),
			[]ucan.Capability[ucan.NoCaveats]{spaceCap},
			delegation.WithExpiration(ucan.Now()-100), // Expired 100 seconds ago
		)
		require.NoError(t, err)

		err = c.AddProofs(expiredDel)
		require.NoError(t, err)

		spaces, err := c.Spaces()
		require.NoError(t, err)
		require.Empty(t, spaces)
	})

	t.Run("excludes future delegations", func(t *testing.T) {
		c := testutil.Must(client.NewClient())(t)
		space := testutil.Must(signer.Generate())(t)

		// Create a delegation that's not yet valid
		spaceCap := ucan.NewCapability("space/*", space.DID().String(), ucan.NoCaveats{})
		futureDel, err := delegation.Delegate(
			c.Issuer(),
			c.Issuer(),
			[]ucan.Capability[ucan.NoCaveats]{spaceCap},
			delegation.WithNotBefore(ucan.Now()+100), // Valid 100 seconds from now
		)
		require.NoError(t, err)

		err = c.AddProofs(futureDel)
		require.NoError(t, err)

		spaces, err := c.Spaces()
		require.NoError(t, err)
		require.Empty(t, spaces)
	})

	t.Run("handles nested ucan:* proofs", func(t *testing.T) {
		c := testutil.Must(client.NewClient())(t)
		space := testutil.Must(signer.Generate())(t)
		intermediate := testutil.Must(signer.Generate())(t)

		// Create a delegation with a specific space
		specificCap := ucan.NewCapability("space/*", space.DID().String(), ucan.NoCaveats{})
		specificDel, err := delegation.Delegate(intermediate, c.Issuer(), []ucan.Capability[ucan.NoCaveats]{specificCap})
		require.NoError(t, err)

		// Create an intermediate delegation with ucan:* that includes the specific delegation
		intermediateCap := ucan.NewCapability("space/*", "ucan:*", ucan.NoCaveats{})
		intermediateDel, err := delegation.Delegate(
			intermediate,
			c.Issuer(),
			[]ucan.Capability[ucan.NoCaveats]{intermediateCap},
			delegation.WithProof(delegation.FromDelegation(specificDel)),
		)
		require.NoError(t, err)

		// Create a top-level delegation with ucan:* that includes the intermediate
		topCap := ucan.NewCapability("space/*", "ucan:*", ucan.NoCaveats{})
		topDel, err := delegation.Delegate(
			c.Issuer(),
			c.Issuer(),
			[]ucan.Capability[ucan.NoCaveats]{topCap},
			delegation.WithProof(delegation.FromDelegation(intermediateDel)),
		)
		require.NoError(t, err)

		err = c.AddProofs(topDel)
		require.NoError(t, err)

		spaces, err := c.Spaces()
		require.NoError(t, err)
		require.Len(t, spaces, 1)
		require.Equal(t, space.DID(), spaces[0].DID())
	})

	t.Run("handles multiple capabilities in single delegation", func(t *testing.T) {
		c := testutil.Must(client.NewClient())(t)
		space1 := testutil.Must(signer.Generate())(t)
		space2 := testutil.Must(signer.Generate())(t)

		// Create a delegation with multiple space/* capabilities for different spaces
		cap1 := ucan.NewCapability("space/*", space1.DID().String(), ucan.NoCaveats{})
		cap2 := ucan.NewCapability("space/*", space2.DID().String(), ucan.NoCaveats{})
		multiDel, err := delegation.Delegate(
			c.Issuer(),
			c.Issuer(),
			[]ucan.Capability[ucan.NoCaveats]{cap1, cap2},
		)
		require.NoError(t, err)

		err = c.AddProofs(multiDel)
		require.NoError(t, err)

		spaces, err := c.Spaces()
		require.NoError(t, err)
		require.Len(t, spaces, 2)

		spaceDIDs := []string{spaces[0].DID().String(), spaces[1].DID().String()}
		require.ElementsMatch(t, []string{space1.DID().String(), space2.DID().String()}, spaceDIDs)
	})
}

func TestSpace(t *testing.T) {
	t.Run("DID returns the space DID", func(t *testing.T) {
		c := testutil.Must(client.NewClient())(t)
		space := testutil.Must(signer.Generate())(t)

		spaceCap := ucan.NewCapability("space/*", space.DID().String(), ucan.NoCaveats{})
		spaceDel, err := delegation.Delegate(c.Issuer(), c.Issuer(), []ucan.Capability[ucan.NoCaveats]{spaceCap})
		require.NoError(t, err)

		err = c.AddProofs(spaceDel)
		require.NoError(t, err)

		spaces, err := c.Spaces()
		require.NoError(t, err)
		require.Len(t, spaces, 1)
		require.Equal(t, space.DID(), spaces[0].DID())
	})

	t.Run("AccessProofs returns the delegation that granted access", func(t *testing.T) {
		c := testutil.Must(client.NewClient())(t)
		space := testutil.Must(signer.Generate())(t)

		spaceCap := ucan.NewCapability("space/*", space.DID().String(), ucan.NoCaveats{})
		spaceDel, err := delegation.Delegate(c.Issuer(), c.Issuer(), []ucan.Capability[ucan.NoCaveats]{spaceCap})
		require.NoError(t, err)

		err = c.AddProofs(spaceDel)
		require.NoError(t, err)

		spaces, err := c.Spaces()
		require.NoError(t, err)
		require.Len(t, spaces, 1)

		proofs := spaces[0].AccessProofs()
		require.Len(t, proofs, 1)
		require.Equal(t, spaceDel.Link(), proofs[0].Link())
	})

	t.Run("AccessProofs includes multiple delegations for same space", func(t *testing.T) {
		c := testutil.Must(client.NewClient())(t)
		space := testutil.Must(signer.Generate())(t)
		issuer1 := testutil.Must(signer.Generate())(t)
		issuer2 := testutil.Must(signer.Generate())(t)

		// Create two different delegations for the same space from different issuers
		// (different issuers ensure the delegations have different CIDs)
		cap1 := ucan.NewCapability("space/*", space.DID().String(), ucan.NoCaveats{})
		del1, err := delegation.Delegate(issuer1, c.Issuer(), []ucan.Capability[ucan.NoCaveats]{cap1})
		require.NoError(t, err)

		cap2 := ucan.NewCapability("space/*", space.DID().String(), ucan.NoCaveats{})
		del2, err := delegation.Delegate(issuer2, c.Issuer(), []ucan.Capability[ucan.NoCaveats]{cap2})
		require.NoError(t, err)

		// Verify they have different CIDs (ensuring we're testing what we think we are)
		require.NotEqual(t, del1.Link(), del2.Link(), "delegations should have different CIDs")

		err = c.AddProofs(del1, del2)
		require.NoError(t, err)

		spaces, err := c.Spaces()
		require.NoError(t, err)
		require.Len(t, spaces, 1)

		proofs := spaces[0].AccessProofs()
		require.Len(t, proofs, 2)

		proofCIDs := []string{proofs[0].Link().String(), proofs[1].Link().String()}
		require.ElementsMatch(t, []string{del1.Link().String(), del2.Link().String()}, proofCIDs)
	})

	t.Run("AccessProofs includes chain for ucan:* delegations", func(t *testing.T) {
		c := testutil.Must(client.NewClient())(t)
		space := testutil.Must(signer.Generate())(t)
		issuer := testutil.Must(signer.Generate())(t)

		// Create a delegation with a specific space
		specificCap := ucan.NewCapability("space/*", space.DID().String(), ucan.NoCaveats{})
		specificDel, err := delegation.Delegate(issuer, c.Issuer(), []ucan.Capability[ucan.NoCaveats]{specificCap})
		require.NoError(t, err)

		// Create a delegation with ucan:* that includes the specific delegation as a proof
		wildcardCap := ucan.NewCapability("space/*", "ucan:*", ucan.NoCaveats{})
		wildcardDel, err := delegation.Delegate(
			c.Issuer(),
			c.Issuer(),
			[]ucan.Capability[ucan.NoCaveats]{wildcardCap},
			delegation.WithProof(delegation.FromDelegation(specificDel)),
		)
		require.NoError(t, err)

		err = c.AddProofs(wildcardDel)
		require.NoError(t, err)

		spaces, err := c.Spaces()
		require.NoError(t, err)
		require.Len(t, spaces, 1)

		proofs := spaces[0].AccessProofs()
		// Should include both the specific delegation and the wildcard delegation
		require.Len(t, proofs, 2)

		proofCIDs := []string{proofs[0].Link().String(), proofs[1].Link().String()}
		require.ElementsMatch(t, []string{specificDel.Link().String(), wildcardDel.Link().String()}, proofCIDs)
	})

	t.Run("AccessProofs deduplicates same delegation appearing multiple times", func(t *testing.T) {
		c := testutil.Must(client.NewClient())(t)
		space := testutil.Must(signer.Generate())(t)

		// Create a delegation with two capabilities for the same space
		// This would cause the delegation to be added twice without deduplication
		cap1 := ucan.NewCapability("space/*", space.DID().String(), ucan.NoCaveats{})
		cap2 := ucan.NewCapability("space/*", space.DID().String(), ucan.NoCaveats{})
		multiDel, err := delegation.Delegate(
			c.Issuer(),
			c.Issuer(),
			[]ucan.Capability[ucan.NoCaveats]{cap1, cap2},
		)
		require.NoError(t, err)

		err = c.AddProofs(multiDel)
		require.NoError(t, err)

		spaces, err := c.Spaces()
		require.NoError(t, err)
		require.Len(t, spaces, 1)

		proofs := spaces[0].AccessProofs()
		// Should deduplicate: only one proof even though the delegation has two capabilities for the same space
		require.Len(t, proofs, 1)
		require.Equal(t, multiDel.Link(), proofs[0].Link())
	})

	t.Run("Names returns names from access proof facts", func(t *testing.T) {
		c := testutil.Must(client.NewClient())(t)
		space := testutil.Must(signer.Generate())(t)

		spaceCap := ucan.NewCapability("space/*", space.DID().String(), ucan.NoCaveats{})
		spaceDel, err := delegation.Delegate(
			c.Issuer(),
			c.Issuer(),
			[]ucan.Capability[ucan.NoCaveats]{spaceCap},
			delegation.WithFacts([]ucan.FactBuilder{
				client.NewSpaceNameFact("my cool space"),
			}),
		)
		require.NoError(t, err)

		err = c.AddProofs(spaceDel)
		require.NoError(t, err)

		spaces, err := c.Spaces()
		require.NoError(t, err)
		require.Len(t, spaces, 1)
		require.Equal(t, []string{"my cool space"}, spaces[0].Names())
	})

	t.Run("Names returns empty slice when no facts have names", func(t *testing.T) {
		c := testutil.Must(client.NewClient())(t)
		space := testutil.Must(signer.Generate())(t)

		spaceCap := ucan.NewCapability("space/*", space.DID().String(), ucan.NoCaveats{})
		spaceDel, err := delegation.Delegate(
			c.Issuer(),
			c.Issuer(),
			[]ucan.Capability[ucan.NoCaveats]{spaceCap},
		)
		require.NoError(t, err)

		err = c.AddProofs(spaceDel)
		require.NoError(t, err)

		spaces, err := c.Spaces()
		require.NoError(t, err)
		require.Len(t, spaces, 1)
		require.Nil(t, spaces[0].Names())
	})

	t.Run("Names returns names from multiple access proofs", func(t *testing.T) {
		c := testutil.Must(client.NewClient())(t)
		space := testutil.Must(signer.Generate())(t)
		issuer1 := testutil.Must(signer.Generate())(t)
		issuer2 := testutil.Must(signer.Generate())(t)

		spaceCap := ucan.NewCapability("space/*", space.DID().String(), ucan.NoCaveats{})

		del1, err := delegation.Delegate(
			issuer1,
			c.Issuer(),
			[]ucan.Capability[ucan.NoCaveats]{spaceCap},
			delegation.WithFacts([]ucan.FactBuilder{
				client.NewSpaceNameFact("name one"),
			}),
		)
		require.NoError(t, err)

		del2, err := delegation.Delegate(
			issuer2,
			c.Issuer(),
			[]ucan.Capability[ucan.NoCaveats]{spaceCap},
			delegation.WithFacts([]ucan.FactBuilder{
				client.NewSpaceNameFact("name two"),
			}),
		)
		require.NoError(t, err)

		err = c.AddProofs(del1, del2)
		require.NoError(t, err)

		spaces, err := c.Spaces()
		require.NoError(t, err)
		require.Len(t, spaces, 1)
		require.ElementsMatch(t, []string{"name one", "name two"}, spaces[0].Names())
	})

	t.Run("Names returns names from ucan:* delegation proofs", func(t *testing.T) {
		c := testutil.Must(client.NewClient())(t)
		space := testutil.Must(signer.Generate())(t)
		issuer := testutil.Must(signer.Generate())(t)

		// Create a delegation with a specific space and a name fact
		specificCap := ucan.NewCapability("space/*", space.DID().String(), ucan.NoCaveats{})
		specificDel, err := delegation.Delegate(
			issuer,
			c.Issuer(),
			[]ucan.Capability[ucan.NoCaveats]{specificCap},
			delegation.WithFacts([]ucan.FactBuilder{
				client.NewSpaceNameFact("named space"),
			}),
		)
		require.NoError(t, err)

		// Create a ucan:* delegation that includes the specific delegation as a proof
		wildcardCap := ucan.NewCapability("space/*", "ucan:*", ucan.NoCaveats{})
		wildcardDel, err := delegation.Delegate(
			c.Issuer(),
			c.Issuer(),
			[]ucan.Capability[ucan.NoCaveats]{wildcardCap},
			delegation.WithProof(delegation.FromDelegation(specificDel)),
		)
		require.NoError(t, err)

		err = c.AddProofs(wildcardDel)
		require.NoError(t, err)

		spaces, err := c.Spaces()
		require.NoError(t, err)
		require.Len(t, spaces, 1)
		require.Contains(t, spaces[0].Names(), "named space")
	})

	t.Run("Names deduplicates identical names", func(t *testing.T) {
		c := testutil.Must(client.NewClient())(t)
		space := testutil.Must(signer.Generate())(t)
		issuer1 := testutil.Must(signer.Generate())(t)
		issuer2 := testutil.Must(signer.Generate())(t)

		spaceCap := ucan.NewCapability("space/*", space.DID().String(), ucan.NoCaveats{})

		del1, err := delegation.Delegate(
			issuer1,
			c.Issuer(),
			[]ucan.Capability[ucan.NoCaveats]{spaceCap},
			delegation.WithFacts([]ucan.FactBuilder{
				client.NewSpaceNameFact("same name"),
			}),
		)
		require.NoError(t, err)

		del2, err := delegation.Delegate(
			issuer2,
			c.Issuer(),
			[]ucan.Capability[ucan.NoCaveats]{spaceCap},
			delegation.WithFacts([]ucan.FactBuilder{
				client.NewSpaceNameFact("same name"),
			}),
		)
		require.NoError(t, err)

		err = c.AddProofs(del1, del2)
		require.NoError(t, err)

		spaces, err := c.Spaces()
		require.NoError(t, err)
		require.Len(t, spaces, 1)
		require.Equal(t, []string{"same name"}, spaces[0].Names())
	})
}

func TestSpacesNamed(t *testing.T) {
	t.Run("returns empty slice when no spaces exist", func(t *testing.T) {
		c := testutil.Must(client.NewClient())(t)

		spaces, err := c.SpacesNamed("some name")
		require.NoError(t, err)
		require.Empty(t, spaces)
	})

	t.Run("returns empty slice when no spaces have the given name", func(t *testing.T) {
		c := testutil.Must(client.NewClient())(t)
		space := testutil.Must(signer.Generate())(t)

		spaceCap := ucan.NewCapability("space/*", space.DID().String(), ucan.NoCaveats{})
		spaceDel, err := delegation.Delegate(
			c.Issuer(),
			c.Issuer(),
			[]ucan.Capability[ucan.NoCaveats]{spaceCap},
			delegation.WithFacts([]ucan.FactBuilder{
				client.NewSpaceNameFact("some name"),
			}),
		)
		require.NoError(t, err)

		err = c.AddProofs(spaceDel)
		require.NoError(t, err)

		spaces, err := c.SpacesNamed("different name")
		require.NoError(t, err)
		require.Empty(t, spaces)
	})

	t.Run("returns space with matching name", func(t *testing.T) {
		c := testutil.Must(client.NewClient())(t)
		space := testutil.Must(signer.Generate())(t)

		spaceCap := ucan.NewCapability("space/*", space.DID().String(), ucan.NoCaveats{})
		spaceDel, err := delegation.Delegate(
			c.Issuer(),
			c.Issuer(),
			[]ucan.Capability[ucan.NoCaveats]{spaceCap},
			delegation.WithFacts([]ucan.FactBuilder{
				client.NewSpaceNameFact("my space"),
			}),
		)
		require.NoError(t, err)

		err = c.AddProofs(spaceDel)
		require.NoError(t, err)

		spaces, err := c.SpacesNamed("my space")
		require.NoError(t, err)
		require.Len(t, spaces, 1)
		require.Equal(t, space.DID(), spaces[0].DID())
	})

	t.Run("returns multiple spaces with same name", func(t *testing.T) {
		c := testutil.Must(client.NewClient())(t)
		space1 := testutil.Must(signer.Generate())(t)
		space2 := testutil.Must(signer.Generate())(t)

		spaceCap1 := ucan.NewCapability("space/*", space1.DID().String(), ucan.NoCaveats{})
		del1, err := delegation.Delegate(
			c.Issuer(),
			c.Issuer(),
			[]ucan.Capability[ucan.NoCaveats]{spaceCap1},
			delegation.WithFacts([]ucan.FactBuilder{
				client.NewSpaceNameFact("shared name"),
			}),
		)
		require.NoError(t, err)

		spaceCap2 := ucan.NewCapability("space/*", space2.DID().String(), ucan.NoCaveats{})
		del2, err := delegation.Delegate(
			c.Issuer(),
			c.Issuer(),
			[]ucan.Capability[ucan.NoCaveats]{spaceCap2},
			delegation.WithFacts([]ucan.FactBuilder{
				client.NewSpaceNameFact("shared name"),
			}),
		)
		require.NoError(t, err)

		err = c.AddProofs(del1, del2)
		require.NoError(t, err)

		spaces, err := c.SpacesNamed("shared name")
		require.NoError(t, err)
		require.Len(t, spaces, 2)

		spaceDIDs := []string{spaces[0].DID().String(), spaces[1].DID().String()}
		require.ElementsMatch(t, []string{space1.DID().String(), space2.DID().String()}, spaceDIDs)
	})

	t.Run("filters to only spaces with matching name", func(t *testing.T) {
		c := testutil.Must(client.NewClient())(t)
		space1 := testutil.Must(signer.Generate())(t)
		space2 := testutil.Must(signer.Generate())(t)

		spaceCap1 := ucan.NewCapability("space/*", space1.DID().String(), ucan.NoCaveats{})
		del1, err := delegation.Delegate(
			c.Issuer(),
			c.Issuer(),
			[]ucan.Capability[ucan.NoCaveats]{spaceCap1},
			delegation.WithFacts([]ucan.FactBuilder{
				client.NewSpaceNameFact("target name"),
			}),
		)
		require.NoError(t, err)

		spaceCap2 := ucan.NewCapability("space/*", space2.DID().String(), ucan.NoCaveats{})
		del2, err := delegation.Delegate(
			c.Issuer(),
			c.Issuer(),
			[]ucan.Capability[ucan.NoCaveats]{spaceCap2},
			delegation.WithFacts([]ucan.FactBuilder{
				client.NewSpaceNameFact("other name"),
			}),
		)
		require.NoError(t, err)

		err = c.AddProofs(del1, del2)
		require.NoError(t, err)

		spaces, err := c.SpacesNamed("target name")
		require.NoError(t, err)
		require.Len(t, spaces, 1)
		require.Equal(t, space1.DID(), spaces[0].DID())
	})

	t.Run("matches space with multiple names", func(t *testing.T) {
		c := testutil.Must(client.NewClient())(t)
		space := testutil.Must(signer.Generate())(t)
		issuer1 := testutil.Must(signer.Generate())(t)
		issuer2 := testutil.Must(signer.Generate())(t)

		spaceCap := ucan.NewCapability("space/*", space.DID().String(), ucan.NoCaveats{})

		del1, err := delegation.Delegate(
			issuer1,
			c.Issuer(),
			[]ucan.Capability[ucan.NoCaveats]{spaceCap},
			delegation.WithFacts([]ucan.FactBuilder{
				client.NewSpaceNameFact("first name"),
			}),
		)
		require.NoError(t, err)

		del2, err := delegation.Delegate(
			issuer2,
			c.Issuer(),
			[]ucan.Capability[ucan.NoCaveats]{spaceCap},
			delegation.WithFacts([]ucan.FactBuilder{
				client.NewSpaceNameFact("second name"),
				client.NewSpaceNameFact("third name"),
			}),
		)
		require.NoError(t, err)

		err = c.AddProofs(del1, del2)
		require.NoError(t, err)

		spaces1, err := c.SpacesNamed("first name")
		require.NoError(t, err)
		spaces2, err := c.SpacesNamed("second name")
		require.NoError(t, err)
		spaces3, err := c.SpacesNamed("third name")
		require.NoError(t, err)

		require.Len(t, spaces1, 1)
		require.Equal(t, space.DID(), spaces1[0].DID())
		require.Len(t, spaces2, 1)
		require.Equal(t, space.DID(), spaces2[0].DID())
		require.Len(t, spaces3, 1)
		require.Equal(t, space.DID(), spaces3[0].DID())
	})
}

func TestSpaceNamed(t *testing.T) {
	t.Run("returns SpaceNotFoundError when no spaces exist", func(t *testing.T) {
		c := testutil.Must(client.NewClient())(t)

		_, err := c.SpaceNamed("some name")
		require.Error(t, err)

		var notFoundErr client.SpaceNotFoundError
		require.ErrorAs(t, err, &notFoundErr)
		require.Equal(t, "some name", notFoundErr.Name)
	})

	t.Run("returns SpaceNotFoundError when no space has the given name", func(t *testing.T) {
		c := testutil.Must(client.NewClient())(t)
		space := testutil.Must(signer.Generate())(t)

		spaceCap := ucan.NewCapability("space/*", space.DID().String(), ucan.NoCaveats{})
		spaceDel, err := delegation.Delegate(
			c.Issuer(),
			c.Issuer(),
			[]ucan.Capability[ucan.NoCaveats]{spaceCap},
			delegation.WithFacts([]ucan.FactBuilder{
				client.NewSpaceNameFact("some name"),
			}),
		)
		require.NoError(t, err)

		err = c.AddProofs(spaceDel)
		require.NoError(t, err)

		_, err = c.SpaceNamed("different name")
		require.Error(t, err)

		var notFoundErr client.SpaceNotFoundError
		require.ErrorAs(t, err, &notFoundErr)
		require.Equal(t, "different name", notFoundErr.Name)
	})

	t.Run("returns space with matching name", func(t *testing.T) {
		c := testutil.Must(client.NewClient())(t)
		space := testutil.Must(signer.Generate())(t)

		spaceCap := ucan.NewCapability("space/*", space.DID().String(), ucan.NoCaveats{})
		spaceDel, err := delegation.Delegate(
			c.Issuer(),
			c.Issuer(),
			[]ucan.Capability[ucan.NoCaveats]{spaceCap},
			delegation.WithFacts([]ucan.FactBuilder{
				client.NewSpaceNameFact("my space"),
			}),
		)
		require.NoError(t, err)

		err = c.AddProofs(spaceDel)
		require.NoError(t, err)

		result, err := c.SpaceNamed("my space")
		require.NoError(t, err)
		require.Equal(t, space.DID(), result.DID())
	})

	t.Run("returns MultipleSpacesFoundError when multiple spaces have the same name", func(t *testing.T) {
		c := testutil.Must(client.NewClient())(t)
		space1 := testutil.Must(signer.Generate())(t)
		space2 := testutil.Must(signer.Generate())(t)

		spaceCap1 := ucan.NewCapability("space/*", space1.DID().String(), ucan.NoCaveats{})
		del1, err := delegation.Delegate(
			c.Issuer(),
			c.Issuer(),
			[]ucan.Capability[ucan.NoCaveats]{spaceCap1},
			delegation.WithFacts([]ucan.FactBuilder{
				client.NewSpaceNameFact("shared name"),
			}),
		)
		require.NoError(t, err)

		spaceCap2 := ucan.NewCapability("space/*", space2.DID().String(), ucan.NoCaveats{})
		del2, err := delegation.Delegate(
			c.Issuer(),
			c.Issuer(),
			[]ucan.Capability[ucan.NoCaveats]{spaceCap2},
			delegation.WithFacts([]ucan.FactBuilder{
				client.NewSpaceNameFact("shared name"),
			}),
		)
		require.NoError(t, err)

		err = c.AddProofs(del1, del2)
		require.NoError(t, err)

		_, err = c.SpaceNamed("shared name")
		require.Error(t, err)

		var multipleErr client.MultipleSpacesFoundError
		require.ErrorAs(t, err, &multipleErr)
		require.Equal(t, "shared name", multipleErr.Name)
		expected := []string{space1.DID().String(), space2.DID().String()}
		actual := []string{multipleErr.Spaces[0].DID().String(), multipleErr.Spaces[1].DID().String()}
		require.ElementsMatch(t, expected, actual)
	})
}
