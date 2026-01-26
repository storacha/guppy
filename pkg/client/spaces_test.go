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
		require.Equal(t, space.DID(), spaces[0])
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
		require.ElementsMatch(t, []string{space1.DID().String(), space2.DID().String()},
			[]string{spaces[0].String(), spaces[1].String()})
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
		require.Equal(t, space.DID(), spaces[0])
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
		require.Equal(t, space.DID(), spaces[0])
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
		require.Equal(t, space.DID(), spaces[0])
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
		require.Equal(t, space.DID(), spaces[0])
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
		require.ElementsMatch(t, []string{space1.DID().String(), space2.DID().String()},
			[]string{spaces[0].String(), spaces[1].String()})
	})
}
