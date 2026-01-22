package client_test

import (
	"testing"

	spaceblobcap "github.com/storacha/go-libstoracha/capabilities/space/blob"
	captypes "github.com/storacha/go-libstoracha/capabilities/types"
	ucancap "github.com/storacha/go-libstoracha/capabilities/ucan"
	uploadcap "github.com/storacha/go-libstoracha/capabilities/upload"
	"github.com/storacha/go-libstoracha/testutil"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/stretchr/testify/require"

	"github.com/storacha/guppy/pkg/agentstore"
	"github.com/storacha/guppy/pkg/client"
)

func TestReset(t *testing.T) {
	store := testutil.Must(agentstore.NewMemory())(t)
	c := testutil.Must(client.NewClient(client.WithStore(store)))(t)
	res, err := c.Proofs()
	require.NoError(t, err)
	require.Empty(t, res, "expected no proofs to be present initially")

	issuer := c.Issuer()

	// Some arbitrary delegation
	del := testutil.Must(uploadcap.Get.Delegate(
		c.Issuer(),
		c.Issuer(),
		c.Issuer().DID().String(),
		uploadcap.GetCaveats{Root: testutil.RandomCID(t)},
	))(t)

	err = c.AddProofs(del)
	require.NoError(t, err)
	res, err = c.Proofs()
	require.NoError(t, err)
	require.Equal(t, []delegation.Delegation{del}, res, "expected one proof to be added")

	err = c.Reset()
	require.NoError(t, err, "expected reset to succeed")
	res, err = c.Proofs()
	require.NoError(t, err)
	require.Empty(t, res, "expected all proofs to be removed after reset")
	require.Equal(t, c.DID(), issuer.DID(), "expected issuer to remain unchanged after reset")

	// Verify store state
	storedPrincipal, err := store.Principal()
	require.NoError(t, err)
	require.Equal(t, storedPrincipal.DID(), issuer.DID(), "expected saved principal to be the issuer")

	storedDelegations, err := store.Delegations()
	require.NoError(t, err)
	require.Empty(t, storedDelegations, "expected saved proofs to be empty")
}

func TestProofs(t *testing.T) {
	c := testutil.Must(client.NewClient())(t)

	// Create delegations with different capabilities
	uploadDel := testutil.Must(uploadcap.Add.Delegate(
		c.Issuer(),
		c.Issuer(),
		c.Issuer().DID().String(),
		uploadcap.AddCaveats{Root: testutil.RandomCID(t), Shards: nil},
	))(t)

	blobDel := testutil.Must(spaceblobcap.Add.Delegate(
		c.Issuer(),
		c.Issuer(),
		c.Issuer().DID().String(),
		spaceblobcap.AddCaveats{Blob: captypes.Blob{Digest: testutil.RandomMultihash(t), Size: 100}},
	))(t)

	// Create an expired delegation
	expiredDel := testutil.Must(uploadcap.Get.Delegate(
		c.Issuer(),
		c.Issuer(),
		c.Issuer().DID().String(),
		uploadcap.GetCaveats{Root: testutil.RandomCID(t)},
		delegation.WithExpiration(ucan.Now()-100), // Expired 100 seconds ago
	))(t)

	// Create a delegation that's not yet valid
	futureDel := testutil.Must(uploadcap.Get.Delegate(
		c.Issuer(),
		c.Issuer(),
		c.Issuer().DID().String(),
		uploadcap.GetCaveats{Root: testutil.RandomCID(t)},
		delegation.WithNotBefore(ucan.Now()+100), // Valid 100 seconds from now
	))(t)

	err := c.AddProofs(uploadDel, blobDel, expiredDel, futureDel)
	require.NoError(t, err)

	t.Run("no query returns all non-expired, valid delegations", func(t *testing.T) {
		proofs, err := c.Proofs()
		require.NoError(t, err)
		require.ElementsMatch(t, []delegation.Delegation{uploadDel, blobDel}, proofs, "should return 2 non-expired delegations")
	})

	t.Run("query by specific ability", func(t *testing.T) {
		proofs, err := c.Proofs(agentstore.CapabilityQuery{
			Can:  "upload/add",
			With: c.Issuer().DID().String(),
		})
		require.NoError(t, err)
		require.ElementsMatch(t, []delegation.Delegation{uploadDel}, proofs, "should return 1 upload/add delegation")
	})

	t.Run("query by specific resource", func(t *testing.T) {
		proofs, err := c.Proofs(agentstore.CapabilityQuery{
			Can:  "upload/add",
			With: c.Issuer().DID().String(),
		})
		require.NoError(t, err)
		require.ElementsMatch(t, []delegation.Delegation{uploadDel}, proofs, "should return delegations matching the resource")
	})

	t.Run("multiple queries", func(t *testing.T) {
		proofs, err := c.Proofs(
			agentstore.CapabilityQuery{Can: "upload/add", With: c.Issuer().DID().String()},
			agentstore.CapabilityQuery{Can: "space/blob/add", With: c.Issuer().DID().String()},
		)
		require.NoError(t, err)
		require.ElementsMatch(t, []delegation.Delegation{uploadDel, blobDel}, proofs, "should return delegations matching either query")
	})

	t.Run("non-matching query", func(t *testing.T) {
		proofs, err := c.Proofs(agentstore.CapabilityQuery{
			Can:  "nonexistent/capability",
			With: "ucan:*",
		})
		require.NoError(t, err)
		require.Empty(t, proofs, "should return no delegations for non-matching query")
	})

	t.Run("excludes expired delegations", func(t *testing.T) {
		// Expired delegations should be excluded
		proofs, err := c.Proofs()
		require.NoError(t, err)
		require.NotContains(t, proofs, expiredDel, "should not include expired delegation")
	})

	t.Run("excludes future delegations", func(t *testing.T) {
		// Future delegations should be excluded
		proofs, err := c.Proofs()
		require.NoError(t, err)
		require.NotContains(t, proofs, futureDel, "should not include future delegation")
	})

	t.Run("session proofs", func(t *testing.T) {
		c := testutil.Must(client.NewClient())(t)

		// Create another principal that will issue the original authorization
		issuer := testutil.Must(signer.Generate())(t)

		// Create an authorization delegation from issuer to client
		authDel := testutil.Must(uploadcap.Add.Delegate(
			issuer,
			c.Issuer(),
			c.Issuer().DID().String(),
			uploadcap.AddCaveats{Root: testutil.RandomCID(t), Shards: nil},
		))(t)

		// Create a session proof (ucan/attest) that attests to the authorization
		sessionProof := testutil.Must(ucancap.Attest.Delegate(
			issuer,
			c.Issuer(),
			issuer.DID().String(),
			ucancap.AttestCaveats{Proof: authDel.Link()},
		))(t)

		// Add both to the client
		err := c.AddProofs(authDel, sessionProof)
		require.NoError(t, err)

		t.Run("includes session proofs with authorization", func(t *testing.T) {
			// Query for proofs - should get both the authorization and its session proof
			proofs, err := c.Proofs()
			require.NoError(t, err)
			require.ElementsMatch(t, []delegation.Delegation{authDel, sessionProof}, proofs,
				"should return both authorization and session proof")
		})

		t.Run("includes session proofs when querying by capability", func(t *testing.T) {
			// Query by specific capability - should get both the matching authorization and its session proof
			proofs, err := c.Proofs(agentstore.CapabilityQuery{
				Can:  "upload/add",
				With: c.Issuer().DID().String(),
			})
			require.NoError(t, err)
			require.ElementsMatch(t, []delegation.Delegation{authDel, sessionProof}, proofs,
				"should return authorization and its session proof when querying by capability")
		})

		t.Run("excludes expired session proofs", func(t *testing.T) {
			c := testutil.Must(client.NewClient())(t)

			// Create another principal that will issue the original authorization
			issuer := testutil.Must(signer.Generate())(t)

			// Create an authorization delegation
			authDel := testutil.Must(uploadcap.Add.Delegate(
				issuer,
				c.Issuer(),
				c.Issuer().DID().String(),
				uploadcap.AddCaveats{Root: testutil.RandomCID(t), Shards: nil},
			))(t)

			// Create an expired session proof
			expiredSessionProof := testutil.Must(ucancap.Attest.Delegate(
				issuer,
				c.Issuer(),
				issuer.DID().String(),
				ucancap.AttestCaveats{Proof: authDel.Link()},
				delegation.WithExpiration(ucan.Now()-100), // Expired
			))(t)

			err := c.AddProofs(authDel, expiredSessionProof)
			require.NoError(t, err)

			// Should only return the authorization, not the expired session proof
			proofs, err := c.Proofs()
			require.NoError(t, err)
			require.ElementsMatch(t, []delegation.Delegation{authDel}, proofs,
				"should exclude expired session proofs")
		})
	})

	t.Run("ability wildcard matching", func(t *testing.T) {
		c := testutil.Must(client.NewClient())(t)

		// Create delegations with specific and wildcard capabilities (all with ucan:* resource)
		specificCap := ucan.NewCapability("upload/add", "ucan:*", ucan.NoCaveats{})
		specificDel, err := delegation.Delegate(c.Issuer(), c.Issuer(), []ucan.Capability[ucan.NoCaveats]{specificCap})
		require.NoError(t, err)

		// Create a delegation with a namespace wildcard capability (upload/*)
		namespaceCap := ucan.NewCapability("upload/*", "ucan:*", ucan.NoCaveats{})
		namespaceDel, err := delegation.Delegate(c.Issuer(), c.Issuer(), []ucan.Capability[ucan.NoCaveats]{namespaceCap})
		require.NoError(t, err)

		// Create a delegation with a global wildcard capability (*)
		globalCap := ucan.NewCapability("*", "ucan:*", ucan.NoCaveats{})
		globalDel, err := delegation.Delegate(c.Issuer(), c.Issuer(), []ucan.Capability[ucan.NoCaveats]{globalCap})
		require.NoError(t, err)

		err = c.AddProofs(specificDel, namespaceDel, globalDel)
		require.NoError(t, err)

		t.Run("specific query matches exact, namespace wildcard, and global wildcard", func(t *testing.T) {
			// Searching for upload/add should find:
			// - upload/add (exact match)
			// - upload/* (namespace wildcard)
			// - * (global wildcard)
			proofs, err := c.Proofs(agentstore.CapabilityQuery{
				Can:  "upload/add",
				With: "ucan:*",
			})
			require.NoError(t, err)
			require.ElementsMatch(t, []delegation.Delegation{specificDel, namespaceDel, globalDel}, proofs,
				"should find exact match, namespace wildcard, and global wildcard")
		})

		t.Run("namespace wildcard query only matches namespace and global wildcards", func(t *testing.T) {
			// Searching for upload/* should find:
			// - upload/* (exact match)
			// - * (global wildcard)
			// NOT upload/add (too specific)
			proofs, err := c.Proofs(agentstore.CapabilityQuery{
				Can:  "upload/*",
				With: "ucan:*",
			})
			require.NoError(t, err)
			require.ElementsMatch(t, []delegation.Delegation{namespaceDel, globalDel}, proofs,
				"should find namespace and global wildcards, not specific abilities")
		})

		t.Run("global wildcard query only matches global wildcard capability", func(t *testing.T) {
			// Searching for * should only match delegations with * capability
			proofs, err := c.Proofs(agentstore.CapabilityQuery{
				Can:  "*",
				With: "ucan:*",
			})
			require.NoError(t, err)
			require.ElementsMatch(t, []delegation.Delegation{globalDel}, proofs,
				"should only match global wildcard capability when query is *")
		})
	})

	t.Run("resource wildcard matching", func(t *testing.T) {
		c := testutil.Must(client.NewClient())(t)
		space := testutil.Must(signer.Generate())(t)

		// Create a delegation with a specific resource (space DID)
		specificResourceDel := testutil.Must(uploadcap.Add.Delegate(
			c.Issuer(),
			c.Issuer(),
			space.DID().String(),
			uploadcap.AddCaveats{Root: testutil.RandomCID(t), Shards: nil},
		))(t)

		// Create a delegation with the resource wildcard (ucan:*)
		wildcardResourceCap := ucan.NewCapability("upload/add", "ucan:*", ucan.NoCaveats{})
		wildcardResourceDel, err := delegation.Delegate(c.Issuer(), c.Issuer(), []ucan.Capability[ucan.NoCaveats]{wildcardResourceCap})
		require.NoError(t, err)

		err = c.AddProofs(specificResourceDel, wildcardResourceDel)
		require.NoError(t, err)

		t.Run("specific resource query matches exact and wildcard resources", func(t *testing.T) {
			// Searching for a specific resource should find:
			// - delegations with that exact resource
			// - delegations with ucan:* (matches any resource)
			proofs, err := c.Proofs(agentstore.CapabilityQuery{
				Can:  "upload/add",
				With: space.DID().String(),
			})
			require.NoError(t, err)
			require.ElementsMatch(t, []delegation.Delegation{specificResourceDel, wildcardResourceDel}, proofs,
				"should match both exact resource and wildcard resource")
		})

		t.Run("wildcard resource query only matches wildcard resources", func(t *testing.T) {
			// Searching for ucan:* should only match delegations with ucan:*
			// NOT delegations with specific resources (they're too specific)
			proofs, err := c.Proofs(agentstore.CapabilityQuery{
				Can:  "upload/add",
				With: "ucan:*",
			})
			require.NoError(t, err)
			require.ElementsMatch(t, []delegation.Delegation{wildcardResourceDel}, proofs,
				"should only match wildcard resource when query is ucan:*")
		})
	})
}

func TestWithAdditionalProofs(t *testing.T) {
	t.Run("includes additional proofs in Proofs() results", func(t *testing.T) {
		// Create a store to verify what gets saved
		store := testutil.Must(agentstore.NewMemory())(t)

		// Create delegations
		s := testutil.Must(signer.Generate())(t)
		storedDel := testutil.Must(uploadcap.Add.Delegate(
			s,
			s,
			s.DID().String(),
			uploadcap.AddCaveats{Root: testutil.RandomCID(t), Shards: nil},
		))(t)

		additionalDel := testutil.Must(uploadcap.Get.Delegate(
			s,
			s,
			s.DID().String(),
			uploadcap.GetCaveats{Root: testutil.RandomCID(t)},
		))(t)

		// Create client with store and additional proofs
		c := testutil.Must(client.NewClient(
			client.WithStore(store),
			client.WithPrincipal(s),
			client.WithAdditionalProofs(additionalDel),
		))(t)

		// Add a proof to the store
		err := c.AddProofs(storedDel)
		require.NoError(t, err)

		// Verify that only the stored delegation was saved
		storedDelegations, err := store.Delegations()
		require.NoError(t, err)
		require.Equal(t, []delegation.Delegation{storedDel}, storedDelegations,
			"only stored delegation should be saved to storage")

		// Verify that Proofs() returns both stored and additional proofs
		proofs, err := c.Proofs()
		require.NoError(t, err)
		require.ElementsMatch(t, []delegation.Delegation{storedDel, additionalDel}, proofs,
			"Proofs() should return both stored and additional proofs")
	})

	t.Run("additional proofs not saved to storage", func(t *testing.T) {
		store := testutil.Must(agentstore.NewMemory())(t)

		s := testutil.Must(signer.Generate())(t)
		additionalDel := testutil.Must(uploadcap.Get.Delegate(
			s,
			s,
			s.DID().String(),
			uploadcap.GetCaveats{Root: testutil.RandomCID(t)},
		))(t)

		// Create client with additional proofs
		c := testutil.Must(client.NewClient(
			client.WithStore(store),
			client.WithPrincipal(s),
			client.WithAdditionalProofs(additionalDel),
		))(t)

		// Verify that additional proofs were not saved to storage
		storedDelegations, err := store.Delegations()
		require.NoError(t, err)
		require.Empty(t, storedDelegations,
			"additional proofs should not be saved to storage")

		// But they should be returned by Proofs()
		proofs, err := c.Proofs()
		require.NoError(t, err)
		require.ElementsMatch(t, []delegation.Delegation{additionalDel}, proofs,
			"additional proofs should be returned by Proofs()")
	})

	t.Run("additional proofs respect filtering", func(t *testing.T) {
		s := testutil.Must(signer.Generate())(t)

		uploadDel := testutil.Must(uploadcap.Add.Delegate(
			s,
			s,
			s.DID().String(),
			uploadcap.AddCaveats{Root: testutil.RandomCID(t), Shards: nil},
		))(t)

		blobDel := testutil.Must(spaceblobcap.Add.Delegate(
			s,
			s,
			s.DID().String(),
			spaceblobcap.AddCaveats{Blob: captypes.Blob{Digest: testutil.RandomMultihash(t), Size: 100}},
		))(t)

		// Create client with both delegations as additional proofs
		c := testutil.Must(client.NewClient(
			client.WithPrincipal(s),
			client.WithAdditionalProofs(uploadDel, blobDel),
		))(t)

		// Query for only upload capabilities
		proofs, err := c.Proofs(agentstore.CapabilityQuery{
			Can:  "upload/add",
			With: s.DID().String(),
		})
		require.NoError(t, err)
		require.ElementsMatch(t, []delegation.Delegation{uploadDel}, proofs,
			"should filter additional proofs by capability query")
	})

	t.Run("additional proofs exclude expired delegations", func(t *testing.T) {
		s := testutil.Must(signer.Generate())(t)

		validDel := testutil.Must(uploadcap.Add.Delegate(
			s,
			s,
			s.DID().String(),
			uploadcap.AddCaveats{Root: testutil.RandomCID(t), Shards: nil},
		))(t)

		expiredDel := testutil.Must(uploadcap.Get.Delegate(
			s,
			s,
			s.DID().String(),
			uploadcap.GetCaveats{Root: testutil.RandomCID(t)},
			delegation.WithExpiration(ucan.Now()-100), // Expired 100 seconds ago
		))(t)

		// Create client with both delegations as additional proofs
		c := testutil.Must(client.NewClient(
			client.WithPrincipal(s),
			client.WithAdditionalProofs(validDel, expiredDel),
		))(t)

		// Only the valid delegation should be returned
		proofs, err := c.Proofs()
		require.NoError(t, err)
		require.ElementsMatch(t, []delegation.Delegation{validDel}, proofs,
			"should exclude expired additional proofs")
	})

	t.Run("Reset does not affect additional proofs", func(t *testing.T) {
		store := testutil.Must(agentstore.NewMemory())(t)

		s := testutil.Must(signer.Generate())(t)

		storedDel := testutil.Must(uploadcap.Add.Delegate(
			s,
			s,
			s.DID().String(),
			uploadcap.AddCaveats{Root: testutil.RandomCID(t), Shards: nil},
		))(t)

		additionalDel := testutil.Must(uploadcap.Get.Delegate(
			s,
			s,
			s.DID().String(),
			uploadcap.GetCaveats{Root: testutil.RandomCID(t)},
		))(t)

		// Create client with additional proofs
		c := testutil.Must(client.NewClient(
			client.WithStore(store),
			client.WithPrincipal(s),
			client.WithAdditionalProofs(additionalDel),
		))(t)

		// Add a stored proof
		err := c.AddProofs(storedDel)
		require.NoError(t, err)

		// Verify both are returned
		proofs, err := c.Proofs()
		require.NoError(t, err)
		require.ElementsMatch(t, []delegation.Delegation{storedDel, additionalDel}, proofs)

		// Reset the client
		err = c.Reset()
		require.NoError(t, err)

		// Additional proofs should still be there, but stored proof should be gone
		proofs, err = c.Proofs()
		require.NoError(t, err)
		require.ElementsMatch(t, []delegation.Delegation{additionalDel}, proofs,
			"additional proofs should remain after reset, but stored proofs should be cleared")

		// Verify storage was cleared
		storedDelegations, err := store.Delegations()
		require.NoError(t, err)
		require.Empty(t, storedDelegations,
			"stored delegations should be cleared after reset")
	})
}
