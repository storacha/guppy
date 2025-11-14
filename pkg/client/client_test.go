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
	"github.com/storacha/guppy/pkg/agentdata"
	"github.com/storacha/guppy/pkg/client"
	"github.com/stretchr/testify/require"
)

func TestReset(t *testing.T) {
	var savedData agentdata.AgentData
	c := testutil.Must(client.NewClient(client.WithSaveFn(func(data agentdata.AgentData) error {
		savedData = data
		return nil
	})))(t)
	require.Empty(t, c.Proofs(), "expected no proofs to be present initially")

	issuer := c.Issuer()

	// Some arbitrary delegation
	del := testutil.Must(uploadcap.Get.Delegate(
		c.Issuer(),
		c.Issuer(),
		c.Issuer().DID().String(),
		uploadcap.GetCaveats{Root: testutil.RandomCID(t)},
	))(t)

	err := c.AddProofs(del)
	require.NoError(t, err)
	require.Equal(t, []delegation.Delegation{del}, c.Proofs(), "expected one proof to be added")

	// Clear `savedData` so we prove that the saved data is set during the reset.
	savedData = agentdata.AgentData{}

	err = c.Reset()
	require.NoError(t, err, "expected reset to succeed")
	require.Empty(t, c.Proofs(), "expected all proofs to be removed after reset")
	require.Equal(t, c.DID(), issuer.DID(), "expected issuer to remain unchanged after reset")

	require.Equal(t, savedData.Principal, issuer, "expected saved principal to be the issuer")
	require.Empty(t, savedData.Delegations, "expected saved proofs to be empty")
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
		proofs := c.Proofs()
		require.ElementsMatch(t, []delegation.Delegation{uploadDel, blobDel}, proofs, "should return 2 non-expired delegations")
	})

	t.Run("query by specific ability", func(t *testing.T) {
		proofs := c.Proofs(client.CapabilityQuery{
			Can:  "upload/add",
			With: "ucan:*",
		})
		require.ElementsMatch(t, []delegation.Delegation{uploadDel}, proofs, "should return 1 upload/add delegation")
	})

	t.Run("query by specific resource", func(t *testing.T) {
		proofs := c.Proofs(client.CapabilityQuery{
			Can:  "upload/add",
			With: c.Issuer().DID().String(),
		})
		require.ElementsMatch(t, []delegation.Delegation{uploadDel}, proofs, "should return delegations matching the resource")
	})

	t.Run("multiple queries", func(t *testing.T) {
		proofs := c.Proofs(
			client.CapabilityQuery{Can: "upload/add", With: "ucan:*"},
			client.CapabilityQuery{Can: "space/blob/add", With: "ucan:*"},
		)
		require.ElementsMatch(t, []delegation.Delegation{uploadDel, blobDel}, proofs, "should return delegations matching either query")
	})

	t.Run("non-matching query", func(t *testing.T) {
		proofs := c.Proofs(client.CapabilityQuery{
			Can:  "nonexistent/capability",
			With: "ucan:*",
		})
		require.Empty(t, proofs, "should return no delegations for non-matching query")
	})

	t.Run("excludes expired delegations", func(t *testing.T) {
		// Expired delegations should be excluded
		proofs := c.Proofs()
		require.NotContains(t, proofs, expiredDel, "should not include expired delegation")
	})

	t.Run("excludes future delegations", func(t *testing.T) {
		// Future delegations should be excluded
		proofs := c.Proofs()
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
			proofs := c.Proofs()
			require.ElementsMatch(t, []delegation.Delegation{authDel, sessionProof}, proofs,
				"should return both authorization and session proof")
		})

		t.Run("includes session proofs when querying by capability", func(t *testing.T) {
			// Query by specific capability - should get both the matching authorization and its session proof
			proofs := c.Proofs(client.CapabilityQuery{
				Can:  "upload/add",
				With: "ucan:*",
			})
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
			proofs := c.Proofs()
			require.ElementsMatch(t, []delegation.Delegation{authDel}, proofs,
				"should exclude expired session proofs")
		})
	})

	t.Run("wildcard matching", func(t *testing.T) {
		c := testutil.Must(client.NewClient())(t)

		// Create delegations with specific and wildcard capabilities
		specificDel := testutil.Must(uploadcap.Add.Delegate(
			c.Issuer(),
			c.Issuer(),
			c.Issuer().DID().String(),
			uploadcap.AddCaveats{Root: testutil.RandomCID(t), Shards: nil},
		))(t)

		// Create a delegation with a namespace wildcard capability (upload/*)
		namespaceCap := ucan.NewCapability("upload/*", c.Issuer().DID().String(), ucan.NoCaveats{})
		namespaceDel, err := delegation.Delegate(c.Issuer(), c.Issuer(), []ucan.Capability[ucan.NoCaveats]{namespaceCap})
		require.NoError(t, err)

		// Create a delegation with a global wildcard capability (*)
		globalCap := ucan.NewCapability("*", c.Issuer().DID().String(), ucan.NoCaveats{})
		globalDel, err := delegation.Delegate(c.Issuer(), c.Issuer(), []ucan.Capability[ucan.NoCaveats]{globalCap})
		require.NoError(t, err)

		err = c.AddProofs(specificDel, namespaceDel, globalDel)
		require.NoError(t, err)

		t.Run("specific query matches exact, namespace wildcard, and global wildcard", func(t *testing.T) {
			// Searching for upload/add should find:
			// - upload/add (exact match)
			// - upload/* (namespace wildcard)
			// - * (global wildcard)
			proofs := c.Proofs(client.CapabilityQuery{
				Can:  "upload/add",
				With: "ucan:*",
			})
			require.ElementsMatch(t, []delegation.Delegation{specificDel, namespaceDel, globalDel}, proofs,
				"should find exact match, namespace wildcard, and global wildcard")
		})

		t.Run("namespace wildcard query only matches namespace and global wildcards", func(t *testing.T) {
			// Searching for upload/* should find:
			// - upload/* (exact match)
			// - * (global wildcard)
			// NOT upload/add (too specific)
			proofs := c.Proofs(client.CapabilityQuery{
				Can:  "upload/*",
				With: "ucan:*",
			})
			require.ElementsMatch(t, []delegation.Delegation{namespaceDel, globalDel}, proofs,
				"should find namespace and global wildcards, not specific abilities")
		})

		t.Run("global wildcard query only matches global wildcard capability", func(t *testing.T) {
			// Searching for * should only match delegations with * capability
			proofs := c.Proofs(client.CapabilityQuery{
				Can:  "*",
				With: "ucan:*",
			})
			require.ElementsMatch(t, []delegation.Delegation{globalDel}, proofs,
				"should only match global wildcard capability when query is *")
		})
	})
}
