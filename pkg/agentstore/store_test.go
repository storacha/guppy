package agentstore

import (
	"testing"

	"github.com/ipld/go-ipld-prime/datamodel"
	spaceblobcap "github.com/storacha/go-libstoracha/capabilities/space/blob"
	captypes "github.com/storacha/go-libstoracha/capabilities/types"
	ucancap "github.com/storacha/go-libstoracha/capabilities/ucan"
	uploadcap "github.com/storacha/go-libstoracha/capabilities/upload"
	"github.com/storacha/go-libstoracha/testutil"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/stretchr/testify/require"
	"github.com/ipfs/go-cid"
)

// delegationLinks extracts the CID links from a slice of delegations for comparison.
// This is necessary because delegations may not compare equal after serialization/deserialization.
func delegationLinks(dels []delegation.Delegation) []datamodel.Link {
	links := make([]datamodel.Link, len(dels))
	for i, d := range dels {
		links[i] = d.Link()
	}
	return links
}

func TestStore(t *testing.T) {
	stores := map[string]func(t *testing.T) Store{
		"MemStore": func(t *testing.T) Store {
			s, err := NewMemory()
			require.NoError(t, err)
			return s
		},
		"FsStore": func(t *testing.T) Store {
			s, err := NewFs(t.TempDir())
			require.NoError(t, err)
			return s
		},
	}

	for name, newStore := range stores {
		t.Run(name, func(t *testing.T) {
			t.Run("Principal", func(t *testing.T) { testPrincipal(t, newStore) })
			t.Run("Delegations", func(t *testing.T) { testDelegations(t, newStore) })
			t.Run("Query", func(t *testing.T) { testQuery(t, newStore) })
			t.Run("Remove", func(t *testing.T) { testRemove(t, newStore) })
			t.Run("Reset", func(t *testing.T) { testReset(t, newStore) })
		})
	}
}

func testPrincipal(t *testing.T, newStore func(t *testing.T) Store) {
	t.Run("has principal after creation", func(t *testing.T) {
		s := newStore(t)
		has, err := s.HasPrincipal()
		require.NoError(t, err)
		require.True(t, has, "store should have a principal after creation")
	})

	t.Run("get principal", func(t *testing.T) {
		s := newStore(t)
		p, err := s.Principal()
		require.NoError(t, err)
		require.NotNil(t, p, "principal should not be nil")
		require.NotEmpty(t, p.DID().String(), "principal should have a DID")
	})

	t.Run("set principal", func(t *testing.T) {
		s := newStore(t)
		newPrincipal := testutil.Must(signer.Generate())(t)

		err := s.SetPrincipal(newPrincipal)
		require.NoError(t, err)

		p, err := s.Principal()
		require.NoError(t, err)
		require.Equal(t, newPrincipal.DID(), p.DID(), "principal should be updated")
	})
}

func testDelegations(t *testing.T, newStore func(t *testing.T) Store) {
	t.Run("empty delegations initially", func(t *testing.T) {
		s := newStore(t)
		delegs, err := s.Delegations()
		require.NoError(t, err)
		require.Empty(t, delegs, "delegations should be empty initially")
	})

	t.Run("add delegations", func(t *testing.T) {
		s := newStore(t)
		p, err := s.Principal()
		require.NoError(t, err)

		del := testutil.Must(uploadcap.Add.Delegate(
			p,
			p,
			p.DID().String(),
			uploadcap.AddCaveats{Root: testutil.RandomCID(t), Shards: nil},
		))(t)

		err = s.AddDelegations(del)
		require.NoError(t, err)

		delegs, err := s.Delegations()
		require.NoError(t, err)
		require.Len(t, delegs, 1, "should have one delegation")
		require.Equal(t, del.Link(), delegs[0].Link(), "delegation should match")
	})

	t.Run("add multiple delegations", func(t *testing.T) {
		s := newStore(t)
		p, err := s.Principal()
		require.NoError(t, err)

		del1 := testutil.Must(uploadcap.Add.Delegate(
			p, p, p.DID().String(),
			uploadcap.AddCaveats{Root: testutil.RandomCID(t), Shards: nil},
		))(t)

		del2 := testutil.Must(uploadcap.Get.Delegate(
			p, p, p.DID().String(),
			uploadcap.GetCaveats{Root: testutil.RandomCID(t)},
		))(t)

		err = s.AddDelegations(del1, del2)
		require.NoError(t, err)

		delegs, err := s.Delegations()
		require.NoError(t, err)
		require.Len(t, delegs, 2, "should have two delegations")
	})
}

func testQuery(t *testing.T, newStore func(t *testing.T) Store) {
	t.Run("no query returns all non-expired delegations", func(t *testing.T) {
		s := newStore(t)
		p, err := s.Principal()
		require.NoError(t, err)

		uploadDel := testutil.Must(uploadcap.Add.Delegate(
			p, p, p.DID().String(),
			uploadcap.AddCaveats{Root: testutil.RandomCID(t), Shards: nil},
		))(t)

		blobDel := testutil.Must(spaceblobcap.Add.Delegate(
			p, p, p.DID().String(),
			spaceblobcap.AddCaveats{Blob: captypes.Blob{Digest: testutil.RandomMultihash(t), Size: 100}},
		))(t)

		err = s.AddDelegations(uploadDel, blobDel)
		require.NoError(t, err)

		proofs, err := s.Query()
		require.NoError(t, err)
		require.ElementsMatch(t, delegationLinks([]delegation.Delegation{uploadDel, blobDel}), delegationLinks(proofs))
	})

	t.Run("query by specific ability", func(t *testing.T) {
		s := newStore(t)
		p, err := s.Principal()
		require.NoError(t, err)

		uploadDel := testutil.Must(uploadcap.Add.Delegate(
			p, p, p.DID().String(),
			uploadcap.AddCaveats{Root: testutil.RandomCID(t), Shards: nil},
		))(t)

		blobDel := testutil.Must(spaceblobcap.Add.Delegate(
			p, p, p.DID().String(),
			spaceblobcap.AddCaveats{Blob: captypes.Blob{Digest: testutil.RandomMultihash(t), Size: 100}},
		))(t)

		err = s.AddDelegations(uploadDel, blobDel)
		require.NoError(t, err)

		proofs, err := s.Query(CapabilityQuery{
			Can:  "upload/add",
			With: p.DID().String(),
		})
		require.NoError(t, err)
		require.ElementsMatch(t, delegationLinks([]delegation.Delegation{uploadDel}), delegationLinks(proofs))
	})

	t.Run("query with empty resource matches any resource", func(t *testing.T) {
		s := newStore(t)
		p, err := s.Principal()
		require.NoError(t, err)

		del := testutil.Must(uploadcap.Add.Delegate(
			p, p, p.DID().String(),
			uploadcap.AddCaveats{Root: testutil.RandomCID(t), Shards: nil},
		))(t)

		err = s.AddDelegations(del)
		require.NoError(t, err)

		proofs, err := s.Query(CapabilityQuery{
			Can:  "upload/add",
			With: "", // Empty should match any
		})
		require.NoError(t, err)
		require.ElementsMatch(t, delegationLinks([]delegation.Delegation{del}), delegationLinks(proofs))
	})

	t.Run("multiple queries", func(t *testing.T) {
		s := newStore(t)
		p, err := s.Principal()
		require.NoError(t, err)

		uploadDel := testutil.Must(uploadcap.Add.Delegate(
			p, p, p.DID().String(),
			uploadcap.AddCaveats{Root: testutil.RandomCID(t), Shards: nil},
		))(t)

		blobDel := testutil.Must(spaceblobcap.Add.Delegate(
			p, p, p.DID().String(),
			spaceblobcap.AddCaveats{Blob: captypes.Blob{Digest: testutil.RandomMultihash(t), Size: 100}},
		))(t)

		err = s.AddDelegations(uploadDel, blobDel)
		require.NoError(t, err)

		proofs, err := s.Query(
			CapabilityQuery{Can: "upload/add", With: p.DID().String()},
			CapabilityQuery{Can: "space/blob/add", With: p.DID().String()},
		)
		require.NoError(t, err)
		require.ElementsMatch(t, delegationLinks([]delegation.Delegation{uploadDel, blobDel}), delegationLinks(proofs))
	})

	t.Run("non-matching query", func(t *testing.T) {
		s := newStore(t)
		p, err := s.Principal()
		require.NoError(t, err)

		del := testutil.Must(uploadcap.Add.Delegate(
			p, p, p.DID().String(),
			uploadcap.AddCaveats{Root: testutil.RandomCID(t), Shards: nil},
		))(t)

		err = s.AddDelegations(del)
		require.NoError(t, err)

		proofs, err := s.Query(CapabilityQuery{
			Can:  "nonexistent/capability",
			With: "ucan:*",
		})
		require.NoError(t, err)
		require.Empty(t, proofs)
	})

	t.Run("excludes expired delegations", func(t *testing.T) {
		s := newStore(t)
		p, err := s.Principal()
		require.NoError(t, err)

		validDel := testutil.Must(uploadcap.Add.Delegate(
			p, p, p.DID().String(),
			uploadcap.AddCaveats{Root: testutil.RandomCID(t), Shards: nil},
		))(t)

		expiredDel := testutil.Must(uploadcap.Get.Delegate(
			p, p, p.DID().String(),
			uploadcap.GetCaveats{Root: testutil.RandomCID(t)},
			delegation.WithExpiration(ucan.Now()-100), // Expired 100 seconds ago
		))(t)

		err = s.AddDelegations(validDel, expiredDel)
		require.NoError(t, err)

		proofs, err := s.Query()
		require.NoError(t, err)
		require.ElementsMatch(t, delegationLinks([]delegation.Delegation{validDel}), delegationLinks(proofs))
	})

	t.Run("excludes future delegations", func(t *testing.T) {
		s := newStore(t)
		p, err := s.Principal()
		require.NoError(t, err)

		validDel := testutil.Must(uploadcap.Add.Delegate(
			p, p, p.DID().String(),
			uploadcap.AddCaveats{Root: testutil.RandomCID(t), Shards: nil},
		))(t)

		futureDel := testutil.Must(uploadcap.Get.Delegate(
			p, p, p.DID().String(),
			uploadcap.GetCaveats{Root: testutil.RandomCID(t)},
			delegation.WithNotBefore(ucan.Now()+100), // Valid 100 seconds from now
		))(t)

		err = s.AddDelegations(validDel, futureDel)
		require.NoError(t, err)

		proofs, err := s.Query()
		require.NoError(t, err)
		require.ElementsMatch(t, delegationLinks([]delegation.Delegation{validDel}), delegationLinks(proofs))
	})

	t.Run("session proofs", func(t *testing.T) {
		t.Run("includes session proofs with authorization", func(t *testing.T) {
			s := newStore(t)
			p, err := s.Principal()
			require.NoError(t, err)

			issuer := testutil.Must(signer.Generate())(t)

			authDel := testutil.Must(uploadcap.Add.Delegate(
				issuer, p, p.DID().String(),
				uploadcap.AddCaveats{Root: testutil.RandomCID(t), Shards: nil},
			))(t)

			sessionProof := testutil.Must(ucancap.Attest.Delegate(
				issuer, p, issuer.DID().String(),
				ucancap.AttestCaveats{Proof: authDel.Link()},
			))(t)

			err = s.AddDelegations(authDel, sessionProof)
			require.NoError(t, err)

			proofs, err := s.Query()
			require.NoError(t, err)
			require.ElementsMatch(t, delegationLinks([]delegation.Delegation{authDel, sessionProof}), delegationLinks(proofs))
		})

		t.Run("includes session proofs when querying by capability", func(t *testing.T) {
			s := newStore(t)
			p, err := s.Principal()
			require.NoError(t, err)

			issuer := testutil.Must(signer.Generate())(t)

			authDel := testutil.Must(uploadcap.Add.Delegate(
				issuer, p, p.DID().String(),
				uploadcap.AddCaveats{Root: testutil.RandomCID(t), Shards: nil},
			))(t)

			sessionProof := testutil.Must(ucancap.Attest.Delegate(
				issuer, p, issuer.DID().String(),
				ucancap.AttestCaveats{Proof: authDel.Link()},
			))(t)

			err = s.AddDelegations(authDel, sessionProof)
			require.NoError(t, err)

			proofs, err := s.Query(CapabilityQuery{
				Can:  "upload/add",
				With: p.DID().String(),
			})
			require.NoError(t, err)
			require.ElementsMatch(t, delegationLinks([]delegation.Delegation{authDel, sessionProof}), delegationLinks(proofs))
		})

		t.Run("excludes expired session proofs", func(t *testing.T) {
			s := newStore(t)
			p, err := s.Principal()
			require.NoError(t, err)

			issuer := testutil.Must(signer.Generate())(t)

			authDel := testutil.Must(uploadcap.Add.Delegate(
				issuer, p, p.DID().String(),
				uploadcap.AddCaveats{Root: testutil.RandomCID(t), Shards: nil},
			))(t)

			expiredSessionProof := testutil.Must(ucancap.Attest.Delegate(
				issuer, p, issuer.DID().String(),
				ucancap.AttestCaveats{Proof: authDel.Link()},
				delegation.WithExpiration(ucan.Now()-100), // Expired
			))(t)

			err = s.AddDelegations(authDel, expiredSessionProof)
			require.NoError(t, err)

			proofs, err := s.Query()
			require.NoError(t, err)
			require.ElementsMatch(t, delegationLinks([]delegation.Delegation{authDel}), delegationLinks(proofs))
		})
	})

	t.Run("ability wildcard matching", func(t *testing.T) {
		s := newStore(t)
		p, err := s.Principal()
		require.NoError(t, err)

		specificCap := ucan.NewCapability("upload/add", "ucan:*", ucan.NoCaveats{})
		specificDel, err := delegation.Delegate(p, p, []ucan.Capability[ucan.NoCaveats]{specificCap})
		require.NoError(t, err)

		namespaceCap := ucan.NewCapability("upload/*", "ucan:*", ucan.NoCaveats{})
		namespaceDel, err := delegation.Delegate(p, p, []ucan.Capability[ucan.NoCaveats]{namespaceCap})
		require.NoError(t, err)

		globalCap := ucan.NewCapability("*", "ucan:*", ucan.NoCaveats{})
		globalDel, err := delegation.Delegate(p, p, []ucan.Capability[ucan.NoCaveats]{globalCap})
		require.NoError(t, err)

		err = s.AddDelegations(specificDel, namespaceDel, globalDel)
		require.NoError(t, err)

		t.Run("specific query matches exact, namespace wildcard, and global wildcard", func(t *testing.T) {
			proofs, err := s.Query(CapabilityQuery{
				Can:  "upload/add",
				With: "ucan:*",
			})
			require.NoError(t, err)
			require.ElementsMatch(t, delegationLinks([]delegation.Delegation{specificDel, namespaceDel, globalDel}), delegationLinks(proofs))
		})

		t.Run("namespace wildcard query only matches namespace and global wildcards", func(t *testing.T) {
			proofs, err := s.Query(CapabilityQuery{
				Can:  "upload/*",
				With: "ucan:*",
			})
			require.NoError(t, err)
			require.ElementsMatch(t, delegationLinks([]delegation.Delegation{namespaceDel, globalDel}), delegationLinks(proofs))
		})

		t.Run("global wildcard query only matches global wildcard capability", func(t *testing.T) {
			proofs, err := s.Query(CapabilityQuery{
				Can:  "*",
				With: "ucan:*",
			})
			require.NoError(t, err)
			require.ElementsMatch(t, delegationLinks([]delegation.Delegation{globalDel}), delegationLinks(proofs))
		})
	})

	t.Run("resource wildcard matching", func(t *testing.T) {
		s := newStore(t)
		p, err := s.Principal()
		require.NoError(t, err)
		space := testutil.Must(signer.Generate())(t)

		specificResourceDel := testutil.Must(uploadcap.Add.Delegate(
			p, p, space.DID().String(),
			uploadcap.AddCaveats{Root: testutil.RandomCID(t), Shards: nil},
		))(t)

		wildcardResourceCap := ucan.NewCapability("upload/add", "ucan:*", ucan.NoCaveats{})
		wildcardResourceDel, err := delegation.Delegate(p, p, []ucan.Capability[ucan.NoCaveats]{wildcardResourceCap})
		require.NoError(t, err)

		err = s.AddDelegations(specificResourceDel, wildcardResourceDel)
		require.NoError(t, err)

		t.Run("specific resource query matches exact and wildcard resources", func(t *testing.T) {
			proofs, err := s.Query(CapabilityQuery{
				Can:  "upload/add",
				With: space.DID().String(),
			})
			require.NoError(t, err)
			require.ElementsMatch(t, delegationLinks([]delegation.Delegation{specificResourceDel, wildcardResourceDel}), delegationLinks(proofs))
		})

		t.Run("wildcard resource query only matches wildcard resources", func(t *testing.T) {
			proofs, err := s.Query(CapabilityQuery{
				Can:  "upload/add",
				With: "ucan:*",
			})
			require.NoError(t, err)
			require.ElementsMatch(t, delegationLinks([]delegation.Delegation{wildcardResourceDel}), delegationLinks(proofs))
		})
	})
}

func testRemove(t *testing.T, newStore func(t *testing.T) Store) {
	t.Run("removes a specific delegation", func(t *testing.T) {
		s := newStore(t)
		p, err := s.Principal()
		require.NoError(t, err)

		del1 := testutil.Must(uploadcap.Add.Delegate(
			p, p, p.DID().String(),
			uploadcap.AddCaveats{Root: testutil.RandomCID(t), Shards: nil},
		))(t)

		del2 := testutil.Must(uploadcap.Get.Delegate(
			p, p, p.DID().String(),
			uploadcap.GetCaveats{Root: testutil.RandomCID(t)},
		))(t)

		err = s.AddDelegations(del1, del2)
		require.NoError(t, err)

		targetCid, err := cid.Parse(del1.Link().String())
		require.NoError(t, err)
		err = s.RemoveDelegation(targetCid)
		require.NoError(t, err)

		delegs, err := s.Delegations()
		require.NoError(t, err)
		require.Len(t, delegs, 1, "should have exactly one delegation remaining")
		require.Equal(t, del2.Link().String(), delegs[0].Link().String(), "the correct delegation should remain")
	})
}

func testReset(t *testing.T, newStore func(t *testing.T) Store) {
	t.Run("clears delegations but preserves principal", func(t *testing.T) {
		s := newStore(t)
		p, err := s.Principal()
		require.NoError(t, err)

		del := testutil.Must(uploadcap.Add.Delegate(
			p, p, p.DID().String(),
			uploadcap.AddCaveats{Root: testutil.RandomCID(t), Shards: nil},
		))(t)

		err = s.AddDelegations(del)
		require.NoError(t, err)

		delegs, err := s.Delegations()
		require.NoError(t, err)
		require.Len(t, delegs, 1)

		err = s.Reset()
		require.NoError(t, err)

		delegs, err = s.Delegations()
		require.NoError(t, err)
		require.Empty(t, delegs, "delegations should be empty after reset")

		newP, err := s.Principal()
		require.NoError(t, err)
		require.Equal(t, p.DID(), newP.DID(), "principal should be preserved after reset")
	})
}
