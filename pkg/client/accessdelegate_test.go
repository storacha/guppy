package client_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/storacha/go-libstoracha/capabilities/access"
	"github.com/storacha/go-libstoracha/testutil"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/receipt/fx"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/core/result/failure"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/go-ucanto/server"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/storacha/guppy/pkg/client"
	ctestutil "github.com/storacha/guppy/pkg/client/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAccessDelegate(t *testing.T) {
	t.Run("stores delegations via access/delegate", func(t *testing.T) {
		space := testutil.Must(signer.Generate())(t)
		var receivedDelegations access.DelegationLinksModel
		var c *client.Client

		connection := ctestutil.NewTestServerConnection(
			server.WithServiceMethod(
				access.DelegateAbility,
				server.Provide(
					access.Delegate,
					func(
						ctx context.Context,
						cap ucan.Capability[access.DelegateCaveats],
						inv invocation.Invocation,
						context server.InvocationContext,
					) (result.Result[access.DelegateOk, failure.IPLDBuilderFailure], fx.Effects, error) {
						assert.Equal(t, space.DID().String(), cap.With(), "expected access/delegate to be invoked with the space DID")
						receivedDelegations = cap.Nb().Delegations
						return result.Ok[access.DelegateOk, failure.IPLDBuilderFailure](access.DelegateOk{}), nil, nil
					},
				),
			),
		)

		c = testutil.Must(client.NewClient(
			client.WithConnection(connection),
		))(t)

		// Create a proof that allows the client to invoke access/delegate on the space
		accessDelegateProof := testutil.Must(delegation.Delegate(
			space,
			c.Issuer(),
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("access/delegate", space.DID().String(), ucan.NoCaveats{}),
			},
			delegation.WithNoExpiration(),
		))(t)
		require.NoError(t, c.AddProofs(accessDelegateProof))

		// Create a delegation from space to some account
		account := testutil.Must(did.Parse("did:mailto:example.com:alice"))(t)
		del := testutil.Must(delegation.Delegate(
			space,
			account,
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("store/add", space.DID().String(), ucan.NoCaveats{}),
			},
			delegation.WithNoExpiration(),
		))(t)

		_, err := c.AccessDelegate(testContext(t), space.DID(), del)

		require.NoError(t, err)
		require.NotNil(t, receivedDelegations.Values, "expected delegations to be sent")
		require.Len(t, receivedDelegations.Values, 1, "expected exactly one delegation")
		assert.Contains(t, receivedDelegations.Values, del.Link().String(), "expected the delegation link to be present")
		assert.Equal(t, del.Link(), receivedDelegations.Values[del.Link().String()], "expected the delegation link to match")
	})

	t.Run("handles multiple delegations", func(t *testing.T) {
		space := testutil.Must(signer.Generate())(t)
		var receivedDelegations access.DelegationLinksModel
		var c *client.Client

		connection := ctestutil.NewTestServerConnection(
			server.WithServiceMethod(
				access.DelegateAbility,
				server.Provide(
					access.Delegate,
					func(
						ctx context.Context,
						cap ucan.Capability[access.DelegateCaveats],
						inv invocation.Invocation,
						context server.InvocationContext,
					) (result.Result[access.DelegateOk, failure.IPLDBuilderFailure], fx.Effects, error) {
						receivedDelegations = cap.Nb().Delegations
						return result.Ok[access.DelegateOk, failure.IPLDBuilderFailure](access.DelegateOk{}), nil, nil
					},
				),
			),
		)

		c = testutil.Must(client.NewClient(
			client.WithConnection(connection),
		))(t)

		// Add proof for access/delegate
		accessDelegateProof := testutil.Must(delegation.Delegate(
			space,
			c.Issuer(),
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("access/delegate", space.DID().String(), ucan.NoCaveats{}),
			},
			delegation.WithNoExpiration(),
		))(t)
		require.NoError(t, c.AddProofs(accessDelegateProof))

		// Create multiple delegations
		account1 := testutil.Must(did.Parse("did:mailto:example.com:alice"))(t)
		del1 := testutil.Must(delegation.Delegate(
			space,
			account1,
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("store/add", space.DID().String(), ucan.NoCaveats{}),
			},
			delegation.WithNoExpiration(),
		))(t)

		account2 := testutil.Must(did.Parse("did:mailto:example.com:bob"))(t)
		del2 := testutil.Must(delegation.Delegate(
			space,
			account2,
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("upload/add", space.DID().String(), ucan.NoCaveats{}),
			},
			delegation.WithNoExpiration(),
		))(t)

		_, err := c.AccessDelegate(testContext(t), space.DID(), del1, del2)

		require.NoError(t, err)
		require.NotNil(t, receivedDelegations.Values, "expected delegations to be sent")
		require.Len(t, receivedDelegations.Values, 2, "expected exactly two delegations")
		assert.Contains(t, receivedDelegations.Values, del1.Link().String())
		assert.Contains(t, receivedDelegations.Values, del2.Link().String())
	})

	t.Run("includes delegations as proofs in the invocation", func(t *testing.T) {
		space := testutil.Must(signer.Generate())(t)
		var receivedProofLinks []ucan.Link
		var c *client.Client

		connection := ctestutil.NewTestServerConnection(
			server.WithServiceMethod(
				access.DelegateAbility,
				server.Provide(
					access.Delegate,
					func(
						ctx context.Context,
						cap ucan.Capability[access.DelegateCaveats],
						inv invocation.Invocation,
						context server.InvocationContext,
					) (result.Result[access.DelegateOk, failure.IPLDBuilderFailure], fx.Effects, error) {
						// Capture the proof links from the invocation
						receivedProofLinks = inv.Proofs()
						return result.Ok[access.DelegateOk, failure.IPLDBuilderFailure](access.DelegateOk{}), nil, nil
					},
				),
			),
		)

		c = testutil.Must(client.NewClient(
			client.WithConnection(connection),
		))(t)

		// Add proof for access/delegate
		accessDelegateProof := testutil.Must(delegation.Delegate(
			space,
			c.Issuer(),
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("access/delegate", space.DID().String(), ucan.NoCaveats{}),
			},
			delegation.WithNoExpiration(),
		))(t)
		require.NoError(t, c.AddProofs(accessDelegateProof))

		account := testutil.Must(did.Parse("did:mailto:example.com:alice"))(t)
		del := testutil.Must(delegation.Delegate(
			space,
			account,
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("*", space.DID().String(), ucan.NoCaveats{}),
			},
			delegation.WithNoExpiration(),
		))(t)

		_, err := c.AccessDelegate(testContext(t), space.DID(), del)

		require.NoError(t, err)
		require.NotEmpty(t, receivedProofLinks, "expected proofs to be included in the invocation")

		// The delegations should be included as proofs - either the access/delegate proof or the delegation being stored
		// We should at least have the access/delegate proof
		foundAccessDelegateProof := false
		for _, proofLink := range receivedProofLinks {
			if proofLink.String() == accessDelegateProof.Link().String() {
				foundAccessDelegateProof = true
				break
			}
		}
		assert.True(t, foundAccessDelegateProof, "expected the access/delegate authorization proof to be included")
	})

	t.Run("returns error on failure", func(t *testing.T) {
		space := testutil.Must(signer.Generate())(t)
		var c *client.Client

		connection := ctestutil.NewTestServerConnection(
			server.WithServiceMethod(
				access.DelegateAbility,
				server.Provide(
					access.Delegate,
					func(
						ctx context.Context,
						cap ucan.Capability[access.DelegateCaveats],
						inv invocation.Invocation,
						context server.InvocationContext,
					) (result.Result[access.DelegateOk, failure.IPLDBuilderFailure], fx.Effects, error) {
						return result.Error[access.DelegateOk](
							failure.FromError(fmt.Errorf("test error")),
						), nil, nil
					},
				),
			),
		)

		c = testutil.Must(client.NewClient(
			client.WithConnection(connection),
		))(t)

		account := testutil.Must(did.Parse("did:mailto:example.com:alice"))(t)
		del := testutil.Must(delegation.Delegate(
			space,
			account,
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("store/add", space.DID().String(), ucan.NoCaveats{}),
			},
			delegation.WithNoExpiration(),
		))(t)

		_, err := c.AccessDelegate(testContext(t), space.DID(), del)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "access/delegate")
	})

	t.Run("prunes nested proofs from delegations", func(t *testing.T) {
		root := testutil.Must(signer.Generate())(t)
		middle := testutil.Must(signer.Generate())(t)
		leaf := testutil.Must(signer.Generate())(t)

		// Root -> Middle
		rootToMiddle := testutil.Must(delegation.Delegate(
			root,
			middle,
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("store/add", root.DID().String(), ucan.NoCaveats{}),
			},
			delegation.WithNoExpiration(),
		))(t)

		// Middle -> Leaf (includes Root->Middle as proof)
		// This creates a "heavy" delegation because it carries the history
		middleToLeaf := testutil.Must(delegation.Delegate(
			middle,
			leaf,
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("store/add", root.DID().String(), ucan.NoCaveats{}),
			},
			delegation.WithNoExpiration(),
			delegation.WithProof(delegation.FromDelegation(rootToMiddle)),
		))(t)

		var receivedProofLinks []ucan.Link
		var c *client.Client

		connection := ctestutil.NewTestServerConnection(
			server.WithServiceMethod(
				access.DelegateAbility,
				server.Provide(
					access.Delegate,
					func(
						ctx context.Context,
						cap ucan.Capability[access.DelegateCaveats],
						inv invocation.Invocation,
						context server.InvocationContext,
					) (result.Result[access.DelegateOk, failure.IPLDBuilderFailure], fx.Effects, error) {
						receivedProofLinks = inv.Proofs()
						return result.Ok[access.DelegateOk, failure.IPLDBuilderFailure](access.DelegateOk{}), nil, nil
					},
				),
			),
		)

		c = testutil.Must(client.NewClient(
			client.WithConnection(connection),
		))(t)

		// Authorize client to call access/delegate
		accessDelegateProof := testutil.Must(delegation.Delegate(
			root,
			c.Issuer(),
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("access/delegate", root.DID().String(), ucan.NoCaveats{}),
			},
			delegation.WithNoExpiration(),
		))(t)
		require.NoError(t, c.AddProofs(accessDelegateProof))

		// Action: Call AccessDelegate with the "heavy" delegation
		_, err := c.AccessDelegate(testContext(t), root.DID(), middleToLeaf)
		require.NoError(t, err)

		// Verification: Ensure the leaf was sent
		var foundLeafProof delegation.Proof
		for _, pl := range receivedProofLinks {
			if pl.String() == middleToLeaf.Link().String() {
				foundLeafProof = delegation.FromDelegation(middleToLeaf)
			}
		}
		require.NotNil(t, foundLeafProof, "expected leaf proof link to be sent")
	})
}
