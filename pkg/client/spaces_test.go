package client_test

import (
	"testing"

	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/storacha/go-libstoracha/testutil"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/ipld"
	"github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/stretchr/testify/require"

	"github.com/storacha/guppy/pkg/agentstore"
	"github.com/storacha/guppy/pkg/client"
)

type nameFact struct {
	Name string
}

func (n nameFact) ToIPLD() (map[string]ipld.Node, error) {
	return map[string]ipld.Node{
		"name": basicnode.NewString(n.Name),
	}, nil
}

func TestSpacesWithName(t *testing.T) {
	store := testutil.Must(agentstore.NewMemory())(t)
	c := testutil.Must(client.NewClient(client.WithStore(store)))(t)
	space1 := testutil.Must(signer.Generate())(t)
	space2 := testutil.Must(signer.Generate())(t)

	// Create delegation for space1 with a name
	// Using ucan.NewCapability for "space/*" to match what c.Spaces() queries
	cap1 := ucan.NewCapability("space/*", space1.DID().String(), ucan.NoCaveats{})
	del1, err := delegation.Delegate(
		space1,
		c.Issuer(),
		[]ucan.Capability[ucan.NoCaveats]{cap1},
		delegation.WithFacts([]ucan.FactBuilder{nameFact{Name: "space-one"}}),
		delegation.WithNoExpiration(),
	)
	require.NoError(t, err)

	// Create delegation for space2 without a name
	cap2 := ucan.NewCapability("space/*", space2.DID().String(), ucan.NoCaveats{})
	del2, err := delegation.Delegate(
		space2,
		c.Issuer(),
		[]ucan.Capability[ucan.NoCaveats]{cap2},
		delegation.WithNoExpiration(),
	)
	require.NoError(t, err)

	err = c.AddProofs(del1, del2)
	require.NoError(t, err)

	spaces, err := c.Spaces()
	require.NoError(t, err)
	require.Len(t, spaces, 2)

	var s1, s2 client.Space
	for _, s := range spaces {
		if s.DID == space1.DID() {
			s1 = s
		} else if s.DID == space2.DID() {
			s2 = s
		}
	}

	require.Equal(t, "space-one", s1.Name, "expected space1 to have name 'space-one'")
	require.Equal(t, space1.DID(), s1.DID)
	require.Equal(t, "", s2.Name, "expected space2 to have no name")
	require.Equal(t, space2.DID(), s2.DID)
}
