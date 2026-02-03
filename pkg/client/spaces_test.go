package client_test

import (
	"testing"

	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	spacecap "github.com/storacha/go-libstoracha/capabilities/space"
	"github.com/storacha/go-libstoracha/testutil"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/ipld"
	"github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/stretchr/testify/require"

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
	c := testutil.Must(client.NewClient())(t)
	space1 := testutil.Must(signer.Generate())(t)
	space2 := testutil.Must(signer.Generate())(t)

	// Create delegation for space1 with a name
	del1 := testutil.Must(spacecap.Info.Delegate(
		space1,
		c.Issuer(),
		space1.DID().String(),
		spacecap.InfoCaveats{},
		delegation.WithFacts([]ucan.FactBuilder{nameFact{Name: "space-one"}}),
	))(t)

	// Create delegation for space2 without a name
	del2 := testutil.Must(spacecap.Info.Delegate(
		space2,
		c.Issuer(),
		space2.DID().String(),
		spacecap.InfoCaveats{},
	))(t)

	err := c.AddProofs(del1, del2)
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

	require.Equal(t, "space-one", s1.Name)
	require.Equal(t, space1.DID(), s1.DID)
	require.Equal(t, "", s2.Name)
	require.Equal(t, space2.DID(), s2.DID)
}
