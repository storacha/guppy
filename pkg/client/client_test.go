package client_test

import (
	"testing"

	uploadcap "github.com/storacha/go-libstoracha/capabilities/upload"
	"github.com/storacha/go-libstoracha/testutil"
	"github.com/storacha/go-ucanto/core/delegation"
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
