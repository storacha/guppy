package agentstore

import (
	"crypto/rand"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/multiformats/go-multihash"
	"github.com/storacha/go-libstoracha/capabilities/space/blob"
	"github.com/storacha/go-libstoracha/capabilities/types"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/ipld"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/stretchr/testify/require"
)

func TestRoundTripAgentData(t *testing.T) {
	agentPrincipal, err := signer.Generate()
	require.NoError(t, err)

	del, err := newDelegation()

	require.NoError(t, err)

	agentData := AgentData{
		Principal:   agentPrincipal,
		Delegations: []delegation.Delegation{del},
	}

	str, err := json.Marshal(agentData)
	require.NoError(t, err)

	var agentDataReturned AgentData
	err = json.Unmarshal(str, &agentDataReturned)
	require.NoError(t, err)

	require.Equal(t, agentData.Principal, agentDataReturned.Principal)
	require.Equal(t, delegationsCIDs(agentData), delegationsCIDs(agentDataReturned))
}

func TestWriteReadAgentData(t *testing.T) {
	dataFilePath := filepath.Join(t.TempDir(), "agentdata.json")

	agentPrincipal, err := signer.Generate()
	require.NoError(t, err)
	del, err := newDelegation()
	require.NoError(t, err)

	agentData := AgentData{
		Principal:   agentPrincipal,
		Delegations: []delegation.Delegation{del},
	}

	err = writeToFile(dataFilePath, agentData)
	require.NoError(t, err)

	agentDataReturned, err := readFromFile(dataFilePath)
	require.NoError(t, err)

	require.Equal(t, agentData.Principal, agentDataReturned.Principal)
	require.Equal(t, delegationsCIDs(agentData), delegationsCIDs(agentDataReturned))
}

func newDelegation() (delegation.Delegation, error) {
	signer, err := signer.Generate()
	if err != nil {
		return nil, err
	}

	audienceDid, err := did.Parse("did:mailto:example.com:alice")
	if err != nil {
		return nil, err
	}

	bytes := make([]byte, 128)
	_, err = rand.Read(bytes)
	if err != nil {
		return nil, err
	}

	digest, err := multihash.Sum(bytes, multihash.SHA2_256, -1)
	if err != nil {
		return nil, err
	}

	return blob.Add.Delegate(
		signer,
		audienceDid,
		signer.DID().String(),
		blob.AddCaveats{
			Blob: types.Blob{
				Digest: digest,
				Size:   uint64(len(bytes)),
			},
		},
	)
}

func delegationsCIDs(d AgentData) []ipld.Link {
	cids := make([]ipld.Link, len(d.Delegations))
	for i, d := range d.Delegations {
		cids[i] = d.Link()
	}
	return cids
}
