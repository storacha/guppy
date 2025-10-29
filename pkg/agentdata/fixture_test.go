package agentdata_test

import (
	"crypto/rand"

	"github.com/multiformats/go-multihash"
	"github.com/storacha/go-libstoracha/capabilities/space/blob"
	"github.com/storacha/go-libstoracha/capabilities/types"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/ipld"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/guppy/pkg/agentdata"
)

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

func delegationsCIDs(d agentdata.AgentData) []ipld.Link {
	cids := make([]ipld.Link, len(d.Delegations))
	for i, d := range d.Delegations {
		cids[i] = d.Link()
	}
	return cids
}
