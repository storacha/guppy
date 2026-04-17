package agentstore

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multibase"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"github.com/multiformats/go-varint"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/principal"
	ed25519signer "github.com/storacha/go-ucanto/principal/ed25519/signer"
	rsasigner "github.com/storacha/go-ucanto/principal/rsa/signer"
	"github.com/storacha/go-ucanto/ucan"
)

type Store interface {
	HasPrincipal() (bool, error)
	Principal() (principal.Signer, error)
	SetPrincipal(principal principal.Signer) error
	Delegations() ([]delegation.Delegation, error)
	AddDelegations(delegations ...delegation.Delegation) error
	RemoveDelegation(id cid.Cid) error
	Reset() error
	Query(queries ...CapabilityQuery) ([]delegation.Delegation, error)
}

// CapabilityQuery represents a query to filter proofs by capability.
type CapabilityQuery struct {
	// Can is the ability to match (e.g., "store/add"). Use "*" to match all abilities.
	Can ucan.Ability
	// With is the resource to match. Use "ucan:*" to match all resources.
	With ucan.Resource
}

type AgentData struct {
	Principal   principal.Signer
	Delegations []delegation.Delegation
}

type agentDataSerialized struct {
	Principal   []byte
	Delegations []string
}

func (ad AgentData) MarshalJSON() ([]byte, error) {
	delegations := make([]string, 0, len(ad.Delegations))
	for _, d := range ad.Delegations {
		b, err := io.ReadAll(d.Archive())
		if err != nil {
			return nil, fmt.Errorf("reading delegation archive: %w", err)
		}
		digest, err := multihash.Sum(b, uint64(multicodec.Identity), -1)
		if err != nil {
			return nil, fmt.Errorf("creating multihash: %w", err)
		}
		cid := cid.NewCidV1(uint64(multicodec.Car), digest)
		b64, err := cid.StringOfBase(multibase.Base64)
		if err != nil {
			return nil, fmt.Errorf("encoding delegation cid to base64: %w", err)
		}
		delegations = append(delegations, b64)
	}

	return json.Marshal(agentDataSerialized{
		Principal:   ad.Principal.Encode(),
		Delegations: delegations,
	})
}

func (ad *AgentData) UnmarshalJSON(b []byte) error {
	var s agentDataSerialized
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}

	// Principal

	code, err := varint.ReadUvarint(bytes.NewReader(s.Principal))
	if err != nil {
		return fmt.Errorf("reading private key codec: %s", err)
	}

	switch code {
	case ed25519signer.Code:
		ad.Principal, err = ed25519signer.Decode(s.Principal)
		if err != nil {
			return err
		}

	case rsasigner.Code:
		ad.Principal, err = rsasigner.Decode(s.Principal)
		if err != nil {
			return err
		}

	default:
		return fmt.Errorf("invalid private key codec: %d", code)
	}

	// Delegations

	ad.Delegations = make([]delegation.Delegation, len(s.Delegations))
	for i, b64 := range s.Delegations {
		cid, err := cid.Decode(b64)
		if err != nil {
			return fmt.Errorf("decoding delegation cid %d: %w", i, err)
		}
		if cid.Prefix().Codec != uint64(multicodec.Car) {
			return fmt.Errorf("invalid delegation codec %d for delegation %d, expected CAR (%d)", cid.Prefix().Codec, i, multicodec.Car)
		}
		if cid.Prefix().MhType != uint64(multicodec.Identity) {
			return fmt.Errorf("invalid delegation multihash type %d for delegation %d, expected Identity (%d)", cid.Prefix().MhType, i, multicodec.Identity)
		}
		decoded, err := multihash.Decode(cid.Hash())
		if err != nil {
			return fmt.Errorf("decoding delegation multihash %d: %w", i, err)
		}
		d, err := delegation.Extract(decoded.Digest)
		if err != nil {
			return fmt.Errorf("decoding delegation %d: %w", i, err)
		}
		ad.Delegations[i] = d
	}

	return nil
}

func readFromFile(path string) (AgentData, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return AgentData{}, fmt.Errorf("failed to read agent data from %q: %w", path, err)
	}

	var ad AgentData
	err = json.Unmarshal(b, &ad)
	if err != nil {
		return AgentData{}, fmt.Errorf("failed to unmarshal agent data from %q: %w", path, err)
	}
	return ad, nil
}

func writeToFile(path string, data AgentData) error {
	b, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0600)
}
