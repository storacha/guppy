package encryption

import (
	"bytes"
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
)

// KMSInfo identifies the key management system and algorithm used to wrap a DEK.
type KMSInfo struct {
	Provider  string // e.g. "google-kms" ot "local"
	KeyID     string // KMS key identifier (spaceDID withour did:key)
	Algorithm string // e.g. "RSA-OAEP-256" or "AES-256-GCM"
}

// EncryptedMetadata is the per-file CBOR block uploaded alongside encrypted content.
// It is compatible with JS @storacha/encrypt-upload-client.
type EncryptedMetadata struct {
	EncryptedDataCID      cid.Cid // CID of the encrypted UnixFS root for this file
	EncryptedSymmetricKey []byte  // wrapped CBOR({path, dek}) blob
	Space                 string  // space DID
	Path                  string  // plaintext file path (optional)
	KMS                   KMSInfo
}

// EncodeMetadataBlock serialises meta as a dag-cbor block and returns its CID and raw bytes.
// Field names match the TypeScript EncryptedMetadata interface for cross-client compatibility.
// CID is computed as dag-cbor multicodec (0x71) + SHA-256 of the encoded bytes.
func EncodeMetadataBlock(meta EncryptedMetadata) (cid.Cid, []byte, error) {
	nb := basicnode.Prototype.Map.NewBuilder()
	ma, err := nb.BeginMap(5)
	if err != nil {
		return cid.Undef, nil, fmt.Errorf("beginning metadata map: %w", err)
	}

	if err := ma.AssembleKey().AssignString("encryptedDataCID"); err != nil {
		return cid.Undef, nil, err
	}
	if err := ma.AssembleValue().AssignLink(cidlink.Link{Cid: meta.EncryptedDataCID}); err != nil {
		return cid.Undef, nil, fmt.Errorf("assigning encryptedDataCID: %w", err)
	}

	if err := ma.AssembleKey().AssignString("encryptedSymmetricKey"); err != nil {
		return cid.Undef, nil, err
	}
	if err := ma.AssembleValue().AssignBytes(meta.EncryptedSymmetricKey); err != nil {
		return cid.Undef, nil, err
	}

	if err := ma.AssembleKey().AssignString("space"); err != nil {
		return cid.Undef, nil, err
	}
	if err := ma.AssembleValue().AssignString(meta.Space); err != nil {
		return cid.Undef, nil, err
	}

	if err := ma.AssembleKey().AssignString("path"); err != nil {
		return cid.Undef, nil, err
	}
	if err := ma.AssembleValue().AssignString(meta.Path); err != nil {
		return cid.Undef, nil, err
	}

	if err := ma.AssembleKey().AssignString("kms"); err != nil {
		return cid.Undef, nil, err
	}
	kmsVal, err := ma.AssembleValue().BeginMap(3)
	if err != nil {
		return cid.Undef, nil, fmt.Errorf("beginning kms map: %w", err)
	}
	if err := kmsVal.AssembleKey().AssignString("provider"); err != nil {
		return cid.Undef, nil, err
	}
	if err := kmsVal.AssembleValue().AssignString(meta.KMS.Provider); err != nil {
		return cid.Undef, nil, err
	}
	if err := kmsVal.AssembleKey().AssignString("keyId"); err != nil {
		return cid.Undef, nil, err
	}
	if err := kmsVal.AssembleValue().AssignString(meta.KMS.KeyID); err != nil {
		return cid.Undef, nil, err
	}
	if err := kmsVal.AssembleKey().AssignString("algorithm"); err != nil {
		return cid.Undef, nil, err
	}
	if err := kmsVal.AssembleValue().AssignString(meta.KMS.Algorithm); err != nil {
		return cid.Undef, nil, err
	}
	if err := kmsVal.Finish(); err != nil {
		return cid.Undef, nil, fmt.Errorf("finishing kms map: %w", err)
	}

	if err := ma.Finish(); err != nil {
		return cid.Undef, nil, fmt.Errorf("finishing metadata map: %w", err)
	}

	var buf bytes.Buffer
	if err := dagcbor.Encode(nb.Build(), &buf); err != nil {
		return cid.Undef, nil, fmt.Errorf("dag-cbor encoding metadata block: %w", err)
	}
	data := buf.Bytes()

	digest, err := multihash.Sum(data, multihash.SHA2_256, -1)
	if err != nil {
		return cid.Undef, nil, fmt.Errorf("computing metadata block multihash: %w", err)
	}
	c := cid.NewCidV1(uint64(multicodec.DagCbor), digest)

	return c, data, nil
}

// DecodeMetadataBlock deserialises a dag-cbor metadata block produced by EncodeMetadataBlock.
func DecodeMetadataBlock(data []byte) (EncryptedMetadata, error) {
	nb := basicnode.Prototype.Map.NewBuilder()
	if err := dagcbor.Decode(nb, bytes.NewReader(data)); err != nil {
		return EncryptedMetadata{}, fmt.Errorf("dag-cbor decoding metadata block: %w", err)
	}
	n := nb.Build()

	var meta EncryptedMetadata

	// encryptedDataCID
	cidNode, err := n.LookupByString("encryptedDataCID")
	if err != nil {
		return EncryptedMetadata{}, fmt.Errorf("missing encryptedDataCID: %w", err)
	}
	link, err := cidNode.AsLink()
	if err != nil {
		return EncryptedMetadata{}, fmt.Errorf("encryptedDataCID is not a link: %w", err)
	}
	cl, ok := link.(cidlink.Link)
	if !ok {
		return EncryptedMetadata{}, fmt.Errorf("encryptedDataCID link is not a CID link")
	}
	meta.EncryptedDataCID = cl.Cid

	// encryptedSymmetricKey
	eskNode, err := n.LookupByString("encryptedSymmetricKey")
	if err != nil {
		return EncryptedMetadata{}, fmt.Errorf("missing encryptedSymmetricKey: %w", err)
	}
	meta.EncryptedSymmetricKey, err = eskNode.AsBytes()
	if err != nil {
		return EncryptedMetadata{}, fmt.Errorf("encryptedSymmetricKey is not bytes: %w", err)
	}

	// space
	spaceNode, err := n.LookupByString("space")
	if err != nil {
		return EncryptedMetadata{}, fmt.Errorf("missing space: %w", err)
	}
	meta.Space, err = spaceNode.AsString()
	if err != nil {
		return EncryptedMetadata{}, fmt.Errorf("space is not a string: %w", err)
	}

	// path
	pathNode, err := n.LookupByString("path")
	if err != nil {
		return EncryptedMetadata{}, fmt.Errorf("missing path: %w", err)
	}
	meta.Path, err = pathNode.AsString()
	if err != nil {
		return EncryptedMetadata{}, fmt.Errorf("path is not a string: %w", err)
	}

	// kms
	kmsNode, err := n.LookupByString("kms")
	if err != nil {
		return EncryptedMetadata{}, fmt.Errorf("missing kms: %w", err)
	}

	providerNode, err := kmsNode.LookupByString("provider")
	if err != nil {
		return EncryptedMetadata{}, fmt.Errorf("missing kms.provider: %w", err)
	}
	meta.KMS.Provider, err = providerNode.AsString()
	if err != nil {
		return EncryptedMetadata{}, fmt.Errorf("kms.provider is not a string: %w", err)
	}

	keyIDNode, err := kmsNode.LookupByString("keyId")
	if err != nil {
		return EncryptedMetadata{}, fmt.Errorf("missing kms.keyId: %w", err)
	}
	meta.KMS.KeyID, err = keyIDNode.AsString()
	if err != nil {
		return EncryptedMetadata{}, fmt.Errorf("kms.keyId is not a string: %w", err)
	}

	algorithmNode, err := kmsNode.LookupByString("algorithm")
	if err != nil {
		return EncryptedMetadata{}, fmt.Errorf("missing kms.algorithm: %w", err)
	}
	meta.KMS.Algorithm, err = algorithmNode.AsString()
	if err != nil {
		return EncryptedMetadata{}, fmt.Errorf("kms.algorithm is not a string: %w", err)
	}

	return meta, nil
}
