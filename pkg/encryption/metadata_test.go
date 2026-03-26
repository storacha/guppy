package encryption_test

import (
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"github.com/storacha/guppy/pkg/encryption"
	"github.com/stretchr/testify/require"
)

// randomCID builds a deterministic dag-cbor CID from a seed byte for test fixtures.
func randomCID(t *testing.T, seed byte) cid.Cid {
	t.Helper()
	digest, err := multihash.Sum([]byte{seed}, multihash.SHA2_256, -1)
	require.NoError(t, err)
	return cid.NewCidV1(uint64(multicodec.DagCbor), digest)
}

func sampleMeta(t *testing.T) encryption.EncryptedMetadata {
	t.Helper()
	return encryption.EncryptedMetadata{
		EncryptedDataCID:      randomCID(t, 0x01),
		EncryptedSymmetricKey: []byte("wrapped-dek-payload-bytes"),
		Space:                 "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK",
		Path:                  "/backups/db.tar",
		KMS: encryption.KMSInfo{
			Provider:  "google-kms",
			KeyID:     "z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK",
			Algorithm: "RSA-OAEP-256",
		},
	}
}

func TestEncodeDecodeMetadataBlock_RoundTrip(t *testing.T) {
	meta := sampleMeta(t)

	c, data, err := encryption.EncodeMetadataBlock(meta)
	require.NoError(t, err)
	require.True(t, c.Defined(), "CID must be defined")
	require.NotEmpty(t, data)

	decoded, err := encryption.DecodeMetadataBlock(data)
	require.NoError(t, err)

	require.Equal(t, meta.EncryptedDataCID, decoded.EncryptedDataCID)
	require.Equal(t, meta.EncryptedSymmetricKey, decoded.EncryptedSymmetricKey)
	require.Equal(t, meta.Space, decoded.Space)
	require.Equal(t, meta.Path, decoded.Path)
	require.Equal(t, meta.KMS.Provider, decoded.KMS.Provider)
	require.Equal(t, meta.KMS.KeyID, decoded.KMS.KeyID)
	require.Equal(t, meta.KMS.Algorithm, decoded.KMS.Algorithm)
}

func TestEncodeMetadataBlock_CIDIsDagCBOR(t *testing.T) {
	meta := sampleMeta(t)

	c, data, err := encryption.EncodeMetadataBlock(meta)
	require.NoError(t, err)

	// CID codec must be dag-cbor (0x71)
	require.Equal(t, uint64(multicodec.DagCbor), c.Prefix().Codec)
	// CID multihash must be SHA2-256
	require.Equal(t, uint64(multihash.SHA2_256), c.Prefix().MhType)

	// CID must be the SHA2-256 of the dag-cbor bytes
	expectedDigest, err := multihash.Sum(data, multihash.SHA2_256, -1)
	require.NoError(t, err)
	expectedCID := cid.NewCidV1(uint64(multicodec.DagCbor), expectedDigest)
	require.Equal(t, expectedCID, c)
}

func TestEncodeMetadataBlock_Deterministic(t *testing.T) {
	meta := sampleMeta(t)

	c1, data1, err := encryption.EncodeMetadataBlock(meta)
	require.NoError(t, err)

	c2, data2, err := encryption.EncodeMetadataBlock(meta)
	require.NoError(t, err)

	require.Equal(t, data1, data2, "identical inputs must produce identical bytes")
	require.Equal(t, c1, c2, "identical inputs must produce identical CID")
}

func TestEncodeMetadataBlock_LocalMode(t *testing.T) {
	meta := encryption.EncryptedMetadata{
		EncryptedDataCID:      randomCID(t, 0x02),
		EncryptedSymmetricKey: []byte("local-gcm-wrapped-dek"),
		Space:                 "did:key:z6Mk1234",
		Path:                  "/data/file.bin",
		KMS: encryption.KMSInfo{
			Provider:  "local",
			KeyID:     "z6Mk1234",
			Algorithm: "AES-256-GCM",
		},
	}

	c, data, err := encryption.EncodeMetadataBlock(meta)
	require.NoError(t, err)
	require.True(t, c.Defined())

	decoded, err := encryption.DecodeMetadataBlock(data)
	require.NoError(t, err)
	require.Equal(t, meta.KMS.Provider, decoded.KMS.Provider)
	require.Equal(t, meta.KMS.Algorithm, decoded.KMS.Algorithm)
}
