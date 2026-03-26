package encryption_test

import (
	"bytes"
	"testing"

	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/storacha/guppy/pkg/encryption"
	"github.com/stretchr/testify/require"
)

func TestGenerateDEK_Length(t *testing.T) {
	dek, err := encryption.GenerateDEK()
	require.NoError(t, err)
	require.Len(t, dek, 32, "DEK must be exactly 32 bytes (AES-256)")
}

func TestGenerateDEK_Unique(t *testing.T) {
	dek1, err := encryption.GenerateDEK()
	require.NoError(t, err)

	dek2, err := encryption.GenerateDEK()
	require.NoError(t, err)

	require.NotEqual(t, dek1, dek2, "two consecutive DEKs must differ")
}

func TestSerializeWrappedPayload_ValidDagCBOR(t *testing.T) {
	dek := make([]byte, 32)
	data, err := encryption.SerializeWrappedPayload("/tmp/test.txt", dek)
	require.NoError(t, err)
	require.NotEmpty(t, data)

	// Must be valid dag-cbor
	nb := basicnode.Prototype.Map.NewBuilder()
	err = dagcbor.Decode(nb, bytes.NewReader(data))
	require.NoError(t, err)
}

func TestSerializeWrappedPayload_FieldsRoundTrip(t *testing.T) {
	path := "/data/myfile.bin"
	dek := []byte("12345678901234567890123456789012") // 32 bytes

	data, err := encryption.SerializeWrappedPayload(path, dek)
	require.NoError(t, err)

	nb := basicnode.Prototype.Map.NewBuilder()
	err = dagcbor.Decode(nb, bytes.NewReader(data))
	require.NoError(t, err)
	n := nb.Build()

	pathNode, err := n.LookupByString("path")
	require.NoError(t, err)
	gotPath, err := pathNode.AsString()
	require.NoError(t, err)
	require.Equal(t, path, gotPath)

	dekNode, err := n.LookupByString("dek")
	require.NoError(t, err)
	gotDEK, err := dekNode.AsBytes()
	require.NoError(t, err)
	require.Equal(t, dek, gotDEK)
}

func TestSerializeWrappedPayload_Deterministic(t *testing.T) {
	path := "/tmp/file.txt"
	dek := make([]byte, 32)

	out1, err := encryption.SerializeWrappedPayload(path, dek)
	require.NoError(t, err)

	out2, err := encryption.SerializeWrappedPayload(path, dek)
	require.NoError(t, err)

	require.Equal(t, out1, out2, "same path+dek must produce identical bytes")
}

func TestSerializeWrappedPayload_DifferentPathsDifferentBytes(t *testing.T) {
	dek := make([]byte, 32)

	out1, err := encryption.SerializeWrappedPayload("/path/a.txt", dek)
	require.NoError(t, err)

	out2, err := encryption.SerializeWrappedPayload("/path/b.txt", dek)
	require.NoError(t, err)

	require.NotEqual(t, out1, out2)
}
