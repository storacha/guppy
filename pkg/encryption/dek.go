package encryption

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io"

	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
)

// GenerateDEK generates a random 256-bit (32-byte) Data Encryption Key.
func GenerateDEK() ([]byte, error) {
	dek := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, dek); err != nil {
		return nil, fmt.Errorf("generating DEK: %w", err)
	}
	return dek, nil
}

// SerializeWrappedPayload encodes {path, dek} as a dag-cbor map.
// The resulting bytes are what gets encrypted by the KEK (local AES-GCM or KMS RSA-OAEP).
// Binding the path to the DEK cryptographically ties each DEK to its file.
func SerializeWrappedPayload(path string, dek []byte) ([]byte, error) {
	nb := basicnode.Prototype.Map.NewBuilder()
	ma, err := nb.BeginMap(2)
	if err != nil {
		return nil, fmt.Errorf("building wrapped payload map: %w", err)
	}
	if err := ma.AssembleKey().AssignString("path"); err != nil {
		return nil, err
	}
	if err := ma.AssembleValue().AssignString(path); err != nil {
		return nil, err
	}
	if err := ma.AssembleKey().AssignString("dek"); err != nil {
		return nil, err
	}
	if err := ma.AssembleValue().AssignBytes(dek); err != nil {
		return nil, err
	}
	if err := ma.Finish(); err != nil {
		return nil, fmt.Errorf("finishing wrapped payload map: %w", err)
	}

	var buf bytes.Buffer
	if err := dagcbor.Encode(nb.Build(), &buf); err != nil {
		return nil, fmt.Errorf("dag-cbor encoding wrapped payload: %w", err)
	}
	return buf.Bytes(), nil
}
