package nodemeta

import (
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/datamodel"
)

const AES256CTRMetaTag = 1

type AES256CTRMeta struct {
	IV []byte
}

func (m AES256CTRMeta) MarshalBinary() ([]byte, error) {
	return m.IV, nil
}

func (m *AES256CTRMeta) UnmarshalBinary(data []byte) error {
	m.IV = data
	return nil
}

func ExtractAES256CTRMetadata(node datamodel.Node, cid cid.Cid, data []byte) ([]byte, error) {
	// For AES-256-CTR encrypted nodes, we want to store the IV as metadata so
	// that we can decrypt the node later. The IV is the first 16 bytes of the
	// encrypted data.
	if len(data) < 16 {
		return nil, fmt.Errorf("invalid encrypted data: too short to contain IV")
	}
	metaBytes, err := Encode(NodeMetadata[AES256CTRMeta]{
		Tag:   AES256CTRMetaTag,
		Value: AES256CTRMeta{IV: data[:16]},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to encode AES-256-CTR metadata for node %s: %w", cid, err)
	}
	return metaBytes, nil
}
