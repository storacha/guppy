package nodemeta_test

import (
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/storacha/guppy/pkg/preparation/dags/nodemeta"
	"github.com/stretchr/testify/require"
)

func TestAES256CTRMeta_MarshalBinary(t *testing.T) {
	tests := []struct {
		name    string
		iv      []byte
		wantNil bool
	}{
		{
			name: "standard 16-byte IV",
			iv:   []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
		},
		{
			name:    "nil IV",
			iv:      nil,
			wantNil: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := nodemeta.AES256CTRMeta{IV: tt.iv}
			got, err := m.MarshalBinary()
			require.NoError(t, err)
			if tt.wantNil {
				require.Nil(t, got)
			} else {
				require.Equal(t, tt.iv, got)
			}
		})
	}
}

func TestAES256CTRMeta_UnmarshalBinary(t *testing.T) {
	tests := []struct {
		name string
		iv   []byte
	}{
		{
			name: "standard 16-byte IV",
			iv:   []byte{15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0},
		},
		{
			name: "all zeros",
			iv:   []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			name: "all ones",
			iv:   []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var m nodemeta.AES256CTRMeta
			require.NoError(t, m.UnmarshalBinary(tt.iv))
			require.Equal(t, tt.iv, m.IV)
		})
	}
}

func TestAES256CTRMeta_RoundTrip(t *testing.T) {
	tests := []struct {
		name string
		iv   []byte
	}{
		{
			name: "sequential bytes",
			iv:   []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
		},
		{
			name: "arbitrary bytes",
			iv:   []byte{0xde, 0xad, 0xbe, 0xef, 0xca, 0xfe, 0xba, 0xbe, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			original := nodemeta.AES256CTRMeta{IV: tt.iv}
			data, err := original.MarshalBinary()
			require.NoError(t, err)

			var decoded nodemeta.AES256CTRMeta
			require.NoError(t, decoded.UnmarshalBinary(data))
			require.Equal(t, tt.iv, decoded.IV)
		})
	}
}

func TestExtractAES256CTRMetadata(t *testing.T) {
	iv := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	wantEncoded, err := nodemeta.Encode(nodemeta.NodeMetadata[nodemeta.AES256CTRMeta]{
		Tag:   nodemeta.AES256CTRMetaTag,
		Value: nodemeta.AES256CTRMeta{IV: iv},
	})
	require.NoError(t, err)

	tests := []struct {
		name    string
		data    []byte
		want    []byte
		wantErr bool
	}{
		{
			name: "IV followed by ciphertext",
			data: append(append([]byte{}, iv...), []byte("ciphertext payload")...),
			want: wantEncoded,
		},
		{
			name: "exactly 16 bytes",
			data: iv,
			want: wantEncoded,
		},
		{
			name:    "too short",
			data:    []byte{1, 2, 3},
			wantErr: true,
		},
		{
			name:    "empty",
			data:    []byte{},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := nodemeta.ExtractAES256CTRMetadata(nil, cid.Undef, tt.data)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestEncodeDecode_AES256CTR(t *testing.T) {
	tests := []struct {
		name string
		iv   []byte
	}{
		{
			name: "standard IV",
			iv:   []byte{0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99},
		},
		{
			name: "all zeros",
			iv:   []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			original := nodemeta.NodeMetadata[nodemeta.AES256CTRMeta]{
				Tag:   nodemeta.AES256CTRMetaTag,
				Value: nodemeta.AES256CTRMeta{IV: tt.iv},
			}

			encoded, err := nodemeta.Encode(original)
			require.NoError(t, err)
			require.NotEmpty(t, encoded)

			decoded, err := nodemeta.Decode[*nodemeta.AES256CTRMeta](encoded)
			require.NoError(t, err)
			require.Equal(t, int64(nodemeta.AES256CTRMetaTag), decoded.Tag)
			require.Equal(t, tt.iv, decoded.Value.IV)
		})
	}
}

func TestDecode_InvalidData(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{name: "not valid CBOR", data: []byte("not valid cbor")},
		{name: "empty", data: []byte{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := nodemeta.Decode[*nodemeta.AES256CTRMeta](tt.data)
			require.Error(t, err)
		})
	}
}
