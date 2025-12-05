package shards

import (
	"encoding"
	"fmt"
	"hash"
	"io"

	commcid "github.com/filecoin-project/go-fil-commcid"
	"github.com/ipfs/go-cid"
	"github.com/minio/sha256-simd"
	"github.com/multiformats/go-multihash"
	commp "github.com/storacha/go-fil-commp-hashhash"
	"github.com/storacha/guppy/pkg/preparation/shards/model"
	"github.com/storacha/guppy/pkg/preparation/types"
)

type marshallableHash interface {
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
	hash.Hash
}

type shardHashState struct {
	digestHash marshallableHash
	commpCalc  marshallableHash
}

func newShardHashState() *shardHashState {
	return &shardHashState{
		digestHash: sha256.New().(marshallableHash),
		commpCalc:  &commp.Calc{},
	}
}

func fromShard(shard *model.Shard) (*shardHashState, error) {
	digestHash := sha256.New().(marshallableHash)
	commPCalc := &commp.Calc{}
	if shard.DigestStateUpTo() > 0 {
		if err := digestHash.UnmarshalBinary(shard.DigestState()); err != nil {
			return nil, fmt.Errorf("unmarshaling digest hash state: %w", err)
		}
		if err := commPCalc.UnmarshalBinary(shard.PieceCIDState()); err != nil {
			return nil, fmt.Errorf("unmarshaling piece CID state: %w", err)
		}
	}
	return &shardHashState{
		digestHash: digestHash,
		commpCalc:  commPCalc,
	}, nil
}

var _ io.Writer = (*shardHashState)(nil)

func (s *shardHashState) Write(p []byte) (int, error) {
	if _, err := s.digestHash.Write(p); err != nil {
		return 0, err
	}
	if _, err := s.commpCalc.Write(p); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (s *shardHashState) marshal() ([]byte, []byte, error) {
	digestState, err := s.digestHash.MarshalBinary()
	if err != nil {
		return nil, nil, fmt.Errorf("marshaling digest hash state: %w", err)
	}
	pieceCIDState, err := s.commpCalc.MarshalBinary()
	if err != nil {
		return nil, nil, fmt.Errorf("marshaling piece CID state: %w", err)
	}
	return digestState, pieceCIDState, nil
}

func (s *shardHashState) finalize(shardSize uint64) (multihash.Multihash, cid.Cid, error) {
	shardDigest, err := multihash.Encode(s.digestHash.Sum(nil), multihash.SHA2_256)
	if err != nil {
		return nil, cid.Undef, fmt.Errorf("encoding shard digest: %w", err)
	}

	// If the shard is too small to form a piece CID, return undefined.
	if shardSize < types.MinPiecePayload {
		return shardDigest, cid.Undef, nil
	}

	pieceDigest := s.commpCalc.Sum(nil)

	pieceCID, err := commcid.DataCommitmentToPieceCidv2(pieceDigest, shardSize)
	if err != nil {
		return nil, cid.Undef, fmt.Errorf("computing piece CID: %w", err)
	}

	return shardDigest, pieceCID, nil
}
