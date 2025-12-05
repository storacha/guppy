package shards

import (
	"crypto/sha256"
	"encoding"
	"fmt"
	"hash"
	"io"

	commcid "github.com/filecoin-project/go-fil-commcid"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	commp "github.com/storacha/go-fil-commp-hashhash"
	"github.com/storacha/guppy/pkg/preparation/shards/model"
	"github.com/storacha/guppy/pkg/preparation/types"
)

type shardHashState struct {
	carHash   hash.Hash
	commpCalc *commp.Calc
}

func newShardHashState() *shardHashState {
	return &shardHashState{
		carHash:   sha256.New(),
		commpCalc: &commp.Calc{},
	}
}

func fromShard(shard *model.Shard) (*shardHashState, error) {
	carHash := sha256.New()
	commPCalc := &commp.Calc{}
	if shard.DigestStateUpTo() > 0 {
		if err := carHash.(encoding.BinaryUnmarshaler).UnmarshalBinary(shard.DigestState()); err != nil {
			return nil, fmt.Errorf("unmarshaling car hash state: %w", err)
		}
		if err := commPCalc.UnmarshalBinary(shard.PieceCIDState()); err != nil {
			return nil, fmt.Errorf("unmarshaling piece CID state: %w", err)
		}
	}
	return &shardHashState{
		carHash:   carHash,
		commpCalc: commPCalc,
	}, nil
}

var _ io.Writer = (*shardHashState)(nil)

func (s *shardHashState) Write(p []byte) (int, error) {
	if _, err := s.carHash.Write(p); err != nil {
		return 0, err
	}
	if _, err := s.commpCalc.Write(p); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (s *shardHashState) marshal() ([]byte, []byte, error) {
	carState, err := s.carHash.(encoding.BinaryMarshaler).MarshalBinary()
	if err != nil {
		return nil, nil, fmt.Errorf("marshaling car hash state: %w", err)
	}
	pieceCIDState, err := s.commpCalc.MarshalBinary()
	if err != nil {
		return nil, nil, fmt.Errorf("marshaling piece CID state: %w", err)
	}
	return carState, pieceCIDState, nil
}

func (s *shardHashState) finalize(shardSize uint64) (multihash.Multihash, cid.Cid, error) {
	carDigest, err := multihash.Encode(s.carHash.Sum(nil), multihash.SHA2_256)
	if err != nil {
		return nil, cid.Undef, fmt.Errorf("encoding car digest: %w", err)
	}

	// If the shard is too small to form a piece CID, return undefined.
	if shardSize < types.MinPiecePayload {
		return carDigest, cid.Undef, nil
	}

	pieceDigest, _, err := s.commpCalc.Digest()
	if err != nil {
		return nil, cid.Undef, fmt.Errorf("computing piece digest: %w", err)
	}

	pieceCID, err := commcid.DataCommitmentToPieceCidv2(pieceDigest, shardSize)
	if err != nil {
		return nil, cid.Undef, fmt.Errorf("computing piece CID: %w", err)
	}

	return carDigest, pieceCID, nil
}
