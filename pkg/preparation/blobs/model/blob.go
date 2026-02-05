package model

import (
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/guppy/pkg/client"
	"github.com/storacha/guppy/pkg/preparation/types"
	"github.com/storacha/guppy/pkg/preparation/types/id"
)

// BlobState represents the state of a blob (shard or index).
type BlobState string

const (
	// BlobStateOpen indicates that a blob is still accepting new nodes.
	BlobStateOpen BlobState = "open"
	// BlobStateClosed indicates that a blob is no longer accepting nodes, but
	// is not yet added to the space.
	BlobStateClosed BlobState = "closed"
	// BlobStateUploaded indicates that a blob has been uploaded to the space, requires post-processing.
	BlobStateUploaded BlobState = "uploaded"
	// BlobStateAdded indicates that a blob has been added to the space.
	BlobStateAdded BlobState = "added"
)

type Blob interface {
	ID() id.ID
	UploadID() id.UploadID
	CID() cid.Cid
	Digest() multihash.Multihash
	PieceCID() cid.Cid
	Size() uint64
	State() BlobState
	Location() invocation.Invocation
	PDPAccept() invocation.Invocation
	SpaceBlobAdded(client.AddedBlob) error
	Added() error
	isBlob()
}

func validBlobState(state BlobState) bool {
	switch state {
	case BlobStateOpen, BlobStateClosed, BlobStateUploaded, BlobStateAdded:
		return true
	default:
		return false
	}
}

type blob struct {
	id         id.ShardID
	uploadID   id.UploadID
	size       uint64
	sliceCount int
	digest     multihash.Multihash
	pieceCID   cid.Cid
	state      BlobState
	location   invocation.Invocation
	pdpAccept  invocation.Invocation
}

func (b *blob) ID() id.ShardID {
	return b.id
}

func (b *blob) UploadID() id.UploadID {
	return b.uploadID
}

func (b *blob) State() BlobState {
	return b.state
}

func (b *blob) Size() uint64 {
	return b.size
}

func (b *blob) SliceCount() int {
	return b.sliceCount
}

func (b *blob) Digest() multihash.Multihash {
	return b.digest
}

func (b *blob) PieceCID() cid.Cid {
	return b.pieceCID
}

func (b *blob) CID() cid.Cid {
	if b.digest == nil {
		return cid.Undef
	}
	return cid.NewCidV1(uint64(multicodec.Car), b.digest)
}

func (b *blob) Location() invocation.Invocation {
	return b.location
}

func (b *blob) PDPAccept() invocation.Invocation {
	return b.pdpAccept
}

func (b *blob) String() string {
	if b.CID() != cid.Undef {
		return fmt.Sprintf("Shard[id=%s, cid=%s]", b.id, b.CID())
	}
	return fmt.Sprintf("Shard[id=%s]", b.id)
}

func (b *blob) Added() error {
	if b.state != BlobStateUploaded {
		return fmt.Errorf("cannot add shard in state %s", b.state)
	}
	b.state = BlobStateAdded
	return nil
}

// validation conditions -- should not be callable externally, all Shards outside this module MUST be valid
func validateBlob(b *blob) (*blob, error) {
	if b.uploadID == id.Nil {
		return nil, types.EmptyError{Field: "uploadID"}
	}
	if !validBlobState(b.state) {
		return nil, fmt.Errorf("invalid shard state: %s", b.state)
	}
	return b, nil
}

type Shard struct {
	blob
	digestStateUpTo uint64
	digestState     []byte
	pieceCIDState   []byte
}

func (s *Shard) isBlob() {}

func (s *Shard) DigestStateUpTo() uint64 {
	return s.digestStateUpTo
}

func (s *Shard) DigestState() []byte {
	return s.digestState
}

func (s *Shard) PieceCIDState() []byte {
	return s.pieceCIDState
}

var _ Blob = &Shard{}

// NewShard creates a new Shard with the given initial size.
func NewShard(uploadID id.UploadID, size uint64, digestState []byte, pieceCIDState []byte) (*Shard, error) {
	s := &Shard{
		blob: blob{
			id:       id.New(),
			uploadID: uploadID,
			size:     size,
			digest:   multihash.Multihash{},
			pieceCID: cid.Undef,
			state:    BlobStateOpen,
		},
		digestStateUpTo: size,
		digestState:     digestState,
		pieceCIDState:   pieceCIDState,
	}
	if _, err := validateBlob(&s.blob); err != nil {
		return nil, fmt.Errorf("failed to create Shard: %w", err)
	}
	return s, nil
}

// State changes

func (s *Shard) Close(digest multihash.Multihash, pieceCID cid.Cid) error {
	if s.state != BlobStateOpen {
		return fmt.Errorf("cannot close shard in state %s", s.state)
	}
	if digest == nil {
		return fmt.Errorf("CAR digest cannot be nil when closing shard")
	}

	s.digest = digest
	s.pieceCID = pieceCID

	s.state = BlobStateClosed
	return nil
}

// SpaceBlobAdded records the result of a successful `space/blob/add`. The shard
// must be in `BlobStateClosed`, or it should not have been `space/blob/add`ed
// to begin with. `location` must be non-nil, because it represents the fact
// that the shard has truly been added. The `pdpAccept` may be nil. The `digest`
// and `size` of the `addedBlob` must match the shard's digest and size.
func (s *Shard) SpaceBlobAdded(addedBlob client.AddedBlob) error {
	if s.state != BlobStateClosed {
		return fmt.Errorf("cannot add shard in state %s", s.state)
	}
	if addedBlob.Location == nil {
		return fmt.Errorf("location invocation cannot be nil")
	}
	if addedBlob.Digest.B58String() != s.Digest().B58String() {
		return fmt.Errorf("added blob %s digest mismatch: expected %x, got %x", s, s.Digest(), addedBlob.Digest)
	}
	s.state = BlobStateUploaded
	s.location = addedBlob.Location
	s.pdpAccept = addedBlob.PDPAccept
	return nil
}

type ShardScanner func(
	id *id.ShardID,
	uploadID *id.UploadID,
	size *uint64,
	sliceCount *int,
	digest *multihash.Multihash,
	pieceCID *cid.Cid,
	digestStateUpTo *uint64,
	digestState *[]byte,
	pieceCIDState *[]byte,
	state *BlobState,
	location *invocation.Invocation,
	pdpAccept *invocation.Invocation,
) error

func ReadShardFromDatabase(scanner ShardScanner) (*Shard, error) {
	shard := &Shard{}
	err := scanner(
		&shard.id,
		&shard.uploadID,
		&shard.size,
		&shard.sliceCount,
		&shard.digest,
		&shard.pieceCID,
		&shard.digestStateUpTo,
		&shard.digestState,
		&shard.pieceCIDState,
		&shard.state,
		&shard.location,
		&shard.pdpAccept,
	)
	if err != nil {
		return nil, fmt.Errorf("reading shard from database: %w", err)
	}
	if _, err := validateBlob(&shard.blob); err != nil {
		return nil, fmt.Errorf("invalid shard: %w", err)
	}
	return shard, nil
}

// ShardWriter is a function type for writing a Shard to the database.
type ShardWriter func(
	id id.ShardID,
	uploadID id.UploadID,
	size uint64,
	sliceCount int,
	digest multihash.Multihash,
	pieceCID cid.Cid,
	digestStateUpTo uint64,
	digestState []byte,
	pieceCIDState []byte,
	state BlobState,
	location invocation.Invocation,
	pdpAccept invocation.Invocation,
) error

// WriteShardToDatabase writes a Shard to the database using the provided writer function.
func WriteShardToDatabase(shard *Shard, writer ShardWriter) error {
	return writer(
		shard.id,
		shard.uploadID,
		shard.size,
		shard.sliceCount,
		shard.digest,
		shard.pieceCID,
		shard.digestStateUpTo,
		shard.digestState,
		shard.pieceCIDState,
		shard.state,
		shard.location,
		shard.pdpAccept,
	)
}

type Index struct {
	blob
}

func (i *Index) isBlob() {}

var _ Blob = &Index{}

func NewIndex(uploadID id.UploadID) (*Index, error) {
	i := &Index{
		blob: blob{
			id:       id.New(),
			uploadID: uploadID,
			state:    BlobStateOpen,
		},
	}
	if _, err := validateBlob(&i.blob); err != nil {
		return nil, fmt.Errorf("failed to create Index: %w", err)
	}
	return i, nil
}

// State Mutations

func (i *Index) Close() error {
	if i.state != BlobStateOpen {
		return fmt.Errorf("cannot set digest for index %s in state %s", i.id, i.state)
	}
	i.state = BlobStateClosed
	return nil
}

// SpaceBlobAdded records the result of a successful `space/blob/add`. The index
// must be in `BlobStateClosed`, or it should not have been `space/blob/add`ed
// to begin with. `location` must be non-nil, because it represents the fact
// that the index has truly been added. The `pdpAccept` may be nil. The `digest`
// and `size` will be recorded as well.
func (i *Index) SpaceBlobAdded(addedBlob client.AddedBlob) error {
	if i.state != BlobStateClosed {
		return fmt.Errorf("cannot add index in state %s", i.state)
	}
	if addedBlob.Location == nil {
		return fmt.Errorf("location invocation cannot be nil")
	}
	i.state = BlobStateUploaded
	i.digest = addedBlob.Digest
	i.size = addedBlob.Size
	i.location = addedBlob.Location
	i.pdpAccept = addedBlob.PDPAccept
	return nil
}

type IndexScanner func(
	id *id.IndexID,
	uploadID *id.UploadID,
	size *uint64,
	sliceCount *int,
	digest *multihash.Multihash,
	pieceCID *cid.Cid,
	state *BlobState,
	location *invocation.Invocation,
	pdpAccept *invocation.Invocation,
) error

func ReadIndexFromDatabase(scanner IndexScanner) (*Index, error) {
	index := &Index{}
	err := scanner(
		&index.id,
		&index.uploadID,
		&index.size,
		&index.sliceCount,
		&index.digest,
		&index.pieceCID,
		&index.state,
		&index.location,
		&index.pdpAccept,
	)
	if err != nil {
		return nil, fmt.Errorf("reading index from database: %w", err)
	}
	if _, err := validateBlob(&index.blob); err != nil {
		return nil, fmt.Errorf("invalid index: %w", err)
	}
	return index, nil
}

// IndexWriter is a function type for writing a Index to the database.
type IndexWriter func(
	id id.IndexID,
	uploadID id.UploadID,
	size uint64,
	sliceCount int,
	digest multihash.Multihash,
	pieceCID cid.Cid,
	state BlobState,
	location invocation.Invocation,
	pdpAccept invocation.Invocation,
) error

// WriteIndexToDatabase writes a Index to the database using the provided writer function.
func WriteIndexToDatabase(index *Index, writer IndexWriter) error {
	return writer(
		index.id,
		index.uploadID,
		index.size,
		index.sliceCount,
		index.digest,
		index.pieceCID,
		index.state,
		index.location,
		index.pdpAccept,
	)
}
