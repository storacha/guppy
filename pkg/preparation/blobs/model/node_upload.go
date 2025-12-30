package model

import (
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/guppy/pkg/preparation/types"
	"github.com/storacha/guppy/pkg/preparation/types/id"
)

// NodeUpload represents the association of a node with an upload,
// and optionally its assignment to a shard.
type NodeUpload struct {
	nodeCID     cid.Cid
	spaceDID    did.DID
	uploadID    id.UploadID
	shardID     *id.ShardID // nil if not yet assigned to shard
	shardOffset *uint64     // nil if not yet assigned to shard
}

// Accessors

func (nu *NodeUpload) NodeCID() cid.Cid {
	return nu.nodeCID
}

func (nu *NodeUpload) SpaceDID() did.DID {
	return nu.spaceDID
}

func (nu *NodeUpload) UploadID() id.UploadID {
	return nu.uploadID
}

// ShardID returns the shard ID this node is assigned to, or id.Nil if not assigned.
func (nu *NodeUpload) ShardID() id.ShardID {
	if nu.shardID == nil {
		return id.Nil
	}
	return *nu.shardID
}

// ShardOffset returns the offset of this node in the shard, or 0 if not assigned.
func (nu *NodeUpload) ShardOffset() uint64 {
	if nu.shardOffset == nil {
		return 0
	}
	return *nu.shardOffset
}

func (nu *NodeUpload) HasShard() bool {
	return nu.shardID != nil
}

// validation
func validateNodeUpload(nu *NodeUpload) error {
	if nu.nodeCID == cid.Undef {
		return types.EmptyError{Field: "nodeCID"}
	}
	if !nu.spaceDID.Defined() {
		return types.EmptyError{Field: "spaceDID"}
	}
	if nu.uploadID == id.Nil {
		return types.EmptyError{Field: "uploadID"}
	}
	// If shardID is set, shardOffset must also be set (and vice versa)
	if (nu.shardID != nil) != (nu.shardOffset != nil) {
		return fmt.Errorf("shardID and shardOffset must both be set or both be nil")
	}
	return nil
}

// NewNodeUpload creates a new NodeUpload without shard assignment.
func NewNodeUpload(nodeCID cid.Cid, spaceDID did.DID, uploadID id.UploadID) (*NodeUpload, error) {
	nu := &NodeUpload{
		nodeCID:  nodeCID,
		spaceDID: spaceDID,
		uploadID: uploadID,
	}
	if err := validateNodeUpload(nu); err != nil {
		return nil, fmt.Errorf("failed to create NodeUpload: %w", err)
	}
	return nu, nil
}

// AssignToShard assigns this node to a shard with the given offset.
func (nu *NodeUpload) AssignToShard(shardID id.ShardID, offset uint64) error {
	if nu.shardID != nil {
		return fmt.Errorf("node %s already assigned to shard %s", nu.nodeCID, *nu.shardID)
	}
	nu.shardID = &shardID
	nu.shardOffset = &offset
	return nil
}

// NodeUploadScanner is a function type for scanning a NodeUpload from the database.
// shardID and shardOffset are double pointers to enable properly setting nil values.
type NodeUploadScanner func(
	nodeCID *cid.Cid,
	spaceDID *did.DID,
	uploadID *id.UploadID,
	shardID **id.ShardID,
	shardOffset **uint64,
) error

// ReadNodeUploadFromDatabase reads a NodeUpload from the database using the provided scanner.
func ReadNodeUploadFromDatabase(scanner NodeUploadScanner) (*NodeUpload, error) {
	var nu NodeUpload

	err := scanner(&nu.nodeCID, &nu.spaceDID, &nu.uploadID, &nu.shardID, &nu.shardOffset)
	if err != nil {
		return nil, fmt.Errorf("reading node upload from database: %w", err)
	}

	if err := validateNodeUpload(&nu); err != nil {
		return nil, fmt.Errorf("invalid node upload: %w", err)
	}
	return &nu, nil
}

// NodeUploadWriter is a function type for writing a NodeUpload to the database.
type NodeUploadWriter func(
	nodeCID cid.Cid,
	spaceDID did.DID,
	uploadID id.UploadID,
	shardID *id.ShardID,
	shardOffset *uint64,
) error

// WriteNodeUploadToDatabase writes a NodeUpload to the database using the provided writer.
func WriteNodeUploadToDatabase(nu *NodeUpload, writer NodeUploadWriter) error {
	return writer(nu.nodeCID, nu.spaceDID, nu.uploadID, nu.shardID, nu.shardOffset)
}
