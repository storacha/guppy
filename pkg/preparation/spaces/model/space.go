package model

import (
	"errors"
	"fmt"
	"time"

	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/guppy/pkg/preparation/types"
)

// MaxShardSize is the maximum allowed size for a shard, set to 4GB
const MaxShardSize = 4 << 30

// MinShardSize is the minimum allowed size for a shard, set to 128 bytes
const MinShardSize = 128

// DefaultShardSize is the default size for a shard, set to 512MB
const DefaultShardSize = 512 << 20 // default shard size = 512MB

// ErrShardSizeTooLarge indicates that the shard size is larger than the maximum allowed size.
var ErrShardSizeTooLarge = errors.New("Shard size must be less than 4GB")

// ErrShardSizeTooSmall indicates that the shard size is smaller than the minimum allowed size.
var ErrShardSizeTooSmall = errors.New("Shard size must be at least 128 bytes")

// Space represents the space for an upload or uploads
type Space struct {
	did       did.DID
	name      string
	createdAt time.Time

	shardSize uint64 // blob size in bytes
}

// DID returns the unique identifier of the space.
func (u *Space) DID() did.DID {
	return u.did
}

// Name returns the name of the space.
func (u *Space) Name() string {
	return u.name
}

// CreatedAt returns the creation time of the space.
func (u *Space) CreatedAt() time.Time {
	return u.createdAt
}

// ShardSize returns the size of each shard in bytes.
func (u *Space) ShardSize() uint64 {
	return u.shardSize
}

// SpaceOption is a functional option type for configuring a Space.
type SpaceOption func(*Space) error

// WithShardSize sets the size of each shard in bytes for the space.
// The shard size must be between 128 bytes and 4GB.
func WithShardSize(shardSize uint64) SpaceOption {
	return func(u *Space) error {
		u.shardSize = shardSize
		return nil
	}
}

// validateSpace checks if the space is valid.
func validateSpace(u *Space) (*Space, error) {
	if u.did.String() == "" {
		return nil, types.ErrEmpty{Field: "did"}
	}
	if u.name == "" {
		return nil, types.ErrEmpty{Field: "name"}
	}
	if u.shardSize >= MaxShardSize {
		return nil, ErrShardSizeTooLarge
	}
	if u.shardSize < MinShardSize {
		return nil, ErrShardSizeTooSmall
	}
	return u, nil
}

// NewSpace creates a new Space instance with the given name and options.
func NewSpace(spaceDID did.DID, name string, opts ...SpaceOption) (*Space, error) {
	u := &Space{
		did:       spaceDID,
		name:      name,
		shardSize: DefaultShardSize, // default shard size
		createdAt: time.Now().UTC().Truncate(time.Second),
	}
	for _, opt := range opts {
		if err := opt(u); err != nil {
			return nil, err
		}
	}
	return validateSpace(u)
}

// SpaceRowScanner is a function type for scanning a space row from the database.
type SpaceRowScanner func(did *did.DID, name *string, createdAt *time.Time, shardSize *uint64) error

// ReadSpaceFromDatabase reads a Space from the database using the provided scanner function.
func ReadSpaceFromDatabase(scanner SpaceRowScanner) (*Space, error) {
	space := &Space{}
	err := scanner(&space.did, &space.name, &space.createdAt, &space.shardSize)
	if err != nil {
		return nil, fmt.Errorf("reading space from database: %w", err)
	}
	return validateSpace(space)
}
