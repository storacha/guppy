package model

import (
	"errors"
	"fmt"
	"time"

	"github.com/storacha/guppy/pkg/preparation/types"
	"github.com/storacha/guppy/pkg/preparation/types/id"
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

// Configuration represents the configuration for an upload or uploads
type Configuration struct {
	id        id.ConfigurationID
	name      string
	createdAt time.Time

	shardSize uint64 // blob size in bytes
}

// ID returns the unique identifier of the configuration.
func (u *Configuration) ID() id.ConfigurationID {
	return u.id
}

// Name returns the name of the configuration.
func (u *Configuration) Name() string {
	return u.name
}

// CreatedAt returns the creation time of the configuration.
func (u *Configuration) CreatedAt() time.Time {
	return u.createdAt
}

// ShardSize returns the size of each shard in bytes.
func (u *Configuration) ShardSize() uint64 {
	return u.shardSize
}

// ConfigurationOption is a functional option type for configuring a Configuration.
type ConfigurationOption func(*Configuration) error

// WithShardSize sets the size of each shard in bytes for the configuration.
// The shard size must be between 128 bytes and 4GB.
func WithShardSize(shardSize uint64) ConfigurationOption {
	return func(u *Configuration) error {
		u.shardSize = shardSize
		return nil
	}
}

// validateConfiguration checks if the configuration is valid.
func validateConfiguration(u *Configuration) (*Configuration, error) {
	if u.id == id.Nil {
		return nil, types.ErrEmpty{Field: "id"}
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

// NewConfiguration creates a new Configuration instance with the given name and options.
func NewConfiguration(name string, opts ...ConfigurationOption) (*Configuration, error) {
	u := &Configuration{
		id:        id.New(),
		name:      name,
		shardSize: DefaultShardSize, // default shard size
		createdAt: time.Now().UTC().Truncate(time.Second),
	}
	for _, opt := range opts {
		if err := opt(u); err != nil {
			return nil, err
		}
	}
	return validateConfiguration(u)
}

// ConfigurationRowScanner is a function type for scanning a configuration row from the database.
type ConfigurationRowScanner func(id *id.ConfigurationID, name *string, createdAt *time.Time, shardSize *uint64) error

// ReadConfigurationFromDatabase reads a Configuration from the database using the provided scanner function.
func ReadConfigurationFromDatabase(scanner ConfigurationRowScanner) (*Configuration, error) {
	configuration := &Configuration{}
	err := scanner(&configuration.id, &configuration.name, &configuration.createdAt, &configuration.shardSize)
	if err != nil {
		return nil, fmt.Errorf("reading configuration from database: %w", err)
	}
	return validateConfiguration(configuration)
}
