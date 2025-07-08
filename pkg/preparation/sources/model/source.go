package model

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/storacha/guppy/pkg/preparation/types"
)

// SourceKind represents the kind/type of a source.
type SourceKind string

// ConnectionParams holds connection parameters for a source.
type ConnectionParams []byte

const (
	// LocalSourceKind represents a local source kind.
	LocalSourceKind SourceKind = "local"
)

// Source represents a data source.
type Source struct {
	id               types.SourceID
	name             string
	createdAt        time.Time
	updatedAt        time.Time
	kind             SourceKind
	path             string // Path is the path to the storage root.
	connectionParams ConnectionParams
}

// ID returns the unique identifier of the source.
func (s *Source) ID() types.SourceID {
	return s.id
}

// Name returns the name of the source.
func (s *Source) Name() string {
	return s.name
}

// CreatedAt returns the creation time of the source.
func (s *Source) CreatedAt() time.Time {
	return s.createdAt
}

// UpdatedAt returns the last update time of the source.
func (s *Source) UpdatedAt() time.Time {
	return s.updatedAt
}

// Path returns the storage root path of the source.
func (s *Source) Path() string {
	return s.path
}

// Kind returns the kind/type of the source.
func (s *Source) Kind() SourceKind {
	return s.kind
}

// ConnectionParams returns the connection parameters of the source.
func (s *Source) ConnectionParams() ConnectionParams {
	return s.connectionParams
}

func validateSource(s *Source) (*Source, error) {
	if s.id == uuid.Nil {
		return nil, types.ErrEmpty{Field: "id"}
	}
	if s.name == "" {
		return nil, types.ErrEmpty{Field: "name"}
	}
	return s, nil
}

// SourceOption is a function that configures a Source.
type SourceOption func(*Source) error

// NewSource creates and returns a new Source instance with the specified name and path.
// Additional configuration options can be provided via Option functions.
// Returns the created Source or an error if any option fails or validation does not pass.
func NewSource(name string, path string, opts ...SourceOption) (*Source, error) {
	src := &Source{
		id:        uuid.New(),
		name:      name,
		createdAt: time.Now().UTC().Truncate(time.Second),
		updatedAt: time.Now().UTC().Truncate(time.Second),
		path:      path,
		kind:      LocalSourceKind,
	}
	for _, opt := range opts {
		if err := opt(src); err != nil {
			return nil, err
		}
	}
	return validateSource(src)
}

// SourceRowScanner is a function type for scanning a source row from the database.
type SourceRowScanner func(id *types.SourceID, name *string, createdAt *time.Time, updatedAt *time.Time, kind *SourceKind, path *string, connectionParamsBytes *[]byte) error

// ReadSourceFromDatabase reads a Source from the database using the provided scanner function.
func ReadSourceFromDatabase(scanner SourceRowScanner) (*Source, error) {
	src := &Source{}
	var connectionParamsBytes []byte
	err := scanner(&src.id, &src.name, &src.createdAt, &src.updatedAt, &src.kind, &src.path, &connectionParamsBytes)
	if err != nil {
		return nil, fmt.Errorf("reading source from database: %w", err)
	}
	src.connectionParams = connectionParamsBytes
	return validateSource(src)
}
