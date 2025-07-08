package sources

import (
	"context"

	"github.com/storacha/guppy/pkg/preparation/sources/model"
	"github.com/storacha/guppy/pkg/preparation/types/id"
)

// Repo defines the interface for a repository that manages sources.
type Repo interface {
	// GetSourceByID retrieves a source by its unique ID.
	GetSourceByID(ctx context.Context, sourceID id.SourceID) (*model.Source, error)
	// GetSourceByName retrieves a source by its name.
	GetSourceByName(ctx context.Context, name string) (*model.Source, error)
	// CreateSource creates a new source with the given name, path, and options.
	CreateSource(ctx context.Context, name string, path string, options ...model.SourceOption) (*model.Source, error)
	// UpdateSource updates the given source in the repository.
	UpdateSource(ctx context.Context, src *model.Source) error
}
