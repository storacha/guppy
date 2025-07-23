package spaces

import (
	"context"

	"github.com/storacha/guppy/pkg/preparation/spaces/model"
	"github.com/storacha/guppy/pkg/preparation/types/id"
)

type Repo interface {
	// GetSpaceByID retrieves a space by its unique ID.
	GetSpaceByID(ctx context.Context, spaceID id.SpaceID) (*model.Space, error)
	// GetSpaceByName retrieves a space by its name.
	GetSpaceByName(ctx context.Context, name string) (*model.Space, error)
	// CreateSpace creates a new space with the given name and options.
	CreateSpace(ctx context.Context, name string, options ...model.SpaceOption) (*model.Space, error)
	// DeleteSpace deletes the space by its unique ID.
	DeleteSpace(ctx context.Context, spaceID id.SpaceID) error
	// ListSpaces lists all spaces in the repository.
	ListSpaces(ctx context.Context) ([]*model.Space, error)
	// AddSourceToSpace creates a new space source mapping with the given space ID and source ID.
	AddSourceToSpace(ctx context.Context, spaceID id.SpaceID, sourceID id.SourceID) error
	// RemoveSourceFromSpace removes the space source mapping by space ID and source ID.
	RemoveSourceFromSpace(ctx context.Context, spaceID id.SpaceID, sourceID id.SourceID) error
}
