package spaces

import (
	"context"

	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/guppy/pkg/preparation/spaces/model"
	"github.com/storacha/guppy/pkg/preparation/types/id"
)

type Repo interface {
	// GetSpaceByDID retrieves a space by its unique DID.
	GetSpaceByDID(ctx context.Context, spaceDID did.DID) (*model.Space, error)
	// GetSpaceByName retrieves a space by its name.
	GetSpaceByName(ctx context.Context, name string) (*model.Space, error)
	// FindOrCreateSpace finds an existing space by DID or creates a new one.
	FindOrCreateSpace(ctx context.Context, did did.DID, name string, options ...model.SpaceOption) (*model.Space, error)
	// DeleteSpace deletes the space by its unique DID.
	DeleteSpace(ctx context.Context, spaceDID did.DID) error
	// ListSpaces lists all spaces in the repository.
	ListSpaces(ctx context.Context) ([]*model.Space, error)
	// AddSourceToSpace creates a new space source mapping with the given space DID and source ID.
	AddSourceToSpace(ctx context.Context, spaceDID did.DID, sourceID id.SourceID) error
	// RemoveSourceFromSpace removes the space source mapping by space DID and source ID.
	RemoveSourceFromSpace(ctx context.Context, spaceDID did.DID, sourceID id.SourceID) error
}
