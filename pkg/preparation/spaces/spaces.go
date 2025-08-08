package spaces

import (
	"context"

	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/guppy/pkg/preparation/spaces/model"
)

// API defines the interface for managing spaces.
type API struct {
	Repo Repo
}

// CreateSpace creates a new space with the given name and options.
func (a API) FindOrCreateSpace(ctx context.Context, did did.DID, name string, options ...model.SpaceOption) (*model.Space, error) {
	return a.Repo.FindOrCreateSpace(ctx, did, name, options...)
}
