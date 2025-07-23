package spaces

import (
	"context"

	"github.com/storacha/guppy/pkg/preparation/spaces/model"
)

type API struct {
	Repo Repo
}

// CreateSpace creates a new space with the given name and options.
func (a API) CreateSpace(ctx context.Context, name string, options ...model.SpaceOption) (*model.Space, error) {
	return a.Repo.CreateSpace(ctx, name, options...)
}
