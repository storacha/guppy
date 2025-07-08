package configurations

import (
	"context"

	"github.com/storacha/guppy/pkg/preparation/configurations/model"
)

type API struct {
	Repo Repo
}

// CreateConfiguration creates a new configuration with the given name and options.
func (a API) CreateConfiguration(ctx context.Context, name string, options ...model.ConfigurationOption) (*model.Configuration, error) {
	return a.Repo.CreateConfiguration(ctx, name, options...)
}
