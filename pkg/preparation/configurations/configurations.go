package configurations

import (
	"context"

	"github.com/storacha/guppy/pkg/preparation/configurations/model"
)

type ConfigurationsAPI struct {
	Repo Repo
}

// CreateConfiguration creates a new configuration with the given name and options.
func (u ConfigurationsAPI) CreateConfiguration(ctx context.Context, name string, options ...model.ConfigurationOption) (*model.Configuration, error) {
	return u.Repo.CreateConfiguration(ctx, name, options...)
}
