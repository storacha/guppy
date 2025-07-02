package configurations

import (
	"context"

	"github.com/storacha/guppy/pkg/preparation/configurations/model"
	"github.com/storacha/guppy/pkg/preparation/types"
)

type Repo interface {
	// GetConfigurationByID retrieves a configuration by its unique ID.
	GetConfigurationByID(ctx context.Context, configID types.ConfigurationID) (*model.Configuration, error)
	// GetConfigurationByName retrieves a configuration by its name.
	GetConfigurationByName(ctx context.Context, name string) (*model.Configuration, error)
	// CreateConfiguration creates a new configuration with the given name and options.
	CreateConfiguration(ctx context.Context, name string, options ...model.ConfigurationOption) (*model.Configuration, error)
	// DeleteConfiguration deletes the configuration by its unique ID.
	DeleteConfiguration(ctx context.Context, configID types.ConfigurationID) error
	// ListConfigurations lists all configurations in the repository.
	ListConfigurations(ctx context.Context) ([]*model.Configuration, error)
	// AddSourceToConfiguration creates a new configuration source mapping with the given configuration ID and source ID.
	AddSourceToConfiguration(ctx context.Context, configID types.ConfigurationID, sourceID types.SourceID) error
	// RemoveSourceFromConfiguration removes the configuration source mapping by configuration ID and source ID.
	RemoveSourceFromConfiguration(ctx context.Context, configID types.ConfigurationID, sourceID types.SourceID) error
}
