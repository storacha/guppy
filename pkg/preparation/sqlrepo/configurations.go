package sqlrepo

import (
	"database/sql"
	"errors"
	"time"

	"github.com/storacha/guppy/pkg/preparation/configurations"
	configurationsmodel "github.com/storacha/guppy/pkg/preparation/configurations/model"
	"github.com/storacha/guppy/pkg/preparation/types"
)

var _ configurations.Repo = (*repo)(nil)

// GetConfigurationByID retrieves a configuration by its unique ID from the repository.
func (r *repo) GetConfigurationByID(configurationID types.ConfigurationID) (*configurationsmodel.Configuration, error) {
	row := r.db.QueryRow(
		`SELECT id, name, created_at, shard_size FROM configurations WHERE id = ?`, configurationID,
	)
	configuration, err := configurationsmodel.ReadConfigurationFromDatabase(func(id *types.ConfigurationID, name *string, createdAt *time.Time, shardSize *uint64) error {
		return row.Scan(id, name, createdAt, shardSize)
	})
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return configuration, err
}

// GetConfigurationByName retrieves a configuration by its name from the repository.
func (r *repo) GetConfigurationByName(name string) (*configurationsmodel.Configuration, error) {
	row := r.db.QueryRow(
		`SELECT id, name, created_at, shard_size FROM configurations WHERE name = ?`, name,
	)
	configuration, err := configurationsmodel.ReadConfigurationFromDatabase(func(id *types.ConfigurationID, name *string, createdAt *time.Time, shardSize *uint64) error {
		return row.Scan(id, name, createdAt, shardSize)
	})
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return configuration, err
}

// CreateConfiguration creates a new configuration in the repository with the given name and options.
func (r *repo) CreateConfiguration(name string, options ...configurationsmodel.ConfigurationOption) (*configurationsmodel.Configuration, error) {
	configuration, err := configurationsmodel.NewConfiguration(name, options...)
	if err != nil {
		return nil, err
	}
	_, err = r.db.Exec(
		`INSERT INTO configurations (id, name, created_at, shard_size) VALUES (?, ?, ?, ?)`,
		configuration.ID(), configuration.Name(), configuration.CreatedAt(), configuration.ShardSize(),
	)
	if err != nil {
		return nil, err
	}
	return configuration, nil
}

// DeleteConfiguration deletes a configuration from the repository.
func (r *repo) DeleteConfiguration(configurationID types.ConfigurationID) error {
	_, err := r.db.Exec(
		`DELETE FROM configurations WHERE id = ?`,
		configurationID,
	)
	if err != nil {
		return err
	}
	// Also delete associated configuration sources
	_, err = r.db.Exec(
		`DELETE FROM configuration_sources WHERE configuration_id = ?`,
		configurationID,
	)
	return err
}

// ListConfigurations lists all configurations in the repository.
func (r *repo) ListConfigurations() ([]*configurationsmodel.Configuration, error) {
	rows, err := r.db.Query(
		`SELECT id, name, created_at, shard_size FROM configurations`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var configurations []*configurationsmodel.Configuration
	for rows.Next() {
		configuration, err := configurationsmodel.ReadConfigurationFromDatabase(func(id *types.ConfigurationID, name *string, createdAt *time.Time, shardSize *uint64) error {
			return rows.Scan(id, name, createdAt, shardSize)
		})
		if err != nil {
			return nil, err
		}
		if configuration == nil {
			continue
		}
		configurations = append(configurations, configuration)
	}
	return configurations, nil
}

// AddSourceToConfiguration adds a source to a configuration in the repository.
func (r *repo) AddSourceToConfiguration(configurationID types.ConfigurationID, sourceID types.SourceID) error {
	_, err := r.db.Exec(
		`INSERT INTO configuration_sources (configuration_id, source_id) VALUES (?, ?)`,
		configurationID, sourceID,
	)
	return err
}

// RemoveSourceFromConfiguration removes a source from a configuration in the repository.
func (r *repo) RemoveSourceFromConfiguration(configurationID types.ConfigurationID, sourceID types.SourceID) error {
	_, err := r.db.Exec(
		`DELETE FROM configuration_sources WHERE configuration_id = ? AND source_id = ?`,
		configurationID, sourceID,
	)
	return err
}

// ListConfigurationSources lists all sources associated with a given configuration ID.
func (r *repo) ListConfigurationSources(configurationID types.ConfigurationID) ([]types.SourceID, error) {
	rows, err := r.db.Query(
		`SELECT cs.source_id
		FROM configuration_sources cs
		WHERE cs.configuration_id = ?`, configurationID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var sources []types.SourceID
	for rows.Next() {
		var sourceID types.SourceID
		if err := rows.Scan(&sourceID); err != nil {
			return nil, err
		}
		sources = append(sources, sourceID)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return sources, nil
}
