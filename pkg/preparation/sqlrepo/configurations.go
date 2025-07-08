package sqlrepo

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/storacha/guppy/pkg/preparation/configurations"
	configurationsmodel "github.com/storacha/guppy/pkg/preparation/configurations/model"
	"github.com/storacha/guppy/pkg/preparation/types/id"
)

var _ configurations.Repo = (*repo)(nil)

// CreateConfiguration creates a new configuration in the repository with the given name and options.
func (r *repo) CreateConfiguration(ctx context.Context, name string, options ...configurationsmodel.ConfigurationOption) (*configurationsmodel.Configuration, error) {
	configuration, err := configurationsmodel.NewConfiguration(name, options...)
	if err != nil {
		return nil, fmt.Errorf("failed to create configuration model: %w", err)
	}

	_, err = r.db.ExecContext(ctx,
		`INSERT INTO configurations (
			id,
			name,
			created_at,
			shard_size
		) VALUES (?, ?, ?, ?)`,
		configuration.ID(),
		configuration.Name(),
		configuration.CreatedAt().Unix(),
		configuration.ShardSize(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to insert configuration into database: %w", err)
	}
	return configuration, nil
}

// GetConfigurationByID retrieves a configuration by its unique ID from the repository.
func (r *repo) GetConfigurationByID(ctx context.Context, configurationID id.ConfigurationID) (*configurationsmodel.Configuration, error) {
	row := r.db.QueryRowContext(ctx,
		`SELECT
			id,
			name,
			created_at,
			shard_size
		FROM configurations WHERE id = ?`, configurationID,
	)
	return r.getConfigurationFromRow(row)
}

// GetConfigurationByName retrieves a configuration by its name from the repository.
func (r *repo) GetConfigurationByName(ctx context.Context, name string) (*configurationsmodel.Configuration, error) {
	row := r.db.QueryRowContext(ctx,
		`SELECT
			id,
			name,
			created_at,
			shard_size
		FROM configurations WHERE name = ?`, name,
	)
	return r.getConfigurationFromRow(row)
}

func (r *repo) getConfigurationFromRow(row *sql.Row) (*configurationsmodel.Configuration, error) {
	configuration, err := configurationsmodel.ReadConfigurationFromDatabase(func(
		id *id.ConfigurationID,
		name *string,
		createdAt *time.Time,
		shardSize *uint64,
	) error {
		return row.Scan(
			id,
			name,
			timestampScanner(createdAt),
			shardSize,
		)
	})
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return configuration, err
}

// DeleteConfiguration deletes a configuration from the repository.
func (r *repo) DeleteConfiguration(ctx context.Context, configurationID id.ConfigurationID) error {
	_, err := r.db.ExecContext(ctx,
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
func (r *repo) ListConfigurations(ctx context.Context) ([]*configurationsmodel.Configuration, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, name, created_at, shard_size FROM configurations`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var configurations []*configurationsmodel.Configuration
	for rows.Next() {
		configuration, err := configurationsmodel.ReadConfigurationFromDatabase(func(id *id.ConfigurationID, name *string, createdAt *time.Time, shardSize *uint64) error {
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
func (r *repo) AddSourceToConfiguration(ctx context.Context, configurationID id.ConfigurationID, sourceID id.SourceID) error {
	_, err := r.db.ExecContext(ctx,
		`INSERT INTO configuration_sources (configuration_id, source_id) VALUES (?, ?)`,
		configurationID, sourceID,
	)
	if err != nil {
		return fmt.Errorf("failed to add source to configuration: %w", err)
	}
	return nil
}

// RemoveSourceFromConfiguration removes a source from a configuration in the repository.
func (r *repo) RemoveSourceFromConfiguration(ctx context.Context, configurationID id.ConfigurationID, sourceID id.SourceID) error {
	_, err := r.db.ExecContext(ctx,
		`DELETE FROM configuration_sources WHERE configuration_id = ? AND source_id = ?`,
		configurationID, sourceID,
	)
	if err != nil {
		return fmt.Errorf("failed to remove source from configuration: %w", err)
	}
	return nil
}
