package sqlrepo

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/storacha/guppy/pkg/preparation/sources"
	sourcemodel "github.com/storacha/guppy/pkg/preparation/sources/model"
	"github.com/storacha/guppy/pkg/preparation/types"
)

var _ sources.Repo = (*repo)(nil)

// CreateSource creates a new source in the repository with the given name, path, and options.
func (r *repo) CreateSource(ctx context.Context, name string, path string, options ...sourcemodel.SourceOption) (*sourcemodel.Source, error) {
	src, err := sourcemodel.NewSource(name, path, options...)
	if err != nil {
		return nil, fmt.Errorf("failed to create source model: %w", err)
	}

	// Grab a copy of the ID to get a slice of, to appease the DB driver.
	id := src.ID()
	_, err = r.db.ExecContext(
		ctx,
		`INSERT INTO sources (
			id,
			name,
			created_at,
			updated_at,
			kind,
			path,
			connection_params
		) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		id[:],
		src.Name(),
		src.CreatedAt().Unix(),
		src.UpdatedAt().Unix(),
		src.Kind(),
		src.Path(),
		src.ConnectionParams(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to insert source into database: %w", err)
	}
	return src, nil
}

// GetSourceByID retrieves a source by its unique ID from the repository.
func (r *repo) GetSourceByID(ctx context.Context, sourceID types.SourceID) (*sourcemodel.Source, error) {
	row := r.db.QueryRowContext(ctx,
		`SELECT
			id,
			name,
			created_at,
			updated_at,
			kind,
			path,
			connection_params
		FROM sources WHERE id = ?`, sourceID[:],
	)

	return r.getSourceFromRow(row)
}

// GetSourceByName retrieves a source by its name from the repository.
func (r *repo) GetSourceByName(ctx context.Context, name string) (*sourcemodel.Source, error) {
	row := r.db.QueryRowContext(ctx,
		`SELECT
			id,
			name,
			created_at,
			updated_at,
			kind,
			path,
			connection_params
		FROM sources WHERE name = ?`, name,
	)

	return r.getSourceFromRow(row)
}

func (r *repo) getSourceFromRow(row *sql.Row) (*sourcemodel.Source, error) {
	src, err := sourcemodel.ReadSourceFromDatabase(func(
		id *types.SourceID,
		name *string,
		createdAt,
		updatedAt *time.Time,
		kind *sourcemodel.SourceKind,
		path *string,
		connectionParamsBytes *[]byte,
	) error {
		err := row.Scan(
			id,
			name,
			timestampScanner(createdAt),
			timestampScanner(updatedAt),
			kind,
			path,
			connectionParamsBytes,
		)
		if err != nil {
			return fmt.Errorf("failed to scan source: %w", err)
		}

		return nil
	})
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return src, err
}

// UpdateSource updates the given source in the repository.
func (r *repo) UpdateSource(ctx context.Context, src *sourcemodel.Source) error {
	_, err := r.db.ExecContext(ctx,
		`UPDATE sources SET name = ?, updated_at = ?, kind = ?, path = ?, connection_params = ? WHERE id = ?`,
		src.Name(), src.UpdatedAt(), src.Kind(), src.Path(), src.ConnectionParams(), src.ID(),
	)
	return err
}
