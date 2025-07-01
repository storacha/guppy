package sqlrepo

import (
	"context"
	"database/sql"
	"errors"
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
		return nil, err
	}
	_, err = r.db.ExecContext(ctx,
		`INSERT INTO sources (id, name, created_at, updated_at, kind, path, connection_params) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		src.ID(), src.Name(), src.CreatedAt(), src.UpdatedAt(), src.Kind(), src.Path(), src.ConnectionParams(),
	)
	if err != nil {
		return nil, err
	}
	return src, nil
}

// GetSourceByID retrieves a source by its unique ID from the repository.
func (r *repo) GetSourceByID(ctx context.Context, sourceID types.SourceID) (*sourcemodel.Source, error) {
	row := r.db.QueryRowContext(ctx,
		`SELECT id, name, created_at, updated_at, kind, path, connection_params FROM sources WHERE id = ?`, sourceID,
	)
	src, err := sourcemodel.ReadSourceFromDatabase(func(id *types.SourceID, name *string, createdAt, updatedAt *time.Time, kind *sourcemodel.SourceKind, path *string, connectionParams *sourcemodel.ConnectionParams) error {
		var ca, ua sql.NullTime
		err := row.Scan(id, name, &ca, &ua, kind, path, connectionParams)
		if err != nil {
			return err
		}
		if ca.Valid {
			*createdAt = ca.Time
		}
		if ua.Valid {
			*updatedAt = ua.Time
		}
		return nil
	})
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return src, err
}

// GetSourceByName retrieves a source by its name from the repository.
func (r *repo) GetSourceByName(ctx context.Context, name string) (*sourcemodel.Source, error) {
	row := r.db.QueryRowContext(ctx,
		`SELECT id, name, created_at, updated_at, kind, path, connection_params FROM sources WHERE name = ?`, name,
	)
	src, err := sourcemodel.ReadSourceFromDatabase(func(id *types.SourceID, name *string, createdAt, updatedAt *time.Time, kind *sourcemodel.SourceKind, path *string, connectionParams *sourcemodel.ConnectionParams) error {
		var ca, ua sql.NullTime
		err := row.Scan(id, name, &ca, &ua, kind, path, connectionParams)
		if err != nil {
			return err
		}
		if ca.Valid {
			*createdAt = ca.Time
		}
		if ua.Valid {
			*updatedAt = ua.Time
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
