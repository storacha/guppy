package sqlrepo

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/storacha/guppy/pkg/preparation/sources"
	sourcemodel "github.com/storacha/guppy/pkg/preparation/sources/model"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo/util"
	"github.com/storacha/guppy/pkg/preparation/types/id"
)

var _ sources.Repo = (*Repo)(nil)

// CreateSource creates a new source in the repository with the given name, path, and options.
func (r *Repo) CreateSource(ctx context.Context, name string, path string, options ...sourcemodel.SourceOption) (*sourcemodel.Source, error) {
	// resolve the specific path to its absolute path
	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve absolute path from %s: %w", path, err)
	}

	// reuse an existing source pointing at the same path if one already exists
	// NB(forrest): one intention of the source abstraction is that they may have different kinds
	// at present, there is only a local kind, so the existence check on a normalized path here is sufficient.
	// In the future when sources may have different kinds this check will need adjustment.
	existing, err := r.getSourceByPath(ctx, absPath)
	if err != nil {
		return nil, fmt.Errorf("failed to check existing sources by path: %w", err)
	}
	if existing != nil {
		return existing, nil
	}

	src, err := sourcemodel.NewSource(name, absPath, options...)
	if err != nil {
		return nil, fmt.Errorf("failed to create source model: %w", err)
	}

	stmt, err := r.prepareStmt(ctx, `
		INSERT INTO sources (id, name, created_at, updated_at, kind, path, connection_params)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare insert source statement: %w", err)
	}

	connectionParams := src.ConnectionParams()
	_, err = stmt.ExecContext(
		ctx,
		src.ID(),
		src.Name(),
		src.CreatedAt().Unix(),
		src.UpdatedAt().Unix(),
		src.Kind(),
		src.Path(),
		util.DbBytes(&connectionParams),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to insert source into database: %w", err)
	}
	return src, nil
}

// GetSourceByID retrieves a source by its unique ID from the repository.
func (r *Repo) GetSourceByID(ctx context.Context, sourceID id.SourceID) (*sourcemodel.Source, error) {
	stmt, err := r.prepareStmt(ctx, `
		SELECT id, name, created_at, updated_at, kind, path, connection_params
		FROM sources
		WHERE id = ?
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare get source by ID statement: %w", err)
	}
	row := stmt.QueryRowContext(ctx, sourceID)
	return r.getSourceFromRow(row)
}

// GetSourceByName retrieves a source by its name from the repository.
func (r *Repo) GetSourceByName(ctx context.Context, name string) (*sourcemodel.Source, error) {
	stmt, err := r.prepareStmt(ctx, `
		SELECT id, name, created_at, updated_at, kind, path, connection_params
		FROM sources
		WHERE name = ?
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare get source by name statement: %w", err)
	}
	row := stmt.QueryRowContext(ctx, name)
	return r.getSourceFromRow(row)
}

func (r *Repo) getSourceByPath(ctx context.Context, path string) (*sourcemodel.Source, error) {
	stmt, err := r.prepareStmt(ctx, `
		SELECT id, name, created_at, updated_at, kind, path, connection_params
		FROM sources
		WHERE path = ?
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare get source by path statement: %w", err)
	}
	row := stmt.QueryRowContext(ctx, path)
	return r.getSourceFromRow(row)
}

func (r *Repo) getSourceFromRow(row *sql.Row) (*sourcemodel.Source, error) {
	src, err := sourcemodel.ReadSourceFromDatabase(func(
		id *id.SourceID,
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
			util.TimestampScanner(createdAt),
			util.TimestampScanner(updatedAt),
			kind,
			path,
			util.DbBytes(connectionParamsBytes),
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
func (r *Repo) UpdateSource(ctx context.Context, src *sourcemodel.Source) error {
	connectionParams := src.ConnectionParams()
	stmt, err := r.prepareStmt(ctx, `
		UPDATE sources
		SET name = ?, updated_at = ?, kind = ?, path = ?, connection_params = ?
		WHERE id = ?
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare update source statement: %w", err)
	}
	_, err = stmt.ExecContext(ctx,
		src.Name(), src.UpdatedAt(), src.Kind(), src.Path(), util.DbBytes(&connectionParams), src.ID(),
	)
	return err
}
