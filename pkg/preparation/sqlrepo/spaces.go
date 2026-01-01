package sqlrepo

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/guppy/pkg/preparation/spaces"
	spacesmodel "github.com/storacha/guppy/pkg/preparation/spaces/model"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo/util"
	"github.com/storacha/guppy/pkg/preparation/types/id"
)

var _ spaces.Repo = (*Repo)(nil)

// FindOrCreateSpace finds an existing space or creates a new one in the repository with the given name and options.
func (r *Repo) FindOrCreateSpace(ctx context.Context, did did.DID, name string, options ...spacesmodel.SpaceOption) (*spacesmodel.Space, error) {
	space, err := r.GetSpaceByDID(ctx, did)
	if err != nil {
		return nil, fmt.Errorf("failed to get space by DID: %w", err)
	}
	if space != nil {
		return space, nil
	}
	space, err = spacesmodel.NewSpace(did, name, options...)
	if err != nil {
		return nil, fmt.Errorf("failed to create space model: %w", err)
	}

	stmt, err := r.prepareStmt(ctx, `INSERT INTO spaces (did, name, created_at, shard_size) VALUES (?, ?, ?, ?)`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare insert space statement: %w", err)
	}

	spaceDID := space.DID()
	_, err = stmt.ExecContext(ctx,
		util.DbDID(&spaceDID),
		space.Name(),
		space.CreatedAt().Unix(),
		space.ShardSize(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to insert space into database: %w", err)
	}
	return space, nil
}

// GetSpaceByDID retrieves a space by its unique DID from the repository.
func (r *Repo) GetSpaceByDID(ctx context.Context, spaceDID did.DID) (*spacesmodel.Space, error) {
	stmt, err := r.prepareStmt(ctx, `SELECT did, name, created_at, shard_size FROM spaces WHERE did = ?`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare get space by DID statement: %w", err)
	}
	row := stmt.QueryRowContext(ctx, util.DbDID(&spaceDID))
	return r.getSpaceFromRow(row)
}

// GetSpaceByName retrieves a space by its name from the repository.
func (r *Repo) GetSpaceByName(ctx context.Context, name string) (*spacesmodel.Space, error) {
	stmt, err := r.prepareStmt(ctx, `SELECT did, name, created_at, shard_size FROM spaces WHERE name = ?`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare get space by name statement: %w", err)
	}
	row := stmt.QueryRowContext(ctx, name)
	return r.getSpaceFromRow(row)
}

func (r *Repo) getSpaceFromRow(row *sql.Row) (*spacesmodel.Space, error) {
	space, err := spacesmodel.ReadSpaceFromDatabase(func(
		did *did.DID,
		name *string,
		createdAt *time.Time,
		shardSize *uint64,
	) error {
		return row.Scan(
			util.DbDID(did),
			name,
			util.TimestampScanner(createdAt),
			shardSize,
		)
	})
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return space, err
}

// DeleteSpace deletes a space from the repository.
func (r *Repo) DeleteSpace(ctx context.Context, spaceDID did.DID) error {
	stmt, err := r.prepareStmt(ctx, `DELETE FROM spaces WHERE did = ?`)
	if err != nil {
		return fmt.Errorf("failed to prepare delete space statement: %w", err)
	}
	_, err = stmt.ExecContext(ctx, util.DbDID(&spaceDID))
	if err != nil {
		return err
	}
	// Also delete associated space sources
	stmt, err = r.prepareStmt(ctx, `DELETE FROM space_sources WHERE space_did = ?`)
	if err != nil {
		return fmt.Errorf("failed to prepare delete space sources statement: %w", err)
	}
	_, err = stmt.ExecContext(ctx, util.DbDID(&spaceDID))
	return err
}

// ListSpaces lists all spaces in the repository.
func (r *Repo) ListSpaces(ctx context.Context) ([]*spacesmodel.Space, error) {
	stmt, err := r.prepareStmt(ctx, `SELECT did, name, created_at, shard_size FROM spaces`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare list spaces statement: %w", err)
	}
	rows, err := stmt.QueryContext(ctx)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var spaces []*spacesmodel.Space
	for rows.Next() {
		space, err := spacesmodel.ReadSpaceFromDatabase(func(did *did.DID, name *string, createdAt *time.Time, shardSize *uint64) error {
			return rows.Scan(util.DbDID(did), name, createdAt, shardSize)
		})
		if err != nil {
			return nil, err
		}
		if space == nil {
			continue
		}
		spaces = append(spaces, space)
	}
	return spaces, nil
}

// AddSourceToSpace adds a source to a space in the repository.
func (r *Repo) AddSourceToSpace(ctx context.Context, spaceDID did.DID, sourceID id.SourceID) error {
	stmt, err := r.prepareStmt(ctx, `INSERT INTO space_sources (space_did, source_id) VALUES (?, ?)`)
	if err != nil {
		return fmt.Errorf("failed to prepare add source to space statement: %w", err)
	}
	_, err = stmt.ExecContext(ctx, util.DbDID(&spaceDID), sourceID)
	if err != nil {
		return fmt.Errorf("failed to add source to space: %w", err)
	}
	return nil
}

// RemoveSourceFromSpace removes a source from a space in the repository.
func (r *Repo) RemoveSourceFromSpace(ctx context.Context, spaceDID did.DID, sourceID id.SourceID) error {
	stmt, err := r.prepareStmt(ctx, `DELETE FROM space_sources WHERE space_did = ? AND source_id = ?`)
	if err != nil {
		return fmt.Errorf("failed to prepare remove source from space statement: %w", err)
	}
	_, err = stmt.ExecContext(ctx, util.DbDID(&spaceDID), sourceID)
	if err != nil {
		return fmt.Errorf("failed to remove source from space: %w", err)
	}
	return nil
}
