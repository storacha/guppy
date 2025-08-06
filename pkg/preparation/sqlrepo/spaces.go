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

var _ spaces.Repo = (*repo)(nil)

// CreateSpace creates a new space in the repository with the given name and options.
func (r *repo) CreateSpace(ctx context.Context, name string, options ...spacesmodel.SpaceOption) (*spacesmodel.Space, error) {
	space, err := spacesmodel.NewSpace(name, options...)
	if err != nil {
		return nil, fmt.Errorf("failed to create space model: %w", err)
	}

	_, err = r.db.ExecContext(ctx,
		`INSERT INTO spaces (
			did,
			name,
			created_at,
			shard_size
		) VALUES (?, ?, ?, ?)`,
		space.DID(),
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
func (r *repo) GetSpaceByDID(ctx context.Context, spaceDID did.DID) (*spacesmodel.Space, error) {
	row := r.db.QueryRowContext(ctx,
		`SELECT
			did,
			name,
			created_at,
			shard_size
		FROM spaces WHERE did = ?`, spaceDID,
	)
	return r.getSpaceFromRow(row)
}

// GetSpaceByUploadID retrieves the space associated with an upload.
func (r *repo) GetSpaceByUploadID(ctx context.Context, uploadID id.UploadID) (*spacesmodel.Space, error) {
	row := r.db.QueryRowContext(ctx,
		`SELECT
			s.id,
			s.name,
			s.created_at,
			s.shard_size
		FROM spaces s
		INNER JOIN uploads u ON u.space_id = s.id
		WHERE u.id = ?`, uploadID,
	)
	return r.getSpaceFromRow(row)
}

// GetSpaceByName retrieves a space by its name from the repository.
func (r *repo) GetSpaceByName(ctx context.Context, name string) (*spacesmodel.Space, error) {
	row := r.db.QueryRowContext(ctx,
		`SELECT
			did,
			name,
			created_at,
			shard_size
		FROM spaces WHERE name = ?`, name,
	)
	return r.getSpaceFromRow(row)
}

func (r *repo) getSpaceFromRow(row *sql.Row) (*spacesmodel.Space, error) {
	space, err := spacesmodel.ReadSpaceFromDatabase(func(
		did *did.DID,
		name *string,
		createdAt *time.Time,
		shardSize *uint64,
	) error {
		return row.Scan(
			did,
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
func (r *repo) DeleteSpace(ctx context.Context, spaceDID did.DID) error {
	_, err := r.db.ExecContext(ctx,
		`DELETE FROM spaces WHERE did = ?`,
		spaceDID,
	)
	if err != nil {
		return err
	}
	// Also delete associated space sources
	_, err = r.db.Exec(
		`DELETE FROM space_sources WHERE space_did = ?`,
		spaceDID,
	)
	return err
}

// ListSpaces lists all spaces in the repository.
func (r *repo) ListSpaces(ctx context.Context) ([]*spacesmodel.Space, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT did, name, created_at, shard_size FROM spaces`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var spaces []*spacesmodel.Space
	for rows.Next() {
		space, err := spacesmodel.ReadSpaceFromDatabase(func(did *did.DID, name *string, createdAt *time.Time, shardSize *uint64) error {
			return rows.Scan(did, name, createdAt, shardSize)
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
func (r *repo) AddSourceToSpace(ctx context.Context, spaceDID did.DID, sourceID id.SourceID) error {
	_, err := r.db.ExecContext(ctx,
		`INSERT INTO space_sources (space_did, source_id) VALUES (?, ?)`,
		spaceDID, sourceID,
	)
	if err != nil {
		return fmt.Errorf("failed to add source to space: %w", err)
	}
	return nil
}

// RemoveSourceFromSpace removes a source from a space in the repository.
func (r *repo) RemoveSourceFromSpace(ctx context.Context, spaceDID did.DID, sourceID id.SourceID) error {
	_, err := r.db.ExecContext(ctx,
		`DELETE FROM space_sources WHERE space_did = ? AND source_id = ?`,
		spaceDID, sourceID,
	)
	if err != nil {
		return fmt.Errorf("failed to remove source from space: %w", err)
	}
	return nil
}
