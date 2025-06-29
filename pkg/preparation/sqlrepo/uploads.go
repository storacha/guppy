package sqlrepo

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/storacha/guppy/pkg/preparation/types"
	"github.com/storacha/guppy/pkg/preparation/uploads"
	"github.com/storacha/guppy/pkg/preparation/uploads/model"
)

var _ uploads.Repo = (*repo)(nil)

// GetUploadByID retrieves an upload by its unique ID from the repository.
func (r *repo) GetUploadByID(ctx context.Context, uploadID types.UploadID) (*model.Upload, error) {
	row := r.db.QueryRowContext(ctx,
		`SELECT id, configuration_id, source_id, created_at, updated_at, state, error_message FROM uploads WHERE id = ?`, uploadID,
	)
	upload, err := model.ReadUploadFromDatabase(func(id, configurationID, sourceID *types.SourceID, createdAt, updatedAt *time.Time, state *model.UploadState, errorMessage **string) error {
		var nullErrorMessage sql.NullString
		err := row.Scan(id, configurationID, sourceID, timestampScanner(createdAt), timestampScanner(updatedAt), state, &nullErrorMessage)
		if err != nil {
			return err
		}
		if nullErrorMessage.Valid {
			*errorMessage = &nullErrorMessage.String
		} else {
			*errorMessage = nil
		}
		return nil
	})
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return upload, err
}

// GetSourceIDForUploadID retrieves the source ID associated with a given upload ID.
func (r *repo) GetSourceIDForUploadID(ctx context.Context, uploadID types.UploadID) (types.SourceID, error) {
	row := r.db.QueryRowContext(ctx,
		`SELECT source_id FROM uploads WHERE id = ?`, uploadID,
	)
	var sourceID types.SourceID
	err := row.Scan(&sourceID)
	if err != nil {
		return types.SourceID{}, err
	}
	return sourceID, nil
}

// CreateUploads creates uploads for a given configuration and source IDs.
func (r *repo) CreateUploads(ctx context.Context, configurationID types.ConfigurationID, sourceIDs []types.SourceID) ([]*model.Upload, error) {
	var uploads []*model.Upload
	for _, sourceID := range sourceIDs {
		upload, err := model.NewUpload(configurationID, sourceID)
		if err != nil {
			return nil, err
		}
		insertQuery := `INSERT INTO uploads (id, configuration_id, source_id, created_at, updated_at, state, error_message) VALUES (?, ?, ?, ?, ?, ?, ?)`
		err = model.WriteUploadToDatabase(func(id, configurationID, sourceID types.SourceID, createdAt, updatedAt time.Time, state model.UploadState, errorMessage *string) error {
			_, err := r.db.ExecContext(ctx,
				insertQuery, id, configurationID, sourceID, createdAt.Unix(), updatedAt.Unix(), state, NullString(errorMessage))
			return err
		}, upload)
		if err != nil {
			return nil, err
		}
		uploads = append(uploads, upload)
	}
	return uploads, nil
}

// UpdateUpload implements uploads.Repo.
func (r *repo) UpdateUpload(ctx context.Context, upload *model.Upload) error {
	updateQuery := `UPDATE uploads SET configuration_id = $2, source_id = $3, created_at = $4, updated_at = $5, state = $6, error_message = $7 WHERE id = $1`
	return model.WriteUploadToDatabase(func(id, configurationID, sourceID types.UploadID, createdAt, updatedAt time.Time, state model.UploadState, errorMessage *string) error {
		_, err := r.db.ExecContext(ctx,
			updateQuery,
			id, configurationID, sourceID, createdAt.Unix(), updatedAt.Unix(), state, NullString(errorMessage))
		return err
	}, upload)
}
