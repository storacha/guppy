package sqlrepo

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/ipfs/go-cid"
	dagmodel "github.com/storacha/guppy/pkg/preparation/dags/model"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo/util"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	"github.com/storacha/guppy/pkg/preparation/uploads"
	"github.com/storacha/guppy/pkg/preparation/uploads/model"
)

var _ uploads.Repo = (*repo)(nil)

// GetUploadByID retrieves an upload by its unique ID from the repository.
func (r *repo) GetUploadByID(ctx context.Context, uploadID id.UploadID) (*model.Upload, error) {
	row := r.db.QueryRowContext(ctx,
		`SELECT
			id,
			configuration_id,
			source_id,
			created_at,
			updated_at,
			state,
			error_message,
			root_fs_entry_id,
			root_cid
		FROM uploads
		WHERE id = ?`,
		uploadID,
	)
	upload, err := model.ReadUploadFromDatabase(func(
		id,
		configurationID,
		sourceID *id.SourceID,
		createdAt,
		updatedAt *time.Time,
		state *model.UploadState,
		errorMessage **string,
		rootFSEntryID **id.FSEntryID,
		rootCID *cid.Cid,
	) error {
		var nullErrorMessage sql.NullString
		err := row.Scan(
			id,
			configurationID,
			sourceID,
			util.TimestampScanner(createdAt),
			util.TimestampScanner(updatedAt),
			state,
			&nullErrorMessage,
			rootFSEntryID,
			util.DbCid(rootCID),
		)
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
func (r *repo) GetSourceIDForUploadID(ctx context.Context, uploadID id.UploadID) (id.SourceID, error) {
	row := r.db.QueryRowContext(ctx,
		`SELECT source_id FROM uploads WHERE id = ?`, uploadID,
	)
	var sourceID id.SourceID
	err := row.Scan(&sourceID)
	if err != nil {
		return id.SourceID{}, err
	}
	return sourceID, nil
}

// CreateUploads creates uploads for a given configuration and source IDs.
func (r *repo) CreateUploads(ctx context.Context, configurationID id.ConfigurationID, sourceIDs []id.SourceID) ([]*model.Upload, error) {
	var uploads []*model.Upload
	for _, sourceID := range sourceIDs {
		upload, err := model.NewUpload(configurationID, sourceID)
		if err != nil {
			return nil, fmt.Errorf("failed to instantiate upload for configuration %s and source %s: %w", configurationID, sourceID, err)
		}

		insertQuery := `
			INSERT INTO uploads (
				id,
				configuration_id,
				source_id,
				created_at,
				updated_at,
				state,
				error_message,
				root_fs_entry_id,
				root_cid
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`

		err = model.WriteUploadToDatabase(func(
			id,
			configurationID,
			sourceID id.SourceID,
			createdAt,
			updatedAt time.Time,
			state model.UploadState,
			errorMessage *string,
			rootFSEntryID *id.FSEntryID,
			rootCID cid.Cid,
		) error {
			_, err := r.db.ExecContext(ctx,
				insertQuery,
				id,
				configurationID,
				sourceID,
				createdAt.Unix(),
				updatedAt.Unix(),
				state,
				NullString(errorMessage),
				Null(rootFSEntryID),
				util.DbCid(&rootCID),
			)
			return err
		}, upload)
		if err != nil {
			return nil, fmt.Errorf("failed to write upload to database for configuration %s and source %s: %w", configurationID, sourceID, err)
		}
		uploads = append(uploads, upload)
	}
	return uploads, nil
}

// UpdateUpload implements uploads.Repo.
func (r *repo) UpdateUpload(ctx context.Context, upload *model.Upload) error {
	updateQuery := `UPDATE uploads SET configuration_id = $2, source_id = $3, created_at = $4, updated_at = $5, state = $6, error_message = $7, root_fs_entry_id = $8, root_cid = $9 WHERE id = $1`
	return model.WriteUploadToDatabase(func(id, configurationID, sourceID id.UploadID, createdAt, updatedAt time.Time, state model.UploadState, errorMessage *string, rootFSEntryID *id.FSEntryID, rootCID cid.Cid) error {
		_, err := r.db.ExecContext(ctx,
			updateQuery,
			id, configurationID, sourceID, createdAt.Unix(), updatedAt.Unix(), state, NullString(errorMessage), Null(rootFSEntryID), util.DbCid(&rootCID))
		return err
	}, upload)
}

func (r *repo) CIDForFSEntry(ctx context.Context, fsEntryID id.FSEntryID) (cid.Cid, error) {

	query := `SELECT fs_entry_id, upload_id, created_at, updated_at, state, error_message, cid, kind FROM dag_scans WHERE fs_entry_id = $1`
	row := r.db.QueryRowContext(ctx, query, fsEntryID)
	ds, err := dagmodel.ReadDAGScanFromDatabase(r.dagScanScanner(row))
	if err != nil {
		return cid.Undef, err
	}
	if ds.State() != dagmodel.DAGScanStateCompleted {
		return cid.Undef, uploads.IncompleteDagScanError{DagScan: ds}
	}
	return ds.CID(), nil
}

func (r *repo) newDAGScan(fsEntryID id.FSEntryID, isDirectory bool, uploadID id.UploadID) (dagmodel.DAGScan, error) {
	if isDirectory {
		return dagmodel.NewDirectoryDAGScan(fsEntryID, uploadID)
	}
	return dagmodel.NewFileDAGScan(fsEntryID, uploadID)
}

func (r *repo) CreateDAGScan(ctx context.Context, fsEntryID id.FSEntryID, isDirectory bool, uploadID id.UploadID) (dagmodel.DAGScan, error) {
	log.Debugf("Creating DAG scan for fsEntryID: %s, isDirectory: %t, uploadID: %s", fsEntryID, isDirectory, uploadID)
	dagScan, err := r.newDAGScan(fsEntryID, isDirectory, uploadID)
	if err != nil {
		return nil, err
	}

	return dagScan, dagmodel.WriteDAGScanToDatabase(dagScan, func(kind string, fsEntryID id.FSEntryID, uploadID id.UploadID, createdAt time.Time, updatedAt time.Time, errorMessage *string, state dagmodel.DAGScanState, cid cid.Cid) error {
		_, err := r.db.ExecContext(ctx,
			`INSERT INTO dag_scans (kind, fs_entry_id, upload_id, created_at, updated_at, error_message, state, cid) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			kind,
			fsEntryID,
			uploadID,
			createdAt.Unix(),
			updatedAt.Unix(),
			errorMessage,
			state,
			util.DbCid(&cid),
		)
		return err
	})
}

// ListConfigurationSources lists all sources associated with a given configuration ID.
func (r *repo) ListConfigurationSources(ctx context.Context, configurationID id.ConfigurationID) ([]id.SourceID, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT cs.source_id
		FROM configuration_sources cs
		WHERE cs.configuration_id = ?`, configurationID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var sources []id.SourceID
	for rows.Next() {
		var sourceID id.SourceID
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
