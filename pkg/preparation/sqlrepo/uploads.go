package sqlrepo

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/guppy/pkg/bus/events"
	dagmodel "github.com/storacha/guppy/pkg/preparation/dags/model"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo/util"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	"github.com/storacha/guppy/pkg/preparation/uploads"
	"github.com/storacha/guppy/pkg/preparation/uploads/model"
)

var _ uploads.Repo = (*Repo)(nil)

// GetUploadByID retrieves an upload by its unique ID from the repository.
func (r *Repo) GetUploadByID(ctx context.Context, uploadID id.UploadID) (*model.Upload, error) {
	stmt, err := r.prepareStmt(ctx, `
		SELECT id, space_did, source_id, created_at, updated_at, root_fs_entry_id, root_cid
		FROM uploads
		WHERE id = ?
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare statement: %w", err)
	}
	row := stmt.QueryRowContext(ctx, uploadID)
	upload, err := model.ReadUploadFromDatabase(func(
		id *id.UploadID,
		spaceDID *did.DID,
		sourceID *id.SourceID,
		createdAt,
		updatedAt *time.Time,
		rootFSEntryID *id.FSEntryID,
		rootCID *cid.Cid,
	) error {
		err := row.Scan(
			id,
			util.DbDID(spaceDID),
			sourceID,
			util.TimestampScanner(createdAt),
			util.TimestampScanner(updatedAt),
			rootFSEntryID,
			util.DbCID(rootCID),
		)
		if err != nil {
			return err
		}
		return nil
	})
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return upload, err
}

// FindOrCreateUploads creates uploads for a given space and source IDs.
func (r *Repo) FindOrCreateUploads(ctx context.Context, spaceDID did.DID, sourceIDs []id.SourceID) ([]*model.Upload, error) {
	if len(sourceIDs) == 0 {
		return nil, nil
	}
	insertQuery, err := r.prepareStmt(ctx, `
			INSERT INTO uploads (
				id,
				space_did,
				source_id,
				created_at,
				updated_at,
				root_fs_entry_id,
				root_cid
			)
			VALUES (?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(space_did, source_id)
				-- Force conflicts to return the row rather than completely ignore it.
				DO UPDATE SET id = uploads.id
			RETURNING
				id,
				space_did,
				source_id,
				created_at,
				updated_at,
				root_fs_entry_id,
				root_cid
			`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare statement: %w", err)
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	insertQuery = tx.StmtContext(ctx, insertQuery)
	var uploads []*model.Upload
	for _, sourceID := range sourceIDs {
		upload, err := model.NewUpload(spaceDID, sourceID)
		if err != nil {
			return nil, fmt.Errorf("failed to instantiate upload for space %s and source %s: %w", spaceDID, sourceID, err)
		}

		err = model.WriteUploadToDatabase(func(
			uploadID id.UploadID,
			spaceDID did.DID,
			sourceID id.SourceID,
			createdAt,
			updatedAt time.Time,
			rootFSEntryID id.FSEntryID,
			rootCID cid.Cid,
		) error {
			row := insertQuery.QueryRowContext(ctx,
				uploadID,
				util.DbDID(&spaceDID),
				sourceID,
				createdAt.Unix(),
				updatedAt.Unix(),
				util.DbID(&rootFSEntryID),
				util.DbCID(&rootCID),
			)

			readUpload, err := model.ReadUploadFromDatabase(func(
				id *id.UploadID,
				spaceDID *did.DID,
				sourceID *id.SourceID,
				createdAt,
				updatedAt *time.Time,
				rootFSEntryID *id.FSEntryID,
				rootCID *cid.Cid,
			) error {
				return row.Scan(
					id,
					util.DbDID(spaceDID),
					sourceID,
					util.TimestampScanner(createdAt),
					util.TimestampScanner(updatedAt),
					rootFSEntryID,
					util.DbCID(rootCID),
				)
			})
			if err != nil {
				return err
			}
			uploads = append(uploads, readUpload)
			return nil
		}, upload)

		if err != nil {
			return nil, fmt.Errorf("failed to write upload to database for space %s and source %s: %w", spaceDID, sourceID, err)
		}
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return uploads, nil
}

// UpdateUpload implements uploads.Repo.
func (r *Repo) UpdateUpload(ctx context.Context, upload *model.Upload) error {
	return model.WriteUploadToDatabase(func(id id.UploadID, spaceDID did.DID, sourceID id.SourceID, createdAt, updatedAt time.Time, rootFSEntryID id.FSEntryID, rootCID cid.Cid) error {
		stmt, err := r.prepareStmt(ctx, `
			UPDATE uploads
			SET space_did = ?, source_id = ?, created_at = ?, updated_at = ?, root_fs_entry_id = ?, root_cid = ?
			WHERE id = ?
		`)
		if err != nil {
			return fmt.Errorf("failed to prepare statement: %w", err)
		}
		_, err = stmt.ExecContext(ctx, util.DbDID(&spaceDID), sourceID, createdAt.Unix(), updatedAt.Unix(), util.DbID(&rootFSEntryID), util.DbCID(&rootCID), id)
		return err
	}, upload)
}

func (r *Repo) CIDForFSEntry(ctx context.Context, fsEntryID id.FSEntryID) (cid.Cid, error) {
	stmt, err := r.prepareStmt(ctx, `
		SELECT fs_entry_id, upload_id, space_did, created_at, updated_at, cid, kind
		FROM dag_scans
		WHERE fs_entry_id = ?
	`)
	if err != nil {
		return cid.Undef, fmt.Errorf("failed to prepare statement: %w", err)
	}
	row := stmt.QueryRowContext(ctx, fsEntryID)
	ds, err := dagmodel.ReadDAGScanFromDatabase(r.dagScanScanner(row))
	if err != nil {
		return cid.Undef, err
	}
	if !ds.CID().Defined() {
		return cid.Undef, fmt.Errorf("DAG scan for fs entry %s is not completed", fsEntryID)
	}
	return ds.CID(), nil
}

func (r *Repo) newDAGScan(fsEntryID id.FSEntryID, isDirectory bool, uploadID id.UploadID, spaceDID did.DID) (dagmodel.DAGScan, error) {
	if isDirectory {
		return dagmodel.NewDirectoryDAGScan(fsEntryID, uploadID, spaceDID)
	}
	return dagmodel.NewFileDAGScan(fsEntryID, uploadID, spaceDID)
}

func (r *Repo) CreateDAGScan(ctx context.Context, fsEntryID id.FSEntryID, isDirectory bool, uploadID id.UploadID, spaceDID did.DID) (dagmodel.DAGScan, error) {
	log.Debugf("Creating DAG scan for fsEntryID: %s, isDirectory: %t, uploadID: %s", fsEntryID, isDirectory, uploadID)
	dagScan, err := r.newDAGScan(fsEntryID, isDirectory, uploadID, spaceDID)
	if err != nil {
		return nil, err
	}

	return dagScan, dagmodel.WriteDAGScanToDatabase(dagScan, func(
		kind string,
		fsEntryID id.FSEntryID,
		uploadID id.UploadID,
		spaceDID did.DID,
		createdAt time.Time,
		updatedAt time.Time,
		cid cid.Cid,
	) error {
		stmt, err := r.prepareStmt(ctx, `
			INSERT INTO dag_scans (kind, fs_entry_id, upload_id, space_did, created_at, updated_at, cid)
			VALUES (?, ?, ?, ?, ?, ?, ?)
		`)
		if err != nil {
			return fmt.Errorf("failed to prepare statement: %w", err)
		}
		_, err = stmt.ExecContext(ctx, kind, fsEntryID, uploadID, util.DbDID(&spaceDID), createdAt.Unix(), updatedAt.Unix(), util.DbCID(&cid))
		r.bus.Publish(events.TopicDagScan(uploadID), events.DAGScanView{
			FSEntryID: fsEntryID,
			Created:   createdAt,
			Updated:   updatedAt,
			CID:       cid,
		})
		return err
	})
}

// ListSpaceSources lists all sources associated with a given space DID.
func (r *Repo) ListSpaceSources(ctx context.Context, spaceDID did.DID) ([]id.SourceID, error) {
	stmt, err := r.prepareStmt(ctx, `
		SELECT cs.source_id
		FROM space_sources cs
		WHERE cs.space_did = ?
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare statement: %w", err)
	}
	rows, err := stmt.QueryContext(ctx, util.DbDID(&spaceDID))
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
