package sqlrepo

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"time"

	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/guppy/pkg/bus/events"
	scanmodel "github.com/storacha/guppy/pkg/preparation/scans/model"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo/util"

	"github.com/storacha/guppy/pkg/preparation/types/id"
)

// FindOrCreateFile finds or creates a file entry in the repository with the given parameters.
// If the file already exists, it returns the existing file and false.
// If the file does not exist, it creates a new file entry and returns it along with true.
func (r *Repo) FindOrCreateFile(ctx context.Context, path string, lastModified time.Time, mode fs.FileMode, size uint64, checksum []byte, sourceID id.SourceID, spaceDID did.DID) (*scanmodel.File, bool, error) {
	if mode.IsDir() {
		return nil, false, errors.New("cannot create a file with directory mode")
	}
	entry, created, err := r.insertOrGetFSEntry(ctx, path, lastModified, mode, size, checksum, sourceID, spaceDID)
	if err != nil {
		return nil, false, fmt.Errorf("failed to find or create file entry: %w", err)
	}
	if file, ok := entry.(*scanmodel.File); ok {
		r.bus.Publish(events.TopicFsEntry(sourceID), events.FSScanView{
			Path:      file.Path(),
			IsDir:     false,
			Size:      file.Size(),
			FSEntryID: file.ID(),
		})
		return file, created, nil
	}
	return nil, false, errors.New("found entry is not a file")
}

// FindOrCreateDirectory finds or creates a directory entry in the repository with the given parameters.
// If the directory already exists, it returns the existing directory and false.
// If the directory does not exist, it creates a new directory entry and returns it along with true.
func (r *Repo) FindOrCreateDirectory(ctx context.Context, path string, lastModified time.Time, mode fs.FileMode, checksum []byte, sourceID id.SourceID, spaceDID did.DID) (*scanmodel.Directory, bool, error) {
	log.Debugf("Finding or creating directory: %s", path)
	if !mode.IsDir() {
		return nil, false, errors.New("cannot create a directory with file mode")
	}
	entry, created, err := r.insertOrGetFSEntry(ctx, path, lastModified, mode, 0, checksum, sourceID, spaceDID) // size is not used for directories
	if err != nil {
		return nil, false, fmt.Errorf("failed to find or create directory entry: %w", err)
	}
	if dir, ok := entry.(*scanmodel.Directory); ok {
		if created {
			log.Debugf("Created new directory %s: %s", path, dir.ID())
		}
		r.bus.Publish(events.TopicFsEntry(sourceID), events.FSScanView{
			Path:      dir.Path(),
			IsDir:     true,
			Size:      0,
			FSEntryID: dir.ID(),
		})
		return dir, created, nil
	}
	return nil, false, errors.New("found entry is not a directory")
}

// CreateDirectoryChildren links a directory to its children in the repository.
func (r *Repo) CreateDirectoryChildren(ctx context.Context, parent *scanmodel.Directory, children []scanmodel.FSEntry) error {
	if len(children) == 0 {
		return nil
	}
	insertQuery, err := r.prepareStmt(ctx, `
		INSERT INTO directory_children (directory_id, child_id)
		VALUES (?, ?)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	insertQuery = tx.StmtContext(ctx, insertQuery)

	for _, child := range children {
		_, err := insertQuery.ExecContext(ctx, parent.ID(), child.ID())
		if err != nil {
			return fmt.Errorf("failed to insert directory child relationship for parent %s, child %s: %w", parent.ID(), child.ID(), err)
		}
	}

	return tx.Commit()
}

// DirectoryChildren retrieves the children of a directory from the repository.
func (r *Repo) DirectoryChildren(ctx context.Context, dir *scanmodel.Directory) ([]scanmodel.FSEntry, error) {
	stmt, err := r.prepareStmt(ctx, `
		SELECT fse.id, fse.path, fse.last_modified, fse.mode, fse.size, fse.checksum, fse.source_id, fse.space_did
		FROM directory_children dc
		JOIN fs_entries fse ON dc.child_id = fse.id
		WHERE dc.directory_id = ?
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare statement: %w", err)
	}
	rows, err := stmt.QueryContext(ctx, dir.ID())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []scanmodel.FSEntry
	for rows.Next() {
		entry, err := scanmodel.ReadFSEntryFromDatabase(func(
			id *id.FSEntryID,
			path *string,
			lastModified *time.Time,
			mode *fs.FileMode,
			size *uint64,
			checksum *[]byte,
			sourceID *id.SourceID,
			spaceDID *did.DID,
		) error {
			return rows.Scan(
				util.DbID(id),
				path,
				util.TimestampScanner(lastModified),
				mode,
				size,
				util.DbBytes(checksum),
				util.DbID(sourceID),
				util.DbDID(spaceDID),
			)
		})
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// GetFileByID retrieves a file by its unique ID from the repository.
func (r *Repo) GetFileByID(ctx context.Context, fileID id.FSEntryID) (*scanmodel.File, error) {
	stmt, err := r.prepareStmt(ctx, `
		SELECT id, path, last_modified, mode, size, checksum, source_id, space_did
		FROM fs_entries
		WHERE id = ?
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare statement: %w", err)
	}
	row := stmt.QueryRowContext(ctx, fileID)
	file, err := scanmodel.ReadFSEntryFromDatabase(func(id *id.FSEntryID, path *string, lastModified *time.Time, mode *fs.FileMode, size *uint64, checksum *[]byte, sourceID *id.SourceID, spaceDID *did.DID) error {
		return row.Scan(util.DbID(id), path, util.TimestampScanner(lastModified), mode, size, util.DbBytes(checksum), util.DbID(sourceID), util.DbDID(spaceDID))
	})
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if f, ok := file.(*scanmodel.File); ok {
		return f, nil
	}
	return nil, errors.New("found entry is not a file")
}

func (r *Repo) insertOrGetFSEntry(ctx context.Context, path string, lastModified time.Time, mode fs.FileMode, size uint64, checksum []byte, sourceID id.SourceID, spaceDID did.DID) (scanmodel.FSEntry, bool, error) {
	newID := id.New()
	// On a conflict we do a no-op DO UPDATE SET path = excluded.path only to enable RETURNING,
	// so existing row values aren't changed (last_modified/mode/size/checksum stay as stored)
	stmt, err := r.prepareStmt(ctx, `
		INSERT INTO fs_entries (id, path, last_modified, mode, size, checksum, source_id, space_did)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(space_did, source_id, path, last_modified, mode, size, checksum)
		DO UPDATE SET path = excluded.path
		RETURNING id, path, last_modified, mode, size, checksum, source_id, space_did
	`)
	if err != nil {
		return nil, false, fmt.Errorf("failed to prepare statement: %w", err)
	}
	row := stmt.QueryRowContext(ctx,
		util.DbID(&newID),
		path,
		lastModified.Unix(),
		mode,
		size,
		util.DbBytes(&checksum),
		util.DbID(&sourceID),
		util.DbDID(&spaceDID))

	created := true
	entry, err := scanmodel.ReadFSEntryFromDatabase(func(
		id *id.FSEntryID,
		path *string,
		lastModified *time.Time,
		mode *fs.FileMode,
		size *uint64,
		checksum *[]byte,
		sourceID *id.SourceID,
		spaceDID *did.DID,
	) error {
		if err := row.Scan(
			util.DbID(id),
			path,
			util.TimestampScanner(lastModified),
			mode,
			size,
			util.DbBytes(checksum),
			util.DbID(sourceID),
			util.DbDID(spaceDID),
		); err != nil {
			return err
		}
		if *id != newID {
			created = false
		}
		return nil
	})
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return entry, created, nil
}

func (r *Repo) DeleteFSEntry(ctx context.Context, spaceDID did.DID, fsEntryID id.FSEntryID) error {
	stmt, err := r.prepareStmt(ctx, `
		DELETE FROM fs_entries
		WHERE id = ?
		  AND space_did = ?
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	_, err = stmt.ExecContext(ctx, fsEntryID, util.DbDID(&spaceDID))
	if err != nil {
		return fmt.Errorf("failed to delete FS entry for space %s: %w", spaceDID, err)
	}
	return nil
}
