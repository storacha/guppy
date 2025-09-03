package sqlrepo

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"time"

	"github.com/storacha/go-ucanto/did"
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
	entry, err := r.findFSEntry(ctx, path, lastModified, mode, size, checksum, sourceID, spaceDID)
	if err != nil {
		return nil, false, fmt.Errorf("failed to find file entry: %w", err)
	}
	if entry != nil {
		// File already exists, return it
		if file, ok := entry.(*scanmodel.File); ok {
			return file, false, nil
		}
		return nil, false, errors.New("found entry is not a file")
	}

	newfile, err := scanmodel.NewFile(path, lastModified, mode, size, checksum, sourceID, spaceDID)
	if err != nil {
		return nil, false, fmt.Errorf("failed to make new file entry: %w", err)
	}

	err = r.createFSEntry(ctx, newfile)

	if err != nil {
		return nil, false, fmt.Errorf("failed to persist new file entry: %w", err)
	}

	return newfile, true, nil
}

// FindOrCreateDirectory finds or creates a directory entry in the repository with the given parameters.
// If the directory already exists, it returns the existing directory and false.
// If the directory does not exist, it creates a new directory entry and returns it along with true.
func (r *Repo) FindOrCreateDirectory(ctx context.Context, path string, lastModified time.Time, mode fs.FileMode, checksum []byte, sourceID id.SourceID, spaceDID did.DID) (*scanmodel.Directory, bool, error) {
	log.Debugf("Finding or creating directory: %s", path)
	if !mode.IsDir() {
		return nil, false, errors.New("cannot create a directory with file mode")
	}
	entry, err := r.findFSEntry(ctx, path, lastModified, mode, 0, checksum, sourceID, spaceDID) // size is not used for directories
	if err != nil {
		return nil, false, fmt.Errorf("failed to find directory entry: %w", err)
	}
	if entry != nil {
		if dir, ok := entry.(*scanmodel.Directory); ok {
			// Directory already exists, return it
			return dir, false, nil
		}
		return nil, false, errors.New("found entry is not a directory")
	}

	newdir, err := scanmodel.NewDirectory(path, lastModified, mode, checksum, sourceID, spaceDID)
	if err != nil {
		return nil, false, fmt.Errorf("failed to make new directory entry: %w", err)
	}

	err = r.createFSEntry(ctx, newdir)
	if err != nil {
		return nil, false, fmt.Errorf("failed to persist new directory entry: %w", err)
	}

	log.Debugf("Created new directory %s: %s", path, newdir.ID())
	return newdir, true, nil
}

// CreateDirectoryChildren links a directory to its children in the repository.
func (r *Repo) CreateDirectoryChildren(ctx context.Context, parent *scanmodel.Directory, children []scanmodel.FSEntry) error {
	insertQuery := `
		INSERT INTO directory_children (directory_id, child_id)
		VALUES ($1, $2)
	`

	for _, child := range children {
		_, err := r.db.ExecContext(ctx, insertQuery, parent.ID(), child.ID())
		if err != nil {
			return err
		}
	}

	return nil
}

// DirectoryChildren retrieves the children of a directory from the repository.
func (r *Repo) DirectoryChildren(ctx context.Context, dir *scanmodel.Directory) ([]scanmodel.FSEntry, error) {
	query := `
		SELECT fse.id, fse.path, fse.last_modified, fse.mode, fse.size, fse.checksum, fse.source_id, fse.space_did
		FROM directory_children dc
		JOIN fs_entries fse ON dc.child_id = fse.id
		WHERE dc.directory_id = $1
	`
	rows, err := r.db.QueryContext(ctx, query, dir.ID())
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
				id,
				path,
				util.TimestampScanner(lastModified),
				mode,
				size,
				checksum,
				sourceID,
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
	row := r.db.QueryRowContext(ctx,
		`SELECT id, path, last_modified, mode, size, checksum, source_id, space_did FROM fs_entries WHERE id = ?`, fileID,
	)
	file, err := scanmodel.ReadFSEntryFromDatabase(func(id *id.FSEntryID, path *string, lastModified *time.Time, mode *fs.FileMode, size *uint64, checksum *[]byte, sourceID *id.SourceID, spaceDID *did.DID) error {
		return row.Scan(id, path, util.TimestampScanner(lastModified), mode, size, checksum, sourceID, util.DbDID(spaceDID))
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

func (r *Repo) findFSEntry(ctx context.Context, path string, lastModified time.Time, mode fs.FileMode, size uint64, checksum []byte, sourceID id.SourceID, spaceDID did.DID) (scanmodel.FSEntry, error) {
	query := `
		SELECT id, path, last_modified, mode, size, checksum, source_id, space_did
		FROM fs_entries
		WHERE path = $1
		  AND last_modified = $2
		  AND mode = $3
		  AND size = $4
		  AND checksum = $5
		  AND source_id = $6
		  AND space_did = $7
	`
	row := r.db.QueryRowContext(
		ctx,
		query,
		path,
		lastModified.Unix(),
		mode,
		size,
		checksum,
		sourceID,
		util.DbDID(&spaceDID),
	)
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
		return row.Scan(
			id,
			path,
			util.TimestampScanner(lastModified),
			mode,
			size,
			checksum,
			sourceID,
			util.DbDID(spaceDID),
		)
	})
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return entry, err
}

func (r *Repo) createFSEntry(ctx context.Context, entry scanmodel.FSEntry) error {
	insertQuery := `
		INSERT INTO fs_entries (id, path, last_modified, mode, size, checksum, source_id, space_did)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`

	return scanmodel.WriteFSEntryToDatabase(
		entry,
		func(
			id id.FSEntryID,
			path string,
			lastModified time.Time,
			mode fs.FileMode,
			size uint64,
			checksum []byte,
			sourceID id.SourceID,
			spaceDID did.DID,
		) error {
			_, err := r.db.ExecContext(
				ctx,
				insertQuery,
				id,
				path,
				lastModified.Unix(),
				mode,
				size,
				checksum,
				sourceID,
				util.DbDID(&spaceDID),
			)
			return err
		})
}
