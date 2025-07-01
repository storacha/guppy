package sqlrepo

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"time"

	"github.com/storacha/guppy/pkg/preparation/scans"
	scanmodel "github.com/storacha/guppy/pkg/preparation/scans/model"
	"github.com/storacha/guppy/pkg/preparation/types"
)

var _ scans.Repo = (*repo)(nil)

// CreateScan creates a new scan in the repository with the given upload ID.
func (r *repo) CreateScan(ctx context.Context, uploadID types.UploadID) (*scanmodel.Scan, error) {
	scan, err := scanmodel.NewScan(uploadID)
	if err != nil {
		return nil, err
	}

	insertQuery := `
		INSERT INTO scans (
			id,
			upload_id,
			root_id,
			created_at,
			updated_at,
			state,
			error_message
		) VALUES ($1, $2, $3, $4, $5, $6, $7)
	`

	err = scanmodel.WriteScanToDatabase(
		scan,
		func(
			id types.ScanID,
			uploadID types.UploadID,
			rootID *types.FSEntryID,
			createdAt,
			updatedAt time.Time,
			state scanmodel.ScanState,
			errorMessage *string) error {
			result, err := r.db.ExecContext(
				ctx,
				insertQuery,
				id[:],
				uploadID[:],
				Null(rootID),
				createdAt.Unix(),
				updatedAt.Unix(),
				state,
				NullString(errorMessage),
			)
			if err != nil {
				return fmt.Errorf("failed to insert scan: %w", err)
			}
			rowsAffected, err := result.RowsAffected()
			if err != nil {
				return fmt.Errorf("failed to get rows affected: %w", err)
			}
			if rowsAffected == 0 {
				return fmt.Errorf("no scan inserted")
			}
			return err
		},
	)
	if err != nil {
		return nil, err
	}

	return scan, nil
}

func (r *repo) GetScanByID(ctx context.Context, scanID types.ScanID) (*scanmodel.Scan, error) {
	row := r.db.QueryRowContext(ctx,
		`SELECT
			id,
			upload_id,
			root_id,
			created_at,
			updated_at,
			state,
			error_message
		FROM scans WHERE id = ?`, scanID[:],
	)
	scan, err := scanmodel.ReadScanFromDatabase(func(
		id *types.ScanID,
		uploadID *types.UploadID,
		rootID **types.FSEntryID,
		createdAt *time.Time,
		updatedAt *time.Time,
		state *scanmodel.ScanState,
		errorMessage **string) error {
		return row.Scan(
			id,
			uploadID,
			rootID,
			timestampScanner(createdAt),
			timestampScanner(updatedAt),
			state,
			errorMessage,
		)
	})
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return scan, nil
}

// FindOrCreateFile finds or creates a file entry in the repository with the given parameters.
// If the file already exists, it returns the existing file and false.
// If the file does not exist, it creates a new file entry and returns it along with true.
func (r *repo) FindOrCreateFile(ctx context.Context, path string, lastModified time.Time, mode fs.FileMode, size uint64, checksum []byte, sourceID types.SourceID) (*scanmodel.File, bool, error) {
	entry, err := r.findFSEntry(ctx, path, lastModified, mode, size, checksum, sourceID)
	if err != nil {
		return nil, false, err
	}
	if entry != nil {
		// File already exists, return it
		if file, ok := entry.(*scanmodel.File); ok {
			return file, false, nil
		}
		return nil, false, errors.New("found entry is not a file")
	}

	newfile, err := scanmodel.NewFile(path, lastModified, mode, size, checksum, sourceID)
	if err != nil {
		return nil, false, err
	}

	err = r.createFSEntry(ctx, newfile)

	if err != nil {
		return nil, false, err
	}

	return newfile, true, nil
}

// FindOrCreateDirectory finds or creates a directory entry in the repository with the given parameters.
// If the directory already exists, it returns the existing directory and false.
// If the directory does not exist, it creates a new directory entry and returns it along with true.
func (r *repo) FindOrCreateDirectory(ctx context.Context, path string, lastModified time.Time, mode fs.FileMode, checksum []byte, sourceID types.SourceID) (*scanmodel.Directory, bool, error) {
	entry, err := r.findFSEntry(ctx, path, lastModified, mode, 0, checksum, sourceID) // size is not used for directories
	if err != nil {
		return nil, false, err
	}
	if entry != nil {
		if dir, ok := entry.(*scanmodel.Directory); ok {
			// Directory already exists, return it
			return dir, false, nil
		}
		return nil, false, errors.New("found entry is not a directory")
	}

	newdir, err := scanmodel.NewDirectory(path, lastModified, mode, checksum, sourceID)
	if err != nil {
		return nil, false, err
	}

	err = r.createFSEntry(ctx, newdir)
	if err != nil {
		return nil, false, err
	}

	return newdir, true, nil
}

// CreateDirectoryChildren links a directory to its children in the repository.
func (r *repo) CreateDirectoryChildren(ctx context.Context, parent *scanmodel.Directory, children []scanmodel.FSEntry) error {
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
func (r *repo) DirectoryChildren(ctx context.Context, dir *scanmodel.Directory) ([]scanmodel.FSEntry, error) {
	query := `
		SELECT f.id, f.path, f.last_modified, f.mode, f.size, f.checksum, f.source_id
		FROM directory_children dc
		JOIN files f ON dc.child_id = f.id
		WHERE dc.directory_id = $1
	`
	rows, err := r.db.QueryContext(ctx, query, dir.ID())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []scanmodel.FSEntry
	for rows.Next() {
		entry, err := scanmodel.ReadFSEntryFromDatabase(func(id *types.FSEntryID, path *string, lastModified *time.Time, mode *fs.FileMode, size *uint64, checksum *[]byte, sourceID *types.SourceID) error {
			return rows.Scan(id, path, lastModified, mode, size, checksum, sourceID)
		})
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// UpdateScan updates the given scan in the repository.
func (r *repo) UpdateScan(ctx context.Context, scan *scanmodel.Scan) error {
	query := `
		UPDATE scans
		SET upload_id = $2, root_id = $3, created_at = $4, updated_at = $5, state = $6, error_message = $7
		WHERE id = $1
	`

	return scanmodel.WriteScanToDatabase(scan, func(id types.ScanID, uploadID types.UploadID, rootID *types.FSEntryID, createdAt, updatedAt time.Time, state scanmodel.ScanState, errorMessage *string) error {
		_, err := r.db.ExecContext(ctx, query, id, uploadID, Null(rootID), createdAt, updatedAt, state, NullString(errorMessage))
		return err
	})
}

// GetFileByID retrieves a file by its unique ID from the repository.
func (r *repo) GetFileByID(ctx context.Context, fileID types.FSEntryID) (*scanmodel.File, error) {
	row := r.db.QueryRowContext(ctx,
		`SELECT id, path, last_modified, mode, size, checksum, source_id FROM fs_entries WHERE id = ?`, fileID,
	)
	file, err := scanmodel.ReadFSEntryFromDatabase(func(id *types.FSEntryID, path *string, lastModified *time.Time, mode *fs.FileMode, size *uint64, checksum *[]byte, sourceID *types.SourceID) error {
		return row.Scan(id, path, lastModified, mode, size, checksum, sourceID)
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

func (r *repo) findFSEntry(ctx context.Context, path string, lastModified time.Time, mode fs.FileMode, size uint64, checksum []byte, sourceID types.SourceID) (scanmodel.FSEntry, error) {
	query := `
		SELECT id, path, last_modified, mode, size, checksum, source_id
		FROM fs_entries
		WHERE path = $1
		  AND last_modified = $2
		  AND mode = $3
		  AND size = $4
		  AND checksum = $5
		  AND source_id = $6
	`
	row := r.db.QueryRowContext(
		ctx,
		query,
		path,
		lastModified.Unix(),
		mode,
		size,
		checksum,
		sourceID[:],
	)
	entry, err := scanmodel.ReadFSEntryFromDatabase(func(
		id *types.FSEntryID,
		path *string,
		lastModified *time.Time,
		mode *fs.FileMode,
		size *uint64,
		checksum *[]byte,
		sourceID *types.SourceID,
	) error {
		return row.Scan(
			id,
			path,
			timestampScanner(lastModified),
			mode,
			size,
			checksum,
			sourceID,
		)
	})
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return entry, err
}

func (r *repo) createFSEntry(ctx context.Context, entry scanmodel.FSEntry) error {
	insertQuery := `
		INSERT INTO fs_entries (id, path, last_modified, mode, size, checksum, source_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`

	return scanmodel.WriteFSEntryToDatabase(
		entry,
		func(
			id types.FSEntryID,
			path string,
			lastModified time.Time,
			mode fs.FileMode,
			size uint64,
			checksum []byte,
			sourceID types.SourceID,
		) error {
			_, err := r.db.ExecContext(
				ctx,
				insertQuery,
				id[:],
				path,
				lastModified.Unix(),
				mode,
				size,
				checksum,
				sourceID[:],
			)
			return err
		})
}
