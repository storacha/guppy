package scans

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"

	"github.com/storacha/guppy/pkg/preparation/scans/checksum"
	"github.com/storacha/guppy/pkg/preparation/scans/model"
	"github.com/storacha/guppy/pkg/preparation/scans/visitor"
	"github.com/storacha/guppy/pkg/preparation/scans/walker"
	"github.com/storacha/guppy/pkg/preparation/types"
)

// Scans is a dependency container for executing scans on a repository.
type Scans struct {
	Repo               Repo
	UploadSourceLookup UploadSourceLookupFn
	SourceAccessor     SourceAccessorFn
	WalkerFn           WalkerFn
}

// WalkerFn is a function type that defines how to walk the file system.
type WalkerFn func(fsys fs.FS, root string, visitor walker.FSVisitor) (model.FSEntry, error)

// SourceAccessorFn is a function type that retrieves the file system for a given source ID.
type SourceAccessorFn func(ctx context.Context, sourceID types.SourceID) (fs.FS, error)

// UploadSourceLookupFn is a function type that retrieves the source ID for a given upload ID.
type UploadSourceLookupFn func(ctx context.Context, uploadID types.UploadID) (types.SourceID, error)

// ExecuteScan executes a scan on the given source, creating files and directories in the repository.
func (s Scans) ExecuteScan(ctx context.Context, scan *model.Scan, fsEntryCb func(model.FSEntry) error) error {
	err := scan.Start()
	if err != nil {
		return fmt.Errorf("starting scan: %w", err)
	}
	if err := s.Repo.UpdateScan(ctx, scan); err != nil {
		return fmt.Errorf("updating scan: %w", err)
	}
	fsEntry, err := s.executeScan(ctx, scan, fsEntryCb)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			if err := scan.Cancel(); err != nil {
				return fmt.Errorf("canceling scan: %w", err)
			}
		} else {
			if err := scan.Fail(err.Error()); err != nil {
				return fmt.Errorf("failing scan: %w", err)
			}
		}
	} else {
		if err := scan.Complete(fsEntry.ID()); err != nil {
			return fmt.Errorf("completing scan: %w", err)
		}
	}
	if err := s.Repo.UpdateScan(context.WithoutCancel(ctx), scan); err != nil {
		return fmt.Errorf("updating scan after execute: %w", err)
	}
	return nil
}

func (s Scans) executeScan(ctx context.Context, scan *model.Scan, fsEntryCb func(model.FSEntry) error) (model.FSEntry, error) {
	sourceID, err := s.UploadSourceLookup(ctx, scan.UploadID())
	if err != nil {
		return nil, fmt.Errorf("looking up source ID: %w", err)
	}
	fsys, err := s.SourceAccessor(ctx, sourceID)
	if err != nil {
		return nil, fmt.Errorf("accessing source: %w", err)
	}
	fsEntry, err := s.WalkerFn(fsys, ".", visitor.NewScanVisitor(ctx, s.Repo, sourceID, fsEntryCb))
	if err != nil {
		return nil, fmt.Errorf("recursively creating directories: %w", err)
	}
	return fsEntry, nil
}

// OpenFile opens a file for reading, ensuring the checksum matches the expected value.
func (s Scans) OpenFile(ctx context.Context, file *model.File) (fs.File, error) {
	fsys, err := s.SourceAccessor(ctx, file.SourceID())
	if err != nil {
		return nil, fmt.Errorf("accessing source for file %s: %w", file.ID(), err)
	}

	stat, err := fs.Stat(fsys, file.Path())
	checksum := checksum.FileChecksum(file.Path(), stat, file.SourceID())
	if !bytes.Equal(checksum, file.Checksum()) {
		return nil, fmt.Errorf("checksum mismatch for file %s: expected %x, got %x", file.Path(), file.Checksum(), checksum)
	}
	fsFile, err := fsys.Open(file.Path())
	if err != nil {
		return nil, fmt.Errorf("opening file %s: %w", file.Path(), err)
	}
	return fsFile, nil
}

// GetFileByID retrieves a file by its ID from the repository, returning an error if not found.
func (s Scans) GetFileByID(ctx context.Context, fileID types.FSEntryID) (*model.File, error) {
	file, err := s.Repo.GetFileByID(ctx, fileID)
	if err != nil {
		return nil, fmt.Errorf("getting file by ID %s: %w", fileID, err)
	}
	if file == nil {
		return nil, fmt.Errorf("file with ID %s not found", fileID)
	}
	return file, nil
}

// OpenFileByID retrieves a file by its ID and opens it for reading, returning an error if not found or if the file cannot be opened.
func (s Scans) OpenFileByID(ctx context.Context, fileID types.FSEntryID) (fs.File, types.SourceID, string, error) {
	file, err := s.GetFileByID(ctx, fileID)
	if err != nil {
		return nil, types.SourceID{}, "", err
	}
	fsFile, err := s.OpenFile(ctx, file)
	if err != nil {
		return nil, types.SourceID{}, "", err
	}
	return fsFile, file.SourceID(), file.Path(), nil
}
