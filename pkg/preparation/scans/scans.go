package scans

import (
	"bytes"
	"context"
	"fmt"
	"io/fs"

	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/guppy/pkg/preparation/scans/checksum"
	"github.com/storacha/guppy/pkg/preparation/scans/model"
	"github.com/storacha/guppy/pkg/preparation/scans/visitor"
	"github.com/storacha/guppy/pkg/preparation/scans/walker"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	"github.com/storacha/guppy/pkg/preparation/uploads"
	uploadmodel "github.com/storacha/guppy/pkg/preparation/uploads/model"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

var tracer = otel.Tracer("preparation/scans")

// WalkerFunc is a function type that defines how to walk the file system.
type WalkerFunc func(fsys fs.FS, root string, visitor walker.FSVisitor) (model.FSEntry, error)

// SourceAccessorFunc is a function type that retrieves the file system for a given source ID.
type SourceAccessorFunc func(ctx context.Context, sourceID id.SourceID) (fs.FS, error)

// API is a dependency container for executing scans on a repository.
type API struct {
	Repo           Repo
	SourceAccessor SourceAccessorFunc
	WalkerFn       WalkerFunc
}

var _ uploads.ExecuteScanFunc = API{}.ExecuteScan
var _ uploads.RemoveBadFSEntryFunc = API{}.RemoveBadFSEntry

func (a API) ExecuteScan(ctx context.Context, uploadID id.UploadID, fsEntryCb func(model.FSEntry) error) error {
	ctx, span := tracer.Start(ctx, "execute-scan", trace.WithAttributes(
		attribute.String("upload.id", uploadID.String()),
	))
	defer span.End()

	upload, err := a.Repo.GetUploadByID(ctx, uploadID)
	if err != nil {
		return fmt.Errorf("getting upload by ID: %w", err)
	}

	fsEntry, err := a.executeScan(ctx, upload, fsEntryCb)
	if err != nil {
		return fmt.Errorf("executing upload scan: %w", err)
	}

	if err := upload.SetRootFSEntryID(fsEntry.ID()); err != nil {
		return fmt.Errorf("completing upload scan: %w", err)
	}

	if err := a.Repo.UpdateUpload(ctx, upload); err != nil {
		return fmt.Errorf("updating upload: %w", err)
	}
	return nil
}

func (a API) executeScan(ctx context.Context, upload *uploadmodel.Upload, fsEntryCb func(model.FSEntry) error) (model.FSEntry, error) {
	fsys, err := a.SourceAccessor(ctx, upload.SourceID())
	if err != nil {
		return nil, fmt.Errorf("accessing source: %w", err)
	}
	fsEntry, err := a.WalkerFn(fsys, ".", visitor.NewScanVisitor(ctx, a.Repo, upload.SourceID(), upload.SpaceDID(), fsEntryCb))
	if err != nil {
		return nil, fmt.Errorf("recursively creating directories: %w", err)
	}
	return fsEntry, nil
}

// openFile opens a file for reading, ensuring the checksum matches the expected value.
func (a API) openFile(ctx context.Context, file *model.File) (fs.File, error) {
	fsys, err := a.SourceAccessor(ctx, file.SourceID())
	if err != nil {
		return nil, fmt.Errorf("accessing source for file %s: %w", file.ID(), err)
	}

	stat, err := fs.Stat(fsys, file.Path())
	if err != nil {
		return nil, fmt.Errorf("getting file stat for %s: %w", file.Path(), err)
	}
	checksum := checksum.FileChecksum(file.Path(), stat, file.SourceID(), file.SpaceDID())
	if !bytes.Equal(checksum, file.Checksum()) {
		return nil, fmt.Errorf("checksum mismatch for file %s: expected %x, got %x", file.Path(), file.Checksum(), checksum)
	}
	fsFile, err := fsys.Open(file.Path())
	if err != nil {
		return nil, fmt.Errorf("opening file %s: %w", file.Path(), err)
	}
	return fsFile, nil
}

// getFileByID retrieves a file by its ID from the repository, returning an error if not found.
func (a API) getFileByID(ctx context.Context, fileID id.FSEntryID) (*model.File, error) {
	file, err := a.Repo.GetFileByID(ctx, fileID)
	if err != nil {
		return nil, fmt.Errorf("getting file by ID %s: %w", fileID, err)
	}
	if file == nil {
		return nil, fmt.Errorf("file with ID %s not found", fileID)
	}
	return file, nil
}

// OpenFileByID retrieves a file by its ID and opens it for reading, returning an error if not found or if the file cannot be opened.
func (a API) OpenFileByID(ctx context.Context, fileID id.FSEntryID) (fs.File, id.SourceID, string, error) {
	file, err := a.getFileByID(ctx, fileID)
	if err != nil {
		return nil, id.Nil, "", err
	}
	fsFile, err := a.openFile(ctx, file)
	if err != nil {
		return nil, id.Nil, "", err
	}
	return fsFile, file.SourceID(), file.Path(), nil
}

func (a API) RemoveBadFSEntry(ctx context.Context, spaceDID did.DID, fsEntryID id.FSEntryID) error {
	return a.Repo.DeleteFSEntry(ctx, spaceDID, fsEntryID)
}
