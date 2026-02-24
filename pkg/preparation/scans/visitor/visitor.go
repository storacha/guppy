package visitor

import (
	"context"
	"fmt"
	"io/fs"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/guppy/pkg/preparation/scans/checksum"
	"github.com/storacha/guppy/pkg/preparation/scans/model"
	"github.com/storacha/guppy/pkg/preparation/types/id"
)

var log = logging.Logger("preparation/scans/visitor")

const scanLogInterval = 1 << 30 // 1 GiB

// Repo defines the interface for a repository that manages file system entries during a scan
type Repo interface {
	FindOrCreateFile(ctx context.Context, path string, lastModified time.Time, mode fs.FileMode, size uint64, checksum []byte, sourceID id.SourceID, spaceDID did.DID) (*model.File, bool, error)
	FindOrCreateDirectory(ctx context.Context, path string, lastModified time.Time, mode fs.FileMode, checksum []byte, sourceID id.SourceID, spaceDID did.DID) (*model.Directory, bool, error)
	CreateDirectoryChildren(ctx context.Context, parent *model.Directory, children []model.FSEntry) error
	GetFSEntryByPath(ctx context.Context, path string, sourceID id.SourceID, spaceDID did.DID) (model.FSEntry, error)
}

// FSEntryCallback is a function type that is called for each file system entry created during the scan.
type FSEntryCallback func(entry model.FSEntry) error

// ScanVisitor is a struct that implements the walker.FSVisitor interface.
// It is used to visit files and directories during a scan operation, creating or finding them in the repository
type ScanVisitor struct {
	repo            Repo
	ctx             context.Context
	sourceID        id.SourceID
	spaceDID        did.DID
	cb              FSEntryCallback
	assumeUnchanged bool
	bytesScanned    uint64
	bytesAtLastLog  uint64
}

// NewScanVisitor creates a new ScanVisitor with the provided context, repository, source ID, and callback function.
// If assumeUnchanged is true, the visitor will skip entries that already exist
// in the DB (by path, within the same source and space).
func NewScanVisitor(ctx context.Context, repo Repo, sourceID id.SourceID, spaceDID did.DID, assumeUnchanged bool, cb FSEntryCallback) *ScanVisitor {
	return &ScanVisitor{
		repo:            repo,
		ctx:             ctx,
		sourceID:        sourceID,
		spaceDID:        spaceDID,
		assumeUnchanged: assumeUnchanged,
		cb:              cb,
	}
}

// SkipEntry checks if an fs_entry already exists in the DB for this path.
// If assumeUnchanged is true and the entry exists, returns it without stat-ing
// the file or recursing into the directory.
func (v *ScanVisitor) SkipEntry(path string, dirEntry fs.DirEntry) (model.FSEntry, bool) {
	if !v.assumeUnchanged {
		return nil, false
	}
	entry, err := v.repo.GetFSEntryByPath(v.ctx, path, v.sourceID, v.spaceDID)
	if err != nil {
		log.Warnw("error looking up existing fs entry, will re-scan", "path", path, "err", err)
		return nil, false
	}
	if entry == nil {
		return nil, false
	}
	return entry, true
}

// VisitFile is called for each file found during the scan.
// It creates or finds the file in the repository and calls the callback on create if provided.
func (v *ScanVisitor) VisitFile(path string, dirEntry fs.DirEntry) (*model.File, error) {
	info, err := dirEntry.Info()
	if err != nil {
		return nil, fmt.Errorf("reading file info: %w", err)
	}

	v.bytesScanned += uint64(info.Size())
	if v.bytesScanned-v.bytesAtLastLog >= scanLogInterval {
		log.Infow("scanning files", "bytes", v.bytesScanned)
		v.bytesAtLastLog = v.bytesScanned
	}

	file, created, err := v.repo.FindOrCreateFile(v.ctx, path, info.ModTime(), info.Mode(), uint64(info.Size()), checksum.FileChecksum(path, info, v.sourceID, v.spaceDID), v.sourceID, v.spaceDID)
	if err != nil {
		return nil, fmt.Errorf("creating file: %w", err)
	}
	if created && v.cb != nil {
		if err := v.cb(file); err != nil {
			return nil, fmt.Errorf("on file callback: %w", err)
		}
	}
	return file, nil
}

// VisitDirectory is called for each directory found during the scan.
// It creates or finds the directory in the repository, sets its children, and calls the callback on create if provided.
func (v *ScanVisitor) VisitDirectory(path string, dirEntry fs.DirEntry, children []model.FSEntry) (*model.Directory, error) {
	log.Debugf("Visiting directory: %s", path)

	info, err := dirEntry.Info()
	if err != nil {
		return nil, fmt.Errorf("reading directory info: %w", err)
	}
	dir, created, err := v.repo.FindOrCreateDirectory(v.ctx, path, info.ModTime(), info.Mode(), checksum.DirChecksum(path, info, v.sourceID, v.spaceDID, children), v.sourceID, v.spaceDID)
	if err != nil {
		return nil, fmt.Errorf("creating directory: %w", err)
	}
	if created {
		if err := v.repo.CreateDirectoryChildren(v.ctx, dir, children); err != nil {
			return nil, fmt.Errorf("setting directory children: %w", err)
		}
		if v.cb != nil {
			if err := v.cb(dir); err != nil {
				return nil, fmt.Errorf("on directory callback: %w", err)
			}
		}
	}
	return dir, nil
}
