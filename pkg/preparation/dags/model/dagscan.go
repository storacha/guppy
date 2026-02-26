package model

import (
	"fmt"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/guppy/pkg/preparation/types"
	"github.com/storacha/guppy/pkg/preparation/types/id"
)

// DAGScanState represents the state of a DAG scan.
type DAGScanState string

const (
	// DAGScanStatePending indicates that the file system entry is pending scan and has not started yet.
	DAGScanStatePending DAGScanState = "pending"
	// DAGScanStateCompleted indicates that the file system entry has completed successfully.
	DAGScanStateCompleted DAGScanState = "completed"
	// DAGScanStateFailed indicates that the file system entry has failed.
	DAGScanStateFailed DAGScanState = "failed"
	// DAGScanStateCanceled indicates that the file system entry has been canceled.
	DAGScanStateCanceled DAGScanState = "canceled"
)

func TerminatedState(state DAGScanState) bool {
	return state == DAGScanStateCompleted || state == DAGScanStateFailed || state == DAGScanStateCanceled
}

type DAGScan interface {
	FsEntryID() id.FSEntryID
	UploadID() id.UploadID
	SpaceDID() did.DID
	CreatedAt() time.Time
	UpdatedAt() time.Time
	HasCID() bool
	CID() cid.Cid
	Complete(cid cid.Cid) error
	ClearCID() error
	isDAGScan()
}

type dagScan struct {
	fsEntryID id.FSEntryID
	uploadID  id.UploadID
	spaceDID  did.DID
	createdAt time.Time
	updatedAt time.Time
	// cid is the root CID of the DAG, if the scan has been completed, and
	// [cid.Undef] otherwise.
	cid cid.Cid
}

// validation conditions -- should not be callable externally, all scans outside this module MUST be valid
func validateDAGScan(d *dagScan) (*dagScan, error) {
	if d.fsEntryID == id.Nil {
		return nil, types.EmptyError{Field: "fsEntryID"}
	}
	if d.uploadID == id.Nil {
		return nil, types.EmptyError{Field: "uploadID"}
	}
	if !d.spaceDID.Defined() {
		return nil, types.EmptyError{Field: "spaceDID"}
	}
	return d, nil
}

// accessors
func (d *dagScan) FsEntryID() id.FSEntryID {
	return d.fsEntryID
}
func (d *dagScan) UploadID() id.UploadID {
	return d.uploadID
}
func (d *dagScan) SpaceDID() did.DID {
	return d.spaceDID
}
func (d *dagScan) CreatedAt() time.Time {
	return d.createdAt
}
func (d *dagScan) UpdatedAt() time.Time {
	return d.updatedAt
}
func (d *dagScan) HasCID() bool {
	return d.cid != cid.Undef
}
func (d *dagScan) CID() cid.Cid {
	return d.cid
}

func (d *dagScan) Complete(cid cid.Cid) error {
	if d.cid.Defined() {
		return fmt.Errorf("cannot complete completed dag scan %s (cid %s)", d.fsEntryID, d.cid)
	}
	d.updatedAt = time.Now()
	d.cid = cid
	return nil
}

func (d *dagScan) ClearCID() error {
	if !d.cid.Defined() {
		return fmt.Errorf("cannot clear CID from incomplete dag scan %s", d.fsEntryID)
	}
	d.updatedAt = time.Now()
	d.cid = cid.Undef
	return nil
}

type FileDAGScan struct {
	dagScan
}

func (d *FileDAGScan) isDAGScan() {}

type DirectoryDAGScan struct {
	dagScan
}

func (d *DirectoryDAGScan) isDAGScan() {}

// NewFileDAGScan creates a new FileDAGScan with the given fsEntryID.
func NewFileDAGScan(fsEntryID id.FSEntryID, uploadID id.UploadID, spaceDID did.DID) (*FileDAGScan, error) {
	fds := &FileDAGScan{
		dagScan: dagScan{
			fsEntryID: fsEntryID,
			uploadID:  uploadID,
			spaceDID:  spaceDID,
			createdAt: time.Now(),
			updatedAt: time.Now(),
		},
	}
	if _, err := validateDAGScan(&fds.dagScan); err != nil {
		return nil, fmt.Errorf("failed to create FileDAGScan: %w", err)
	}
	return fds, nil
}

// NewDirectoryDAGScan creates a new DirectoryDAGScan with the given fsEntryID.
func NewDirectoryDAGScan(fsEntryID id.FSEntryID, uploadID id.UploadID, spaceDID did.DID) (*DirectoryDAGScan, error) {
	dds := &DirectoryDAGScan{
		dagScan: dagScan{
			fsEntryID: fsEntryID,
			uploadID:  uploadID,
			spaceDID:  spaceDID,
			createdAt: time.Now(),
			updatedAt: time.Now(),
		},
	}
	if _, err := validateDAGScan(&dds.dagScan); err != nil {
		return nil, fmt.Errorf("failed to create DirectoryDAGScan: %w", err)
	}
	return dds, nil
}

// DAGScanWriter is a function type for writing a DAGScan to the database.
type DAGScanWriter func(
	kind string,
	fsEntryID id.FSEntryID,
	uploadID id.UploadID,
	spaceDID did.DID,
	createdAt time.Time,
	updatedAt time.Time,
	cid cid.Cid,
) error

// WriteDAGScanToDatabase writes a DAGScan to the database using the provided writer function.
func WriteDAGScanToDatabase(scan DAGScan, writer DAGScanWriter) error {
	var ds *dagScan
	var kind string
	switch s := scan.(type) {
	case *FileDAGScan:
		ds = &s.dagScan
		kind = "file"
	case *DirectoryDAGScan:
		ds = &s.dagScan
		kind = "directory"
	default:
		return fmt.Errorf("unsupported DAGScan type: %T", scan)
	}
	if scan == nil {
		return fmt.Errorf("cannot write nil DAGScan to database")
	}
	return writer(
		kind,
		ds.fsEntryID,
		ds.uploadID,
		ds.spaceDID,
		ds.createdAt,
		ds.updatedAt,
		ds.cid,
	)
}

// DAGScanScanner is a function type for scanning a DAGScan from the database.
type DAGScanScanner func(
	kind *string,
	fsEntryID *id.FSEntryID,
	uploadID *id.UploadID,
	spaceDID *did.DID,
	createdAt *time.Time,
	updatedAt *time.Time,
	cid *cid.Cid,
) error

// ReadDAGScanFromDatabase reads a DAGScan from the database using the provided scanner function.
func ReadDAGScanFromDatabase(scanner DAGScanScanner) (DAGScan, error) {
	var kind string
	var dagScan dagScan
	err := scanner(
		&kind,
		&dagScan.fsEntryID,
		&dagScan.uploadID,
		&dagScan.spaceDID,
		&dagScan.createdAt,
		&dagScan.updatedAt,
		&dagScan.cid,
	)
	if err != nil {
		return nil, fmt.Errorf("reading dag scan from database: %w", err)
	}
	if _, err := validateDAGScan(&dagScan); err != nil {
		return nil, fmt.Errorf("invalid dag scan data: %w", err)
	}
	switch kind {
	case "file":
		return &FileDAGScan{dagScan: dagScan}, nil
	case "directory":
		return &DirectoryDAGScan{dagScan: dagScan}, nil
	default:
		return nil, fmt.Errorf("unsupported DAGScan kind: %s", kind)
	}
}
