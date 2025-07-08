package model

import (
	"fmt"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/storacha/guppy/pkg/preparation/types"
	"github.com/storacha/guppy/pkg/preparation/types/id"
)

// DAGScanState represents the state of a DAG scan.
type DAGScanState string

const (
	// DAGScanStateAwaitingChildren indicates that a directory entry is awaiting children to be processed.
	DAGScanStateAwaitingChildren DAGScanState = "awaiting_children"
	// DAGScanStatePending indicates that the file system entry is pending scan and has not started yet.
	DAGScanStatePending DAGScanState = "pending"
	// DAGScanStateRunning indicates that the file system entry is currently running.
	DAGScanStateRunning DAGScanState = "running"
	// DAGScanStateCompleted indicates that the file system entry has completed successfully.
	DAGScanStateCompleted DAGScanState = "completed"
	// DAGScanStateFailed indicates that the file system entry has failed.
	DAGScanStateFailed DAGScanState = "failed"
	// DAGScanStateCanceled indicates that the file system entry has been canceled.
	DAGScanStateCanceled DAGScanState = "canceled"
)

func validDAGScanState(state DAGScanState) bool {
	switch state {
	case DAGScanStateAwaitingChildren, DAGScanStatePending, DAGScanStateRunning, DAGScanStateCompleted, DAGScanStateFailed, DAGScanStateCanceled:
		return true
	default:
		return false
	}
}

func TerminatedState(state DAGScanState) bool {
	return state == DAGScanStateCompleted || state == DAGScanStateFailed || state == DAGScanStateCanceled
}

type DAGScan interface {
	FsEntryID() id.FSEntryID
	UploadID() id.UploadID
	CreatedAt() time.Time
	UpdatedAt() time.Time
	Error() error
	State() DAGScanState
	HasCID() bool
	CID() cid.Cid
	Start() error
	Restart() error
	Complete(cid cid.Cid) error
	Fail(errorMessage string) error
	Cancel() error
	isDAGScan()
}

type dagScan struct {
	fsEntryID    id.FSEntryID
	uploadID     id.UploadID
	createdAt    time.Time
	updatedAt    time.Time
	errorMessage *string
	state        DAGScanState
	cid          *cid.Cid // rootID is the ID of the root directory of the scan, if it has been completed
}

// validation conditions -- should not be callable externally, all scans outside this module MUST be valid
func validateDAGScan(d *dagScan) (*dagScan, error) {
	if d.fsEntryID == id.Nil {
		return nil, types.ErrEmpty{"fsEntryID"}
	}
	if !validDAGScanState(d.state) {
		return nil, fmt.Errorf("invalid scan state: %s", d.state)
	}
	if d.errorMessage != nil && d.state != DAGScanStateFailed {
		return nil, fmt.Errorf("error message is set but scan state is not 'failed': %s", d.state)
	}
	if d.cid != nil && d.state != DAGScanStateCompleted {
		return nil, fmt.Errorf("CID is set but scan state is not 'completed': %s", d.state)
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
func (d *dagScan) CreatedAt() time.Time {
	return d.createdAt
}
func (d *dagScan) UpdatedAt() time.Time {
	return d.updatedAt
}
func (d *dagScan) Error() error {
	if d.errorMessage == nil {
		return nil
	}
	return fmt.Errorf("dag scan error: %s", *d.errorMessage)
}
func (d *dagScan) State() DAGScanState {
	return d.state
}
func (d *dagScan) HasCID() bool {
	return d.cid != nil
}
func (d *dagScan) CID() cid.Cid {
	if d.cid == nil {
		return cid.Undef // Return an undefined CID if not set
	}
	return *d.cid
}

func (d *dagScan) Fail(errorMessage string) error {
	if TerminatedState(d.state) {
		return fmt.Errorf("cannot fail dag scan in state %s", d.state)
	}
	d.state = DAGScanStateFailed
	d.errorMessage = &errorMessage
	d.updatedAt = time.Now()
	return nil
}

func (d *dagScan) Complete(cid cid.Cid) error {
	if d.state != DAGScanStateRunning {
		return fmt.Errorf("cannot complete dag scan in state %s", d.state)
	}
	d.state = DAGScanStateCompleted
	d.errorMessage = nil
	d.updatedAt = time.Now()
	d.cid = &cid
	return nil
}

func (d *dagScan) Cancel() error {
	if TerminatedState(d.state) {
		return fmt.Errorf("cannot cancel dag scan in state %s", d.state)
	}
	d.state = DAGScanStateCanceled
	d.errorMessage = nil
	d.updatedAt = time.Now()
	return nil
}

func (d *dagScan) Start() error {
	if d.state != DAGScanStatePending {
		return fmt.Errorf("cannot start dag scan in state %s", d.state)
	}
	d.state = DAGScanStateRunning
	d.errorMessage = nil
	d.updatedAt = time.Now()
	return nil
}

func (d *dagScan) Restart() error {
	if d.state != DAGScanStateRunning && d.state != DAGScanStateCanceled {
		return fmt.Errorf("cannot restart dag scan in state %s", d.state)
	}
	d.state = DAGScanStatePending
	d.errorMessage = nil
	d.updatedAt = time.Now()
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

func (d *DirectoryDAGScan) ChildrenCompleted() error {
	if d.state != DAGScanStateAwaitingChildren {
		return fmt.Errorf("cannot finish children in state %s", d.state)
	}
	d.state = DAGScanStatePending
	d.updatedAt = time.Now()
	return nil
}

// NewFileDAGScan creates a new FileDAGScan with the given fsEntryID.
func NewFileDAGScan(fsEntryID id.FSEntryID, uploadID id.UploadID) (*FileDAGScan, error) {
	fds := &FileDAGScan{
		dagScan: dagScan{
			fsEntryID: fsEntryID,
			uploadID:  uploadID,
			createdAt: time.Now(),
			updatedAt: time.Now(),
			state:     DAGScanStatePending,
		},
	}
	if _, err := validateDAGScan(&fds.dagScan); err != nil {
		return nil, fmt.Errorf("failed to create FileDAGScan: %w", err)
	}
	return fds, nil
}

// NewDirectoryDAGScan creates a new DirectoryDAGScan with the given fsEntryID.
func NewDirectoryDAGScan(fsEntryID id.FSEntryID, uploadID id.UploadID) (*DirectoryDAGScan, error) {
	dds := &DirectoryDAGScan{
		dagScan: dagScan{
			fsEntryID: fsEntryID,
			uploadID:  uploadID,
			createdAt: time.Now(),
			updatedAt: time.Now(),
			state:     DAGScanStateAwaitingChildren,
		},
	}
	if _, err := validateDAGScan(&dds.dagScan); err != nil {
		return nil, fmt.Errorf("failed to create DirectoryDAGScan: %w", err)
	}
	return dds, nil
}

// DAGScanWriter is a function type for writing a DAGScan to the database.
type DAGScanWriter func(kind string, fsEntryID id.FSEntryID, uploadID id.UploadID, createdAt time.Time, updatedAt time.Time, errorMessage *string, state DAGScanState, cid *cid.Cid) error

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
		ds.createdAt,
		ds.updatedAt,
		ds.errorMessage,
		ds.state,
		ds.cid,
	)
}

// DAGScanScanner is a function type for scanning a DAGScan from the database.
type DAGScanScanner func(kind *string, fsEntryID *id.FSEntryID, uploadID *id.UploadID, createdAt *time.Time, updatedAt *time.Time, errorMessage **string, state *DAGScanState, cid **cid.Cid) error

// ReadDAGScanFromDatabase reads a DAGScan from the database using the provided scanner function.
func ReadDAGScanFromDatabase(scanner DAGScanScanner) (DAGScan, error) {
	var kind string
	var dagScan dagScan
	err := scanner(&kind, &dagScan.fsEntryID, &dagScan.uploadID, &dagScan.createdAt, &dagScan.updatedAt, &dagScan.errorMessage, &dagScan.state, &dagScan.cid)
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
