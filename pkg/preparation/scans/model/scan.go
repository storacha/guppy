package model

import (
	"fmt"
	"time"

	"github.com/storacha/guppy/pkg/preparation/types"
	"github.com/storacha/guppy/pkg/preparation/types/id"
)

// ScanState represents the state of a scan.
type ScanState string

const (
	// ScanStatePending indicates that the scan is pending and has not started yet.
	ScanStatePending ScanState = "pending"
	// ScanStateRunning indicates that the scan is currently running.
	ScanStateRunning ScanState = "running"
	// ScanStateCompleted indicates that the scan has completed successfully.
	ScanStateCompleted ScanState = "completed"
	// ScanStateFailed indicates that the scan has failed.
	ScanStateFailed ScanState = "failed"
	// ScanStateCanceled indicates that the scan has been canceled.
	ScanStateCanceled ScanState = "canceled"
)

func validScanState(state ScanState) bool {
	switch state {
	case ScanStatePending, ScanStateRunning, ScanStateCompleted, ScanStateFailed, ScanStateCanceled:
		return true
	default:
		return false
	}
}

// Scan represents a single scan of a source, usually associated with an upload
type Scan struct {
	id           id.ScanID
	uploadID     id.UploadID
	rootID       *id.FSEntryID // rootID is the ID of the root directory of the scan, if it has been completed
	createdAt    time.Time
	updatedAt    time.Time
	errorMessage *string
	state        ScanState
}

// validation conditions -- should not be callable externally, all scans outside this module MUST be valid
func validateScan(s *Scan) (*Scan, error) {
	if s.id == id.Nil {
		return nil, types.ErrEmpty{"id"}
	}
	if s.uploadID == id.Nil {
		return nil, types.ErrEmpty{"update id"}
	}
	if !validScanState(s.state) {
		return nil, fmt.Errorf("invalid scan state: %s", s.state)
	}
	if s.errorMessage != nil && s.state != ScanStateFailed {
		return nil, fmt.Errorf("error message is set but scan state is not 'failed': %s", s.state)
	}
	if s.rootID != nil && s.state != ScanStateCompleted {
		return nil, fmt.Errorf("root ID is set but scan state is not 'completed': %s", s.state)
	}
	return s, nil
}

// accessors

func (s *Scan) ID() id.ScanID {
	return s.id
}

func (s *Scan) UploadID() id.UploadID {
	return s.uploadID
}

func (s *Scan) CreatedAt() time.Time {
	return s.createdAt
}

func (s *Scan) UpdatedAt() time.Time {
	return s.updatedAt
}

func (s *Scan) Error() error {
	if s.errorMessage == nil {
		return nil
	}
	return fmt.Errorf("scan error: %s", *s.errorMessage)
}
func (s *Scan) State() ScanState {
	return s.state
}

func (s *Scan) HasRootID() bool {
	return s.rootID != nil
}

func (s *Scan) RootID() id.FSEntryID {
	if s.rootID == nil {
		return id.Nil // Return an empty FSEntryID if rootID is not set
	}
	return *s.rootID
}

func (s *Scan) Fail(errorMessage string) error {
	if s.state == ScanStateCompleted || s.state == ScanStateCanceled {
		return fmt.Errorf("cannot fail scan in state %s", s.state)
	}
	s.state = ScanStateFailed
	s.errorMessage = &errorMessage
	s.updatedAt = time.Now()
	return nil
}

func (s *Scan) Complete(rootID id.FSEntryID) error {
	if s.state != ScanStateRunning {
		return fmt.Errorf("cannot complete scan in state %s", s.state)
	}
	s.state = ScanStateCompleted
	s.errorMessage = nil
	s.updatedAt = time.Now()
	s.rootID = &rootID
	return nil
}

func (s *Scan) Cancel() error {
	if s.state == ScanStateCompleted || s.state == ScanStateFailed {
		return fmt.Errorf("cannot cancel scan in state %s", s.state)
	}
	s.state = ScanStateCanceled
	s.errorMessage = nil
	s.updatedAt = time.Now()
	return nil
}

func (s *Scan) Start() error {
	if s.state != ScanStatePending {
		return fmt.Errorf("cannot start scan in state %s", s.state)
	}
	s.state = ScanStateRunning
	s.errorMessage = nil
	s.updatedAt = time.Now()
	return nil
}

func NewScan(uploadID id.UploadID) (*Scan, error) {
	scan := &Scan{
		id:        id.New(),
		uploadID:  uploadID,
		state:     ScanStatePending,
		createdAt: time.Now().UTC().Truncate(time.Second),
		updatedAt: time.Now().UTC().Truncate(time.Second),
	}
	return validateScan(scan)
}

type ScanScanner func(
	id *id.ScanID,
	uploadID *id.UploadID,
	rootID **id.FSEntryID,
	createdAt *time.Time,
	updatedAt *time.Time,
	state *ScanState,
	errorMessage **string,
) error

func ReadScanFromDatabase(scanner ScanScanner) (*Scan, error) {
	scan := &Scan{}
	err := scanner(
		&scan.id,
		&scan.uploadID,
		&scan.rootID,
		&scan.createdAt,
		&scan.updatedAt,
		&scan.state,
		&scan.errorMessage,
	)
	if err != nil {
		return nil, fmt.Errorf("reading scan from database: %w", err)
	}
	return validateScan(scan)
}

type ScanWriter func(
	id id.ScanID,
	uploadID id.UploadID,
	rootID *id.FSEntryID,
	createdAt time.Time,
	updatedAt time.Time,
	state ScanState,
	errorMessage *string,
) error

func WriteScanToDatabase(scan *Scan, writer ScanWriter) error {
	return writer(
		scan.id,
		scan.uploadID,
		scan.rootID,
		scan.createdAt,
		scan.updatedAt,
		scan.state,
		scan.errorMessage,
	)
}
