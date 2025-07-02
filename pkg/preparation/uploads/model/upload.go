package model

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/storacha/guppy/pkg/preparation/types"
)

// UploadState represents the state of a scan.
type UploadState string

const (
	// UploadStatePending indicates that the upload is pending and has not started yet.
	UploadStatePending UploadState = "pending"
	// UploadStateScanning indicates that the upload is currently scanning.
	UploadStateScanning UploadState = "scanning"
	// UploadStateGeneratingDAG indicates that the upload is currently generating a DAG.
	UploadStateGeneratingDAG UploadState = "generating_dag"
	// UploadStateSharding indicates that the upload is currently sharding data.
	UploadStateSharding UploadState = "sharding"
	// UploadStateUploading indicates that the upload is currently uploading data.
	UploadStateUploading UploadState = "uploading"
	// UploadStateCompleted indicates that the upload has completed successfully.
	UploadStateCompleted UploadState = "completed"
	// UploadStateFailed indicates that the upload has failed.
	UploadStateFailed UploadState = "failed"
	// UploadStateCancelled indicates that the upload has been cancelled.
	UploadStateCancelled UploadState = "cancelled"
)

func validUploadState(state UploadState) bool {
	switch state {
	case UploadStatePending, UploadStateScanning, UploadStateGeneratingDAG, UploadStateSharding, UploadStateUploading, UploadStateCompleted, UploadStateFailed, UploadStateCancelled:
		return true
	default:
		return false
	}
}

func TerminatedState(state UploadState) bool {
	return state == UploadStateCompleted || state == UploadStateFailed || state == UploadStateCancelled
}

func RestartableState(state UploadState) bool {
	return state == UploadStateScanning || state == UploadStateGeneratingDAG || state == UploadStateSharding || state == UploadStateUploading || state == UploadStateCancelled
}

// Upload represents the process of full or partial upload of data from a source, eventually represented as an upload in storacha.
type Upload struct {
	id              types.UploadID
	configurationID types.ConfigurationID
	sourceID        types.SourceID
	createdAt       time.Time
	updatedAt       time.Time        // The last time the upload was updated
	state           UploadState      // The current state of the upload
	errorMessage    *string          // Optional error message if the upload fails
	rootFSEntryID   *types.FSEntryID // The ID of the root file system entry associated with this upload, if any
	rootCID         *cid.Cid         // The root CID of the upload, if applicable
}

// ID returns the unique identifier of the upload.
func (u *Upload) ID() types.UploadID {
	return u.id
}

// ConfigurationID returns the ID of the configuration associated with the upload.
func (u *Upload) ConfigurationID() types.ConfigurationID {
	return u.configurationID
}

// SourceID returns the ID of the source associated with the upload.
func (u *Upload) SourceID() types.SourceID {
	return u.sourceID
}

// CreatedAt returns the creation time of the upload.
func (u *Upload) CreatedAt() time.Time {
	return u.createdAt
}

// State returns the current state of the upload.
func (u *Upload) State() UploadState {
	return u.state
}

// ErrorMessage returns the error message associated with the upload, if any.
func (u *Upload) Error() error {
	if u.errorMessage == nil {
		return nil
	}
	return fmt.Errorf("upload error: %s", *u.errorMessage)
}

func (u *Upload) HasRootFSEntryID() bool {
	return u.rootFSEntryID != nil
}

func (u *Upload) RootFSEntryID() types.FSEntryID {
	if u.rootFSEntryID == nil {
		return uuid.Nil // Return an empty FSEntryID if rootFSEntryID is not set
	}
	return *u.rootFSEntryID
}

func (u *Upload) Fail(errorMessage string) error {
	if TerminatedState(u.state) {
		return fmt.Errorf("cannot fail upload in state %s", u.state)
	}
	u.state = UploadStateFailed
	u.errorMessage = &errorMessage
	u.updatedAt = time.Now()
	return nil
}

func (u *Upload) Complete() error {
	if u.state != UploadStateUploading {
		return fmt.Errorf("cannot complete upload in state %s", u.state)
	}
	u.state = UploadStateCompleted
	u.errorMessage = nil
	u.updatedAt = time.Now()
	return nil
}

func (u *Upload) Cancel() error {
	if TerminatedState(u.state) {
		return fmt.Errorf("cannot cancel upload in state %s", u.state)
	}
	u.state = UploadStateCancelled
	u.errorMessage = nil
	u.updatedAt = time.Now()
	return nil
}

func (u *Upload) Start() error {
	if u.state != UploadStatePending {
		return fmt.Errorf("cannot start upload in state %s", u.state)
	}
	u.state = UploadStateScanning
	u.errorMessage = nil
	u.updatedAt = time.Now()
	return nil
}

func (u *Upload) ScanComplete(rootFSEntryID types.FSEntryID) error {
	if u.state != UploadStateScanning {
		return fmt.Errorf("cannot complete scan in state %s", u.state)
	}
	u.state = UploadStateGeneratingDAG
	u.errorMessage = nil
	u.rootFSEntryID = &rootFSEntryID
	u.updatedAt = time.Now()
	return nil
}

func (u *Upload) DAGGenerationComplete(rootCID cid.Cid) error {
	if u.state != UploadStateGeneratingDAG {
		return fmt.Errorf("cannot complete DAG generation in state %s", u.state)
	}
	u.state = UploadStateSharding
	u.errorMessage = nil
	u.rootCID = &rootCID
	u.updatedAt = time.Now()
	return nil
}

func (u *Upload) ShardingComplete() error {
	if u.state != UploadStateSharding {
		return fmt.Errorf("cannot complete sharding in state %s", u.state)
	}
	u.state = UploadStateUploading
	u.errorMessage = nil
	u.updatedAt = time.Now()
	return nil
}

func (u *Upload) Restart() error {
	if !RestartableState(u.state) {
		return fmt.Errorf("cannot restart upload in state %s", u.state)
	}
	u.state = UploadStatePending
	u.rootFSEntryID = nil // Reset root file system entry ID
	u.rootCID = nil       // Reset root CID if applicable
	u.errorMessage = nil
	u.updatedAt = time.Now()
	return nil
}

func validateUpload(upload *Upload) error {

	if upload.id == uuid.Nil {
		return types.ErrEmpty{"upload ID"}
	}
	if upload.configurationID == uuid.Nil {
		return types.ErrEmpty{"configuration ID"}
	}
	if upload.sourceID == uuid.Nil {
		return types.ErrEmpty{"source ID"}
	}
	if upload.createdAt.IsZero() {
		return types.ErrEmpty{"created at"}
	}
	if !validUploadState(upload.state) {
		return fmt.Errorf("invalid upload state: %s", upload.state)
	}
	if upload.errorMessage != nil && upload.state != UploadStateFailed {
		return fmt.Errorf("error message is set but upload state is not 'failed': %s", upload.state)
	}
	if upload.rootFSEntryID != nil && (upload.state == UploadStatePending || upload.state == UploadStateScanning) {
		return fmt.Errorf("root file system entry ID is set but upload has not completed file system scan")
	}
	if upload.rootCID != nil && (upload.state == UploadStatePending || upload.state == UploadStateScanning || upload.state == UploadStateGeneratingDAG) {
		return fmt.Errorf("root CID is set but upload has not completed file system scan")
	}
	if upload.updatedAt.IsZero() {
		return types.ErrEmpty{"updated at"}
	}
	return nil
}

// NewUpload creates a new Upload instance with the given parameters.
func NewUpload(configurationID types.ConfigurationID, sourceID types.SourceID) (*Upload, error) {
	upload := &Upload{
		id:              uuid.New(),
		configurationID: configurationID,
		sourceID:        sourceID,
		createdAt:       time.Now(),
		updatedAt:       time.Now(),
		state:           UploadStatePending,
		errorMessage:    nil,
	}
	if err := validateUpload(upload); err != nil {
		return nil, err
	}
	return upload, nil
}

// UploadWriter is a function type that defines the signature for writing uploads to a database row
type UploadWriter func(id types.UploadID, configurationID types.ConfigurationID, sourceID types.SourceID, createdAt time.Time, updatedAt time.Time, state UploadState, errorMessage *string, rootFSEntryID *types.FSEntryID, rootCID *cid.Cid) error

// WriteUploadToDatabase writes an upload to the database using the provided writer function.
func WriteUploadToDatabase(writer UploadWriter, upload *Upload) error {
	return writer(upload.id, upload.configurationID, upload.sourceID, upload.createdAt, upload.updatedAt, upload.state, upload.errorMessage, upload.rootFSEntryID, upload.rootCID)
}

// UploadScanner is a function type that defines the signature for scanning uploads from a database row
type UploadScanner func(id *types.UploadID, configurationID *types.ConfigurationID, sourceID *types.SourceID, createdAt *time.Time, updatedAt *time.Time, state *UploadState, errorMessage **string, rootFSEntryID **types.FSEntryID, rootCID **cid.Cid) error

// ReadUploadFromDatabase reads an upload from the database using the provided scanner function.
func ReadUploadFromDatabase(scanner UploadScanner) (*Upload, error) {
	var upload Upload

	if err := scanner(&upload.id, &upload.configurationID, &upload.sourceID, &upload.createdAt, &upload.updatedAt, &upload.state, &upload.errorMessage, &upload.rootFSEntryID, &upload.rootCID); err != nil {
		return nil, err
	}

	if err := validateUpload(&upload); err != nil {
		return nil, err
	}

	return &upload, nil
}
