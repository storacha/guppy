package model

import (
	"fmt"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/guppy/pkg/preparation/types"
	"github.com/storacha/guppy/pkg/preparation/types/id"
)

// UploadState represents the state of a scan.
type UploadState string

const (
	// UploadStatePending indicates that the upload has been created, but never
	// started.
	UploadStatePending UploadState = "pending"

	// UploadStateStarted indicates that the upload has been started, but nothing
	// is complete yet.
	UploadStateStarted UploadState = "started"

	// UploadStateScanned indicates that the upload has completed the file system
	// scan.
	UploadStateScanned UploadState = "scanned"

	// UploadStateDagged indicates that the upload has completed the DAG scan and
	// sharding.
	UploadStateDagged UploadState = "dagged"

	// UploadStateCompleted indicates that the entire upload has completed
	// successfully.
	UploadStateCompleted UploadState = "completed"
)

func validUploadState(state UploadState) bool {
	switch state {
	case UploadStatePending, UploadStateStarted, UploadStateScanned, UploadStateDagged, UploadStateCompleted:
		return true
	default:
		return false
	}
}

// Upload represents the process of full or partial upload of data from a source, eventually represented as an upload in storacha.
type Upload struct {
	id              id.UploadID
	spaceDID        did.DID
	sourceID        id.SourceID
	createdAt       time.Time
	updatedAt       time.Time     // The last time the upload was updated
	state           UploadState   // The current state of the upload
	errorMessage    *string       // Optional error message if the upload fails
	rootFSEntryID   *id.FSEntryID // The ID of the root file system entry associated with this upload, if any
	rootCID         cid.Cid       // The root CID of the upload, if applicable
}

// ID returns the unique identifier of the upload.
func (u *Upload) ID() id.UploadID {
	return u.id
}

// SpaceDID returns the DID of the space associated with the upload.
func (u *Upload) SpaceDID() did.DID {
	return u.spaceDID
}


// SourceID returns the ID of the source associated with the upload.
func (u *Upload) SourceID() id.SourceID {
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

func (u *Upload) RootFSEntryID() id.FSEntryID {
	if u.rootFSEntryID == nil {
		return id.Nil // Return an empty FSEntryID if rootFSEntryID is not set
	}
	return *u.rootFSEntryID
}

func (u *Upload) RootCID() cid.Cid {
	return u.rootCID
}

func (u *Upload) Fail(errorMessage string) error {
	if u.state == UploadStateCompleted {
		return fmt.Errorf("cannot fail upload in state %s", u.state)
	}
	u.errorMessage = &errorMessage
	u.updatedAt = time.Now()
	return nil
}

func (u *Upload) Complete() error {
	if u.state != UploadStateDagged {
		return fmt.Errorf("cannot complete upload in state %s", u.state)
	}
	u.state = UploadStateCompleted
	u.errorMessage = nil
	u.updatedAt = time.Now()
	return nil
}

func (u *Upload) Cancel() error {
	return u.Fail("upload was canceled")
}

func (u *Upload) Start() error {
	if u.state != UploadStatePending {
		return fmt.Errorf("cannot start upload in state %s", u.state)
	}
	u.state = UploadStateStarted
	u.errorMessage = nil
	u.updatedAt = time.Now()
	return nil
}

func (u *Upload) SetRootFSEntryID(rootFSEntryID id.FSEntryID) error {
	u.rootFSEntryID = &rootFSEntryID
	u.updatedAt = time.Now()
	return nil
}

func (u *Upload) SetRootCID(rootCID cid.Cid) error {
	u.rootCID = rootCID
	u.updatedAt = time.Now()
	return nil
}

func validateUpload(upload *Upload) error {
	if upload.id == id.Nil {
		return types.ErrEmpty{Field: "upload ID"}
	}
	if upload.spaceDID.String() == "" {
		return types.ErrEmpty{Field: "space DID"}
	}
	if upload.sourceID == id.Nil {
		return types.ErrEmpty{Field: "source ID"}
	}
	if upload.createdAt.IsZero() {
		return types.ErrEmpty{Field: "created at"}
	}
	if !validUploadState(upload.state) {
		return fmt.Errorf("invalid upload state: %s", upload.state)
	}
	if upload.rootFSEntryID != nil && (upload.state == UploadStatePending || upload.state == UploadStateStarted) {
		return fmt.Errorf("root file system entry ID is set but upload has not completed file system scan")
	}
	if upload.rootCID != cid.Undef && (upload.state == UploadStatePending || upload.state == UploadStateStarted || upload.state == UploadStateScanned) {
		return fmt.Errorf("root CID is set but upload has not completed file system scan")
	}
	if upload.updatedAt.IsZero() {
		return types.ErrEmpty{Field: "updated at"}
	}
	return nil
}

// NewUpload creates a new Upload instance with the given parameters.
func NewUpload(spaceDID did.DID, sourceID id.SourceID) (*Upload, error) {
	upload := &Upload{
		id:       id.New(),
		spaceDID: spaceDID,
		sourceID:        sourceID,
		createdAt:       time.Now().UTC().Truncate(time.Second),
		updatedAt:       time.Now().UTC().Truncate(time.Second),
		state:           UploadStatePending,
		errorMessage:    nil,
	}
	if err := validateUpload(upload); err != nil {
		return nil, err
	}
	return upload, nil
}

// UploadWriter is a function type that defines the signature for writing uploads to a database row
type UploadWriter func(id id.UploadID, spaceDID did.DID, sourceID id.SourceID, createdAt time.Time, updatedAt time.Time, state UploadState, errorMessage *string, rootFSEntryID *id.FSEntryID, rootCID cid.Cid) error

// WriteUploadToDatabase writes an upload to the database using the provided writer function.
func WriteUploadToDatabase(writer UploadWriter, upload *Upload) error {
	return writer(upload.id, upload.spaceDID, upload.sourceID, upload.createdAt, upload.updatedAt, upload.state, upload.errorMessage, upload.rootFSEntryID, upload.rootCID)
}

// UploadScanner is a function type that defines the signature for scanning uploads from a database row
type UploadScanner func(id *id.UploadID, spaceDID *did.DID, sourceID *id.SourceID, createdAt *time.Time, updatedAt *time.Time, state *UploadState, errorMessage **string, rootFSEntryID **id.FSEntryID, rootCID *cid.Cid) error

// ReadUploadFromDatabase reads an upload from the database using the provided scanner function.
func ReadUploadFromDatabase(scanner UploadScanner) (*Upload, error) {
	var upload Upload

	if err := scanner(&upload.id, &upload.spaceDID, &upload.sourceID, &upload.createdAt, &upload.updatedAt, &upload.state, &upload.errorMessage, &upload.rootFSEntryID, &upload.rootCID); err != nil {
		return nil, err
	}

	if err := validateUpload(&upload); err != nil {
		return nil, err
	}

	return &upload, nil
}
