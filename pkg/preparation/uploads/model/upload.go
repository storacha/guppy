package model

import (
	"time"

	"github.com/ipfs/go-cid"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/guppy/pkg/preparation/types"
	"github.com/storacha/guppy/pkg/preparation/types/id"
)

// Upload represents the process of full or partial upload of data from a source, eventually represented as an upload in storacha.
type Upload struct {
	id            id.UploadID
	spaceDID      did.DID
	sourceID      id.SourceID
	createdAt     time.Time
	updatedAt     time.Time    // The last time the upload was updated
	rootFSEntryID id.FSEntryID // The ID of the root file system entry associated with this upload, if any
	rootCID       cid.Cid      // The root CID of the upload, if applicable
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

func (u *Upload) HasRootFSEntryID() bool {
	return u.rootFSEntryID != id.Nil
}

func (u *Upload) RootFSEntryID() id.FSEntryID {
	if u.rootFSEntryID == id.Nil {
		return id.Nil // Return an empty FSEntryID if rootFSEntryID is not set
	}
	return u.rootFSEntryID
}

func (u *Upload) RootCID() cid.Cid {
	return u.rootCID
}

func (u *Upload) SetRootFSEntryID(rootFSEntryID id.FSEntryID) error {
	u.rootFSEntryID = rootFSEntryID
	u.updatedAt = time.Now()
	return nil
}

func (u *Upload) SetRootCID(rootCID cid.Cid) error {
	u.rootCID = rootCID
	u.updatedAt = time.Now()
	return nil
}

// Invalidate clears the root CID and root FSEntryID of the upload, meaning that
// it needs to be scanned again.
func (u *Upload) Invalidate() error {
	u.rootCID = cid.Undef
	u.rootFSEntryID = id.Nil
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
	if upload.updatedAt.IsZero() {
		return types.ErrEmpty{Field: "updated at"}
	}
	return nil
}

// NewUpload creates a new Upload instance with the given parameters.
func NewUpload(spaceDID did.DID, sourceID id.SourceID) (*Upload, error) {
	upload := &Upload{
		id:        id.New(),
		spaceDID:  spaceDID,
		sourceID:  sourceID,
		createdAt: time.Now().UTC().Truncate(time.Second),
		updatedAt: time.Now().UTC().Truncate(time.Second),
	}
	if err := validateUpload(upload); err != nil {
		return nil, err
	}
	return upload, nil
}

// UploadWriter is a function type that defines the signature for writing uploads to a database row
type UploadWriter func(id id.UploadID, spaceDID did.DID, sourceID id.SourceID, createdAt time.Time, updatedAt time.Time, rootFSEntryID id.FSEntryID, rootCID cid.Cid) error

// WriteUploadToDatabase writes an upload to the database using the provided writer function.
func WriteUploadToDatabase(writer UploadWriter, upload *Upload) error {
	return writer(upload.id, upload.spaceDID, upload.sourceID, upload.createdAt, upload.updatedAt, upload.rootFSEntryID, upload.rootCID)
}

// UploadScanner is a function type that defines the signature for scanning uploads from a database row
type UploadScanner func(id *id.UploadID, spaceDID *did.DID, sourceID *id.SourceID, createdAt *time.Time, updatedAt *time.Time, rootFSEntryID *id.FSEntryID, rootCID *cid.Cid) error

// ReadUploadFromDatabase reads an upload from the database using the provided scanner function.
func ReadUploadFromDatabase(scanner UploadScanner) (*Upload, error) {
	var upload Upload

	if err := scanner(&upload.id, &upload.spaceDID, &upload.sourceID, &upload.createdAt, &upload.updatedAt, &upload.rootFSEntryID, &upload.rootCID); err != nil {
		return nil, err
	}

	if err := validateUpload(&upload); err != nil {
		return nil, err
	}

	return &upload, nil
}
