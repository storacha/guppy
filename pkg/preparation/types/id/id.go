package id

import (
	"database/sql"
	"database/sql/driver"

	"github.com/google/uuid"
)

type ID uuid.UUID

var _ driver.Valuer = (*ID)(nil)
var _ sql.Scanner = (*ID)(nil)

var Nil = ID(uuid.Nil)

func New() ID {
	return ID(uuid.New())
}

func (id ID) Value() (driver.Value, error) {
	return id[:], nil
}

func (id *ID) Scan(src any) error {
	var u uuid.UUID
	if err := u.Scan(src); err != nil {
		return err
	}
	*id = ID(u)
	return nil
}

func (id ID) String() string {
	return uuid.UUID(id).String()
}

// SourceID is an alias for ID and uniquely identifies a source.
type SourceID = ID

// ConfigurationID is an alias for ID and uniquely identifies a configuration.
type ConfigurationID = ID

// UploadID is an alias for ID and uniquely identifies an upload.
type UploadID = ID

// ScanID is an alias for ID and uniquely identifies a scan.
type ScanID = ID

// FSEntryID is an alias for ID and uniquely identifies a filesystem entry.
type FSEntryID = ID

// FSEntryID is an alias for ID and uniquely identifies a shard.
type ShardID = ID
