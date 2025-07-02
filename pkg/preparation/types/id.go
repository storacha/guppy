package types

import "github.com/google/uuid"

// SourceID is an alias for uuid.UUID and uniquely identifies a source.
type SourceID uuid.UUID

// ConfigurationID is an alias for uuid.UUID and uniquely identifies a configuration.
type ConfigurationID uuid.UUID

// UploadID is an alias for uuid.UUID and uniquely identifies an upload.
type UploadID uuid.UUID

// ScanID is an alias for uuid.UUID and uniquely identifies a scan.
type ScanID uuid.UUID

// FSEntryID is an alias for uuid.UUID and uniquely identifies a filesystem entry.
type FSEntryID uuid.UUID
