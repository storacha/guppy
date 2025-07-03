package types

import "github.com/google/uuid"

// SourceID uniquely identifies a source.
type SourceID uuid.UUID

// ConfigurationID uniquely identifies a configuration.
type ConfigurationID uuid.UUID

// UploadID uniquely identifies an upload.
type UploadID uuid.UUID

// ScanID uniquely identifies a scan.
type ScanID uuid.UUID

// FSEntryID uniquely identifies a filesystem entry.
type FSEntryID uuid.UUID
