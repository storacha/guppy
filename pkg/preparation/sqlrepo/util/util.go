package util

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/guppy/pkg/preparation/types/id"
)

// timestampScanner returns a [sql.Scanner] that scans a timestamp (as an
// integer of Unix time in seconds) into the given [time.Time] pointer.
func TimestampScanner(t *time.Time) tsScanner {
	return tsScanner{dst: t}
}

type tsScanner struct {
	dst *time.Time
}

var _ sql.Scanner = tsScanner{}

func (ts tsScanner) Scan(value any) error {
	if value == nil {
		*ts.dst = time.Time{}
		return nil
	}
	switch v := value.(type) {
	case int64:
		*ts.dst = time.Unix(v, 0).UTC()
	default:
		return fmt.Errorf("unsupported type for timestamp scanning: %T (%v)", v, v)
	}
	return nil
}

// DbCid returns a [sql.Scanner] that scans a CID from a `[]byte` DB value (a
// `BLOB`), and a [driver.Valuer] that writes a CID to the DB as a `[]byte` (a
// `BLOB`), treating [cid.Undef] as `NULL`, and vice versa.
func DbCid(cid *cid.Cid) dbCid {
	return dbCid{cid: cid}
}

// dbCid returns a sql.Scanner that scans a CID from a byte slice into the
type dbCid struct {
	cid *cid.Cid
}

var _ driver.Valuer = dbCid{}
var _ sql.Scanner = dbCid{}

func (dc dbCid) Value() (driver.Value, error) {
	if dc.cid == nil || dc.cid.Equals(cid.Undef) {
		return nil, nil // Value should be `NULL` (returned as `nil`)
	}
	return dc.cid.Bytes(), nil
}

func (dc dbCid) Scan(value any) error {
	if value == nil {
		*dc.cid = cid.Undef
		return nil
	}
	switch v := value.(type) {
	case []byte:
		c, err := cid.Cast(v)
		if err != nil {
			return fmt.Errorf("failed to cast to cid: %w", err)
		}
		*dc.cid = c
	default:
		return fmt.Errorf("unsupported type for cid scanning: %T (%v)", v, v)
	}
	return nil
}

// DbID returns a [sql.Scanner] that scans an [id.ID] (a UUID) from a `[]byte`
// DB value (a `BLOB`), and a [driver.Valuer] that writes an [id.ID] to the DB
// as a `[]byte` (a `BLOB`), treating [id.Nil] as `NULL`, and vice versa.
func DbID(id *id.ID) dbID {
	return dbID{id: id}
}

// dbID returns a sql.Scanner that scans a CID from a byte slice into the
type dbID struct {
	id *id.ID
}

var _ driver.Valuer = dbID{}
var _ sql.Scanner = dbID{}

func (dc dbID) Value() (driver.Value, error) {
	if dc.id == nil || *dc.id == id.Nil {
		return nil, nil // Value should be `NULL` (returned as `nil`)
	}
	return (*dc.id)[:], nil
}

func (dc dbID) Scan(value any) error {
	if value == nil {
		*dc.id = id.Nil
		return nil
	}
	switch v := value.(type) {
	case []byte:
		if len(v) != 16 {
			return fmt.Errorf("failed to cast to id: invalid length %d", len(v))
		}
		var i id.ID
		copy(i[:], v)
		*dc.id = i
	default:
		return fmt.Errorf("unsupported type for id scanning: %T (%v)", v, v)
	}
	return nil
}

func DbDID(did *did.DID) dbDID {
	return dbDID{did: did}
}

type dbDID struct {
	did *did.DID
}

var _ driver.Valuer = dbDID{}
var _ sql.Scanner = dbDID{}

func (dd dbDID) Value() (driver.Value, error) {
	if dd.did == nil || !dd.did.Defined() {
		return nil, nil // Return nil for undefined DID
	}
	return dd.did.Bytes(), nil
}

func (dd dbDID) Scan(value any) error {
	if value == nil {
		*dd.did = did.Undef
		return nil
	}
	switch v := value.(type) {
	case []byte:
		d, err := did.Decode(v)
		if err != nil {
			return fmt.Errorf("failed to cast to did: %w", err)
		}
		*dd.did = d
	default:
		return fmt.Errorf("unsupported type for did scanning: %T (%v)", v, v)
	}
	return nil
}

// DbBytes returns a [sql.Scanner] that scans a type with underlying type
// `[]byte` from a `[]byte` DB value (a `BLOB`), and a [driver.Valuer] that
// writes that type to the DB as a `[]byte` (a `BLOB`), treating nil and empty
// slices as `NULL`, and vice versa.
func DbBytes[V ~[]byte](v *V) dbBytes[V] {
	return dbBytes[V]{v: v}
}

type dbBytes[V ~[]byte] struct {
	v *V
}

var _ driver.Valuer = dbBytes[[]byte]{}
var _ sql.Scanner = dbBytes[[]byte]{}

func (db dbBytes[V]) Value() (driver.Value, error) {
	if db.v == nil || *db.v == nil || len(*db.v) == 0 {
		return nil, nil // Return nil for nil or empty slices
	}
	return *db.v, nil
}

func (db dbBytes[V]) Scan(value any) error {
	var zero V

	if value == nil {
		*db.v = zero
		return nil
	}

	b, ok := value.([]byte)
	if !ok {
		return fmt.Errorf("unsupported type for TK scanning: %T (%v)", value, value)
	}
	*db.v = b

	return nil
}
