package util

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/storacha/go-ucanto/did"
)

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

// timestampScanner returns a sql.Scanner that scans a timestamp (as an integer
// of Unix time in seconds) into the given time.Time pointer.
func TimestampScanner(t *time.Time) tsScanner {
	return tsScanner{dst: t}
}

func DbCid(cid *cid.Cid) dbCid {
	return dbCid{cid: cid}
}

func DbDID(did *did.DID) dbDID {
	return dbDID{did: did}
}

// dbCid returns a sql.Scanner that scans a CID from a byte slice into the
type dbCid struct {
	cid *cid.Cid
}

var _ driver.Valuer = dbCid{}
var _ sql.Scanner = dbCid{}

func (dc dbCid) Value() (driver.Value, error) {
	if dc.cid == nil || dc.cid.Equals(cid.Undef) {
		return nil, nil // Return nil for undefined CID
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
