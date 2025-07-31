package util

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/ipfs/go-cid"
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

// CidScanner returns a sql.Scanner that scans a CID from a byte slice into the
type CidScanner struct {
	Dst *cid.Cid
}

var _ sql.Scanner = CidScanner{}

func (cs CidScanner) Scan(value any) error {
	if value == nil {
		*cs.Dst = cid.Undef
		return nil
	}
	switch v := value.(type) {
	case []byte:
		c, err := cid.Cast(v)
		if err != nil {
			return fmt.Errorf("failed to cast to cid: %w", err)
		}
		*cs.Dst = c
	default:
		return fmt.Errorf("unsupported type for cid scanning: %T (%v)", v, v)
	}
	return nil
}
