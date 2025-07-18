package util

import (
	"database/sql"
	"fmt"

	"github.com/ipfs/go-cid"
)

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
