package sqlrepo

import (
	"database/sql"
	"fmt"

	"github.com/ipfs/go-cid"
)

// cidScanner returns a sql.Scanner that scans a CID from a byte slice into the
type cidScanner struct {
	dst *cid.Cid
}

var _ sql.Scanner = cidScanner{}

func (cs cidScanner) Scan(value any) error {
	if value == nil {
		*cs.dst = cid.Undef
		return nil
	}
	switch v := value.(type) {
	case []byte:
		c, err := cid.Cast(v)
		if err != nil {
			return fmt.Errorf("failed to cast to cid: %w", err)
		}
		*cs.dst = c
	default:
		return fmt.Errorf("unsupported type for cid scanning: %T (%v)", v, v)
	}
	return nil
}
