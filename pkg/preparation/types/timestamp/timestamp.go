package timestamp

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"time"
)

type innerTime = time.Time

// Timestamp represents a Unix timestamp with second precision. It can be stored
// in a database as an integer (Unix time in seconds). For convenience, it
// embeds an underlying `time.Time` value; note that all the `time.Time` methods
// which return "times" will still return a `time.Time` value.
type Timestamp struct {
	innerTime
}

var _ driver.Valuer = (*Timestamp)(nil)
var _ sql.Scanner = (*Timestamp)(nil)

func New(t time.Time) Timestamp {
	if t.IsZero() {
		return Timestamp{}
	}
	return Timestamp{innerTime: t.UTC().Truncate(time.Second)}
}

// Now returns the current time as a Timestamp.
func Now() Timestamp {
	return New(time.Now())
}

func (t Timestamp) Value() (driver.Value, error) {
	return t.innerTime.Unix(), nil
}

func (t *Timestamp) Scan(src any) error {
	if src == nil {
		*t = Timestamp{}
		return nil
	}
	switch v := src.(type) {
	case int64:
		*t = Timestamp{innerTime: time.Unix(v, 0).UTC()}
	default:
		return fmt.Errorf("unsupported type for timestamp scanning: %T (%v)", v, v)
	}
	return nil
}
