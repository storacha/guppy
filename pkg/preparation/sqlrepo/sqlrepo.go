package sqlrepo

import (
	"database/sql"
	"embed"

	logging "github.com/ipfs/go-log/v2"

	"github.com/storacha/guppy/pkg/bus"
)

//go:embed schema.sql
var Schema string

//go:embed migrations/*.sql
var MigrationsFS embed.FS

var log = logging.Logger("preparation/sqlrepo")

func NullString(s *string) sql.NullString {
	if s == nil {
		return sql.NullString{Valid: false}
	}
	return sql.NullString{String: *s, Valid: true}
}

func Null[T any](v *T) sql.Null[T] {
	if v == nil {
		return sql.Null[T]{Valid: false}
	}
	return sql.Null[T]{Valid: true, V: *v}
}

type Option func(*Repo)

func WithEventBus(bus bus.Bus) Option {
	return func(r *Repo) {
		r.bus = bus
	}
}

// New creates a new Repo instance with the given database connection.
func New(db *sql.DB, opts ...Option) *Repo {
	r := &Repo{db: db, bus: &bus.NoopBus{}}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

type Repo struct {
	db  *sql.DB
	bus bus.Publisher
}

func (r *Repo) Close() error {
	return r.db.Close()
}
