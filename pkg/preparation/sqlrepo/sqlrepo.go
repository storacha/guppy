package sqlrepo

import (
	"database/sql"
	"embed"

	"github.com/asaskevich/EventBus"

	logging "github.com/ipfs/go-log/v2"
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

// New creates a new Repo instance with the given database connection.
func New(db *sql.DB) *Repo {
	bus := EventBus.New()
	return &Repo{db: db, bus: bus}
}

type Repo struct {
	db  *sql.DB
	bus EventBus.Bus
}

func (r *Repo) Close() error {
	return r.db.Close()
}

func (r *Repo) Subscriber() EventBus.BusSubscriber {
	return r.bus
}
