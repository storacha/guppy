package sqlrepo

import (
	"database/sql"
	"embed"

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
	return &Repo{db: db}
}

type Repo struct {
	db *sql.DB
}

func (r *Repo) Close() error {
	return r.db.Close()
}
