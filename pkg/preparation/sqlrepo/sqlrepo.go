package sqlrepo

import (
	"database/sql"
	_ "embed"
)

//go:embed schema.sql
var Schema string

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
func New(db *sql.DB) *repo {
	return &repo{db: db}
}

type repo struct {
	db *sql.DB
}
