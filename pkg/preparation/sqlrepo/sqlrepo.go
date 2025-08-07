package sqlrepo

import (
	"context"
	"database/sql"
	_ "embed"

	logging "github.com/ipfs/go-log/v2"
)

//go:embed schema.sql
var Schema string

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
func New(db *sql.DB) *repo {
	return &repo{db: db}
}

type repo struct {
	db *sql.DB
}

// WithTx runs the given function inside a DB transaction.
func (r *repo) WithTx(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	// Ensure transaction is always finalized
	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p) // re-throw panic after rollback
		}
	}()

	if err := fn(tx); err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			log.Errorf("Failed to rollback transaction: %v (original error: %v)", rollbackErr, err)
		}
		return err
	}

	return tx.Commit()
}
