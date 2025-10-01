package repo

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
)

func Make(ctx context.Context, dbPath string) (*sqlrepo.Repo, func() error, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, nil, fmt.Errorf("command failed to open SQLite database at %s: %w", dbPath, err)
	}
	db.SetMaxOpenConns(1)

	_, err = db.ExecContext(ctx, sqlrepo.Schema)
	if err != nil {
		return nil, nil, fmt.Errorf("command failed to execute schema: %w", err)
	}

	repo := sqlrepo.New(db)
	return repo, db.Close, nil
}
