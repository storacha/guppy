package preparation

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/pressly/goose/v3"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	_ "modernc.org/sqlite"
)

const (
	defaultJournalMode      = "WAL"
	defaultSynchronous      = "NORMAL"
	defaultBusyTimeout      = 60 * time.Second
	defaultForeignKeys      = true
	defaultJournalSizeLimit = 256 * 1024 * 1024 // 256MB - limits WAL file growth
)

func OpenRepo(ctx context.Context, dbPath string) (*sqlrepo.Repo, error) {
	var pragmas []string
	pragmas = append(pragmas, fmt.Sprintf("_pragma=journal_mode(%s)", defaultJournalMode))
	pragmas = append(pragmas, fmt.Sprintf("_pragma=busy_timeout(%d)", defaultBusyTimeout.Milliseconds()))
	pragmas = append(pragmas, fmt.Sprintf("_pragma=synchronous(%s)", defaultSynchronous))
	pragmas = append(pragmas, fmt.Sprintf("_pragma=foreign_keys(%d)", bool2int(defaultForeignKeys)))
	pragmas = append(pragmas, fmt.Sprintf("_pragma=journal_size_limit(%d)", defaultJournalSizeLimit))

	connStr := fmt.Sprintf("file:%s?%s", dbPath, strings.Join(pragmas, "&"))
	db, err := sql.Open("sqlite", connStr)

	if err != nil {
		return nil, fmt.Errorf("command failed to open SQLite database at %s: %w", dbPath, err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	_, err = db.ExecContext(ctx, sqlrepo.Schema)
	if err != nil {
		return nil, fmt.Errorf("command failed to execute schema: %w", err)
	}

	// Disable goose logging
	// this isn't ideal, but goose doesn't provide a way to disable logging directly
	// see: https://github.com/pressly/goose/issues/975
	log.Default().SetOutput(io.Discard)

	goose.SetBaseFS(sqlrepo.MigrationsFS)

	if err := goose.SetDialect("sqlite3"); err != nil {
		return nil, fmt.Errorf("failed to set goose dialect: %w", err)
	}

	if err := goose.Up(db, "migrations"); err != nil {
		return nil, fmt.Errorf("failed to apply migrations: %w", err)
	}

	// Re-enable logging
	log.Default().SetOutput(os.Stderr)

	repo := sqlrepo.New(db)

	// Start automatic WAL checkpointing to prevent unbounded WAL growth
	// during long-running upload operations.
	repo.StartAutoCheckpoint(ctx, sqlrepo.DefaultCheckpointInterval)

	return repo, nil
}

func bool2int(b bool) int {
	if b {
		return 1
	}
	return 0
}
