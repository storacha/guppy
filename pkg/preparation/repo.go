package preparation

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	_ "modernc.org/sqlite"
)

const (
	defaultJournalMode = "WAL"
	defaultSynchronous = "NORMAL"
	defaultBusyTimeout = 3 * time.Second
	defaultForeignKeys = true
)

func OpenRepo(ctx context.Context, dbPath string) (*sqlrepo.Repo, error) {
	var pragmas []string
	pragmas = append(pragmas, fmt.Sprintf("_pragma=journal_mode(%s)", defaultJournalMode))
	pragmas = append(pragmas, fmt.Sprintf("_pragma=busy_timeout(%d)", defaultBusyTimeout.Milliseconds()))
	pragmas = append(pragmas, fmt.Sprintf("_pragma=synchronous(%s)", defaultSynchronous))
	pragmas = append(pragmas, fmt.Sprintf("_pragma=foreign_keys(%d)", bool2int(defaultForeignKeys)))

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

	repo := sqlrepo.New(db)
	return repo, nil
}

func bool2int(b bool) int {
	if b {
		return 1
	}
	return 0
}
