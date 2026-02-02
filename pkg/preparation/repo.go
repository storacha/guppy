package preparation

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/lib/pq"
	"github.com/pressly/goose/v3"
	_ "modernc.org/sqlite"

	appconfig "github.com/storacha/guppy/pkg/config"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
)

const (
	defaultJournalMode      = "WAL"
	defaultSynchronous      = "NORMAL"
	defaultBusyTimeout      = 60 * time.Second
	defaultForeignKeys      = true
	defaultJournalSizeLimit = 256 * 1024 * 1024 // 256MB - limits WAL file growth
)

// OpenRepo opens a database connection and applies migrations.
// If cfg.IsPostgres(), it connects to PostgreSQL; otherwise it uses SQLite.
func OpenRepo(ctx context.Context, cfg appconfig.RepoConfig, opts ...sqlrepo.Option) (*sqlrepo.Repo, error) {
	if cfg.IsPostgres() {
		return openPostgresRepo(ctx, cfg.DatabaseURL, opts...)
	}
	return openSQLiteRepo(ctx, cfg.DatabasePath(), opts...)
}

func openSQLiteRepo(ctx context.Context, dbPath string, opts ...sqlrepo.Option) (*sqlrepo.Repo, error) {
	var pragmas []string
	pragmas = append(pragmas, fmt.Sprintf("_pragma=journal_mode(%s)", defaultJournalMode))
	pragmas = append(pragmas, fmt.Sprintf("_pragma=busy_timeout(%d)", defaultBusyTimeout.Milliseconds()))
	pragmas = append(pragmas, fmt.Sprintf("_pragma=synchronous(%s)", defaultSynchronous))
	pragmas = append(pragmas, fmt.Sprintf("_pragma=foreign_keys(%d)", bool2int(defaultForeignKeys)))
	pragmas = append(pragmas, fmt.Sprintf("_pragma=journal_size_limit(%d)", defaultJournalSizeLimit))

	connStr := fmt.Sprintf("file:%s?%s", dbPath, strings.Join(pragmas, "&"))
	db, err := sql.Open("sqlite", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open SQLite database at %s: %w", dbPath, err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	if err := migrate(ctx, db, sqlrepo.DialectSQLite); err != nil {
		return nil, err
	}

	repo, err := sqlrepo.NewWithDialect(db, sqlrepo.DialectSQLite, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create SQL repo: %w", err)
	}

	repo.StartPeriodicCheckpoint(ctx, sqlrepo.DefaultCheckpointInterval)

	return repo, nil
}

func openPostgresRepo(ctx context.Context, connURL string, opts ...sqlrepo.Option) (*sqlrepo.Repo, error) {
	db, err := sql.Open("postgres", connURL)
	if err != nil {
		return nil, fmt.Errorf("failed to open PostgreSQL database: %w", err)
	}

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	// Verify connectivity early so we can give a clear error message.
	if err := db.PingContext(ctx); err != nil {
		if errors.Is(err, pq.ErrSSLNotSupported) {
			return nil, fmt.Errorf("failed to connect to PostgreSQL: %w\nhint: add ?sslmode=disable to your database URL if the server does not support SSL", err)
		}
		return nil, fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}

	if err := migrate(ctx, db, sqlrepo.DialectPostgres); err != nil {
		return nil, err
	}

	repo, err := sqlrepo.NewWithDialect(db, sqlrepo.DialectPostgres, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create SQL repo: %w", err)
	}

	return repo, nil
}

func migrate(ctx context.Context, db *sql.DB, dialect sqlrepo.Dialect) error {
	var gooseDialect goose.Dialect
	if dialect == sqlrepo.DialectPostgres {
		gooseDialect = goose.DialectPostgres
	} else {
		gooseDialect = goose.DialectSQLite3
	}

	provider, err := goose.NewProvider(gooseDialect, db, nil,
		goose.WithGoMigrations(sqlrepo.GooseMigrations(dialect)...),
	)
	if err != nil {
		return fmt.Errorf("failed to create goose provider: %w", err)
	}

	if _, err := provider.Up(ctx); err != nil {
		return fmt.Errorf("failed to apply migrations: %w", err)
	}

	return nil
}

func bool2int(b bool) int {
	if b {
		return 1
	}
	return 0
}
