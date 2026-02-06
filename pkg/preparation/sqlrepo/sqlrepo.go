package sqlrepo

import (
	"context"
	"database/sql"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	logging "github.com/ipfs/go-log/v2"

	"github.com/jmoiron/sqlx"
	"github.com/storacha/guppy/pkg/bus"
)

// Dialect represents the SQL dialect being used.
type Dialect int

const (
	DialectSQLite Dialect = iota
	DialectPostgres
)

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

// DefaultCheckpointInterval is the default interval for automatic WAL checkpointing.
const DefaultCheckpointInterval = 5 * time.Minute

const DefaultPreparedStmtCacheSize = 128

// New creates a new Repo instance with the given database connection.
// Defaults to SQLite dialect.
func New(db *sql.DB, opts ...Option) (*Repo, error) {
	return NewWithDialect(db, DialectSQLite, opts...)
}

// NewWithDialect creates a new Repo instance with the given database connection and dialect.
func NewWithDialect(db *sql.DB, dialect Dialect, opts ...Option) (*Repo, error) {
	cache, err := lru.NewWithEvict(DefaultPreparedStmtCacheSize, func(key string, stmt *sql.Stmt) {
		stmt.Close()
	})
	if err != nil {
		return nil, err
	}
	r := &Repo{db: db, dialect: dialect, bus: &bus.NoopBus{}, preparedStmts: cache}

	for _, opt := range opts {
		opt(r)
	}
	return r, nil
}

type Repo struct {
	db             *sql.DB
	dialect        Dialect
	bus            bus.Publisher
	preparedStmts  *lru.Cache[string, *sql.Stmt]
	checkpointStop chan struct{}
}

// Dialect returns the SQL dialect used by this Repo.
func (r *Repo) Dialect() Dialect {
	return r.dialect
}

// rebind converts a query with ? placeholders to the appropriate dialect.
// For SQLite, it's a no-op. For Postgres, ? becomes $1, $2, etc.
func (r *Repo) rebind(query string) string {
	if r.dialect == DialectPostgres {
		return sqlx.Rebind(sqlx.DOLLAR, query)
	}
	return query
}

// StartPeriodicCheckpoint starts a background goroutine that periodically
// checkpoints the WAL to prevent unbounded growth during long operations.
// Call StopPeriodicCheckpoint to stop it, or it will be stopped when Close is called.
// This is a no-op for PostgreSQL, which manages WAL internally.
func (r *Repo) StartPeriodicCheckpoint(ctx context.Context, interval time.Duration) {
	if r.dialect == DialectPostgres {
		return
	}
	if r.checkpointStop != nil {
		return // already running
	}
	r.checkpointStop = make(chan struct{})
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-r.checkpointStop:
				return
			case <-ticker.C:
				if err := r.Checkpoint(ctx); err != nil {
					log.Warnf("periodic WAL checkpoint failed: %v", err)
				} else {
					log.Debug("periodic WAL checkpoint completed")
				}
			}
		}
	}()
}

func (r *Repo) prepareStmt(ctx context.Context, query string) (*sql.Stmt, error) {
	if stmt, ok := r.preparedStmts.Get(query); ok {
		return stmt, nil
	}
	stmt, err := r.db.PrepareContext(ctx, r.rebind(query))
	if err != nil {
		return nil, err
	}
	_ = r.preparedStmts.Add(query, stmt)
	return stmt, nil
}

// StopPeriodicCheckpoint stops the background checkpoint goroutine if running.
func (r *Repo) StopPeriodicCheckpoint() {
	if r.checkpointStop != nil {
		close(r.checkpointStop)
		r.checkpointStop = nil
	}
}

func (r *Repo) Close() error {
	r.StopPeriodicCheckpoint()
	r.preparedStmts.Purge()
	return r.db.Close()
}

// Checkpoint forces a WAL checkpoint to transfer data from the write-ahead log
// to the main database file. This should be called periodically during long
// operations to prevent unbounded WAL growth. The RESTART mode runs until the WAL
// is fully checkpointed, so that write can start from the beginning of the file.
// This is a no-op for PostgreSQL.
func (r *Repo) Checkpoint(ctx context.Context) error {
	if r.dialect == DialectPostgres {
		return nil
	}
	_, err := r.db.ExecContext(ctx, "PRAGMA wal_checkpoint(RESTART)")
	return err
}
