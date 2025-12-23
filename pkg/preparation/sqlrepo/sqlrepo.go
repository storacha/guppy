package sqlrepo

import (
	"context"
	"database/sql"
	"embed"
	"time"

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

// DefaultCheckpointInterval is the default interval for automatic WAL checkpointing.
const DefaultCheckpointInterval = 5 * time.Minute

// New creates a new Repo instance with the given database connection.
func New(db *sql.DB) *Repo {
	return &Repo{db: db}
}

type Repo struct {
	db             *sql.DB
	checkpointStop chan struct{}
}

// StartAutoCheckpoint starts a background goroutine that periodically
// checkpoints the WAL to prevent unbounded growth during long operations.
// Call StopAutoCheckpoint to stop it, or it will be stopped when Close is called.
func (r *Repo) StartAutoCheckpoint(ctx context.Context, interval time.Duration) {
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

// StopAutoCheckpoint stops the background checkpoint goroutine if running.
func (r *Repo) StopAutoCheckpoint() {
	if r.checkpointStop != nil {
		close(r.checkpointStop)
		r.checkpointStop = nil
	}
}

func (r *Repo) Close() error {
	r.StopAutoCheckpoint()
	return r.db.Close()
}

// Checkpoint forces a WAL checkpoint to transfer data from the write-ahead log
// to the main database file. This should be called periodically during long
// operations to prevent unbounded WAL growth. The TRUNCATE mode resets the WAL
// file to zero bytes after a successful checkpoint.
func (r *Repo) Checkpoint(ctx context.Context) error {
	_, err := r.db.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)")
	return err
}
