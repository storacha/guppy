package sqlrepo

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/pressly/goose/v3"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// migrationVersionRe extracts the numeric version prefix from a migration filename.
var migrationVersionRe = regexp.MustCompile(`^(\d+)_`)

// GooseMigrations reads embedded .sql migration files, transforms the SQL for the
// given dialect, and returns goose Migration objects.
func GooseMigrations(dialect Dialect) []*goose.Migration {
	entries, err := migrationsFS.ReadDir("migrations")
	if err != nil {
		panic(fmt.Sprintf("failed to read embedded migrations: %v", err))
	}

	// Sort entries by name to ensure consistent ordering.
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})

	var migrations []*goose.Migration
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}

		match := migrationVersionRe.FindStringSubmatch(entry.Name())
		if match == nil {
			panic(fmt.Sprintf("migration file %q does not have a version prefix", entry.Name()))
		}
		version, err := strconv.ParseInt(match[1], 10, 64)
		if err != nil {
			panic(fmt.Sprintf("migration file %q has invalid version: %v", entry.Name(), err))
		}

		raw, err := migrationsFS.ReadFile(path.Join("migrations", entry.Name()))
		if err != nil {
			panic(fmt.Sprintf("failed to read migration file %q: %v", entry.Name(), err))
		}

		upSQL, downSQL := parseGooseSQL(string(raw))
		upSQL = TransformSQL(upSQL, dialect)
		downSQL = TransformSQL(downSQL, dialect)

		var up, down *goose.GoFunc
		if upSQL != "" {
			s := upSQL
			up = &goose.GoFunc{RunTx: func(ctx context.Context, tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, s)
				return err
			}}
		}
		if downSQL != "" {
			s := downSQL
			down = &goose.GoFunc{RunTx: func(ctx context.Context, tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, s)
				return err
			}}
		}

		migrations = append(migrations, goose.NewGoMigration(version, up, down))
	}
	return migrations
}

// parseGooseSQL splits a goose-annotated SQL file into up and down sections.
// It looks for "-- +goose Up" and "-- +goose Down" markers, stripping
// "-- +goose StatementBegin" / "-- +goose StatementEnd" annotations.
func parseGooseSQL(content string) (upSQL, downSQL string) {
	var upLines, downLines []string
	var current *[]string

	for _, line := range strings.Split(content, "\n") {
		trimmed := strings.TrimSpace(line)
		switch {
		case trimmed == "-- +goose Up":
			current = &upLines
		case trimmed == "-- +goose Down":
			current = &downLines
		case trimmed == "-- +goose StatementBegin",
			trimmed == "-- +goose StatementEnd":
			// Strip these annotations; we execute the whole section as one statement.
			continue
		default:
			if current != nil {
				*current = append(*current, line)
			}
		}
	}

	return strings.TrimSpace(strings.Join(upLines, "\n")),
		strings.TrimSpace(strings.Join(downLines, "\n"))
}
