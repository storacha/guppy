package sqlrepo

import (
	"regexp"
	"strings"
)

var pragmaRe = regexp.MustCompile(`(?mi)^\s*PRAGMA\s+[^;]*;\s*$`)

// TransformSQL adapts SQL written in SQLite dialect for the target dialect.
// For SQLite, it returns the input unchanged. For Postgres:
//   - Removes PRAGMA statements
//   - Removes the STRICT keyword from CREATE TABLE
//   - Replaces BLOB with BYTEA
//   - Replaces INTEGER with BIGINT (SQLite INTEGER is 64-bit; Postgres INTEGER is 32-bit)
func TransformSQL(sql string, dialect Dialect) string {
	if dialect == DialectSQLite {
		return sql
	}

	// Remove PRAGMA lines
	sql = pragmaRe.ReplaceAllString(sql, "")

	// Remove STRICT keyword from CREATE TABLE (appears as ") STRICT;" at end)
	sql = strings.ReplaceAll(sql, ") STRICT;", ");")

	// Replace BLOB type with BYTEA
	sql = strings.ReplaceAll(sql, " BLOB", " BYTEA")

	// Replace INTEGER with BIGINT to match SQLite's 64-bit integer range
	sql = strings.ReplaceAll(sql, " INTEGER", " BIGINT")

	return sql
}
