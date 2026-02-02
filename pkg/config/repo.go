package config

import (
	"fmt"
	"path/filepath"
	"strings"
)

type RepoConfig struct {
	Dir         string `mapstructure:"data_dir"`
	DatabaseURL string `mapstructure:"database_url"`
}

func (r RepoConfig) Validate() error {
	if r.DatabaseURL == "" && r.Dir == "" {
		return fmt.Errorf("either repo data dir or database URL required")
	}
	return nil
}

func (r RepoConfig) DatabasePath() string {
	return filepath.Join(r.Dir, "preparation.db")
}

// IsPostgres returns true if the configured database URL points to a PostgreSQL server.
func (r RepoConfig) IsPostgres() bool {
	return strings.HasPrefix(r.DatabaseURL, "postgres://") ||
		strings.HasPrefix(r.DatabaseURL, "postgresql://")
}
