package config

import (
	"fmt"
	"path/filepath"
)

type RepoConfig struct {
	Dir string `mapstructure:"data_dir"`
}

func (r RepoConfig) Validate() error {
	if r.Dir == "" {
		return fmt.Errorf("repo path required")
	}
	return nil
}

func (r RepoConfig) DatabasePath() string {
	return filepath.Join(r.Dir, "preparation.db")
}
