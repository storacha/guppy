package config

import (
	"path/filepath"

	"github.com/spf13/viper"
)

type Config struct {
	Identity        IdentityConfig `mapstruct:"identity"`
	Repo            RepoConfig     `mapstruct:"repo"`
	UploadService   ServiceConfig  `mapstruct:"upload_service"`
	IndexingService ServiceConfig  `mapstruct:"indexing_service"`
}

type IdentityConfig struct {
	KeyFile string `mapstructure:"key_file" yaml:"key_file"`
}

type RepoConfig struct {
	DataDir string `mapstructure:"data_dir" yaml:"data_dir"`
}

func (r RepoConfig) AgentDataFilePath() string {
	return filepath.Join(r.DataDir, "store.json")
}

func (r RepoConfig) PreparationDatabaseFilePath() string {
	return filepath.Join(r.DataDir, "preparation.db")
}

type ServiceConfig struct {
	DID string `mapstructure:"did" yaml:"did"`
	URL string `mapstructure:"url" yaml:"url"`
}

func Load() (Config, error) {
	var out Config
	if err := viper.Unmarshal(&out); err != nil {
		return out, err
	}
	return out, nil
}
