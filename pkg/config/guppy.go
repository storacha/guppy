package config

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/viper"

	"github.com/storacha/guppy/pkg/agentdata"
)

type GuppyConfig struct {
	Repo    RepoConfig    `yaml:"repo" mapstructure:"repo"`
	Upload  UploadConfig  `yaml:"upload" mapstructure:"upload"`
	Network NetworkConfig `yaml:"network" mapstructure:"network"`
}

type RepoConfig struct {
	Dir string `yaml:"dir" mapstructure:"dir"`
}

const agentDataFileName = "store.json"

func (r RepoConfig) ReadAgentData() (agentdata.AgentData, error) {
	agentPath := filepath.Join(r.Dir, agentDataFileName)
	return agentdata.ReadFromFile(agentPath)
}

func (r RepoConfig) WriteAgentData(data agentdata.AgentData) error {
	return data.WriteToFile(filepath.Join(r.Dir, agentDataFileName))
}

const uploadDBFileName = "preparation.db"

func (r RepoConfig) UploadDBPath() string {
	return filepath.Join(r.Dir, uploadDBFileName)
}

type UploadConfig struct {
	ShardSize string `yaml:"shard_size" mapstructure:"shard_size"`
}

type NetworkConfig struct {
	UploadService string `yaml:"upload_service" mapstructure:"upload_service"`
	IndexService  string `yaml:"indexer_service" mapstructure:"indexer_service"`
}

func Load() (GuppyConfig, error) {
	var out GuppyConfig
	if err := viper.UnmarshalExact(&out); err != nil {
		return GuppyConfig{}, fmt.Errorf("failed to load config: %w", err)
	}
	// bit of a hack, ensure the dir always exists
	if err := os.MkdirAll(out.Repo.Dir, 0700); err != nil {
		return GuppyConfig{}, fmt.Errorf("failed to create repo dir: %w", err)
	}
	return out, nil
}
