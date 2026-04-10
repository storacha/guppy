package config

import "fmt"

type PreparationConfig struct {
	// Number of replicas to request per shard. Cannot be greater than 3. Not
	// recommended to set less than 3 except for testing purposes. Should be
	// 1 or more if set.
	Replicas uint `mapstructure:"replicas" toml:"replicas"`
}

func (c PreparationConfig) Validate() error {
	if c.Replicas == 0 {
		return fmt.Errorf("preparation.replicas must be greater than 0")
	}
	return nil
}
