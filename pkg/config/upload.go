package config

import "fmt"

type UploadConfig struct {
	// Number of replicas to request per shard. Cannot be greater than 3. Not
	// recommended to set less than 3 except for testing purposes. Should be
	// 1 or more if set.
	Replicas uint `mapstructure:"replicas" toml:"replicas"`
}

func (c UploadConfig) Validate() error {
	if c.Replicas == 0 {
		return fmt.Errorf("upload.replicas must be greater than 0")
	}
	return nil
}
