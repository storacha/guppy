package config

import (
	"fmt"

	"github.com/spf13/viper"
)

type Config struct {
	Repo    RepoConfig    `mapstructure:"repo" toml:"repo"`
	Gateway GatewayConfig `mapstructure:"gateway" toml:"gateway"`
}

func (c Config) Validate() error {
	err := validateConfig(c)
	if err != nil {
		return err
	}
	return c.Gateway.Validate()
}

func Load[T Validatable]() (T, error) {
	var out T
	if err := viper.Unmarshal(&out); err != nil {
		return out, fmt.Errorf("unable to decode config, %w", err)
	}
	if err := out.Validate(); err != nil {
		return out, fmt.Errorf("invalid config, %w", err)
	}
	return out, nil
}
