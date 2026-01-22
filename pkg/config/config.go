package config

import "github.com/spf13/viper"

type Config struct {
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
		return out, err
	}
	if err := out.Validate(); err != nil {
		return out, err
	}
	return out, nil
}
