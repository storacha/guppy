package config

import (
	"fmt"
	"net/http"
	"net/url"
	"path/filepath"

	"github.com/spf13/viper"
	uclient "github.com/storacha/go-ucanto/client"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/transport/car"
	uhttp "github.com/storacha/go-ucanto/transport/http"
)

type Config struct {
	Repo    RepoConfig    `mapstructure:"repo"`
	Client  ClientConfig  `mapstructure:"client"`
	Indexer ServiceConfig `mapstructure:"indexer"`
}

type RepoConfig struct {
	Dir string `mapstructure:"data_dir"`
}

func (r RepoConfig) DatabasePath() string {
	return filepath.Join(r.Dir, "preparation.db")
}

type ClientConfig struct {
	UploadService ServiceConfig `mapstructure:"upload_service"`
}

type ServiceConfig struct {
	DID string `mapstructure:"did" yaml:"did"`
	URL string `mapstructure:"url" yaml:"url"`
}

func (s ServiceConfig) AsConnection(c *http.Client) (uclient.Connection, error) {
	serviceURL, err := url.Parse(s.URL)
	if err != nil {
		return nil, err
	}
	servicePrincipal, err := did.Parse(s.DID)
	if err != nil {
		return nil, err
	}
	channel := uhttp.NewChannel(serviceURL, uhttp.WithClient(c))
	return uclient.NewConnection(servicePrincipal, channel, uclient.WithOutboundCodec(car.NewOutboundCodec()))
}

func Load() (Config, error) {
	if viper.ConfigFileUsed() == "" {
		// if a config file wasn't explicitly set, try and read one in but don't fail since it may not exist
		_ = viper.ReadInConfig()
	} else {
		// else a config file was explicitly set, read it in and fail if we can't
		if err := viper.ReadInConfig(); err != nil {
			return Config{}, fmt.Errorf("could not read config file: %w", err)
		}
	}
	var out Config
	if err := viper.Unmarshal(&out); err != nil {
		return out, fmt.Errorf("unable to decode config, %w", err)
	}
	return out, nil
}
