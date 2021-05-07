package config

import (
	"flag"
	"github.com/go-kit/kit/log"
	"github.com/grafana/loki/clients/pkg/promtail/api"
	"github.com/grafana/loki/clients/pkg/promtail/client/filesystem"
	"github.com/grafana/loki/clients/pkg/promtail/client/kafka"
	"github.com/grafana/loki/clients/pkg/promtail/client/loki"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

type Client interface {
	api.EntryHandler
	// Stop goroutine sending batch of entries without retries.
	StopNow()
}

type ClientKind string

const (
	KafkaClient ClientKind = "kafka"
	LokiClient  ClientKind = "loki"
	FileClient  ClientKind = "file-system"
)

type RunnerAble interface {
	api.EntryHandler
	// Stop goroutine sending batch of entries without retries.
	StopNow()
}

// NOTE the helm chart for promtail and fluent-bit also have defaults for these values, please update to match if you make changes here.

// Config describes configuration for a HTTP pusher client.
type Config struct {
	// lokiconfig
	LokiConfig loki.LokiConfig `yaml:"loki"`
	// kafka
	KafkaConfig kafka.KafkaConfig `yaml:"kafka"`
	// file system config
	FileSystemClient filesystem.FileClientConfig `yaml:"file_system_config"`
}

// RegisterFlags with prefix registers flags where every name is prefixed by
// prefix. If prefix is a non-empty string, prefix should end with a period.
func (c *Config) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	if c.LokiConfig.URL.URL != nil {
		c.LokiConfig.RegisterFlagsWithPrefix(prefix, f)
	} else if c.FileSystemClient.Path != "" {
		c.FileSystemClient.RegisterFlagsWithPrefix(prefix, f)
	}

}

// RegisterFlags registers flags.
func (c *Config) RegisterFlags(flags *flag.FlagSet) {
	if c.LokiConfig.URL.URL != nil {
		c.LokiConfig.RegisterFlags(flags)
	} else if c.FileSystemClient.Path != "" {
		c.FileSystemClient.RegisterFlags(flags)
	}
}

// UnmarshalYAML implement Yaml Unmarshaler
func (c *Config) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type raw Config

	var cfg raw
	if c.LokiConfig.URL.URL != nil {
		// we used flags to set that value, which already has sane default.
		cfg = raw(*c)
	} else {
		// force sane defaults.
		cfg = raw{
			LokiConfig:       loki.DefaultLokiConfig(),
			KafkaConfig:      kafka.DefaultKafkaConfig(),
			FileSystemClient: filesystem.DefaultFileSystemConfig(),
		}
	}

	if err := unmarshal(&cfg); err != nil {
		return err
	}

	*c = Config(cfg)
	return nil
}

// NewClientFromConfig return the an client according to the config
func (c *Config) NewClientFromConfig(reg prometheus.Registerer, logger log.Logger) (Client, error) {
	if c.LokiConfig.URL.URL != nil {
		return loki.New(reg, c.LokiConfig, logger)
	} else if c.FileSystemClient.Path != "" {
		return filesystem.NewFileSystemClient(reg, c.FileSystemClient, logger)
	} else if c.KafkaConfig.Url != "" {
		return kafka.NewKafkaClient(reg, c.KafkaConfig, logger)
	}
	return nil, errors.New("NewClientFromConfig error: unknown client type")
}
