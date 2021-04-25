package config

import (
	"flag"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/grafana/loki/clients/pkg/promtail/client/elastic"
	"github.com/grafana/loki/clients/pkg/promtail/client/loki"
)

type ClientKind string
const (
	ElasticSearchClient ClientKind = "elasticsearch"
	LokiClient ClientKind = "loki"
)

// NOTE the helm chart for promtail and fluent-bit also have defaults for these values, please update to match if you make changes here.


// Config describes configuration for a HTTP pusher client.
type Config struct {
	Kind ClientKind
	// lokiconfig
	LokiConfig loki.LokiConfig
	// elasticsearch
	ElasticSearch elastic.EsClientConfig


}

// RegisterFlags with prefix registers flags where every name is prefixed by
// prefix. If prefix is a non-empty string, prefix should end with a period.
func (c *Config) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	c.LokiConfig.RegisterFlagsWithPrefix(prefix, f)
	c.ElasticSearch.RegisterFlagsWithPrefix(prefix, f)

}

// RegisterFlags registers flags.
func (c *Config) RegisterFlags(flags *flag.FlagSet) {
	c.RegisterFlagsWithPrefix("", flags)
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
		}
		cfg = raw{
			Kind: LokiClient,
			LokiConfig: loki.LokiConfig{
				BackoffConfig: util.BackoffConfig{
					MaxBackoff: loki.MaxBackoff,
					MaxRetries: loki.MaxRetries,
					MinBackoff: loki.MinBackoff,
				},
				BatchSize: loki.BatchSize,
				BatchWait: loki.BatchWait,
				Timeout:   loki.Timeout,
			},
			//BackoffConfig: util.BackoffConfig{
			//	MaxBackoff: MaxBackoff,
			//	MaxRetries: MaxRetries,
			//	MinBackoff: MinBackoff,
			//},
			//BatchSize: BatchSize,
			//BatchWait: BatchWait,
			//Timeout:   Timeout,
		}
	}

	if err := unmarshal(&cfg); err != nil {
		return err
	}

	*c = Config(cfg)
	return nil
}
