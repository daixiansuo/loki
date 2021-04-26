package elastic

import (
	"flag"
	"github.com/cortexproject/cortex/pkg/util"
	"time"
)

const (
	BatchWait      = 1 * time.Second
	BatchSize  int = 1024 * 1024
	MinBackoff     = 500 * time.Millisecond
	MaxBackoff     = 5 * time.Minute
	MaxRetries int = 10
	Timeout        = 10 * time.Second
)


// EsClientConfig elasticsearch client reference configuration
type EsClientConfig struct {
	EsAddress string `yaml:"url"`
	EsUser 	string `yaml:"es_user"`
	EsPassword string `yaml:"es_password"`

	IndexerLabel string `yaml:"indexer_label"`
	IndexerName string `yaml:"indexer_name"`

	Timeout time.Duration

	BatchWait time.Duration
	BatchSize int `yaml:"batch_size"`
	BackoffConfig util.BackoffConfig `yaml:"backoff_config"`

}

func DefaultEsClientConfig()EsClientConfig{
	return EsClientConfig{
		Timeout:       Timeout,
		BatchWait:     BatchWait,
		BatchSize:     BatchSize,
		BackoffConfig: util.BackoffConfig{
			MaxBackoff: MaxBackoff,
			MaxRetries: MaxRetries,
			MinBackoff: MinBackoff,
		},
	}
}

func (c *EsClientConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	// do nothing
	return
}
func(c *EsClientConfig)RegisterFlags(flags *flag.FlagSet){

}