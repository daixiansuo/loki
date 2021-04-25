package elastic

import (
	"flag"
	"github.com/cortexproject/cortex/pkg/util"
	"time"
)



// EsClientConfig elasticsearch client reference configuration
type EsClientConfig struct {
	EsAddress string `json:"es_address"`
	EsUser 	string `json:"es_user"`
	EsPassword string `json:"es_password"`

	IndexerLabel string `json:"indexer_label"`
	IndexerName string `json:"indexer_name"`

	Timeout time.Duration

	BatchWait time.Duration
	BatchSize int
	BackoffConfig util.BackoffConfig `yaml:"backoff_config"`

}


func (c *EsClientConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	// do nothing
	return
}