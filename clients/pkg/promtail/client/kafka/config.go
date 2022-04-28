package kafka

import (
	"github.com/cortexproject/cortex/pkg/util"
	lokiflag "github.com/grafana/loki/pkg/util/flagext"
	"time"
)

const (
	BatchWait      = 1 * time.Second
	BatchSize  int = 10 * 1024 * 1024
	MinBackoff     = 500 * time.Millisecond
	MaxBackoff     = 1 * time.Minute
	MaxRetries int = 10
	Timeout        = 10 * time.Second

	ProducerMaxMessageSize int = 10 * 1024 * 1024
)

type KafkaConfig struct {
	// dia of kakfa that client want to connect
	Url       string `yaml:"url"`
	Protocol  string `yaml:"protocol"`
	Topic     string `yaml:"topic"`
	GroupId   string `yaml:"groupId"`
	Partition int    `yaml:"partition"`

	BatchWait time.Duration `yaml:"batch_wait"`
	// The max number of entries  bytes can be cached in batch buffer
	BatchSize     int                `yaml:"batch_size"`
	BackoffConfig util.BackoffConfig `yaml:"backoff_config"`
	// The labels to add to any time series or alerts when communicating with loki
	ExternalLabels lokiflag.LabelSet `yaml:"external_labels,omitempty"`
	Timeout        time.Duration     `yaml:"timeout"`

	// The max number of message bytes that can be allowed to send to kafka server
	ProducerMaxMessageSize int `yaml:"producer_max_message_size"`
}

func DefaultKafkaConfig() KafkaConfig {
	return KafkaConfig{
		Protocol:  "tcp",
		Topic:     "example_topic",
		GroupId:   "",
		Partition: 0,
		BatchWait: BatchWait,
		BatchSize: BatchSize,
		BackoffConfig: util.BackoffConfig{
			MaxBackoff: MaxBackoff,
			MinBackoff: MinBackoff,
			MaxRetries: MaxRetries,
		},
		ExternalLabels:         lokiflag.LabelSet{},
		Timeout:                Timeout,
		ProducerMaxMessageSize: ProducerMaxMessageSize,
	}
}
