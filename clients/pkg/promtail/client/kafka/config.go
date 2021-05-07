package kafka

import (
	"github.com/cortexproject/cortex/pkg/util"
	lokiflag "github.com/grafana/loki/pkg/util/flagext"
	"time"
)

const (
	BatchWait      = 1 * time.Second
	BatchSize  int = 1024 * 1024
	MinBackoff     = 500 * time.Millisecond
	MaxBackoff     = 1 * time.Minute
	MaxRetries int = 10
	Timeout        = 10 * time.Second

	DefaultKafkaMaxMessageSize = 10048576
)

type KafkaConfig struct {
	// dia of kakfa that client want to connect
	Url       string `yaml:"url"`
	Protocol  string `yaml:"protocol"`
	Topic     string `yaml:"topic"`
	GroupId   string `yaml:"groupId"`
	Partition int    `yaml:"partition"`

	// producer
	//MaxMessageBytes int `json:"max_message_bytes"`


	BatchWait     time.Duration      `yaml:"batch_wait"`
	BatchSize     int                `yaml:"batch_size"`
	BackoffConfig util.BackoffConfig `yaml:"backoff_config"`
	// The labels to add to any time series or alerts when communicating with loki
	ExternalLabels lokiflag.LabelSet `yaml:"external_labels,omitempty"`
	Timeout        time.Duration     `yaml:"timeout"`
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
		ExternalLabels: lokiflag.LabelSet{},
		Timeout:        Timeout,
		//MaxMessageBytes: DefaultKafkaMaxMessageSize,
	}
}
