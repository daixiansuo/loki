package main

import (
	"github.com/go-kit/kit/log"
	loki2 "github.com/grafana/loki/clients/pkg/promtail/client/loki"
	"github.com/prometheus/client_golang/prometheus"
)

// NewClient creates a new client based on the fluentbit configuration.
func NewClient(cfg *config, logger log.Logger) (loki2.Client, error) {
	if cfg.bufferConfig.buffer {
		return NewBuffer(cfg, logger)
	}
	return loki2.New(prometheus.DefaultRegisterer, cfg.clientConfig, logger)
}
