package main

import (
	"fmt"
	loki2 "github.com/grafana/loki/clients/pkg/promtail/client/loki"

	"github.com/go-kit/kit/log"
)

type bufferConfig struct {
	buffer     bool
	bufferType string
	dqueConfig dqueConfig
}

var defaultBufferConfig = bufferConfig{
	buffer:     false,
	bufferType: "dque",
	dqueConfig: defaultDqueConfig,
}

// NewBuffer makes a new buffered Client.
func NewBuffer(cfg *config, logger log.Logger) (loki2.Client, error) {
	switch cfg.bufferConfig.bufferType {
	case "dque":
		return newDque(cfg, logger)
	default:
		return nil, fmt.Errorf("failed to parse bufferType: %s", cfg.bufferConfig.bufferType)
	}
}
