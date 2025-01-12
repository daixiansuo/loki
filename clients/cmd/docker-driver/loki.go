package main

import (
	"bytes"
	loki2 "github.com/grafana/loki/clients/pkg/promtail/client/loki"

	"github.com/docker/docker/daemon/logger"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"

	"github.com/grafana/loki/clients/pkg/logentry/stages"
	"github.com/grafana/loki/clients/pkg/promtail/api"
	"github.com/grafana/loki/pkg/logproto"
)

var jobName = "docker"

type loki struct {
	client  loki2.Client
	handler api.EntryHandler
	labels  model.LabelSet
	logger  log.Logger

	stop func()
}

// New create a new Loki logger that forward logs to Loki instance
func New(logCtx logger.Info, logger log.Logger) (logger.Logger, error) {
	logger = log.With(logger, "container_id", logCtx.ContainerID)
	cfg, err := parseConfig(logCtx)
	if err != nil {
		return nil, err
	}
	c, err := loki2.New(prometheus.DefaultRegisterer, cfg.clientConfig, logger)
	if err != nil {
		return nil, err
	}
	var handler api.EntryHandler = c
	var stop func() = func() {}
	if len(cfg.pipeline.PipelineStages) != 0 {
		pipeline, err := stages.NewPipeline(logger, cfg.pipeline.PipelineStages, &jobName, prometheus.DefaultRegisterer)
		if err != nil {
			return nil, err
		}
		handler = pipeline.Wrap(c)
		stop = handler.Stop
	}
	return &loki{
		client:  c,
		labels:  cfg.labels,
		logger:  logger,
		handler: handler,
		stop:    stop,
	}, nil
}

// Log implements `logger.Logger`
func (l *loki) Log(m *logger.Message) error {
	if len(bytes.Fields(m.Line)) == 0 {
		level.Debug(l.logger).Log("msg", "ignoring empty line", "line", string(m.Line))
		return nil
	}
	lbs := l.labels.Clone()
	if m.Source != "" {
		lbs["source"] = model.LabelValue(m.Source)
	}
	l.handler.Chan() <- api.Entry{
		Labels: lbs,
		Entry: logproto.Entry{
			Timestamp: m.Timestamp,
			Line:      string(m.Line),
		},
	}
	return nil
}

// Log implements `logger.Logger`
func (l *loki) Name() string {
	return driverName
}

// Log implements `logger.Logger`
func (l *loki) Close() error {
	l.stop()
	l.client.StopNow()
	return nil
}
