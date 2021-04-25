package client

import (
	"errors"
	"github.com/grafana/loki/clients/pkg/promtail/client/config"
	"github.com/grafana/loki/clients/pkg/promtail/client/loki"
	"github.com/grafana/loki/clients/pkg/promtail/client/elastic"
	"github.com/sirupsen/logrus"
	"sync"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/grafana/loki/clients/pkg/promtail/api"

	"github.com/grafana/loki/pkg/util/flagext"
)


type Client interface {
	api.EntryHandler
	// Stop goroutine sending batch of entries without retries.
	StopNow()
}

// MultiClient is client pushing to one or more loki instances.
type MultiClient struct {
	clients []Client
	entries chan api.Entry
	wg      sync.WaitGroup

	once sync.Once
}

// NewMulti creates a new client
func NewMulti(reg prometheus.Registerer, logger log.Logger, externalLabels flagext.LabelSet, cfgs ...config.Config) (Client, error) {
	if len(cfgs) == 0 {
		return nil, errors.New("at least one client config should be provided")
	}

	clients := make([]Client, 0, len(cfgs))
	for _, cfg := range cfgs {

		// Merge the provided external labels from the single client config/command line with each client config from
		// `clients`. This is done to allow --client.external-labels=key=value passed at command line to apply to all clients
		// The order here is specified to allow the yaml to override the command line flag if there are any labels
		// which exist in both the command line arguments as well as the yaml, and while this is
		// not typically the order of precedence, the assumption here is someone providing a specific config in
		// yaml is doing so explicitly to make a key specific to a client.
		var client Client
		var err error
		switch cfg.Kind {
		case config.LokiClient:
			cfg.LokiConfig.ExternalLabels = flagext.LabelSet{LabelSet: externalLabels.Merge(cfg.LokiConfig.ExternalLabels.LabelSet)}
			client, err = loki.New(reg, cfg.LokiConfig, logger)
			if err != nil{
				return nil, err
			}
		case config.ElasticSearchClient:
			client,err = elastic.New(reg, cfg.ElasticSearch, logger)
			if err != nil{
				return nil, err
			}
		default:
			logrus.Errorf("unknown client type, current only support send to to elasticsearch or loki ")
		}
		//client, err := loki.New(reg, cfg.LokiConfig, logger)
		//if err != nil {
		//	return nil, err
		//}
		clients = append(clients, client)

	}
	multi := &MultiClient{
		clients: clients,
		entries: make(chan api.Entry),
	}
	multi.start()
	return multi, nil
}

func (m *MultiClient) start() {
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		for e := range m.entries {
			for _, c := range m.clients {
				c.Chan() <- e
			}
		}
	}()
}

func (m *MultiClient) Chan() chan<- api.Entry {
	return m.entries
}

// Stop implements Client
func (m *MultiClient) Stop() {
	m.once.Do(func() { close(m.entries) })
	m.wg.Wait()
	for _, c := range m.clients {
		c.Stop()
	}
}

// StopNow implements Client
func (m *MultiClient) StopNow() {
	for _, c := range m.clients {
		c.StopNow()
	}
}
