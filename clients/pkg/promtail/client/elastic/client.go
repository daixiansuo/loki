package elastic

import (
	"bytes"
	"context"
	"github.com/cenkalti/backoff"
	"github.com/sirupsen/logrus"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/kit/log"
	elasticsearch7 "github.com/elastic/go-elasticsearch/v7"
	"github.com/go-kit/kit/log/level"
	"github.com/grafana/loki/clients/pkg/promtail/api"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"sync"
	"time"
)

const (
	IndexerLabel model.LabelName = "indexer"
)


type client struct {
	cfg EsClientConfig
	entries chan api.Entry
	client *elasticsearch7.Client
	logger  log.Logger
	once sync.Once
	wg   sync.WaitGroup

	indexers []string

	ctx context.Context
	cancelFunc context.CancelFunc

}

// New return an elasticsearch client instance
func New(reg prometheus.Registerer, cfg EsClientConfig, logger log.Logger)(*client,error){
	escli, err := newClientFromConfig(&cfg)
	if err != nil{
		return nil, err
	}

	ctx,cancel := context.WithCancel(context.Background())
	c := &client{
		cfg:     EsClientConfig{},
		entries: make(chan api.Entry),
		client:  escli,
		once:    sync.Once{},
		wg:      sync.WaitGroup{},
		ctx: ctx,
		cancelFunc: cancel,
	}
	c.wg.Add(1)
	return c, nil
}


func newClientFromConfig(cfg *EsClientConfig)(*elasticsearch7.Client,error){
	retryBackOff := backoff.NewExponentialBackOff()
	escfg := elasticsearch7.Config{
		MaxRetries: 5,
		RetryOnStatus: []int{502,503, 504,429},
		RetryBackoff: func(attempt int) time.Duration {
			if attempt == 1{
				retryBackOff.Reset()
			}
			return retryBackOff.NextBackOff()
		},
	}
	if len(cfg.EsUser) != 0{
		escfg.Username = cfg.EsUser
	}
	if len(cfg.EsPassword) != 0{
		escfg.Password = cfg.EsPassword
	}
	if len(cfg.EsAddress) != 0{
		escfg.Addresses = []string{cfg.EsAddress}
	}
	client,err := elasticsearch7.NewClient(escfg)
	return client, err
}


func (c *client) Chan() chan<- api.Entry {
	return c.entries
}


func(c *client)run(){
	batches := map[string]*batch{}
	// 最小等待时间
	minWaitCheckFrequency := 10 * time.Millisecond
	// 最大等待时间
	maxWaitCheckFrequency := c.cfg.BatchWait / 10
	if maxWaitCheckFrequency < minWaitCheckFrequency {
		// 等待时间介于 用户输入/10 和 10毫秒之间
		maxWaitCheckFrequency = minWaitCheckFrequency
	}
	maxWaitCheck := time.NewTicker(maxWaitCheckFrequency)

	defer func() {
		maxWaitCheck.Stop()
		// Send all pending batches
		for indexerId, batch := range batches {
			// 清空所有的batch，将还没发送出去的全部发送出去
			c.sendBatch(indexerId, batch)
		}

		c.wg.Done()
	}()

	for {
		select {
		case e, ok := <-c.entries:
			if !ok {
				return
			}
			indexerId := c.generateIndexerId(&e)
			batch, ok := batches[indexerId]

			// If the batch doesn't exist yet, we create a new one with the entry
			if !ok {
				batches[indexerId] = newBatch(e)
				break
			}

			// If adding the entry to the batch will increase the size over the max
			// size allowed, we do send the current batch and then create a new one
			if batch.sizeBytesAfter(e) > c.cfg.BatchSize {
				c.sendBatch(indexerId, batch)

				batches[indexerId] = newBatch(e)
				break
			}

			// The max size of the batch isn't reached, so we can add the entry
			batch.add(e)

		case <-maxWaitCheck.C:
			// Send all batches whose max wait time has been reached
			for indexer, batch := range batches {
				if batch.age() < c.cfg.BatchWait {
					continue
				}

				c.sendBatch(indexer, batch)
				delete(batches, indexer)
			}
		}
	}
}


// Stop the client.
func (c *client) Stop() {
	c.once.Do(func() { close(c.entries) })
	c.wg.Wait()
}

// StopNow stops the client without retries
func (c *client) StopNow() {
	// cancel will stop retrying http requests.
	c.cancelFunc()
	c.Stop()
}



func (c *client) sendBatch(indexerId string, batch *batch) {
	buf, _, err := batch.encode()
	if err != nil {
		level.Error(c.logger).Log("msg", "error encoding batch", "error", err)
		return
	}


	backoff := util.NewBackoff(c.ctx, c.cfg.BackoffConfig)
	var status int
	for {

		// send uses `timeout` internally, so `context.Background` is good enough.
		status, err = c.send(context.Background(), indexerId, buf)


		// Only retry 429s, 500s and connection-level errors.
		if status > 0 && status != 429 && status/100 != 5 {
			break
		}

		level.Warn(c.logger).Log("msg", "error sending batch, will retry", "status", status, "error", err)

		backoff.Wait()

		// Make sure it sends at least once before checking for retry.
		if !backoff.Ongoing() {
			break
		}
	}

	if err != nil {
		level.Error(c.logger).Log("msg", "final error sending batch", "status", status, "error", err)

	}
}

func (c *client) send(ctx context.Context, indexerId string, buf []byte) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()
	logrus.Errorf("[%v]debug->essend: %v", indexerId,string(buf))
	_,_ = c.client.Bulk(bytes.NewReader(buf), c.client.Bulk.WithIndex(indexerId))
	return 0, nil
	// If the tenant ID is not empty promtail is running in multi-tenant mode, so
	// we should send it to Loki

}



func (c *client) generateIndexerId(e *api.Entry) string {
	// get indexer name
	if indexer, ok := e.Labels[IndexerLabel]; ok {
		return string(indexer)
	}
	return ""
}