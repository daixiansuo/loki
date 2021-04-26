package elastic

import (
	"context"
	"fmt"
	elasticsearch7 "github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esutil"
	"github.com/go-kit/kit/log"
	"github.com/grafana/loki/clients/pkg/promtail/api"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"sync"
	"time"
)

const (
	IndexerLabel model.LabelName = "indexer"
	defaultIndexId = "promtail"
	defaultBatchNum = 1000
)


type Article struct {
	ID        int       `json:"id"`
	Title     string    `json:"title"`
	Body      string    `json:"body"`
	Published time.Time `json:"published"`
	Author    Author    `json:"author"`
}

type Author struct {
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
}


type client struct {
	cfg EsClientConfig
	entries chan api.Entry
	client *elasticsearch7.Client
	logger  log.Logger
	once sync.Once
	wg   sync.WaitGroup

	indexerHash map[string]bool

	ctx context.Context
	cancelFunc context.CancelFunc

}

// New return an elasticsearch client instance
func New(reg prometheus.Registerer, cfg EsClientConfig, logger log.Logger)(*client,error){
	escli, err := newClientFromConfig(&cfg)
	if err != nil{
		return nil, err
	}
	// check elastic server is reachedable
	if err != nil{
		return nil, errors.New(fmt.Sprintf("check elastic client failed %v",err.Error()))
	}

	ctx,cancel := context.WithCancel(context.Background())
	c := &client{
		cfg:     cfg,
		entries: make(chan api.Entry),
		client:  escli,
		once:    sync.Once{},
		wg:      sync.WaitGroup{},
		ctx: ctx,
		cancelFunc: cancel,
		logger: logger,
		indexerHash: make(map[string]bool, 100),
	}
	c.wg.Add(1)
	go c.run()
	return c, nil
}


func newClientFromConfig(cfg *EsClientConfig)(*elasticsearch7.Client,error){

	escfg := elasticsearch7.Config{
		MaxRetries: 5,
		RetryOnStatus: []int{502,503, 504,429},

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
	bulks := map[string]*bulk{}
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
		for indexerId, bulkItems := range bulks {
			// 清空所有的batch，将还没发送出去的全部发送出去
			c.sendBatch(indexerId, bulkItems)
		}
		c.wg.Done()
	}()

	for {
		select {
		case e, ok := <-c.entries:
			if !ok {
				return
			}
			indexerId := generateIndexerId(&e)
			bulk, ok := bulks[indexerId]

			// If the batch doesn't exist yet, we create a new one with the entry
			if !ok {
				bulks[indexerId] = newBulk(e)
				break
			}

			if bulk.numEntryItems() > defaultBatchNum{
				c.sendBatch(indexerId, bulk)
				bulks[indexerId] = newBulk(e)
			}

			// The max size of the batch isn't reached, so we can add the entry
			bulk.add(e)

		case <-maxWaitCheck.C:
			// Send all batches whose max wait time has been reached
			for indexer, batch := range bulks {
				if batch.age() < c.cfg.BatchWait {
					continue
				}
				c.sendBatch(indexer, batch)
				delete(bulks, indexer)
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

func (c *client) sendBatch(indexerId string, batch *bulk) {
	if _,ok := c.indexerHash[indexerId]; !ok {
		// 如果索引不存在，则创建一个新索引
		res, err := c.client.Indices.Create(indexerId)
		if err != nil {
			c.logger.Log("Cannot create index", err)
			return
		}
		c.indexerHash[indexerId] = true
		if res.IsError() {
			c.logger.Log("Cannot create index", res)
			return
		}
		res.Body.Close()
	}

	bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Index:         indexerId,        // The default index name
		Client:        c.client,               // The Elasticsearch client
		FlushInterval: 30 * time.Second, // The periodic flush interval
	})
	if err != nil{
		return
	}

	for _, a := range batch.bulkItems {
		err = bi.Add(
			context.Background(),
			*a,
		)
		// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
	}

	if err := bi.Close(context.Background()); err != nil {
		c.logger.Log("Unexpected error: %s", err)
	}
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

}

func (c *client) send(ctx context.Context, indexerId string, buf []byte) (int, error) {

	return 0, nil

}



func generateIndexerId(e *api.Entry) string {
	// get indexer name
	if indexer, ok := e.Labels[IndexerLabel]; ok {
		return string(indexer)
	}
	return defaultIndexId
}