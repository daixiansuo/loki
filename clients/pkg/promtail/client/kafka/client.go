package kafka

import (
	"context"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/grafana/loki/clients/pkg/promtail/api"
	"github.com/prometheus/client_golang/prometheus"

	"sync"
	"time"

	kafka "github.com/Shopify/sarama"
)

const (
	contentType  = "application/x-protobuf"
	maxErrMsgLen = 1024
	// Label reserved to override the tenant ID while processing
	// pipeline stages
	ReservedLabelTenantID = "__tenant_id__"
	FilenameLabel         = "filename"
	HostLabel             = "host"
)

const (
	MAX_LOGLINE_SIZE = 1024 * 500
)

type metrics struct {
	sendTotalKafkaMessages *prometheus.CounterVec // 已经成功发送的kafka条目
	droppedEntry           *prometheus.CounterVec //丢弃了多少entry，(非法的entry）

	countersWithHost []*prometheus.CounterVec
}

func newMetrics(reg prometheus.Registerer) *metrics {
	var m metrics
	//
	m.sendTotalKafkaMessages = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "promtail",
		Name:      "send_total_kafka_messages",
		Help:      "the number of messages that has been sent to kafka successfully",
	}, []string{HostLabel})

	m.droppedEntry = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "promtail",
		Name:      "droped_log_entry_total",
		Help:      "the number of log entris that dropped for line size is too long",
	}, []string{HostLabel})

	m.countersWithHost = []*prometheus.CounterVec{
		m.sendTotalKafkaMessages, m.droppedEntry,
	}

	if reg != nil {
		m.sendTotalKafkaMessages = mustRegisterOrGet(reg, m.sendTotalKafkaMessages).(*prometheus.CounterVec) // for total
		m.droppedEntry = mustRegisterOrGet(reg, m.droppedEntry).(*prometheus.CounterVec)
	}

	return &m
}

func mustRegisterOrGet(reg prometheus.Registerer, c prometheus.Collector) prometheus.Collector {
	if err := reg.Register(c); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			return are.ExistingCollector.(prometheus.Collector)
		}
		panic(err)
	}
	return c
}

type client struct {
	cfg     KafkaConfig
	entries chan api.Entry
	// add metrics
	metrics *metrics

	//kafkaConn *kafka.Conn
	kafkaProducer kafka.SyncProducer
	//kafkaConnPool map[TopicKind]*kafka.Conn

	logger     log.Logger
	once       sync.Once
	wg         sync.WaitGroup
	ctx        context.Context
	cancelFunc context.CancelFunc
}

// New return an elasticsearch client instance
func NewKafkaClient(reg prometheus.Registerer, cfg KafkaConfig, logger log.Logger) (*client, error) {
	c := &client{
		cfg:     cfg,
		logger:  logger,
		entries: make(chan api.Entry),
		metrics: newMetrics(reg),
	}
	c.ctx, c.cancelFunc = context.WithCancel(context.Background())

	var err error
	if c.kafkaProducer, err = newKafkaProducer(&cfg); err != nil {
		level.Error(c.logger).Log("msg", "create kafka producer failed", "err", err.Error())
		return nil, err
	}

	c.wg.Add(1)
	go c.run()
	return c, nil
}

func newKafkaProducer(cfg *KafkaConfig) (kafka.SyncProducer, error) {
	config := kafka.NewConfig()
	// 1MB = 1048576  10MB = 10485760
	config.Producer.MaxMessageBytes = 10485760
	config.Producer.Timeout = cfg.Timeout
	config.Producer.Return.Successes = true
	config.Producer.Partitioner = kafka.NewRandomPartitioner
	config.Consumer.Fetch.Default = 10485760
	config.Consumer.Fetch.Max = 10485760
	return kafka.NewSyncProducer([]string{cfg.Url}, config)
}

func (c *client) Chan() chan<- api.Entry {
	return c.entries
}

func (c *client) run() {
	level.Info(c.logger).Log("msg", "kafka client start running....")
	batches := map[string]*batch{}
	minWaitCheckFrequency := 10 * time.Millisecond
	maxWaitCheckFrequency := c.cfg.BatchWait / 10
	if maxWaitCheckFrequency < minWaitCheckFrequency {
		maxWaitCheckFrequency = minWaitCheckFrequency
	}

	maxWaitCheck := time.NewTicker(5 * time.Second)
	defer func() {
		maxWaitCheck.Stop()
		// Send all pending batches
		for _, batch := range batches {
			c.sendBatch(batch)
		}
		c.wg.Done()
	}()

	for {
		select {
		case e, ok := <-c.entries:
			if !ok {
				return
			}

			// entry is {{labels map}, lines, timestamp}
			e, tenantId := c.processEntry(e)
			batch, ok := batches[tenantId]

			// If the batch doesn't exist yet, we create a new one with the entry
			if !ok {
				batches[tenantId] = newBatch(e)
				break
			}

			// If adding the entry to the batch will increase the size over the max
			// size allowed, we do send the current batch and then create a new one
			if batch.sizeBytesAfter(e) > c.cfg.BatchSize {
				c.sendBatch(batch)
				batches[tenantId] = newBatch(e)
				break
			}

			// The max size of the batch isn't reached, so we can add the entry
			batch.add(e)

		case <-maxWaitCheck.C:
			// Send all batches whose max wait time has been reached
			for tenantID, batch := range batches {
				if batch.age() < c.cfg.BatchWait {
					continue
				}
				c.sendBatch(batch)
				delete(batches, tenantID)
			}
		}
	}
}

// Stop the client.
func (c *client) Stop() {
	c.once.Do(func() { close(c.entries) })
	if err := c.kafkaProducer.Close(); err != nil {
		level.Error(c.logger).Log("msg", "close kafka writer failed", "err", err)
	}
	c.wg.Wait()
}

// StopNow stops the client without retries
func (c *client) StopNow() {
	// cancel will stop retrying http requests.
	c.cancelFunc()
	c.Stop()
}

func (c *client) processEntry(e api.Entry) (api.Entry, string) {
	return e, ReservedLabelTenantID
}

func (c *client) sendBatch(batch *batch) {
	requests, _, err := batch.encode()
	if err != nil {
		level.Error(c.logger).Log("kafkaEntry", "error encoding batch", "error", err)
		return
	}

	//backoff := util.NewBackoff(c.ctx, c.cfg.BackoffConfig)
	var status int
	for {
		// send uses `timeout` internally, so `context.Background` is good enough.
		//status, err = c.send(context.Background(), tenantID, buf)
		n, err := c.send(requests)
		if err == nil {
			level.Debug(c.logger).Log("msg", "send successfully ", "count", n)
			return
		}
		c.metrics.sendTotalKafkaMessages.WithLabelValues(c.cfg.Url).Inc()
		level.Warn(c.logger).Log("msg", "error sending batch, this batch will be dropped", "status", status, "error", err)
		//backoff.Wait()
		//
		//// Make sure it sends at least once before checking for retry.
		//if !backoff.Ongoing() {
		//	break
		//}
	}
}

func (c *client) send(messages []*kafka.ProducerMessage) (int, error) {
	errs := c.kafkaProducer.SendMessages(messages)
	if errs != nil {
		for _, err := range errs.(kafka.ProducerErrors) {
			level.Error(c.logger).Log("msg", "Write to kafka failed", "error", err)
		}
		return 0, errs
	}
	c.metrics.sendTotalKafkaMessages.WithLabelValues(c.cfg.Url).Add(float64(len(messages)))
	return len(messages), errs

}

// 过滤entry，如果entry line超过最大阀值，日志直接丢弃(只是在kafka端丢弃掉了)
func validateEntry(e *api.Entry) bool {
	if len(e.Line) > MAX_LOGLINE_SIZE {
		return false
	}
	return true
}
