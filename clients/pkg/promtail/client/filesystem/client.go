package filesystem

import (
	"context"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/grafana/loki/clients/pkg/promtail/api"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"os"
	"sync"
)

// const label is used for mark directory and filename

var (
	once sync.Once

	NamespaceLabel      model.LabelName = "namespace"
	ControllerNameLabel model.LabelName = "controller_name"
	InstanceLabel       model.LabelName = "instance"
	FileNameLabel       model.LabelName = "filename"

	defaultNamespace      = "default"
	defaultControllerName = "default_controller"
	defaultInstanceName   = "default_instance"
)

func init(){
	once.Do(func() {
		defaultInstanceName, _ = os.Hostname()
	})
}


// client 描述客户端所需要的信息
type client struct {
	cfg     FileClientConfig
	reg     prometheus.Registerer
	logger  log.Logger
	entries chan api.Entry // 接收来自client的entry

	ctx    context.Context
	cancel context.CancelFunc

	wg   sync.WaitGroup
	once sync.Once


	manager *Manager
}

// 文件处理逻辑，handlerId,
//
func NewFileSystemClient(reg prometheus.Registerer, cfg FileClientConfig, logger log.Logger) (*client, error) {
	client := &client{
		cfg:     cfg,
		reg:     reg,
		logger:  log.With(logger, "client_type", "filesystem"),
		entries: make(chan api.Entry),
		wg:      sync.WaitGroup{},
		once:    sync.Once{},
		manager: newHandlerManager(),
	}
	client.ctx, client.cancel = context.WithCancel(context.Background())
	// run client main loop
	client.wg.Add(1)
	go client.run()
	return client, nil
}

// run function dispatch entry to all relative file handler
func (c *client) run() {
	defer c.wg.Done()
	level.Info(c.logger).Log("msg", "filesystem client start running...")
	for e := range c.entries {
		c.send(&e)
	}
}

func (c *client) send(e *api.Entry) {
	defer func() {
		if r := recover(); r != nil {
			level.Error(c.logger).Log("msg", "send accrue an panic error", "err", r)
		}
	}()
	md := Get()
	err := parseEntry(e, md)
	if err != nil {
		level.Error(c.logger).Log("msg", "get metadata failed", "err", err.Error())
		return
	}
	handler := c.manager.Get(md.HandlerId())
	if handler == nil {
		handler, err = newFileHandler(c.ctx, c.logger, &c.cfg, c.manager, md)
		if err != nil {
			level.Error(c.logger).Log("msg", "generate new handler failed", "err", err.Error())
			return
		}
		if err = c.manager.Register(md.HandlerId(), handler); err != nil {
			level.Error(c.logger).Log("msg", "register failed", "err", err.Error())
		} else {
			level.Info(c.logger).Log("handler register successfully", handler, "id", md.HandlerId())
		}
	}
	Put(md)
	handler.Receiver() <- e.Line
}

func (c *client) Chan() chan<- api.Entry {
	return c.entries
}

func (c *client) Stop() {
	c.once.Do(func() {
		close(c.entries)
		c.cancel()
	})
	if err := c.manager.AwaitComplete();err != nil{
		// todo log
	}
	c.wg.Wait()
}

func (c *client) StopNow() {
	c.Stop()
}