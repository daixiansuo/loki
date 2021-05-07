package filesystem

import (
	"context"
	"fmt"
	"os"
	"path"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/robfig/cron/v3"
)

const (
	_defaultRetryCount    = 4
	_defaultRotateRecycle = 1 * time.Hour
	_defaultRsyncInterval = 10 * time.Second

	_ChannelRotateCh    string = "rotate"
	_ChannelFileMissing string = "filemissing"
)

// 文件操作句柄接口
type Handler interface {
	Receiver() chan string
	Stop()
}

// file handler status begin in initial status

type FileHandler struct {
	// identified a unique handler
	handlerId string
	pathFmt   string // root file path stored
	fileName  string // the file name
	logger    log.Logger
	manager   *Manager

	retryCount  int
	fp          *os.File // handler
	batchBuffer *batch
	fsWatcher   *fsnotify.Watcher
	receiver    chan string

	handlerContext context.Context
	handlerCancel  context.CancelFunc

	wg   sync.WaitGroup
	once sync.Once

	fileMissingCh chan struct{}
	rotateCh      chan struct{}
}

func (h *FileHandler) Receiver() chan string {
	return h.receiver
}

// newFileHandler is create an file handler instance
func newFileHandler(parentCancel context.Context, logger log.Logger, handlerId, pathFmt, fileName string, m *Manager, opts ...string) (*FileHandler, error) {
	var err error
	fd := &FileHandler{
		handlerId:     handlerId,
		fileName:      fileName,
		pathFmt:       pathFmt,
		batchBuffer:   newBatch().reSetSize(1 << 6),
		receiver:      make(chan string),
		logger:        log.With(logger, "handlerId", fileName),
		manager:       m,
		wg:            sync.WaitGroup{},
		fileMissingCh: make(chan struct{}),
		rotateCh:      make(chan struct{}),
		once:          sync.Once{},
	}
	fd.handlerContext, fd.handlerCancel = context.WithCancel(parentCancel)
	// prepare directory that store file
	if err = os.MkdirAll(fmt.Sprintf(fd.pathFmt, date2String(opts...)), os.ModePerm); err != nil {
		return nil, errors.Wrap(err, "create path failed")
	}
	// create file handler
	if fd.fp, err = Open(path.Join(fmt.Sprintf(fd.pathFmt, date2String(opts...)), fileName)); err != nil {
		return nil, err
	}

	if fd.fsWatcher, err = fsnotify.NewWatcher(); err != nil {
		return nil, err
	}
	if err = fd.fsWatcher.Add(path.Join(fmt.Sprintf(fd.pathFmt, date2String(opts...)), fileName)); err != nil {
		return nil, err
	}
	fd.wg.Add(3)
	go fd.mainWorker()
	go fd.watchDog()
	go fd.rotateCycle()

	return fd, nil
}

func (h *FileHandler) mainWorker() {
	level.Info(h.logger).Log("handerId", h.fileName, "fileHandler main worker", h.handlerId)
	ticker := time.NewTicker(_defaultRsyncInterval)
	defer func() {
		ticker.Stop()
		close(h.receiver)
		_ = h.flush()
		h.wg.Done()
		h.Stop()
	}()

	for true {
		select {
		case <-h.rotateCh:
			return
		case <-h.fileMissingCh:
			return
		default:
		}
		select {
		case <-h.handlerContext.Done():
			return
		case <-ticker.C:
			if h.batchBuffer.empty() {
				continue
			}
			if err := h.flush(); err != nil {
				return
			}
		case e := <-h.receiver:
			h.batchBuffer.add(e)
			if !h.batchBuffer.isFull() {
				continue
			}
			if err := h.flush(); err != nil {
				return
			}
		}
	}
}

// flush buffer to file ,if failed and try matched the max try number, then kill this handler
func (h *FileHandler) flush() error {
	defer h.batchBuffer.clean()
	if h.fp == nil {
		return fmt.Errorf("file description is nil")
	}
	_, err := h.fp.Write(h.batchBuffer.encode())
	if err == nil {
		if err = h.fp.Sync(); err == nil {
			return nil
		}
	}
	level.Info(h.logger).Log(h.handlerId, "FileHandler flush buffer to disk failed, and ready retry", "err", err.Error())
	if h.retryCount >= _defaultRetryCount {
		level.Error(h.logger).Log(h.handlerId, "FileHandler flush buffer failed and readched the max retry number, destroy handler")
		return err
	} else {
		level.Error(h.logger).Log(h.handlerId, "FileHandler flush buffer failed  retry", "retry number", h.retryCount)
		// try repair from failure
		if err := h.tryRecoverFromWriteFailure(); err == nil {
			h.retryCount = 0
		} else {
			h.retryCount += 1
		}
	}
	return nil
}

// watchDog monitor if file is delete unexpected
// if file is deleted for some reason, watchDog will close old file handler ,then create new file handler
func (h *FileHandler) watchDog() {
	defer func() {
		h.wg.Done()
		close(h.fileMissingCh)
		_ = h.fsWatcher.Close()
		h.Stop()
	}()
	for true {
		select {
		case ev := <-h.fsWatcher.Events:
			if ev.Op&fsnotify.Remove == fsnotify.Remove {
				h.fileMissingCh <- struct{}{}
			}
		case <-h.handlerContext.Done():
			return
		}
	}
}

func (h *FileHandler) tryRecoverFromWriteFailure() error {
	var err error
	if h.fp != nil {
		_ = h.fp.Close()
	}
	h.fp, err = Open(h.fileName)
	if err != nil {
		return fmt.Errorf("create file handler failed %s, msg: %v", h.fileName, err)
	}
	return nil
}

// check current timestamp is current, if not then rotate file
func (h *FileHandler) rotateCycle() {
	parser := cron.NewParser(
		cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor,
	)
	c := cron.New(cron.WithParser(parser))
	c.AddFunc("1 1 0 * * *", func() {
		level.Info(h.logger).Log("msg", "rotate handler", "time", date2String())
		h.rotateCh <- struct{}{}
	})
	c.Start()
	defer func() {
		h.wg.Done()
		close(h.rotateCh)
		c.Stop()
		h.Stop()
	}()
	for true {
		select {
		//case <-ticker.C:
		//	if minus := time.Now().Second() - h.tomorrowZeroTime().Second(); minus <= 0 {
		//		continue
		//	}
		//	h.rotateCh <- struct{}{}
		case <-h.handlerContext.Done():
			return
		}
	}
}

// clean all resource for ready stop
func (h *FileHandler) Stop() {
	h.once.Do(func() {
		// notify manager to register it self
		// clean all resources
		level.Info(h.logger).Log(h.handlerId, "begin stop handler")

		h.handlerCancel() // notify all taskgoroutine to stop work
		h.wg.Wait()       // wait all work to stop completely
		_ = h.fp.Close()		// 关闭文件具柄
		if h.manager != nil {
			if err := h.manager.unRegister(h.handlerId); err != nil {
				level.Error(h.logger).Log(h.handlerId, "[stop stage]\tunregister handler from manager failed", "err", err.Error())
			} else {
				level.Info(h.logger).Log(h.handlerId, "[stop stage]\tunregister handler from manager successfully")
			}
		}
	})
}

// this method is used for test
func (h *FileHandler) receiveSignal(st string) {
	if st == _ChannelRotateCh {
		h.rotateCh <- struct{}{}
	} else if st == _ChannelFileMissing {
		h.fileMissingCh <- struct{}{}
	}
}
