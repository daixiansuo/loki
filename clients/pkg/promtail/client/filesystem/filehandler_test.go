package filesystem

import (
	"bufio"
	"context"
	"fmt"
	util_log "github.com/cortexproject/cortex/pkg/util/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"path"
	"strconv"
	"testing"
)

const (
	_TemplatePath string = "./tmp/%s"
)

func TestFileHandlerHandle(t *testing.T) {
	cases := map[string]struct {
		HandlerId   string
		manager     *Manager
		entryString []string
	}{
		"Nil Manager": {
			HandlerId: "handler1",
			manager:   nil,
			entryString: []string{
				"first line for handler1",
				"second line for handler1",
				"third line for handler1",
				"fourth line for handler1",
			},
		},
		"with Manager": {
			HandlerId: "handler2",
			manager:   newHandlerManager(),
			entryString: []string{
				"first line for handler2",
				"second line for handler2",
				"third line for handler2",
				"fourth line for handler3",
			},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for caseId, caseInfo := range cases {
		t.Run(caseId, func(t *testing.T) {
			handler, err := newFileHandler(ctx, util_log.Logger, caseId, _TemplatePath, caseInfo.HandlerId, caseInfo.manager)
			assert.NoError(t, err)
			for _, line := range caseInfo.entryString {
				handler.Receiver() <- line
			}
			handler.Stop()
			fileName := path.Join(fmt.Sprintf(_TemplatePath, date2String()), handler.fileName)
			f, err := os.Open(fileName)
			scanner := bufio.NewScanner(f)
			var count int = 0
			for scanner.Scan() {
				require.Equal(t, scanner.Text(), caseInfo.entryString[count])
				count++
			}
			require.Equal(t, count, 4)
			require.NoError(t, f.Close())
			require.NoError(t, err)
			require.NoError(t, os.Remove(fileName))
			require.NoError(t, os.RemoveAll("./tmp"))
		})
	}
}

func TestFileHandler_watchDog(t *testing.T) {
	var (
		EntryNumber int = 100
		testCount   int = 10
		doneCh          = make(chan struct{})
		continueCh      = make(chan struct{})

		basePath = "./tmp/" + FakeNamespace + "/" + FakeController + "/" + "%s" + "/" + FakeController
	)

	manager := newHandlerManager()
	go func(t *testing.T) {
		var err error
		// continue input data
		for count := 0; count < testCount; count++ {
			_dateTpl := "2021-08-2" + strconv.Itoa(count)
			title := "file" + strconv.Itoa(count)
			handler := manager.Get(title)
			handler, err = newFileHandler(context.Background(), util_log.Logger, title, basePath, title, manager, _dateTpl)
			require.NoError(t, err)
			require.NoError(t, manager.Register(title, handler))
			for i := 0; i < EntryNumber; i++ {
				handler.Receiver() <- fmt.Sprintf("line %d for %s", i, title)
			}
			handler.(*FileHandler).receiveSignal(_ChannelFileMissing)
			doneCh <- struct{}{}
			<-continueCh
		}
	}(t)

	for count := 0; count < testCount; count++ {
		<-doneCh
		root := "tmp/fakeNamespace/fakeController/" + date2String("2021-08-2"+strconv.Itoa(count)) + "/fakeController"
		for i := 0; i < 1; i++ {
			title := "file" + strconv.Itoa(count)
			fileName := path.Join(root, title)
			fp, err := os.Open(fileName)
			require.NoError(t, err)
			scanner := bufio.NewScanner(fp)
			var count int = 0
			for scanner.Scan() {
				count++
			}
			require.Equal(t, EntryNumber, count)
			require.NoError(t, os.Remove(fileName))
		}
		//time.Sleep(1 * time.Second)
		continueCh <- struct{}{}
	}
	require.NoError(t, os.RemoveAll("./tmp"))
	manager.Stop()
	require.NoError(t, manager.AwaitComplete())
}

func TestFileHandler_rotate(t *testing.T) {
	var (
		EntryNumber int = 100
		testCount   int = 10
		doneCh          = make(chan struct{})
		continueCh      = make(chan struct{})

		basePath = "./tmp/" + FakeNamespace + "/" + FakeController + "/" + "%s" + "/" + FakeController
	)
	manager := newHandlerManager()
	go func(t *testing.T) {
		var err error
		// continue input data
		for count := 0; count < testCount; count++ {
			_dateTpl := "2021-08-2" + strconv.Itoa(count)
			title := "file" + strconv.Itoa(count)
			handler := manager.Get(title)
			handler, err = newFileHandler(context.Background(), util_log.Logger, title, basePath, title, manager, _dateTpl)
			require.NoError(t, err)
			require.NoError(t, manager.Register(title, handler))
			for i := 0; i < EntryNumber; i++ {
				handler.Receiver() <- fmt.Sprintf("line %d for %s", i, title)
			}
			handler.(*FileHandler).receiveSignal(_ChannelRotateCh)
			doneCh <- struct{}{}
			<-continueCh
		}
	}(t)
	for count := 0; count < testCount; count++ {
		<-doneCh
		root := "tmp/fakeNamespace/fakeController/" + date2String("2021-08-2"+strconv.Itoa(count)) + "/fakeController"
		for i := 0; i < 1; i++ {
			title := "file" + strconv.Itoa(count)
			fileName := path.Join(root, title)
			fp, err := os.Open(fileName)
			require.NoError(t, err)
			scanner := bufio.NewScanner(fp)
			var count int = 0
			for scanner.Scan() {
				count++
			}
			require.Equal(t, EntryNumber, count)
			require.NoError(t, os.Remove(fileName))
		}
		//time.Sleep(1 * time.Second)
		continueCh <- struct{}{}
	}
	require.NoError(t, os.RemoveAll("./tmp"))
	manager.Stop()
	require.NoError(t, manager.AwaitComplete())
}
