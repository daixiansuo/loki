package filesystem

import (
	"bufio"
	"context"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"path"
	"testing"
)



var (
	fakeMt = metadata{
		namespace:      FakeNamespace,
		controllerName: FakeController,
		instance:       FakeInstance,
		fileName:       FakeShortFileName,
		originFilename: FakeFileName,
		isKubernetes:   false,
	}

	fakeCfg = FileClientConfig{Path: "tmp"}
)




func Test_FileHandler_Handler(t *testing.T){
	cases := []struct {
		name string
		handlerId string
		entries []string
		manager *Manager
		expectedReceiveNum int
	}{
		{
			name: "none manager, no line",
			handlerId: "file1",
			entries: []string{},
			manager: nil,
			expectedReceiveNum: 0,
		},
		{
			name: "none manager, 4 lines",
			handlerId: "file2",
			entries: []string{"line1", "line2", "line3", "line4"},
			manager: nil,
			expectedReceiveNum: 4,
		},
		{
			name: "with manager, no line",
			handlerId: "file3",
			entries: []string{},
			manager: newHandlerManager(),
			expectedReceiveNum: 0,
		},
		{
			name: "with manager, 4 line",
			handlerId: "file4",
			entries: []string{"line1", "line2", "line3", "line4"},
			manager: newHandlerManager(),
			expectedReceiveNum: 4,
		},
	}
	for _, tt := range cases{
		t.Run(tt.name, func(t *testing.T) {
			handler, err := newFileHandler(context.Background(), nil, &fakeCfg, tt.manager, &fakeMt )
			require.NoError(t, err)
			for _, line := range tt.entries{
				handler.Receiver() <- line
			}
			handler.Stop()

			fileName := path.Join(handler.directoryName, handler.writeFileName)
			f, err := os.Open(fileName)
			require.NoError(t, err)
			scanner := bufio.NewScanner(f)
			var count int = 0
			for scanner.Scan() {
				require.Equal(t, scanner.Text(), tt.entries[count])
				count++
			}

			require.Equal(t, tt.expectedReceiveNum, count)
			require.NoError(t, f.Close())
			require.NoError(t, os.Remove(fileName))
			require.NoError(t, os.RemoveAll(handler.directoryName))
		})
	}
}


func Test_FileHandler_Rotate(t *testing.T){
	cases := []struct{
		name string
		newDate string
		entries []string
		isKubernetes bool
		expectedDirectory string
	} {
		{
			name: "isKubernetes",
			newDate: "2022-10-11",
			entries: []string{"line1", "line2", "line3", "line4"},
			isKubernetes: true,
			expectedDirectory: "tmp/fakeNamespace/fakeController/2022-10-11/fakeInstance/fakeFile",
		},
		{
			name: "log from filediskcovery",
			newDate: "2022-10-11",
			entries: []string{"line1", "line2", "line3", "line4"},
			isKubernetes: false,
			expectedDirectory: "tmp/fakeNamespace/fakeController/fakeInstance/fakeDir1/fakeDir2/fakeFile",
		},
	}
	
	for _,tt := range cases{
		t.Run(tt.name, func(t *testing.T) {
			copyMt := fakeMt
			copyMt.isKubernetes = tt.isKubernetes
			handler, err := newFileHandler(context.Background(), nil, &fakeCfg, nil, &copyMt )
			assert.NoError(t, err)
			count := 0
			for ; count < len(tt.entries) /2 ; count ++{
				logrus.Infof("send %v\n", tt.entries[count])
				handler.Receiver() <- tt.entries[count]
			}
			println("the count is %v", count)
			oldFilename := handler.directoryName + "/" + handler.writeFileName
			handler.rotateCh <- tt.newDate
			logrus.Errorf("%v", handler.batchBuffer.streams)
			for ; count < len(tt.entries) ; count ++ {
				logrus.Warningf("send %v",tt.entries[count] )
				handler.Receiver() <- tt.entries[count]
			}
			newFilename := path.Join(handler.directoryName, handler.writeFileName)
			handler.Stop()

			oldFp,err := os.Open(oldFilename)
			assert.NoError(t, err)
			scannner := bufio.NewScanner(oldFp)
			count = 0
			for scannner.Scan(){
				require.Equal(t, scannner.Text(), tt.entries[count])
				count ++
			}

			require.Equal(t, tt.expectedDirectory, newFilename)
			require.Equal(t, count, len(tt.entries) / 2)
			require.NoError(t, oldFp.Close())

			require.Equal(t, tt.expectedDirectory, newFilename)
			newFp,err := os.Open(newFilename)
			assert.NoError(t, err)
			scannner = bufio.NewScanner(newFp)

			for scannner.Scan(){
				require.Equal(t, scannner.Text(), tt.entries[count])
				count ++
			}
			require.NoError(t, newFp.Close())
		})
	}
}

func Test_FileHandler_FileWatcher(t *testing.T){

}


