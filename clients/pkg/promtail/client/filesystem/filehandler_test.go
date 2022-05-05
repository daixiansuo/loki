package filesystem

import (
	"bufio"
	"context"
	"github.com/stretchr/testify/require"
	"os"
	"path"
	"testing"
)

const (
	mockPathStringTpl string = "./tmp/%s"
	mockDateString string = "2022-5-5"
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
	fakeMt := metadata{
		namespace:      FakeNamespace,
		controllerName: FakeController,
		instance:       FakeInstance,
		fileName:       FakeShortFileName,
		originFilename: FakeFileName,
		isKubernetes:   false,
	}
	fakeCfg := FileClientConfig{Path: "tmp"}
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

}

func Test_FileHandler_FileWatcher(t *testing.T){

}


