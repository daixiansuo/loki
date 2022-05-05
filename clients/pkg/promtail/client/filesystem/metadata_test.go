package filesystem

import (
	"github.com/grafana/loki/clients/pkg/promtail/api"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

const (
	FakeNamespace     = "fakeNamespace"
	FakeController    = "fakeController"
	FakeInstance      = "fakeInstance"
	FakeFileName      = "fakeDir1/fakeDir2/fakeFile"
	FakeShortFileName = "fakeFile"
	FakeHashId        = FakeNamespace + "." + FakeController + "." + FakeInstance + "." + FakeFileName
)

func TestMetadata(t *testing.T) {
	tests := []struct{
		name string
		entry api.Entry
		expectedNamespace string
		expectedController string
		expectedInstance string
		expectedFilename string
		expectedKubernetes bool
	}{
		{
			name: "full labels",
			entry: api.Entry{Labels: model.LabelSet{NamespaceLabel: FakeNamespace, ControllerNameLabel: FakeController, InstanceLabel: FakeInstance, FileNameLabel: FakeFileName}},
			expectedNamespace: FakeNamespace,
			expectedController: FakeController,
			expectedInstance: FakeInstance,
			expectedFilename: FakeFileName,
			expectedKubernetes: true,
		},
		{
			name: "lack of namespace label",
			entry: api.Entry{Labels: model.LabelSet{ControllerNameLabel: FakeController, InstanceLabel: FakeInstance, FileNameLabel: FakeFileName}},
			expectedNamespace: defaultNamespace,
			expectedController: FakeController,
			expectedInstance: FakeInstance,
			expectedFilename: FakeFileName,
			expectedKubernetes: false,
		},
		{
			name: "lack of instance label",
			entry: api.Entry{Labels: model.LabelSet{NamespaceLabel: FakeNamespace, ControllerNameLabel: FakeController, FileNameLabel: FakeFileName}},
			expectedNamespace: FakeNamespace,
			expectedController: FakeController,
			expectedInstance: defaultInstanceName,
			expectedFilename: FakeFileName,
			expectedKubernetes: true,
		},
		{
			name: "lack of controller label",
			entry: api.Entry{Labels: model.LabelSet{NamespaceLabel: FakeNamespace, InstanceLabel: FakeInstance, FileNameLabel: FakeFileName}},
			expectedNamespace: FakeNamespace,
			expectedController: defaultControllerName,
			expectedInstance: FakeInstance,
			expectedFilename: FakeFileName,
			expectedKubernetes: false,
		},
	}

	for _, tt := range tests{
		t.Run(tt.name, func(t *testing.T) {
			got := Get()
			entry := tt.entry
			require.NoError(t, parseEntry(&entry, got))
			assert.Equal(t, tt.expectedNamespace, got.namespace)
			assert.Equal(t, tt.expectedController, got.controllerName)
			assert.Equal(t, tt.expectedInstance, got.instance)
			assert.Equal(t, tt.expectedFilename, got.originFilename)
			assert.Equal(t, tt.expectedKubernetes, got.isKubernetes)
		})
	}


}

func TestGetDeploymentName(t *testing.T) {
	controller := "dmo-metadata-web-per-deploy-5d485fc4b9"
	except := "dmo-metadata-web-per-deploy"
	assert.Equal(t, DeploymentName(controller), except)
}

func BenchmarkMetadata(b *testing.B) {

}
