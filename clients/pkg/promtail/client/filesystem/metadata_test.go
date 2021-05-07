package filesystem

import (
	"fmt"
	"github.com/grafana/loki/clients/pkg/promtail/api"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
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
	entrys := map[string]api.Entry{
		"complete":            api.Entry{Labels: model.LabelSet{NamespaceLabel: FakeNamespace, ControllerNameLabel: FakeController, InstanceLabel: FakeInstance, FileNameLabel: FakeFileName}},
		defaultNamespace:      api.Entry{Labels: model.LabelSet{ControllerNameLabel: FakeController, InstanceLabel: FakeInstance, FileNameLabel: FakeFileName}},
		defaultInstanceName:   api.Entry{Labels: model.LabelSet{NamespaceLabel: FakeNamespace, ControllerNameLabel: FakeController, FileNameLabel: FakeFileName}},
		defaultControllerName: api.Entry{Labels: model.LabelSet{NamespaceLabel: FakeNamespace, InstanceLabel: FakeInstance, FileNameLabel: FakeFileName}},
	}
	for label, e := range entrys {
		md := Get()
		var hashId string
		assert.NoError(t, parseEntry(&e, md))
		if label == defaultNamespace {
			assert.Equal(t, md.namespace, defaultNamespace)
			hashId = defaultNamespace + "."
		} else {
			assert.Equal(t, md.namespace, FakeNamespace)
			hashId = FakeNamespace + "."
		}
		if label == defaultControllerName {
			assert.Equal(t, md.controllerName, defaultControllerName)
			hashId += defaultControllerName + "."
		} else {
			assert.Equal(t, md.controllerName, FakeController)
			hashId += FakeController + "."
		}
		if label == defaultInstanceName {
			assert.Equal(t, md.instance, defaultInstanceName)
			hashId += defaultInstanceName + "."
		} else {
			assert.Equal(t, md.instance, FakeInstance)
			hashId += FakeInstance + "."
		}
		assert.Equal(t, md.originFilename, FakeFileName)
		assert.Equal(t, md.fileName, FakeShortFileName)
		assert.Equal(t, md.HandlerId(), hashId+FakeFileName)
	}
	md := Get()

	fakeCfg := FileClientConfig{Path: "/fakepath"}
	today := "2021-08-13"
	exceptPath := fakeCfg.Path + "/" + FakeNamespace + "/" + FakeController + "/" + today + "/" + FakeInstance
	entry := entrys["complete"]

	assert.NoError(t, parseEntry(&entry, md))
	assert.Equal(t, exceptPath, fmt.Sprintf(md.BasePathFmt(fakeCfg.Path), today))

}

func TestGetDeploymentName(t *testing.T) {
	controller := "dmo-metadata-web-per-deploy-5d485fc4b9"
	except := "dmo-metadata-web-per-deploy"
	assert.Equal(t, DeploymentName(controller), except)
}

func BenchmarkMetadata(b *testing.B) {

}
