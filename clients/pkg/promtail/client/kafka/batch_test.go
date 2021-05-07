package kafka

import (
	"github.com/grafana/loki/clients/pkg/promtail/api"
	"github.com/grafana/loki/pkg/logproto"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)
var logEntries = []api.Entry{
	{Labels: model.LabelSet{}, Entry: logproto.Entry{Timestamp: time.Unix(1, 0).UTC(), Line: "line1"}},
	{Labels: model.LabelSet{}, Entry: logproto.Entry{Timestamp: time.Unix(2, 0).UTC(), Line: "line2"}},
	{Labels: model.LabelSet{}, Entry: logproto.Entry{Timestamp: time.Unix(3, 0).UTC(), Line: "line3"}},
	{Labels: model.LabelSet{"__tenant_id__": "tenant-1"}, Entry: logproto.Entry{Timestamp: time.Unix(4, 0).UTC(), Line: "line4"}},
	{Labels: model.LabelSet{"__tenant_id__": "tenant-1"}, Entry: logproto.Entry{Timestamp: time.Unix(5, 0).UTC(), Line: "line5"}},
	{Labels: model.LabelSet{"__tenant_id__": "tenant-2"}, Entry: logproto.Entry{Timestamp: time.Unix(6, 0).UTC(), Line: "line6"}},
}

func TestBatch_add(t *testing.T){
	t.Parallel()
	tests := map[string]struct {
		inputEntries      []api.Entry
		expectedSizeBytes int
	}{
		"empty batch": {
			inputEntries:      []api.Entry{},
			expectedSizeBytes: 0,
		},
		"single stream with single log entry": {
			inputEntries: []api.Entry{
				{Labels: model.LabelSet{}, Entry: logEntries[0].Entry},
			},
			expectedSizeBytes: len(logEntries[0].Entry.Line),
		},
		"single stream with multiple log entries": {
			inputEntries: []api.Entry{
				{Labels: model.LabelSet{}, Entry: logEntries[0].Entry},
				{Labels: model.LabelSet{}, Entry: logEntries[1].Entry},
			},
			expectedSizeBytes: len(logEntries[0].Entry.Line) + len(logEntries[1].Entry.Line),
		},
		"multiple streams with multiple log entries": {
			inputEntries: []api.Entry{
				{Labels: model.LabelSet{"type": "a"}, Entry: logEntries[0].Entry},
				{Labels: model.LabelSet{"type": "a"}, Entry: logEntries[1].Entry},
				{Labels: model.LabelSet{"type": "b"}, Entry: logEntries[2].Entry},
			},
			expectedSizeBytes: len(logEntries[0].Entry.Line) + len(logEntries[1].Entry.Line) + len(logEntries[2].Entry.Line),
		},
	}
	for testName, testData := range tests{
		testData := testData
		t.Run(testName, func(t *testing.T) {
			b := newBatch()

			for _, entry := range testData.inputEntries {
				b.add(entry)
			}

			assert.Equal(t, testData.expectedSizeBytes, b.sizeBytes())
		})	}
}

func TestBatch_encode(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		inputBatch           *batch
		expectedEntriesCount int
	}{
		"empty batch": {
			inputBatch:           newBatch(),
			expectedEntriesCount: 0,
		},
		"single stream with single log entry": {
			inputBatch: newBatch(
				api.Entry{Labels: model.LabelSet{}, Entry: logEntries[0].Entry},
			),
			expectedEntriesCount: 1,
		},
		"single stream with multiple log entries": {
			inputBatch: newBatch(
				api.Entry{Labels: model.LabelSet{}, Entry: logEntries[0].Entry},
				api.Entry{Labels: model.LabelSet{}, Entry: logEntries[1].Entry},
			),
			expectedEntriesCount: 2,
		},
		"multiple streams with multiple log entries": {
			inputBatch: newBatch(
				api.Entry{Labels: model.LabelSet{"type": "a"}, Entry: logEntries[0].Entry},
				api.Entry{Labels: model.LabelSet{"type": "a"}, Entry: logEntries[1].Entry},
				api.Entry{Labels: model.LabelSet{"type": "b"}, Entry: logEntries[2].Entry},
			),
			expectedEntriesCount: 3,
		},
	}

	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			_, entriesCount, err := testData.inputBatch.encode()
			require.NoError(t, err)
			assert.Equal(t, testData.expectedEntriesCount, entriesCount)
		})
	}
}