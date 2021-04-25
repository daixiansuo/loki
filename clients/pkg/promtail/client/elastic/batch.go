package elastic

import (
	"bytes"
	"fmt"
	"github.com/grafana/loki/clients/pkg/promtail/api"
	"github.com/grafana/loki/pkg/logproto"
	json "github.com/json-iterator/go"
	"github.com/prometheus/common/model"
	"github.com/sirupsen/logrus"
	"sort"
	"strings"
	"time"
)

type batch struct {
	streams   map[string]*logproto.Stream
	bytes     int
	createdAt time.Time
}

const(
	ReservedLabelTenantID = "__tenant_id__"
)



func newBatch(entries ...api.Entry) *batch {
	b := &batch{
		streams:   map[string]*logproto.Stream{},
		bytes:     0,
		createdAt: time.Now(),
	}

	// Add entries to the batch
	for _, entry := range entries {
		b.add(entry)
	}

	return b
}

// add an entry to the batch
func (b *batch) add(entry api.Entry) {
	b.bytes += len(entry.Line)

	// Append the entry to an already existing stream (if any)
	labels := labelsMapToString(entry.Labels, ReservedLabelTenantID)
	if stream, ok := b.streams[labels]; ok {
		stream.Entries = append(stream.Entries, entry.Entry)
		return
	}

	// Add the entry as a new stream
	b.streams[labels] = &logproto.Stream{
		Labels:  labels,
		Entries: []logproto.Entry{entry.Entry},
	}
}

func labelsMapToString(ls model.LabelSet, without ...model.LabelName) string {
	lstrs := make([]string, 0, len(ls))
Outer:
	for l, v := range ls {
		for _, w := range without {
			if l == w {
				continue Outer
			}
		}
		lstrs = append(lstrs, fmt.Sprintf("%s=%q", l, v))
	}

	sort.Strings(lstrs)
	return fmt.Sprintf("{%s}", strings.Join(lstrs, ", "))
}

// sizeBytes returns the current batch size in bytes
func (b *batch) sizeBytes() int {
	return b.bytes
}

// sizeBytesAfter returns the size of the batch after the input entry
// will be added to the batch itself
func (b *batch) sizeBytesAfter(entry api.Entry) int {
	return b.bytes + len(entry.Line)
}

// age of the batch since its creation
func (b *batch) age() time.Duration {
	return time.Since(b.createdAt)
}

// encode the batch as snappy-compressed push request, and returns
// the encoded bytes and the number of encoded entries
func (b *batch) encode() ([]byte, int, error) {
	var buf bytes.Buffer
	var batchCount int
	for _, stream := range b.streams {
		data,err := json.Marshal(stream)
		if err != nil {
			logrus.Errorf("Cannot encode article %d: %s", stream.Labels, err)
			continue
		}
		//buf.Grow(len(data))
		batchCount += len(stream.Entries)
		buf.Write(data)
	}
	return buf.Bytes(),batchCount , nil
}
