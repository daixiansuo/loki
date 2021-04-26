package elastic

import (
	"bytes"
	"encoding/json"
	"github.com/elastic/go-elasticsearch/v7/esutil"
	"github.com/grafana/loki/clients/pkg/promtail/api"
	"log"
	"time"
)


type bulk struct {
	bulkItems   []*esutil.BulkIndexerItem
	bytes     int
	createdAt time.Time
}



func newBulk(entries ...api.Entry) *bulk {
	b := &bulk{
		bulkItems:   make([]*esutil.BulkIndexerItem,0),
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
func (b *bulk) add(entry api.Entry) {
	b.bytes += len(entry.Line)
	data, err := json.Marshal(entry)
	if err != nil {
		log.Fatalf("Cannot encode article  %s", err)
	}
	// convert entry into bulkIndexItem
	bulkIndexItem := esutil.BulkIndexerItem{
		Action: "index",
		// DocumentID is the (optional) document ID
		// Body is an `io.Reader` with the payload
		Body: bytes.NewReader(data),
	}

	// Append the entry to an already existing stream (if any)

	b.bulkItems = append(b.bulkItems, &bulkIndexItem)
}



func (b *bulk)numEntryItems()int{
	return len(b.bulkItems)
}

// age of the batch since its creation
func (b *bulk) age() time.Duration {
	return time.Since(b.createdAt)
}
