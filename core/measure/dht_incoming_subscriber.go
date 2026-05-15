package measure

import (
	"context"
	"sync"
	"time"

	dht "github.com/libp2p/go-libp2p-kad-dht"
)

// Periodic snapshots of dht.SnapshotIncomingQueries(). Counters are monotonic
// over the process lifetime — diff consecutive samples for per-window rates.
// Cadence matches the dropped-counters: 60s plus a final flush at shutdown.
type DHTIncomingSubscriber struct {
	logger *Logger
}

func newDHTIncomingSubscriber(logger *Logger) *DHTIncomingSubscriber {
	return &DHTIncomingSubscriber{logger: logger}
}

func (s *DHTIncomingSubscriber) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	tick := time.NewTicker(60 * time.Second)
	defer tick.Stop()

	for {
		select {
		case <-tick.C:
			s.emit()
		case <-ctx.Done():
			s.emit()
			return
		}
	}
}

func (s *DHTIncomingSubscriber) emit() {
	s.logger.Log(EventDHTIncoming, dht.SnapshotIncomingQueries())
}
