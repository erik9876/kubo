package measure

import (
	"context"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
)

type PeerstoreTracker struct {
	ps        peerstore.Peerstore
	mu        sync.RWMutex
	firstSeen map[peer.ID]time.Time
	interval  time.Duration
}

func newPeerstoreTracker(ps peerstore.Peerstore, interval time.Duration) *PeerstoreTracker {
	return &PeerstoreTracker{
		ps:        ps,
		mu:        sync.RWMutex{},
		firstSeen: make(map[peer.ID]time.Time),
		interval:  interval,
	}
}

func (t *PeerstoreTracker) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(t.interval)
	defer ticker.Stop()

	t.snapshot()

	for {
		select {
		case <-ticker.C:
			t.snapshot()
		case <-ctx.Done():
			return
		}
	}
}

func (t *PeerstoreTracker) snapshot() {
	peers := t.ps.Peers()
	peerSet := make(map[peer.ID]struct{}, len(peers))
	t.mu.Lock()
	for _, p := range peers {
		peerSet[p] = struct{}{}
		hasAddr := len(t.ps.Addrs(p)) > 0
		_, ok := t.firstSeen[p]
		if hasAddr && !ok {
			t.firstSeen[p] = time.Now()
		} else if !hasAddr && ok {
			delete(t.firstSeen, p)
		}
	}
	for p := range t.firstSeen {
		if _, ok := peerSet[p]; !ok {
			delete(t.firstSeen, p)
		}
	}
	t.mu.Unlock()
}

func (t *PeerstoreTracker) FirstSeen(p peer.ID) (time.Time, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	firstSeen, ok := t.firstSeen[p]
	return firstSeen, ok
}
