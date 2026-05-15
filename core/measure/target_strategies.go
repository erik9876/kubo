package measure

import (
	"context"
	crand "crypto/rand"
	"encoding/json"
	"fmt"
	mrand "math/rand"
	"os"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

type Strategy int

const (
	StrategyRT Strategy = iota
	StrategyPS
	StrategyRandom
)

func (s Strategy) String() string {
	switch s {
	case StrategyRT:
		return "rt"
	case StrategyPS:
		return "ps"
	case StrategyRandom:
		return "random"
	default:
		return "unknown"
	}
}

func (s Strategy) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.String())
}

type Sequencer struct {
	next int
}

func newSequencer() *Sequencer {
	return &Sequencer{
		next: 0,
	}
}

func (s *Sequencer) Next() Strategy {
	strat := Strategy(s.next)
	s.next = (s.next + 1) % 3
	return strat
}

func pickFromRoutingTable(rt *dht.IpfsDHT, exclude peer.ID) (peer.ID, bool) {
	peers := rt.RoutingTable().ListPeers()
	filtered := make([]peer.ID, 0, len(peers))
	for _, p := range peers {
		if p == exclude {
			continue
		}
		filtered = append(filtered, p)
	}
	if len(filtered) == 0 {
		return "", false
	}
	return filtered[mrand.Intn(len(filtered))], true
}

func pickFromPeerstore(h host.Host, exclude peer.ID) (peer.ID, bool) {
	ps := h.Peerstore()
	peers := ps.Peers()
	filtered := make([]peer.ID, 0, len(peers))
	for _, p := range peers {
		if p == exclude {
			continue
		}
		supports, err := ps.SupportsProtocols(p, "/ipfs/kad/1.0.0") // dht server approximation
		if err != nil || len(supports) == 0 {
			continue
		}
		filtered = append(filtered, p)
	}
	if len(filtered) == 0 {
		return "", false
	}
	return filtered[mrand.Intn(len(filtered))], true
}

func pickRandomViaClosestPeers(ctx context.Context, kdht *dht.IpfsDHT, excludeID peer.ID) (peer.ID, bool) {
	randomKey := make([]byte, 32)
	if _, err := crand.Read(randomKey); err != nil {
		fmt.Fprintf(os.Stderr, "measure: error generating random key: %v\n", err)
		return "", false
	}
	ctx = dht.WithLookupOrigin(ctx, "measurement-random-pick")
	peers, err := kdht.GetClosestPeers(ctx, string(randomKey))
	if err != nil {
		fmt.Fprintf(os.Stderr, "measure: error getting closest peers: %v\n", err)
		return "", false
	}
	filtered := make([]peer.ID, 0, len(peers))
	for _, p := range peers {
		if p == excludeID {
			continue
		}
		filtered = append(filtered, p)
	}
	if len(filtered) == 0 {
		return "", false
	}
	return filtered[mrand.Intn(len(filtered))], true
}
