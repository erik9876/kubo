package measure

import (
	"context"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/multiformats/go-multiaddr"
)

type StateSampler struct {
	host     host.Host
	ps       peerstore.Peerstore
	logger   *Logger
	interval time.Duration
}

type stateSamplePayload struct {
	PeerstoreSize   int            `json:"peerstore_size"`
	PeersWithAddrs  int            `json:"peers_with_addrs"`
	ActiveConns     int            `json:"active_conns"`
	TransportCounts map[string]int `json:"transport_counts"`
}

func (s *StateSampler) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			conns := s.host.Network().Conns()
			transportTypes := make(map[string]int)
			for _, conn := range conns {
				transportTypes[transportName(conn.RemoteMultiaddr())]++
			}

			s.logger.Log(EventStateSample, stateSamplePayload{
				PeerstoreSize:   len(s.ps.Peers()),
				PeersWithAddrs:  len(s.ps.PeersWithAddrs()),
				ActiveConns:     len(conns),
				TransportCounts: transportTypes,
			})
		case <-ctx.Done():
			return
		}
	}

}

func transportName(ma multiaddr.Multiaddr) string {
	protocols := ma.Protocols()
	for i := len(protocols) - 1; i >= 0; i-- {
		p := protocols[i]
		switch p.Code {
		case multiaddr.P_WEBRTC, multiaddr.P_WEBRTC_DIRECT:
			return "webrtc"
		case multiaddr.P_WEBTRANSPORT:
			return "webtransport"
		case multiaddr.P_WS, multiaddr.P_WSS:
			return "websocket"
		case multiaddr.P_QUIC_V1, multiaddr.P_QUIC:
			return "quic"
		case multiaddr.P_TCP:
			return "tcp"
		}
	}
	return "unknown"
}
