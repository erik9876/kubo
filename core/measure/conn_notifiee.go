package measure

import (
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	ma "github.com/multiformats/go-multiaddr"
)

type ConnNotifiee struct {
	logger *Logger
}

type connOpenPayload struct {
	ConnID    string `json:"conn_id"`
	PeerID    string `json:"peer_id"`
	Multiaddr string `json:"multiaddr"`
	Inbound   bool   `json:"inbound"`
	Transport string `json:"transport"`
}

type connClosePayload struct {
	ConnID    string    `json:"conn_id"`
	PeerID    string    `json:"peer_id"`
	Multiaddr string    `json:"multiaddr"`
	Inbound   bool      `json:"inbound"`
	Transport string    `json:"transport"`
	OpenTime  time.Time `json:"open_time"`
}

func (n *ConnNotifiee) Connected(_ network.Network, c network.Conn) {
	n.logger.Log(EventConnOpen, connOpenPayload{
		ConnID:    c.ID(),
		PeerID:    c.RemotePeer().String(),
		Multiaddr: c.RemoteMultiaddr().String(),
		Inbound:   c.Stat().Direction == network.DirInbound,
		Transport: transportName(c.RemoteMultiaddr()),
	})
}

func (n *ConnNotifiee) Disconnected(_ network.Network, c network.Conn) {
	n.logger.Log(EventConnClose, connClosePayload{
		ConnID:    c.ID(),
		PeerID:    c.RemotePeer().String(),
		Multiaddr: c.RemoteMultiaddr().String(),
		Inbound:   c.Stat().Direction == network.DirInbound,
		Transport: transportName(c.RemoteMultiaddr()),
		OpenTime:  c.Stat().Opened,
	})
}

func (n *ConnNotifiee) Listen(_ network.Network, _ ma.Multiaddr)      {}
func (n *ConnNotifiee) ListenClose(_ network.Network, _ ma.Multiaddr) {}

// Compile-time check, that interface is met
var _ network.Notifiee = (*ConnNotifiee)(nil)
