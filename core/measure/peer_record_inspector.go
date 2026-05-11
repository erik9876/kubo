package measure

import (
	"fmt"
	"os"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/record"
	ma "github.com/multiformats/go-multiaddr"
)

type peerRecordPayload struct {
	PeerID            string `json:"peer_id"`
	HasEnvelope       bool   `json:"has_envelope"`
	Seq               uint64 `json:"seq"`
	NumAddrsCertified int    `json:"num_addrs_certified"`
	NumAddrsListen    int    `json:"num_addrs_listen"`
	AgentVersion      string `json:"agent_version"`
}

func inspectEnvelope(env *record.Envelope, listenAddrs []ma.Multiaddr, peerID peer.ID, agentVersion string) peerRecordPayload {
	if env == nil {
		return peerRecordPayload{
			PeerID:            peerID.String(),
			HasEnvelope:       false,
			Seq:               0,
			NumAddrsCertified: 0,
			NumAddrsListen:    len(listenAddrs),
			AgentVersion:      agentVersion,
		}
	}
	rec, err := env.Record()
	if err != nil {
		fmt.Fprintf(os.Stderr, "measure: peer-record envelope unwrap failed for %s: %v\n", peerID, err)
		return peerRecordPayload{
			PeerID:         peerID.String(),
			HasEnvelope:    false,
			NumAddrsListen: len(listenAddrs),
			AgentVersion:   agentVersion,
		}
	}
	pr, ok := rec.(*peer.PeerRecord)
	if !ok {
		fmt.Fprintf(os.Stderr, "measure: peer-record envelope contained unexpected type %T for %s\n", rec, peerID)
		return peerRecordPayload{
			PeerID:         peerID.String(),
			HasEnvelope:    false,
			NumAddrsListen: len(listenAddrs),
			AgentVersion:   agentVersion,
		}
	}
	return peerRecordPayload{
		PeerID:            peerID.String(),
		HasEnvelope:       true,
		Seq:               pr.Seq,
		NumAddrsCertified: len(pr.Addrs),
		NumAddrsListen:    len(listenAddrs),
		AgentVersion:      agentVersion,
	}
}
