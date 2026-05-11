package measure

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/host"
)

type PeerRecordSubscriber struct {
	host   host.Host
	logger *Logger
	sub    event.Subscription
}

func newPeerRecordSubscriber(h host.Host, logger *Logger) (*PeerRecordSubscriber, error) {
	sub, err := h.EventBus().Subscribe(new(event.EvtPeerIdentificationCompleted))
	if err != nil {
		return nil, fmt.Errorf("failed to subscribe to peer identification events: %w", err)
	}
	return &PeerRecordSubscriber{
		host:   h,
		logger: logger,
		sub:    sub,
	}, nil
}

func (s *PeerRecordSubscriber) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	defer s.sub.Close()
	for {
		select {
		case raw, ok := <-s.sub.Out():
			if !ok {
				return
			}
			ev, ok := raw.(event.EvtPeerIdentificationCompleted)
			if !ok {
				fmt.Fprintf(os.Stderr, "measure: unexpected event type %T on peer-record sub\n", raw)
				continue
			}
			s.handleEvent(ev)
		case <-ctx.Done():
			return
		}
	}
}

func (s *PeerRecordSubscriber) handleEvent(ev event.EvtPeerIdentificationCompleted) {
	payload := inspectEnvelope(ev.SignedPeerRecord, ev.ListenAddrs, ev.Peer, ev.AgentVersion)
	s.logger.Log(EventPeerRecord, payload)
}
