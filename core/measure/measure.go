package measure

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/ipfs/kubo/core"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

type Service struct {
	host             host.Host
	logger           *Logger
	sampler          *StateSampler
	notifiee         *ConnNotifiee
	lookupSubscriber *LookupSubscriber
	recordSubscriber *PeerRecordSubscriber
	oobPonger        *OOBPonger
	oobPinger        *OOBPinger
	peerstoreTracker *PeerstoreTracker
	cancel           context.CancelFunc
	wg               sync.WaitGroup
}

func Start(ctx context.Context, node *core.IpfsNode, cfg Config) (*Service, error) {
	if cfg.Mode == ModeOff {
		return nil, nil
	}

	logger, err := NewLogger(cfg.LogDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create logger: %w", err)
	}

	sampleCtx, cancel := context.WithCancel(context.Background())

	notifiee := &ConnNotifiee{logger: logger}
	node.PeerHost.Network().Notify(notifiee)

	lookupSubscriber := newLookupSubscriber(logger)

	recordSubscriber, err := newPeerRecordSubscriber(node.PeerHost, logger)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create peer record subscriber: %w", err)
	}

	s := &Service{
		host:   node.PeerHost,
		logger: logger,
		sampler: &StateSampler{
			host:      node.PeerHost,
			ps:        node.PeerHost.Peerstore(),
			logger:    logger,
			interval:  cfg.SampleInterval,
			startTime: time.Now(),
		},
		notifiee:         notifiee,
		lookupSubscriber: lookupSubscriber,
		recordSubscriber: recordSubscriber,
		cancel:           cancel,
	}

	s.wg.Add(1)
	go s.sampler.Run(sampleCtx, &s.wg)

	s.wg.Add(1)
	go lookupSubscriber.Run(sampleCtx, &s.wg)

	dht.SetGlobalLookupHook(lookupSubscriber.onEvent)

	s.wg.Add(1)
	go recordSubscriber.Run(sampleCtx, &s.wg)

	if cfg.Mode == ModePonger {
		s.peerstoreTracker = newPeerstoreTracker(node.PeerHost.Peerstore(), cfg.PeerstoreTrackInterval)
		s.wg.Add(1)
		go s.peerstoreTracker.Run(sampleCtx, &s.wg)

		s.oobPonger = newOOBPonger(cfg.OOBListenAddr, logger, node.PeerHost, node.DHT.WAN, s.peerstoreTracker)
		s.wg.Add(1)
		go s.oobPonger.Run(sampleCtx, &s.wg)
	}
	if cfg.Mode == ModePinger {
		s.oobPinger = newOOBPinger(cfg.PartnerAddr, logger)
		s.wg.Add(1)
		go s.oobPinger.Run(sampleCtx, &s.wg)

		partnerPID, err := peer.Decode(cfg.PartnerPeerID)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("invalid PartnerPeerID %q: %w", cfg.PartnerPeerID, err)
		}
		pinger := newPinger(node.PeerHost, node.DHT.WAN, partnerPID, s.oobPinger, cfg.PingerInterval, logger, newSequencer())
		s.wg.Add(1)
		go pinger.Run(sampleCtx, &s.wg)
	}

	fmt.Fprintf(os.Stderr, "measure: service started in mode=%s logDir=%s\n", cfg.Mode, cfg.LogDir)

	return s, nil
}

func (s *Service) Close() error {
	if s == nil {
		return nil
	}
	fmt.Fprintf(os.Stderr, "measure: service stopping\n")

	dht.ClearGlobalLookupHook()

	if s.notifiee != nil {
		s.host.Network().StopNotify(s.notifiee)
	}

	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()

	if s.logger != nil {
		if err := s.logger.Close(); err != nil {
			return err
		}
	}

	fmt.Fprintf(os.Stderr, "measure: service stopped\n")
	return nil
}
