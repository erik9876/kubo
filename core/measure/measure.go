package measure

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/ipfs/kubo/core"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/host"
)

type Service struct {
	host       host.Host
	logger     *Logger
	sampler    *StateSampler
	notifiee   *ConnNotifiee
	subscriber *LookupSubscriber
	cancel     context.CancelFunc
	wg         sync.WaitGroup
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

	subscriber := newLookupSubscriber(logger)

	s := &Service{
		host:   node.PeerHost,
		logger: logger,
		sampler: &StateSampler{
			host:     node.PeerHost,
			ps:       node.PeerHost.Peerstore(),
			logger:   logger,
			interval: cfg.SampleInterval,
		},
		notifiee:   notifiee,
		subscriber: subscriber,
		cancel:     cancel,
	}

	s.wg.Add(1)
	go s.sampler.Run(sampleCtx, &s.wg)

	s.wg.Add(1)
	go subscriber.Run(sampleCtx, &s.wg)

	dht.SetGlobalLookupHook(subscriber.onEvent)

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
