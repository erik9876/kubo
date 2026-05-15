package measure

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

type Pinger struct {
	host        host.Host
	dht         *dht.IpfsDHT
	partnerPID  peer.ID
	oobPinger   *OOBPinger
	interval    time.Duration
	logger      *Logger
	seq         *Sequencer
	outstanding atomic.Bool
}

type PingerQueryPayload struct {
	QueryID        string    `json:"query_id"`
	Strategy       Strategy  `json:"strategy"`
	TargetPID      peer.ID   `json:"target_pid"`
	SendTime       time.Time `json:"send_time"`
	TotalLatencyMS int64     `json:"total_latency_ms"`
	DoneStatus     string    `json:"done_status"` // "ok" | "ack-timeout" | "done-timeout" | "send-timeout" | "disconnected"
	DoneErr        string    `json:"done_err"`
}

type QuerySkipPayload struct {
	Reason string `json:"reason"` // "prev-pending"
}

type TargetSkipPayload struct {
	Strategy Strategy `json:"strategy"`
	Reason   string   `json:"reason"` // "empty"
}

func newPinger(h host.Host, d *dht.IpfsDHT, partnerPID peer.ID, oob *OOBPinger, interval time.Duration, logger *Logger, seq *Sequencer) *Pinger {
	return &Pinger{
		host:       h,
		dht:        d,
		partnerPID: partnerPID,
		oobPinger:  oob,
		interval:   interval,
		logger:     logger,
		seq:        seq,
	}
}

func (p *Pinger) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	// Wait for in-flight doSend goroutines before returning, so logger.Log
	// is never called after Service.Close has shut the logger down.
	var sendWg sync.WaitGroup
	defer sendWg.Wait()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if p.outstanding.Load() {
				// Sequencer is NOT advanced on skip — strategy ordering across
				// actual sends stays strict RT→PS→Random.
				p.logger.Log(EventQuerySkip, QuerySkipPayload{Reason: "prev-pending"})
				continue
			}
			strategy := p.seq.Next()
			p.outstanding.Store(true)
			sendWg.Add(1)
			go p.doSend(ctx, &sendWg, strategy)
		}
	}
}

func (p *Pinger) doSend(ctx context.Context, wg *sync.WaitGroup, strategy Strategy) {
	defer wg.Done()
	// Clear outstanding only after the log write so a mid-flight tick still
	// emits query-skip rather than racing this goroutine's logger.Log.
	defer p.outstanding.Store(false)

	target, ok := p.pickTarget(ctx, strategy)
	if !ok {
		// Sequencer was already advanced — strategy was attempted, pool was empty.
		p.logger.Log(EventTargetSkip, TargetSkipPayload{Strategy: strategy, Reason: "empty"})
		return
	}

	queryID := uuid.New().String()
	start := time.Now()
	done := p.oobPinger.SendQuery(newOOBQuery(queryID, strategy.String(), target.String()))
	totalLatency := time.Since(start)

	// done.Type is always "done" (wire discriminator). Status taxonomy lives in OK/Err.
	status := "ok"
	if !done.OK {
		status = done.Err
	}

	p.logger.Log(EventPingerQuery, PingerQueryPayload{
		QueryID:        queryID,
		Strategy:       strategy,
		TargetPID:      target,
		SendTime:       start,
		TotalLatencyMS: totalLatency.Milliseconds(),
		DoneStatus:     status,
		DoneErr:        done.Err,
	})
}

func (p *Pinger) pickTarget(ctx context.Context, strategy Strategy) (peer.ID, bool) {
	switch strategy {
	case StrategyRT:
		return pickFromRoutingTable(p.dht, p.partnerPID)
	case StrategyPS:
		return pickFromPeerstore(p.host, p.partnerPID)
	case StrategyRandom:
		// Bound the GetClosestPeers walk so a hanging pick doesn't block outstanding.
		pickCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
		defer cancel()
		return pickRandomViaClosestPeers(pickCtx, p.dht, p.partnerPID)
	default:
		panic(fmt.Sprintf("pinger: unknown strategy %v", strategy))
	}
}
