package measure

import (
	"context"
	"encoding/hex"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/peer"
)

type lookupState struct {
	Origin      string
	StartTime   time.Time
	KeyShort    string
	queried     map[peer.ID]struct{}
	unreachable map[peer.ID]struct{}
	LastUpdate  time.Time
}

type LookupSubscriber struct {
	events  chan *dht.LookupEvent
	states  map[uuid.UUID]*lookupState
	logger  *Logger
	dropped uint64
}

type dhtLookupPayload struct {
	LookupID        string    `json:"lookup_id"`
	Origin          string    `json:"origin"`
	KeyShort        string    `json:"key_short"`
	StartTime       time.Time `json:"start_time"`
	EndTime         time.Time `json:"end_time"`
	DurationMS      int64     `json:"duration_ms"`
	NumQueried      int       `json:"num_queried"`
	NumUnreachable  int       `json:"num_unreachable"`
	TerminateReason string    `json:"terminate_reason"`
}

func newLookupSubscriber(logger *Logger) *LookupSubscriber {
	return &LookupSubscriber{
		events: make(chan *dht.LookupEvent, 4096),
		states: make(map[uuid.UUID]*lookupState),
		logger: logger,
	}
}

func (s *LookupSubscriber) onEvent(event *dht.LookupEvent) {
	select {
	case s.events <- event:
	default:
		atomic.AddUint64(&s.dropped, 1)
	}
}

func (s *LookupSubscriber) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	staleTick := time.NewTicker(30 * time.Second)
	defer staleTick.Stop()

	for {
		select {
		case ev := <-s.events:
			s.handleEvent(ev)
		case <-staleTick.C:
			s.flushStale()
		case <-ctx.Done():
			return
		}
	}
}

func (s *LookupSubscriber) handleEvent(event *dht.LookupEvent) {
	id := event.ID
	state, ok := s.states[id]
	if !ok {
		state = &lookupState{
			Origin:      event.Origin,
			StartTime:   time.Now(),
			KeyShort:    keyShort(event.Key.Key),
			queried:     make(map[peer.ID]struct{}),
			unreachable: make(map[peer.ID]struct{}),
		}
		s.states[id] = state
	}
	state.LastUpdate = time.Now()
	if event.Response != nil {
		for _, qPeer := range event.Response.Queried {
			state.queried[qPeer.Peer] = struct{}{}
		}
		for _, uPeer := range event.Response.Unreachable {
			state.unreachable[uPeer.Peer] = struct{}{}
		}
	}
	if event.Terminate != nil {
		s.emit(id, state, event.Terminate.Reason.String())
		delete(s.states, id)
	}
}

func (s *LookupSubscriber) emit(id uuid.UUID, state *lookupState, terminateReason string) {
	endTime := time.Now()
	s.logger.Log(EventDHTLookup, dhtLookupPayload{
		LookupID:        id.String(),
		Origin:          state.Origin,
		KeyShort:        state.KeyShort,
		StartTime:       state.StartTime,
		EndTime:         endTime,
		DurationMS:      endTime.Sub(state.StartTime).Milliseconds(),
		NumQueried:      len(state.queried),
		NumUnreachable:  len(state.unreachable),
		TerminateReason: terminateReason,
	})
}

func (s *LookupSubscriber) flushStale() {
	for id, state := range s.states {
		if time.Since(state.LastUpdate) > 5*time.Minute {
			s.emit(id, state, "orphaned")
			delete(s.states, id)
		}
	}
}

func keyShort(k string) string {
	b := []byte(k)
	if len(b) > 8 {
		b = b[:8]
	}
	return hex.EncodeToString(b)
}
