package measure

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

type Event struct {
	Timestamp time.Time
	Type      string
	Payload   any
}

type Logger struct {
	events  chan Event
	wg      sync.WaitGroup
	files   map[string]*os.File
	logDir  string
	dropped uint64
}

func NewLogger(logDir string) (*Logger, error) {
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create log dir: %w", err)
	}

	l := &Logger{
		events: make(chan Event, 1024),
		files:  make(map[string]*os.File),
		logDir: logDir,
	}
	l.wg.Add(1)
	go l.writeLoop()

	return l, nil
}

func (l *Logger) safeGetFile(eventType string) (*os.File, error) {
	if f, ok := l.files[eventType]; ok {
		return f, nil
	}
	path := filepath.Join(l.logDir, eventType+".jsonl")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	l.files[eventType] = f
	return f, nil
}

// droppedPayload is the body of EventLoggerDropped / EventLookupDropped.
// The counter is monotonic over the lifetime of the process; consumers should
// diff consecutive samples to get a per-window drop rate.
type droppedPayload struct {
	Dropped uint64 `json:"dropped"`
}

func (l *Logger) writeLoop() {
	defer l.wg.Done()

	syncTicker := time.NewTicker(5 * time.Second)
	defer syncTicker.Stop()
	droppedTicker := time.NewTicker(60 * time.Second)
	defer droppedTicker.Stop()

	for {
		select {
		case event, ok := <-l.events:
			if !ok {
				return
			}
			l.writeEvent(event)

		case <-syncTicker.C:
			for _, f := range l.files {
				f.Sync()
			}

		case <-droppedTicker.C:
			// Write directly via writeEvent (not via the events channel) so
			// the dropped-snapshot itself can never be dropped — that would
			// be the one event you really need when investigating drops.
			l.writeEvent(l.droppedEvent())
		}
	}
}

// writeEvent is the single point of file IO. Called only from writeLoop and
// from Close (after writeLoop has exited), so no synchronization on l.files
// is needed.
func (l *Logger) writeEvent(event Event) {
	file, err := l.safeGetFile(event.Type)
	if err != nil {
		fmt.Fprintf(os.Stderr, "measure: failed to open log file: %v\n", err)
		return
	}
	data, err := json.Marshal(event)
	if err != nil {
		return
	}
	file.Write(append(data, '\n'))
}

func (l *Logger) droppedEvent() Event {
	return Event{
		Timestamp: time.Now(),
		Type:      EventLoggerDropped,
		Payload:   droppedPayload{Dropped: atomic.LoadUint64(&l.dropped)},
	}
}

func (l *Logger) Log(eventType string, payload any) {
	event := Event{time.Now(), eventType, payload}
	select {
	case l.events <- event:
	default:
		atomic.AddUint64(&l.dropped, 1)
	}
}

func (l *Logger) Close() error {
	close(l.events)
	l.wg.Wait()
	// writeLoop is done; safe to touch files map without coordination.
	// Emit a final snapshot so the end-of-run drop total is always on disk.
	l.writeEvent(l.droppedEvent())
	for _, f := range l.files {
		f.Sync()
		f.Close()
	}

	return nil
}
