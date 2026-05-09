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

func (l *Logger) writeLoop() {
	defer l.wg.Done()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case event, ok := <-l.events:
			if !ok {
				return
			}
			file, err := l.safeGetFile(event.Type)
			if err != nil {
				fmt.Fprintf(os.Stderr, "measure: failed to open log file: %v\n", err)
				continue
			}
			data, err := json.Marshal(event)
			if err != nil {
				continue
			}
			file.Write(append(data, '\n'))

		case <-ticker.C:
			for _, f := range l.files {
				f.Sync()
			}
		}
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
	for _, f := range l.files {
		f.Sync()
		f.Close()
	}

	return nil
}
