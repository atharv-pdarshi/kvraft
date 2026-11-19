package client

import (
	"sync"
	"time"
)

// Event records one completed client operation: what was asked, what came back,
// and when. The fault-tolerance tests use the history to prove that every write
// that got an OK can be read back at the end (the durability invariant).
type Event struct {
	Op    string // "put" | "get" | "delete"
	Key   string
	Value string
	OK    bool // operation succeeded
	Found bool // for get
	Start time.Time
	End   time.Time
	Err   error
}

// History is a thread-safe log of client events.
type History struct {
	mu     sync.Mutex
	events []Event
}

// NewHistory returns an empty recorder.
func NewHistory() *History { return &History{} }

func (h *History) add(e Event) {
	h.mu.Lock()
	h.events = append(h.events, e)
	h.mu.Unlock()
}

// Events returns a copy of all recorded events.
func (h *History) Events() []Event {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]Event(nil), h.events...)
}

// AckedWrites returns the final acknowledged value for each key (last writer
// wins by completion time), ignoring deletes' later effects — callers that mix
// deletes should inspect Events directly.
func (h *History) AckedWrites() map[string]string {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := map[string]string{}
	for _, e := range h.events {
		if e.Op == "put" && e.OK {
			out[e.Key] = e.Value
		}
	}
	return out
}
