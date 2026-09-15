package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
)

// hub fans one snapshot out to every connected browser.
//
// Each subscriber gets a buffered channel and is dropped from a broadcast if it
// cannot keep up, so one slow tab can never stall the poll loop or the others.
type hub struct {
	mu          sync.RWMutex
	subscribers map[chan []byte]struct{}
}

func newHub() *hub {
	return &hub{subscribers: map[chan []byte]struct{}{}}
}

// subscribe registers a new listener and returns it with its unsubscribe func.
func (h *hub) subscribe() (chan []byte, func()) {
	ch := make(chan []byte, 4)

	h.mu.Lock()
	h.subscribers[ch] = struct{}{}
	h.mu.Unlock()

	return ch, func() {
		h.mu.Lock()
		delete(h.subscribers, ch)
		h.mu.Unlock()
		close(ch)
	}
}

// broadcast sends a payload to every subscriber, skipping any whose buffer is
// full rather than blocking on it.
func (h *hub) broadcast(payload []byte) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	for ch := range h.subscribers {
		select {
		case ch <- payload:
		default:
		}
	}
}

// count reports how many browsers are connected.
func (h *hub) count() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.subscribers)
}

// handleEvents streams snapshots as Server-Sent Events.
//
// SSE rather than WebSockets: the traffic is strictly one-way, it survives
// proxies without special handling, and browsers reconnect on their own.
func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	// Named explicitly because an SSE stream through a buffering reverse proxy
	// arrives in useless clumps, or not at all.
	w.Header().Set("X-Accel-Buffering", "no")

	events, unsubscribe := s.hub.subscribe()
	defer unsubscribe()

	// Send the current snapshot immediately so a new tab is never blank.
	if snapshot := s.latest(); snapshot != nil {
		if payload, err := json.Marshal(snapshot); err == nil {
			writeEvent(w, payload)
			flusher.Flush()
		}
	}

	for {
		select {
		case <-r.Context().Done():
			return
		case payload, open := <-events:
			if !open {
				return
			}
			writeEvent(w, payload)
			flusher.Flush()
		}
	}
}

func writeEvent(w http.ResponseWriter, payload []byte) {
	fmt.Fprintf(w, "data: %s\n\n", payload)
}
