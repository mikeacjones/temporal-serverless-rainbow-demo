// Package api is the demo's backend: a JSON and Server-Sent-Events API over
// Temporal.
//
// It renders no HTML. The dashboard is a separate static frontend that talks
// to this API, so the two can be built, deployed and scaled independently —
// and so neither one's code is cluttered with the other's concerns.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"go.temporal.io/sdk/client"

	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/deploy"
	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/metrics"
	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/orders"
	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/rollout"
	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/traffic"
)

// Options configure the backend.
type Options struct {
	Client        client.Client
	Deployment    *deploy.Client
	Reader        *metrics.Reader
	Logger        *slog.Logger
	PollInterval  time.Duration
	AllowedOrigin string
	ControlQueue  string

	// MetricsURL is the Temporal server's Prometheus endpoint, the only place
	// the real sync match rate is reported. Empty hides that one gauge.
	MetricsURL string
	// MetricsAPIKey authenticates against Temporal Cloud's metrics endpoint.
	// Unused by a self-hosted server.
	MetricsAPIKey string
	// MetricsNamespace narrows a Cloud reading, whose endpoint spans the
	// whole account.
	MetricsNamespace string

	// TrafficMaxRun is how long generated traffic may flow untouched before
	// stopping itself. Zero uses the generator's own default.
	TrafficMaxRun time.Duration

	// OrderSample is how many live orders each version's column samples. Zero
	// uses the default. It is per version, so one version's burst cannot push
	// another version's orders out of view.
	OrderSample int
}

// Server holds the backend's state: one cached snapshot, one SSE hub, and
// thin controllers over the two control-plane Workflows.
type Server struct {
	deployment *deploy.Client
	reader     *metrics.Reader
	logger     *slog.Logger
	hub        *hub

	traffic  *trafficController
	rollouts *rolloutController
	bursts   *traffic.Burst

	// lastReconcile throttles the fault repair, and lastHistory paces the
	// sparkline samples. Touched only by the poll goroutine, so neither needs
	// a lock.
	lastReconcile time.Time
	lastHistory   time.Time

	// sinceMu guards the counter baseline, which an HTTP handler sets while
	// the poll goroutine reads it.
	sinceMu sync.Mutex
	since   time.Time

	// seenMu guards the last time a client asked for state, which is how a
	// non-streaming caller registers interest.
	seenMu     sync.Mutex
	lastSeen   time.Time
	wasWatched bool

	allowedOrigin string

	// snapshot is replaced wholesale on each poll, so readers never see a
	// half-updated picture.
	mu       sync.RWMutex
	snapshot *Snapshot

	orderSample int

	history    *History
	throughput *throughputMeter
	syncMatch  *metrics.SyncMatchReader

	// Per-version health is twenty visibility queries; cache it briefly.
	healthMu    sync.Mutex
	healthCache map[string]metrics.Health
	healthAt    time.Time
}

// New builds the backend.
func New(opts Options) *Server {
	if opts.PollInterval == 0 {
		opts.PollInterval = time.Second
	}
	if opts.AllowedOrigin == "" {
		opts.AllowedOrigin = "*"
	}
	if opts.ControlQueue == "" {
		opts.ControlQueue = rollout.TaskQueue
	}
	if opts.OrderSample <= 0 {
		opts.OrderSample = defaultOrdersPerVersion
	}

	return &Server{
		deployment:    opts.Deployment,
		reader:        opts.Reader,
		logger:        opts.Logger,
		hub:           newHub(),
		traffic:       &trafficController{c: opts.Client, logger: opts.Logger, maxRun: opts.TrafficMaxRun},
		rollouts:      &rolloutController{c: opts.Client, taskQueue: opts.ControlQueue, logger: opts.Logger},
		bursts:        traffic.NewBurst(opts.Client, opts.ControlQueue, opts.Logger),
		allowedOrigin: opts.AllowedOrigin,
		orderSample:   opts.OrderSample,
		history:       &History{},
		throughput:    &throughputMeter{},
		syncMatch: metrics.NewSyncMatchReader(metrics.SyncMatchOptions{
			URL:       opts.MetricsURL,
			TaskQueue: orders.TaskQueue,
			APIKey:    opts.MetricsAPIKey,
			Namespace: opts.MetricsNamespace,
		}),
	}
}

// Start begins polling Temporal in the background until ctx is cancelled.
func (s *Server) Start(ctx context.Context, interval time.Duration) {
	// Build one snapshot immediately so the first request is not empty.
	s.store(s.build(ctx))
	go s.poll(ctx, interval)
}

// Routes returns the HTTP handler.
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	mux.HandleFunc("GET /api/state", s.handleState)
	mux.HandleFunc("GET /events", s.handleEvents)

	// Traffic shaping.
	mux.HandleFunc("POST /api/traffic/rate", s.handleTrafficRate)
	mux.HandleFunc("POST /api/traffic/spike", s.handleTrafficSpike)
	mux.HandleFunc("POST /api/traffic/split", s.handleTrafficSplit)
	mux.HandleFunc("POST /api/traffic/chaos", s.handleTrafficChaos)
	mux.HandleFunc("POST /api/traffic/stop", s.handleTrafficStop)

	// Counters.
	mux.HandleFunc("POST /api/metrics/reset", s.handleMetricsReset)

	// Automated rollouts.
	mux.HandleFunc("POST /api/rollout", s.handleRolloutStart)
	mux.HandleFunc("POST /api/rollout/pause", s.handleRolloutPause)
	mux.HandleFunc("POST /api/rollout/resume", s.handleRolloutResume)
	mux.HandleFunc("POST /api/rollout/advance", s.handleRolloutAdvance)
	mux.HandleFunc("POST /api/rollout/ramp", s.handleRolloutRamp)
	mux.HandleFunc("POST /api/rollout/abort", s.handleRolloutAbort)

	// Manual routing, for driving versions without the coordinator.
	mux.HandleFunc("POST /api/versions/ramp", s.handleVersionRamp)
	mux.HandleFunc("POST /api/versions/dump", s.handleVersionDump)
	mux.HandleFunc("POST /api/versions/promote", s.handleVersionPromote)
	mux.HandleFunc("POST /api/versions/rollback", s.handleVersionRollback)

	// Recovery for orders stranded on a bad version.
	mux.HandleFunc("POST /api/orders/recover", s.handleRecover)

	return s.withCORS(mux)
}

// store replaces the cached snapshot.
func (s *Server) store(snapshot *Snapshot) {
	s.mu.Lock()
	s.snapshot = snapshot
	s.mu.Unlock()
}

// latest returns the cached snapshot.
func (s *Server) latest() *Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.snapshot
}

// handleState serves the current snapshot, for clients that would rather poll
// than hold an SSE stream open.
func (s *Server) handleState(w http.ResponseWriter, r *http.Request) {
	s.noteInterest()

	snapshot := s.latest()
	if snapshot == nil {
		snapshot = s.build(r.Context())
	}
	writeJSON(w, http.StatusOK, snapshot)
}

// withCORS allows the frontend to be served from a different origin during
// local development. In the containerised setup nginx proxies the API, so
// requests are same-origin and this does nothing.
func (s *Server) withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", s.allowedOrigin)
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// writeJSON writes a JSON response.
func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	if body != nil {
		_ = json.NewEncoder(w).Encode(body)
	}
}

// errorResponse is the shape of every failure, so the frontend can show the
// reason rather than a generic "something went wrong".
type errorResponse struct {
	Error string `json:"error"`
}

// writeError reports a failure to the caller and logs it.
func (s *Server) writeError(w http.ResponseWriter, status int, err error) {
	s.logger.Warn("request failed", "status", status, "err", err)
	writeJSON(w, status, errorResponse{Error: err.Error()})
}

// decode reads a JSON request body.
//
// An empty body is allowed and leaves the target at its zero value, so
// argument-free actions can be called with no payload at all.
func decode(r *http.Request, target any) error {
	if r.Body == nil || r.ContentLength == 0 {
		return nil
	}
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}

// countFrom is the moment the dashboard's counters start from.
//
// Zero means all time, which is the default: an operator opening the dashboard
// should see everything that has happened, not an empty board.
func (s *Server) countFrom() time.Time {
	s.sinceMu.Lock()
	defer s.sinceMu.Unlock()
	return s.since
}

// resetCounters narrows the counters to orders started from now on, or widens
// them back to all time.
//
// Nothing is deleted. The orders behind the old numbers are still there and
// still queryable; only the question the dashboard asks changes. That matters
// for a demo given twice in a morning — the second one deserves clean numbers
// without destroying the evidence from the first.
func (s *Server) resetCounters(all bool) time.Time {
	s.sinceMu.Lock()
	if all {
		s.since = time.Time{}
	} else {
		s.since = time.Now()
	}
	from := s.since
	s.sinceMu.Unlock()

	// Health is cached for ten seconds, so without this the version cards
	// would keep showing pre-reset numbers for long enough to look broken.
	s.healthMu.Lock()
	s.healthCache, s.healthAt = nil, time.Time{}
	s.healthMu.Unlock()

	return from
}

// interestWindow is how long a one-off request for state counts as somebody
// looking.
//
// Long enough that a script polling every few seconds keeps the control plane
// awake, short enough that it goes quiet soon after the last caller leaves.
const interestWindow = 15 * time.Second

// noteInterest records that a client asked for state.
func (s *Server) noteInterest() {
	s.seenMu.Lock()
	s.lastSeen = time.Now()
	s.seenMu.Unlock()
}

// watched reports whether anybody is looking at the dashboard.
func (s *Server) watched() bool {
	s.seenMu.Lock()
	lastSeen := s.lastSeen
	s.seenMu.Unlock()

	now := looking(s.hub.count(), lastSeen, time.Now())

	// Logged on the change only, because it explains a real behaviour switch:
	// while unwatched the control plane's numbers stop refreshing, and
	// somebody reading the logs should be able to see why.
	s.seenMu.Lock()
	if now != s.wasWatched {
		s.wasWatched = now
		s.seenMu.Unlock()
		if now {
			s.logger.Info("someone is watching; resuming control-plane reads")
		} else {
			s.logger.Info("nobody watching; pausing control-plane reads so its worker can idle")
		}
		return now
	}
	s.seenMu.Unlock()

	return now
}

// looking decides whether the dashboard has an audience.
//
// A streaming client is the real signal — a browser holds its SSE connection
// for as long as the page is open. A plain request for state counts too, for a
// while, so that a script or a health check polling the API does not silently
// read frozen numbers just because it does not stream.
func looking(viewers int, lastSeen, now time.Time) bool {
	if viewers > 0 {
		return true
	}
	if lastSeen.IsZero() {
		return false
	}
	return now.Sub(lastSeen) < interestWindow
}
