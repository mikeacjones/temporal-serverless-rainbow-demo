package api

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/config"
	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/deploy"
	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/metrics"
	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/orders"
	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/rollout"
	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/traffic"
)

// defaultOrdersPerVersion is how many live orders each version's column
// samples when ORDERS_PER_VERSION is not set.
//
// Per version, not in total, and that distinction is the whole point. A single
// shared window is ordered newest-first, so one version's burst evicts
// another's orders outright: 250 orders dumped on v3 straight after 250 on v2
// left the sample holding 104 v3 rows and no v2 rows at all, with 500 orders
// running. The v2 column emptied while its work was still in flight.
//
// Comfortably above the column's display cap, so orders leaving the window are
// ones the column was never showing.
const defaultOrdersPerVersion = 40

// historyLength is how many samples the sparklines keep — at a one-second
// poll, about two minutes of history.
const historyLength = 120

// historySample is how often a point is added to the sparklines, independent
// of how often the live numbers are refreshed.
//
// These were the same thing, which made the graphs useless for the event they
// exist to show: at a 1s poll, 120 points is two minutes, and the tail of a
// five-thousand-order spike outlives that easily. The backlog graph would look
// empty minutes after a spike whose peak had simply scrolled off the end.
//
// Sampling the history more slowly than the readouts keeps the payload the
// same size while covering ten minutes instead of two.
var historySample = config.EnvDuration("HISTORY_SAMPLE", 5*time.Second)

// Snapshot is everything the dashboard draws, in one object.
//
// The frontend is deliberately given a complete picture each tick rather than
// deltas: it stays a pure renderer with no state of its own to drift.
type Snapshot struct {
	Now time.Time `json:"now"`

	Deployment deploy.State        `json:"deployment"`
	Totals     metrics.Totals      `json:"totals"`
	Capacity   metrics.Capacity    `json:"capacity"`
	SyncMatch  metrics.SyncMatch   `json:"syncMatch"`
	Orders     []metrics.LiveOrder `json:"orders"`

	// Health is per-version order outcomes, keyed by friendly label.
	Health map[string]metrics.Health `json:"health"`

	// Traffic and Rollout are nil when those Workflows are not running, which
	// is a meaningful state rather than an error.
	Traffic *traffic.State `json:"traffic"`
	Rollout *rollout.State `json:"rollout"`

	// CompletedPerMin is throughput, derived from the change in completed
	// orders between polls.
	CompletedPerMin float64 `json:"completedPerMin"`

	History History `json:"history"`

	// Pipelines tells the UI the shape of each version, so it can draw a
	// rollout as a change in the journey rather than a change in a number.
	Pipelines map[string][]orders.Step `json:"pipelines"`

	Viewers int `json:"viewers"`

	// CountingFrom is when the dashboard's counters were last reset, or the
	// zero time when they cover everything. Sent so the UI can say which it
	// is rather than leaving an operator guessing why a total looks small.
	CountingFrom time.Time `json:"countingFrom"`
}

// History holds the series behind the sparklines.
type History struct {
	Backlog         []int64   `json:"backlog"`
	Workers         []int     `json:"workers"`
	Running         []int64   `json:"running"`
	CompletedPerMin []float64 `json:"completedPerMin"`
	OldestWaitSec   []float64 `json:"oldestWaitSec"`
	SyncMatchPct    []float64 `json:"syncMatchPct"`

	// WorkersByVersion is one running-worker series per version, so each
	// version card can show its own workers arriving and leaving rather than
	// only the fleet total.
	WorkersByVersion map[string][]int `json:"workersByVersion,omitempty"`
}

// pushVersions records each version's current worker count.
//
// Versions that are no longer registered are dropped, so a demo that runs for
// hours does not accumulate series for versions that have been deleted.
func (h *History) pushVersions(versions []deploy.Version) {
	if h.WorkersByVersion == nil {
		h.WorkersByVersion = map[string][]int{}
	}

	registered := make(map[string]bool, len(versions))
	for _, v := range versions {
		registered[v.Label] = true
		h.WorkersByVersion[v.Label] = appendCapped(h.WorkersByVersion[v.Label], v.Workers)
	}
	for label := range h.WorkersByVersion {
		if !registered[label] {
			delete(h.WorkersByVersion, label)
		}
	}
}

// push appends a sample, discarding the oldest once full.
func (h *History) push(c metrics.Capacity, t metrics.Totals, completedPerMin float64, syncMatchPct float64) {
	h.Backlog = appendCapped(h.Backlog, c.BacklogDepth)
	h.Workers = appendCapped(h.Workers, c.Workers)
	h.Running = appendCapped(h.Running, t.Running)
	h.CompletedPerMin = appendCapped(h.CompletedPerMin, completedPerMin)
	h.OldestWaitSec = appendCapped(h.OldestWaitSec, c.OldestWaitSec)
	// Negative means "not known"; carry the previous point so the sparkline
	// does not dive to the floor whenever a reading is missed.
	if syncMatchPct < 0 && len(h.SyncMatchPct) > 0 {
		syncMatchPct = h.SyncMatchPct[len(h.SyncMatchPct)-1]
	}
	h.SyncMatchPct = appendCapped(h.SyncMatchPct, max(syncMatchPct, 0))
}

func appendCapped[T any](series []T, v T) []T {
	series = append(series, v)
	if len(series) > historyLength {
		series = series[len(series)-historyLength:]
	}
	return series
}

// snapshotBudget bounds one snapshot's worth of reads.
//
// Generous because these reads cross a region: the backend runs in us-west-1
// and the namespace lives in ca-central-1. They are issued concurrently, so
// the budget is per-round-trip rather than per-call.
const snapshotBudget = 10 * time.Second

// healthTTL is how long a per-version health reading is reused.
//
// Health is the most expensive thing the dashboard reads: two visibility
// counts per version, and visibility counts are the slow calls — around 380ms
// each against the cloud namespace, where a task-queue describe is 74ms. At
// five versions that is ten of them, and the whole snapshot is built
// synchronously, so the poll interval is really "however long the slowest read
// took". Measured cadences of 5s and 10s came almost entirely from here.
//
// Ten seconds is not a compromise: the rollout coordinator evaluates health on
// its own 10s interval, so a fresher reading was never being acted on. The
// numbers that need to be live — backlog, workers, orders in flight — are
// cheap and stay on every cycle.
const healthTTL = 10 * time.Second

// poll rebuilds the snapshot on a timer and pushes it to every browser.
func (s *Server) poll(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			snapshot := s.build(ctx)
			s.store(snapshot)
			s.reconcileFaults(ctx, snapshot)

			payload, err := json.Marshal(snapshot)
			if err != nil {
				s.logger.Warn("cannot encode snapshot", "err", err)
				continue
			}
			s.hub.broadcast(payload)
		}
	}
}

// build gathers one snapshot.
//
// Every read is issued concurrently and every read is best-effort. Where a
// read fails, the previous snapshot's value is carried forward rather than
// zeroed: a dashboard that blanks a panel because one query timed out is worse
// than one showing a value a second old. That is not cosmetic — a failed Query
// against the rollout Workflow is a *read* failure, and rendering it as "no
// deployment running" in the middle of a rollout is actively misleading.
func (s *Server) build(ctx context.Context) *Snapshot {
	ctx, cancel := context.WithTimeout(ctx, snapshotBudget)
	defer cancel()

	previous := s.latest()

	snapshot := &Snapshot{
		Now:    time.Now(),
		Health: map[string]metrics.Health{},
		// Never nil: the frontend should be able to iterate every collection
		// without null checks, even on a failed read.
		Orders:    []metrics.LiveOrder{},
		Pipelines: pipelines(),
		Viewers:   s.hub.count(),
	}

	var wg sync.WaitGroup
	run := func(f func()) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f()
		}()
	}

	run(func() {
		state, err := s.deployment.Describe(ctx)
		if err != nil {
			s.logger.Debug("describe deployment failed", "err", err)
			if previous != nil {
				snapshot.Deployment = previous.Deployment
			}
			return
		}
		snapshot.Deployment = state
	})

	run(func() {
		totals, err := s.reader.Totals(ctx, s.countFrom())
		if err != nil {
			s.logger.Debug("order totals failed", "err", err)
			if previous != nil {
				snapshot.Totals = previous.Totals
			}
			return
		}
		snapshot.Totals = totals
		snapshot.CompletedPerMin = s.throughput.observe(totals.Completed, snapshot.Now)
	})

	run(func() {
		capacity, err := s.reader.Capacity(ctx, orders.TaskQueue)
		if err != nil {
			s.logger.Debug("task queue stats failed", "err", err)
			if previous != nil {
				snapshot.Capacity = previous.Capacity
			}
			return
		}

		// Running workers come from a separate call, because the task queue
		// response cannot answer it: PollerInfo carries no status, so a
		// finished serverless invocation is indistinguishable from a live one
		// there.
		workers, err := s.reader.RunningWorkers(ctx, orders.TaskQueue)
		if err != nil {
			// A server without worker heartbeats cannot report status. Falling
			// back to pollers overstates the fleet while things shut down, but
			// showing nothing would be worse — and on a long-lived worker,
			// where nothing is constantly shutting down, the two agree.
			s.logger.Debug("running workers unavailable, falling back to pollers", "err", err)
			capacity.Workers = capacity.Pollers
			capacity.WorkersByBuild = capacity.PollersByBuild
		} else {
			capacity.Workers = workers.Total
			capacity.WorkersByBuild = workers.ByBuild
		}

		snapshot.Capacity = capacity
	})

	run(func() {
		live, err := s.reader.RecentOrders(ctx, orders.AllVersionLabels(), s.orderSample)
		if err != nil {
			s.logger.Debug("recent orders failed", "err", err)
			if previous != nil {
				snapshot.Orders = previous.Orders
			}
			return
		}
		snapshot.Orders = live
	})

	run(func() { snapshot.SyncMatch = s.syncMatch.Read(ctx) })

	run(func() {
		if state := s.traffic.state(ctx); state != nil {
			snapshot.Traffic = state
		} else if previous != nil {
			snapshot.Traffic = previous.Traffic
		}
	})

	run(func() {
		if state := s.rollouts.state(ctx); state != nil {
			snapshot.Rollout = state
		} else if previous != nil {
			// A Query that failed does not mean the rollout went away.
			snapshot.Rollout = previous.Rollout
		}
	})

	wg.Wait()

	// Per-version health needs the version list, so it follows the rest — but
	// the versions are read together, and behind a short cache.
	snapshot.Health = s.versionHealth(ctx, snapshot.Deployment.Versions, previous)

	attachWorkers(snapshot)
	applySplit(snapshot)

	// Only the poll goroutine reaches here, so the timestamp needs no lock.
	if snapshot.Now.Sub(s.lastHistory) >= historySample {
		s.lastHistory = snapshot.Now
		s.history.push(snapshot.Capacity, snapshot.Totals, snapshot.CompletedPerMin, snapshot.SyncMatch.RatePct)
		// After attachWorkers, so each version's count is the one just read.
		s.history.pushVersions(snapshot.Deployment.Versions)
	}
	snapshot.History = *s.history
	snapshot.CountingFrom = s.countFrom()

	return snapshot
}

// versionHealth reads every version's order outcomes at once, reusing readings
// that are only a few seconds old.
func (s *Server) versionHealth(
	ctx context.Context,
	versions []deploy.Version,
	previous *Snapshot,
) map[string]metrics.Health {
	out := make(map[string]metrics.Health, len(versions))
	since := s.countFrom()

	s.healthMu.Lock()
	fresh := time.Since(s.healthAt) < healthTTL
	if fresh {
		for label, health := range s.healthCache {
			out[label] = health
		}
	}
	s.healthMu.Unlock()

	if fresh && len(out) > 0 {
		return out
	}

	var (
		wg sync.WaitGroup
		mu sync.Mutex
	)
	for _, v := range versions {
		wg.Add(1)
		go func() {
			defer wg.Done()
			health, err := s.reader.VersionHealth(ctx, v.BuildID, since)
			if err != nil {
				s.logger.Debug("version health failed", "buildId", v.BuildID, "err", err)
				if previous != nil {
					if last, ok := previous.Health[v.Label]; ok {
						mu.Lock()
						out[v.Label] = last
						mu.Unlock()
					}
				}
				return
			}
			mu.Lock()
			out[v.Label] = health
			mu.Unlock()
		}()
	}
	wg.Wait()

	s.healthMu.Lock()
	s.healthCache, s.healthAt = out, time.Now()
	s.healthMu.Unlock()

	return out
}

// attachWorkers tells each version card how many workers it currently has.
//
// This is the serverless story made per-version: dump orders onto an idle
// version and its count climbs from zero while the others stay put.
func attachWorkers(snapshot *Snapshot) {
	byBuild := snapshot.Capacity.WorkersByBuild
	if byBuild == nil {
		return
	}
	for i, v := range snapshot.Deployment.Versions {
		snapshot.Deployment.Versions[i].Workers = byBuild[v.BuildID]
	}
}

// applySplit rewrites the version cards' traffic shares when a split is active.
//
// Without this the cards would keep reporting the deployment's Current and
// Ramping percentages, which a split has taken out of the decision entirely —
// so the dashboard would show 100% going to one version while orders were
// actually being spread across three.
func applySplit(snapshot *Snapshot) {
	if snapshot.Traffic == nil || !snapshot.Traffic.Split.Active() {
		return
	}

	shares := make(map[string]float64, len(snapshot.Traffic.Split))
	total := snapshot.Traffic.Split.Total()
	for _, e := range snapshot.Traffic.Split {
		if total > 0 {
			shares[e.Version] = e.Pct / total * 100
		}
	}

	for i, v := range snapshot.Deployment.Versions {
		snapshot.Deployment.Versions[i].TrafficPct = float32(shares[v.Label])
	}
}

// pipelines exposes each version's step list to the UI.
func pipelines() map[string][]orders.Step {
	out := make(map[string][]orders.Step, len(orders.AllVersions))
	for _, v := range orders.AllVersions {
		out[string(v)] = orders.StepsFor(v)
	}
	return out
}

// throughputMeter converts a rising total into a per-minute rate.
type throughputMeter struct {
	lastCount int64
	lastAt    time.Time
	lastRate  float64
}

// observe records a new cumulative count and returns the current rate.
//
// A drop in the total means the demo was reset, so the meter restarts rather
// than reporting a negative rate.
func (m *throughputMeter) observe(completed int64, at time.Time) float64 {
	defer func() {
		m.lastCount = completed
		m.lastAt = at
	}()

	if m.lastAt.IsZero() || completed < m.lastCount {
		return 0
	}

	elapsed := at.Sub(m.lastAt).Seconds()
	if elapsed <= 0 {
		return m.lastRate
	}

	// Smoothed a little, so the number on screen is readable rather than
	// jittering with every poll.
	rate := float64(completed-m.lastCount) / elapsed * 60
	m.lastRate = m.lastRate*0.5 + rate*0.5
	return m.lastRate
}
