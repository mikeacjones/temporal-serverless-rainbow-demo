package metrics

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Sync match rate is the fraction of tasks handed straight to a worker that
// was already waiting, rather than being written to the backlog first. It is
// the signal Temporal scales serverless workers on.
//
// It is a real metric, but it is not in the task-queue API — it comes from the
// server, and the server reports it in one of two shapes depending on where
// Temporal is running:
//
// Self-hosted (including the local dev server) exposes raw Prometheus text on
// the server's own metrics port, as *cumulative counters*:
//
//	poll_success       every task successfully delivered to a poller
//	poll_success_sync  those that were delivered by a direct match
//
// Newer servers report the same things under a pri_ prefix, from the priority
// matcher, so both families are summed.
//
// Temporal Cloud exposes its OpenMetrics endpoint instead, authenticated with
// an API key from a Metrics-Read-Only service account, as *pre-computed
// per-second rates*:
//
//	temporal_cloud_v1_poll_success_count
//	temporal_cloud_v1_poll_success_sync_count
//
// The distinction matters: counters need a delta between two readings to
// become a rate, whereas the Cloud values are already rates and can simply be
// divided. Both are Prometheus text, so one parser reads either.
const (
	metricPollSuccess     = "poll_success"
	metricPollSuccessSync = "poll_success_sync"
	metricPriPollSuccess  = "pri_poll_success"
	metricPriPollSyncSync = "pri_poll_success_sync"

	metricCloudPollSuccess     = "temporal_cloud_v1_poll_success_count"
	metricCloudPollSuccessSync = "temporal_cloud_v1_poll_success_sync_count"
)

// defaultTTL is how long a reading is reused.
//
// The Cloud endpoint returns every task queue in the account — several
// megabytes — so scraping it on the dashboard's one-second poll would be
// wasteful and slow. The rate it reports is smoothed anyway, so a slightly
// stale value costs nothing.
const defaultTTL = 15 * time.Second

// SyncMatch is the sync match rate over the interval between the last two reads.
type SyncMatch struct {
	// RatePct is the percentage of tasks matched directly to a waiting worker.
	// Negative means not yet known: either metrics are unavailable, or no
	// tasks have been delivered since the previous reading.
	RatePct float64 `json:"ratePct"`
	// Delivered is how many tasks were handed out in the interval, so the UI
	// can tell "100% of a lot" from "100% of three".
	Delivered int64 `json:"delivered"`
	// Available reports whether the metrics endpoint could be read at all.
	Available bool `json:"available"`
}

// SyncMatchReader computes a sync match rate from the server's metrics endpoint.
//
// The counters are cumulative, so a rate has to come from the change between
// two readings. Taking it over the whole lifetime would be useless on a demo:
// one early spike would weigh down the figure for the rest of the session.
type SyncMatchReader struct {
	url       string
	taskQueue string
	// apiKey authenticates against Temporal Cloud's endpoint. Empty for a
	// self-hosted server, which does not authenticate its metrics port.
	apiKey string
	// namespace narrows Cloud readings, whose endpoint covers every namespace
	// in the account. Empty for self-hosted, where the port is already
	// namespace-agnostic.
	namespace string
	client    *http.Client

	mu        sync.Mutex
	lastTotal float64
	lastSync  float64
	haveLast  bool
	lastRate  float64
	cached    SyncMatch
	cachedAt  time.Time
}

// SyncMatchOptions configures a reader.
type SyncMatchOptions struct {
	// URL is the metrics endpoint. Empty disables the reader, and the gauge
	// reports itself unavailable rather than showing a made-up number.
	URL string
	// TaskQueue is the queue to report on.
	TaskQueue string
	// APIKey is required by Temporal Cloud and unused by a self-hosted server.
	APIKey string
	// Namespace narrows a Cloud reading to this namespace.
	Namespace string
}

// NewSyncMatchReader builds a reader for one task queue.
func NewSyncMatchReader(opts SyncMatchOptions) *SyncMatchReader {
	return &SyncMatchReader{
		url:       opts.URL,
		taskQueue: opts.TaskQueue,
		apiKey:    opts.APIKey,
		namespace: opts.Namespace,
		// Generous enough for a multi-megabyte Cloud scrape, but bounded: a
		// missing gauge beats a late snapshot.
		client: &http.Client{Timeout: 20 * time.Second},
	}
}

// Read returns the current sync match rate.
func (r *SyncMatchReader) Read(ctx context.Context) SyncMatch {
	if r.url == "" {
		return SyncMatch{RatePct: -1}
	}

	r.mu.Lock()
	if time.Since(r.cachedAt) < defaultTTL && r.cachedAt != (time.Time{}) {
		cached := r.cached
		r.mu.Unlock()
		return cached
	}
	r.mu.Unlock()

	total, sync, rates, err := r.scrape(ctx)
	if err != nil {
		// One failed scrape should not blank a working gauge. Keep showing the
		// last good reading for a few cycles; only give up if it goes stale.
		r.mu.Lock()
		defer r.mu.Unlock()
		if r.cached.Available && time.Since(r.cachedAt) < 4*defaultTTL {
			return r.cached
		}
		return SyncMatch{RatePct: -1}
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	result := r.computeLocked(total, sync, rates)
	r.cached, r.cachedAt = result, time.Now()
	return result
}

// computeLocked turns a reading into a rate. Cloud values are already
// per-second rates and divide directly; self-hosted values are cumulative
// counters and need the change since the previous reading.
func (r *SyncMatchReader) computeLocked(total, sync float64, rates bool) SyncMatch {
	if rates {
		if total <= 0 {
			// Nothing flowing. Holding the last known rate keeps an idle queue
			// from flickering between a number and a dash.
			return SyncMatch{RatePct: r.lastRate, Available: true}
		}
		r.lastRate = sync / total * 100
		// Delivered is per second here, which is the honest sample size for a
		// rate-based reading.
		return SyncMatch{RatePct: r.lastRate, Delivered: int64(total), Available: true}
	}

	previousTotal, previousSync, had := r.lastTotal, r.lastSync, r.haveLast
	r.lastTotal, r.lastSync, r.haveLast = total, sync, true

	// A counter that went backwards means the server restarted; start over
	// rather than reporting a nonsense negative rate.
	if !had || total < previousTotal || sync < previousSync {
		return SyncMatch{RatePct: -1, Available: true}
	}

	delivered := total - previousTotal
	if delivered == 0 {
		return SyncMatch{RatePct: r.lastRate, Available: true}
	}

	r.lastRate = (sync - previousSync) / delivered * 100
	return SyncMatch{RatePct: r.lastRate, Delivered: int64(delivered), Available: true}
}

// scrape sums the poll metrics for this reader's task queue.
//
// rates reports which shape the numbers are in: true when they came from
// Temporal Cloud's already-per-second metrics, false for self-hosted
// cumulative counters.
func (r *SyncMatchReader) scrape(ctx context.Context) (total, sync float64, rates bool, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.url, nil)
	if err != nil {
		return 0, 0, false, err
	}
	if r.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+r.apiKey)
	}

	resp, err := r.client.Do(req)
	if err != nil {
		return 0, 0, false, fmt.Errorf("read server metrics: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, 0, false, fmt.Errorf("read server metrics: %s", resp.Status)
	}

	// Matching on label text avoids pulling in a Prometheus parser for two
	// metrics. Both label spellings are checked because the self-hosted and
	// Cloud endpoints name the task-queue label differently.
	wantQueue := []string{
		`taskqueue="` + r.taskQueue + `"`,
		`temporal_task_queue="` + r.taskQueue + `"`,
	}
	wantNamespace := ""
	if r.namespace != "" {
		wantNamespace = `temporal_namespace="` + r.namespace + `"`
	}

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	for scanner.Scan() {
		line := scanner.Text()
		if line == "" || line[0] == '#' {
			continue
		}
		if !containsAny(line, wantQueue) {
			continue
		}
		// The Cloud endpoint covers the whole account, so without this a busy
		// neighbour's "orders" queue would be folded into ours.
		if wantNamespace != "" && strings.Contains(line, "temporal_cloud_") &&
			!strings.Contains(line, wantNamespace) {
			continue
		}

		name, value, ok := parseSample(line)
		if !ok {
			continue
		}

		switch name {
		case metricPollSuccess, metricPriPollSuccess:
			total += value
		case metricPollSuccessSync, metricPriPollSyncSync:
			sync += value
		case metricCloudPollSuccess:
			total += value
			rates = true
		case metricCloudPollSuccessSync:
			sync += value
			rates = true
		}
	}
	if err := scanner.Err(); err != nil {
		return 0, 0, false, fmt.Errorf("read server metrics: %w", err)
	}

	return total, sync, rates, nil
}

// containsAny reports whether line contains any of the needles.
func containsAny(line string, needles []string) bool {
	for _, needle := range needles {
		if strings.Contains(line, needle) {
			return true
		}
	}
	return false
}

// parseSample pulls the metric name and value out of one Prometheus text line.
//
// The shape is: name{label="x",...} value [timestamp]
//
// That trailing timestamp is optional and is the trap: Temporal Cloud's
// OpenMetrics output includes it, a self-hosted server's does not. Reading the
// last field on the line therefore works locally and silently returns the
// timestamp on Cloud — which, since both counters get the same timestamp,
// produces a confident and completely wrong 100%. Take the first field after
// the labels instead.
func parseSample(line string) (name string, value float64, ok bool) {
	brace := strings.IndexByte(line, '{')
	if brace < 0 {
		return "", 0, false
	}

	closing := strings.LastIndexByte(line, '}')
	if closing < brace {
		return "", 0, false
	}

	fields := strings.Fields(line[closing+1:])
	if len(fields) == 0 {
		return "", 0, false
	}

	v, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return "", 0, false
	}
	return line[:brace], v, true
}
