package traffic

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
)

// burstBatchTimeout bounds one batch. Generous: the batch paces nothing, so
// this only has to cover the round trips for its own orders.
const burstBatchTimeout = 3 * time.Minute

// Burst starts a one-off batch of orders as Standalone Activities.
//
// A burst is a job, not an orchestration. There is nothing to coordinate
// between batches, nothing to decide afterwards, and no state worth keeping —
// which is exactly what a Standalone Activity is for, and what the generator
// Workflow was a poor fit for.
//
// Routing it through that Workflow cost us twice. Every batch landed in its
// history, and the batches competed with the Workflow's own task processing:
// ten pending StartOrders Activities starved its getState Query, so the
// dashboard's traffic panel froze mid-burst and reported a stale sequence
// number while orders were being started perfectly well. Neither problem
// exists once the burst is started straight from the client.
//
// The Activity is the same one the generator uses for steady traffic, with the
// same registration. Only the execution path differs.
type Burst struct {
	c         client.Client
	taskQueue string
	logger    *slog.Logger
}

// NewBurst builds a burst starter against the control task queue.
func NewBurst(c client.Client, taskQueue string, logger *slog.Logger) *Burst {
	return &Burst{c: c, taskQueue: taskQueue, logger: logger}
}

// BurstRequest is one operator request for a burst of orders.
type BurstRequest struct {
	Count int
	// Version pins every order in the burst to one version, bypassing
	// deployment routing. Empty lets routing decide.
	Version string
	// Chaos is the live fault config, so a burst is subject to whatever fault
	// is currently aimed.
	Chaos ChaosConfig
}

// BurstResult reports what was queued, not what has run.
type BurstResult struct {
	Count     int      `json:"count"`
	Batches   int      `json:"batches"`
	Version   string   `json:"version,omitempty"`
	Queued    []string `json:"queued"`
	TaskQueue string   `json:"taskQueue"`
}

// Start queues a burst and returns as soon as it is durably enqueued.
//
// Deliberately fire and forget: the Activities are not awaited. The server has
// the work the moment each start returns, so waiting would only hold an HTTP
// request open while orders it has already guaranteed are running.
func (b *Burst) Start(ctx context.Context, req BurstRequest) (BurstResult, error) {
	if req.Count < 1 || req.Count > MaxBurst {
		return BurstResult{}, fmt.Errorf("a burst must be between 1 and %d orders, got %d",
			MaxBurst, req.Count)
	}

	// One prefix per burst, so two bursts fired in the same millisecond cannot
	// allocate the same order IDs. It also makes a burst's orders findable on
	// their own: WorkflowId STARTS_WITH the prefix.
	prefix := fmt.Sprintf("burst-%d", time.Now().UnixMilli())

	batches := burstBatches(prefix, req)
	result := BurstResult{Count: req.Count, Version: req.Version, TaskQueue: b.taskQueue}

	for _, batch := range batches {
		id := batchID(prefix, len(result.Queued)+1)

		// Not awaited. The work is durable once this returns, so waiting would
		// only hold the operator's request open on orders already guaranteed.
		_, err := b.c.ExecuteActivity(ctx, client.StartActivityOptions{
			ID:                  id,
			TaskQueue:           b.taskQueue,
			StartToCloseTimeout: burstBatchTimeout,
			HeartbeatTimeout:    30 * time.Second,
			RetryPolicy: &temporal.RetryPolicy{
				InitialInterval:    time.Second,
				BackoffCoefficient: 2,
				MaximumInterval:    30 * time.Second,
				// A retry re-attempts the same order IDs, so orders already
				// started are rejected as duplicates and only the ones that
				// never made it go out. That makes a retry safe, which is what
				// lets the server own the job.
				MaximumAttempts: 3,
			},
			Summary: fmt.Sprintf("burst of %d orders", batch.Count),
		}, ActivityStartOrders, batch)
		if err != nil {
			// Batches already queued stay queued: they are the server's now,
			// and cancelling them would lose orders that are on their way.
			return result, fmt.Errorf("queue burst batch %s (%d of %d queued): %w",
				id, len(result.Queued), len(batches), err)
		}

		result.Batches++
		result.Queued = append(result.Queued, id)
	}

	b.logger.Info("burst queued as standalone activities",
		"count", req.Count, "batches", result.Batches, "version", req.Version, "prefix", prefix)
	return result, nil
}

// batchID names one batch's Activity.
func batchID(prefix string, n int) string {
	return fmt.Sprintf("%s-batch-%d", prefix, n)
}

// burstBatches splits a burst into the batches that will be queued.
//
// Separate and pure because this is where a burst can quietly lose or double
// orders: every batch has to carry a distinct, contiguous slice of the
// sequence, and every one has to carry the same prefix, or two batches write
// the same order IDs and the duplicates are rejected as already started.
func burstBatches(prefix string, req BurstRequest) []StartOrdersRequest {
	// Pinning every order to one version is a split of 100% to it, which
	// reuses the same path a multi-version split takes.
	split := Split(nil)
	if req.Version != "" {
		split = Split{{Version: req.Version, Pct: 100}}
	}

	var out []StartOrdersRequest
	seq := 1
	for remaining := req.Count; remaining > 0; {
		chunk := min(remaining, ordersPerActivity)
		out = append(out, StartOrdersRequest{
			Count:    chunk,
			FirstSeq: seq,
			IDPrefix: prefix,
			Chaos:    req.Chaos,
			Split:    split,
			// No spreading: a burst is meant to arrive all at once.
			SpreadOver: 0,
		})
		seq += chunk
		remaining -= chunk
	}
	return out
}
