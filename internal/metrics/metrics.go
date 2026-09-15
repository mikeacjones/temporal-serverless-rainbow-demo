// Package metrics reads what is happening right now, in bulk.
//
// Everything here is a read: visibility counts, task-queue stats and a sampled
// list of recent orders. It is shared by the dashboard (which shows it) and the
// rollout coordinator (which makes decisions on it), so both are always looking
// at the same numbers.
package metrics

import (
	"context"
	"fmt"
	"sync"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"

	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/orders"
)

// Reader answers questions about live orders and worker capacity.
type Reader struct {
	c              client.Client
	namespace      string
	deploymentName string
}

// NewReader builds a Reader for one namespace and Worker Deployment.
func NewReader(c client.Client, namespace, deploymentName string) *Reader {
	return &Reader{c: c, namespace: namespace, deploymentName: deploymentName}
}

// Health is one version's order outcomes over a window.
type Health struct {
	BuildID   string `json:"buildId"`
	Running   int64  `json:"running"`
	Completed int64  `json:"completed"`
	Failed    int64  `json:"failed"`
	// Degraded counts orders that are still Running but stuck retrying a step.
	// This is the signal that matters most here: an order whose step fails
	// forever never reaches a Failed status, so without it a broken version
	// looks merely slow.
	Degraded int64 `json:"degraded"`
	// Samples is the number of orders that have reached a verdict —
	// completed, failed, or visibly stuck.
	Samples int64 `json:"samples"`
	// ErrorRatePct is bad outcomes as a percentage of Samples.
	ErrorRatePct float64 `json:"errorRatePct"`
}

// VersionHealth counts order outcomes for one Build ID since a point in time.
//
// The window matters: a rollout should judge a candidate on the orders it took
// during *this* rollout, not on history from an earlier attempt.
func (r *Reader) VersionHealth(ctx context.Context, buildID string, since time.Time) (Health, error) {
	h := Health{BuildID: buildID}

	scope := fmt.Sprintf(`WorkflowType = %q AND TemporalWorkerDeploymentVersion = %q`,
		orders.WorkflowTypeName, r.versionValue(buildID))
	if !since.IsZero() {
		scope += fmt.Sprintf(` AND StartTime > %q`, since.UTC().Format(time.RFC3339))
	}

	// Four counts, run together rather than one after another. Sequentially
	// this is four cross-region round trips per version, and with five
	// versions that was twenty round trips on every dashboard poll — enough
	// to starve whatever ran last.
	queries := []struct {
		filter string
		into   *int64
	}{
		{`ExecutionStatus = "Running"`, &h.Running},
		{`ExecutionStatus = "Completed"`, &h.Completed},
		{`ExecutionStatus = "Failed"`, &h.Failed},
		{fmt.Sprintf(`ExecutionStatus = "Running" AND OrderHealth = %q`, orders.HealthDegraded), &h.Degraded},
	}

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		firstErr error
	)
	for _, q := range queries {
		wg.Add(1)
		go func() {
			defer wg.Done()
			n, err := r.count(ctx, scope+" AND "+q.filter)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
				return
			}
			*q.into = n
		}()
	}
	wg.Wait()

	if firstErr != nil {
		return Health{}, firstErr
	}

	h.Samples = h.Completed + h.Failed + h.Degraded
	if h.Samples > 0 {
		h.ErrorRatePct = float64(h.Failed+h.Degraded) / float64(h.Samples) * 100
	}
	return h, nil
}

// versionValue builds the value of the TemporalWorkerDeploymentVersion search
// attribute.
//
// Note the colon. Temporal's own proto comments describe this value as
// "<deployment_name>.<build_id>", and the versioningInfo field on a Workflow
// does use a dot — but the search attribute uses a colon, and querying the dot
// form silently matches nothing at all.
func (r *Reader) versionValue(buildID string) string {
	return r.deploymentName + ":" + buildID
}

// Totals is the whole order population, by outcome.
type Totals struct {
	Running   int64 `json:"running"`
	Completed int64 `json:"completed"`
	Failed    int64 `json:"failed"`
	Degraded  int64 `json:"degraded"`
}

// Totals counts every order by status, plus how many are stuck.
//
// The status breakdown is a single grouped count rather than one query per
// status, because this runs on the dashboard's poll loop.
func (r *Reader) Totals(ctx context.Context) (Totals, error) {
	var t Totals

	resp, err := r.c.CountWorkflow(ctx, &workflowservice.CountWorkflowExecutionsRequest{
		Namespace: r.namespace,
		Query:     fmt.Sprintf(`WorkflowType = %q GROUP BY ExecutionStatus`, orders.WorkflowTypeName),
	})
	if err != nil {
		return Totals{}, fmt.Errorf("count orders by status: %w", err)
	}
	for _, g := range resp.GetGroups() {
		switch groupValue(g) {
		case "Running":
			t.Running = g.GetCount()
		case "Completed":
			t.Completed = g.GetCount()
		case "Failed":
			t.Failed = g.GetCount()
		}
	}

	degraded, err := r.count(ctx, fmt.Sprintf(
		`WorkflowType = %q AND ExecutionStatus = "Running" AND OrderHealth = %q`,
		orders.WorkflowTypeName, orders.HealthDegraded))
	if err != nil {
		return Totals{}, err
	}
	t.Degraded = degraded

	return t, nil
}

// count runs one visibility count query.
func (r *Reader) count(ctx context.Context, query string) (int64, error) {
	resp, err := r.c.CountWorkflow(ctx, &workflowservice.CountWorkflowExecutionsRequest{
		Namespace: r.namespace,
		Query:     query,
	})
	if err != nil {
		return 0, fmt.Errorf("count %q: %w", query, err)
	}
	return resp.GetCount(), nil
}

// Capacity is worker headroom for one task queue: is work being picked up as
// fast as it arrives, and how many workers are listening?
type Capacity struct {
	BacklogDepth int64 `json:"backlogDepth"`

	// OldestWaitSec is how long the oldest queued task has been waiting.
	//
	// This is the honest answer to "are orders being handed straight to a
	// worker?" — zero means yes, anything else is the delay a customer is
	// actually experiencing.
	//
	// It replaces a derived "sync match rate" that was quietly wrong. That
	// figure was computed from whether the backlog was growing:
	//
	//	1 - (addRate - dispatchRate) / addRate
	//
	// which reads 100% whenever the backlog is *shrinking* — so a queue of
	// hundreds of waiting tasks, draining steadily, displayed as "100% handed
	// straight to a worker". It measured the backlog's direction and called it
	// its absence. Temporal exposes no true sync-match counter, but it does
	// expose the wait itself, which is the thing worth showing anyway.
	OldestWaitSec float64 `json:"oldestWaitSec"`

	// AddedPerSec and DispatchedPerSec are the honest form of the trend the
	// old metric was reaching for: work arriving versus work being picked up.
	AddedPerSec      float64 `json:"addedPerSec"`
	DispatchedPerSec float64 `json:"dispatchedPerSec"`

	// Pollers counts live activity-task pollers. With serverless workers each
	// invocation is one poller, so this is a real-time concurrency read —
	// seconds of lag, versus minutes for CloudWatch.
	Pollers int `json:"pollers"`

	// PollersByBuild breaks that total down by the Build ID each poller
	// reported, so a version sitting at zero workers can be seen coming to
	// life. Every poller carries its own deployment options, so this is a
	// regrouping of a response already being fetched — not extra calls.
	PollersByBuild map[string]int `json:"pollersByBuild,omitempty"`
}

// UnversionedBuild is the bucket for pollers that reported no Build ID.
const UnversionedBuild = "unversioned"

// Capacity reports backlog, how long work is waiting, and poller count.
func (r *Reader) Capacity(ctx context.Context, taskQueue string) (Capacity, error) {
	var out Capacity

	for _, tqType := range []enumspb.TaskQueueType{
		enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		enumspb.TASK_QUEUE_TYPE_ACTIVITY,
	} {
		resp, err := r.c.WorkflowService().DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{
			Namespace:     r.namespace,
			TaskQueue:     &taskqueuepb.TaskQueue{Name: taskQueue},
			TaskQueueType: tqType,
			ReportStats:   true,
		})
		if err != nil {
			return Capacity{}, fmt.Errorf("describe task queue %q: %w", taskQueue, err)
		}

		if s := resp.GetStats(); s != nil {
			out.BacklogDepth += s.GetApproximateBacklogCount()
			out.AddedPerSec += float64(s.GetTasksAddRate())
			out.DispatchedPerSec += float64(s.GetTasksDispatchRate())

			// The worst wait across both queues: one queue keeping up does not
			// make up for the other falling behind.
			if age := s.GetApproximateBacklogAge().AsDuration().Seconds(); age > out.OldestWaitSec {
				out.OldestWaitSec = age
			}
		}

		// Activity pollers are the meaningful capacity signal: that is where
		// the actual work happens.
		if tqType == enumspb.TASK_QUEUE_TYPE_ACTIVITY {
			pollers := resp.GetPollers()
			out.Pollers = len(pollers)
			out.PollersByBuild = make(map[string]int, len(pollers))
			for _, p := range pollers {
				build := p.GetDeploymentOptions().GetBuildId()
				if build == "" {
					// An unversioned worker; count it separately rather than
					// attributing it to a version it never claimed.
					build = UnversionedBuild
				}
				out.PollersByBuild[build]++
			}
		}
	}

	return out, nil
}
