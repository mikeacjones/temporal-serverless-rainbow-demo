package metrics

import (
	"context"
	"fmt"

	deploymentpb "go.temporal.io/api/deployment/v1"
	"go.temporal.io/api/workflowservice/v1"
)

// runningStatus is the value ListWorkers filters Status on. The query language
// takes the SDK's shorthand spelling, not the WORKER_STATUS_RUNNING enum name.
const runningStatus = "Running"

const (
	// workerPage is how many workers to ask for per call.
	workerPage = 1000
	// maxWorkerPages bounds the paging so a very large fleet cannot stall a
	// dashboard refresh. A burst has been measured at ~730 workers, so one
	// page is normally the whole fleet.
	maxWorkerPages = 10
)

// WorkerCount is how many workers are actually running, and on which versions.
type WorkerCount struct {
	Total   int            `json:"total"`
	ByBuild map[string]int `json:"byBuild,omitempty"`
}

// RunningWorkers counts only the workers the server reports as Running.
//
// This is deliberately not a poller count, and the gap between the two is
// large enough to mislead. A serverless worker whose invocation has ended
// reports ShuttingDown and lingers in the server's worker list for minutes,
// and its pollers linger in DescribeTaskQueue for just as long. Measured on
// the cloud environment while completely idle: 4 Running against 13
// ShuttingDown, where the poller count read 17 — so a fleet that was almost
// entirely gone looked four times its real size.
//
// Poller autoscaling widens the gap further: pollers per worker is no longer
// fixed at one, so len(PollerInfo) is not a worker count even when every
// worker is healthy.
//
// The Status filter is applied server-side, so a fleet that is mostly shutting
// down does not have to be fetched in order to be discounted.
func (r *Reader) RunningWorkers(ctx context.Context, taskQueue string) (WorkerCount, error) {
	out := WorkerCount{ByBuild: map[string]int{}}
	query := fmt.Sprintf("TaskQueue = %q AND Status = %q", taskQueue, runningStatus)

	var token []byte
	for range maxWorkerPages {
		resp, err := r.c.WorkflowService().ListWorkers(ctx, &workflowservice.ListWorkersRequest{
			Namespace:     r.namespace,
			PageSize:      workerPage,
			Query:         query,
			NextPageToken: token,
		})
		if err != nil {
			return WorkerCount{}, fmt.Errorf("list workers on %q: %w", taskQueue, err)
		}

		countPage(resp, &out)

		token = resp.GetNextPageToken()
		if len(token) == 0 {
			break
		}
	}

	return out, nil
}

// countPage adds one response page to the running total.
//
// The response can carry the same workers in either of two shapes depending on
// server version, so this reads one of them and never both: summing both would
// silently double the headline number on a server that populates each.
func countPage(resp *workflowservice.ListWorkersResponse, into *WorkerCount) {
	if workers := resp.GetWorkers(); len(workers) > 0 {
		for _, w := range workers {
			into.Total++
			into.ByBuild[buildOf(w.GetDeploymentVersion())]++
		}
		return
	}
	for _, w := range resp.GetWorkersInfo() {
		into.Total++
		into.ByBuild[buildOf(w.GetWorkerHeartbeat().GetDeploymentVersion())]++
	}
}

// buildOf names the version a worker reported, bucketing unversioned workers
// rather than attributing them to a version they never claimed.
func buildOf(v *deploymentpb.WorkerDeploymentVersion) string {
	if b := v.GetBuildId(); b != "" {
		return b
	}
	return UnversionedBuild
}
