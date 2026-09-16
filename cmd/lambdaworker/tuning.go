package main

import (
	"go.temporal.io/sdk/worker"

	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/config"
)

// unset distinguishes "the environment said nothing, so keep the SDK's own
// default" from a deliberate 0, which the SDK reads as a real value.
const unset = -1

// defaultMaxPollers caps poller autoscaling. Each poller holds one long poll
// open; a handful per invocation is enough to keep the slots fed, because a
// poller does not wait for the task it fetched — it hands the task to a
// separate goroutine and immediately polls again.
const defaultMaxPollers = 10

// tuneWorker lets the number of pollers follow demand instead of being pinned.
//
// lambdaworker pins activity pollers to 1 and workflow-task pollers to 2. Those
// are defaults, not policy: applyLambdaWorkerDefaults only fills zero-valued
// fields, and it runs *before* the configure callback this is called from, so
// whatever is set here wins.
//
// Undoing them matters for more than the counts. The SDK auto-enrols a worker
// into poller autoscaling only when the poller field is left at zero — so by
// setting it to 1, lambdaworker quietly opts the worker out of autoscaling
// altogether. Setting the behaviour explicitly puts it back without depending
// on whether the namespace advertises auto-enrolment.
func tuneWorker(opts *worker.Options) {
	// Clear the counts before setting a behaviour. The two are mutually
	// exclusive and the SDK *panics* — "cannot set both
	// MaxConcurrentActivityTaskPollers and ActivityTaskPollerBehavior" — which
	// on Lambda means every cold start fails. lambdaworker has already set
	// both counts by the time this runs, so clearing them is required, not
	// defensive.
	opts.MaxConcurrentActivityTaskPollers = 0
	opts.MaxConcurrentWorkflowTaskPollers = 0

	maxPollers := config.EnvInt("WORKER_MAX_POLLERS", defaultMaxPollers)
	opts.ActivityTaskPollerBehavior = autoscalingPollers(maxPollers)
	opts.WorkflowTaskPollerBehavior = autoscalingPollers(maxPollers)

	// Slots are left to lambdaworker unless the environment says otherwise.
	// Passing a fallback here instead would always be a non-zero override, and
	// would silently replace the SDK's Lambda-tuned values with ours.
	if n := config.EnvInt("WORKER_MAX_CONCURRENT_ACTIVITIES", unset); n != unset {
		opts.MaxConcurrentActivityExecutionSize = n
	}
	if n := config.EnvInt("WORKER_MAX_CONCURRENT_WORKFLOW_TASKS", unset); n != unset {
		opts.MaxConcurrentWorkflowTaskExecutionSize = n
	}
}

// autoscalingPollers builds a fresh behaviour per poller type, rather than
// sharing one, so the two cannot end up coupled through it.
func autoscalingPollers(max int) worker.PollerBehavior {
	return worker.NewPollerBehaviorAutoscaling(worker.PollerBehaviorAutoscalingOptions{
		InitialNumberOfPollers: 2,
		MinimumNumberOfPollers: 1,
		MaximumNumberOfPollers: max,
	})
}
