// Package lambdatune adjusts a Temporal worker for running inside AWS Lambda.
//
// Shared by both Lambda entrypoints because the one thing in here that matters
// is easy to get wrong in a way that only shows up at cold start, in
// production, on every invocation.
package lambdatune

import "go.temporal.io/sdk/worker"

// DefaultMaxPollers caps poller autoscaling.
//
// Each poller holds one long poll open. A handful per invocation is enough to
// keep the slots fed, because a poller does not wait for the task it fetched —
// it hands the task to a separate goroutine and immediately polls again.
const DefaultMaxPollers = 10

// AutoscalePollers lets the number of pollers follow demand instead of being
// pinned.
//
// lambdaworker pins activity pollers to 1 and workflow-task pollers to 2.
// Those are defaults, not policy: applyLambdaWorkerDefaults only fills
// zero-valued fields, and it runs before the configure callback this is called
// from, so whatever is set here wins.
//
// Undoing them matters for more than the counts. The SDK auto-enrols a worker
// into poller autoscaling only when the poller field is left at zero — so by
// setting it to 1, lambdaworker quietly opts the worker out of autoscaling
// altogether. Setting the behaviour explicitly puts it back without depending
// on whether the namespace advertises auto-enrolment.
func AutoscalePollers(opts *worker.Options, maxPollers int) {
	if maxPollers <= 0 {
		maxPollers = DefaultMaxPollers
	}

	// Clear the counts before setting a behaviour. The two are mutually
	// exclusive and the SDK *panics* — "cannot set both
	// MaxConcurrentActivityTaskPollers and ActivityTaskPollerBehavior" —
	// which on Lambda means every cold start fails. lambdaworker has already
	// set both counts by the time this runs, so clearing them is required,
	// not defensive.
	opts.MaxConcurrentActivityTaskPollers = 0
	opts.MaxConcurrentWorkflowTaskPollers = 0

	opts.ActivityTaskPollerBehavior = behavior(maxPollers)
	opts.WorkflowTaskPollerBehavior = behavior(maxPollers)
}

// behavior builds a fresh behaviour per poller type, rather than sharing one,
// so the two cannot end up coupled through it.
func behavior(max int) worker.PollerBehavior {
	return worker.NewPollerBehaviorAutoscaling(worker.PollerBehaviorAutoscalingOptions{
		InitialNumberOfPollers: 2,
		MinimumNumberOfPollers: 1,
		MaximumNumberOfPollers: max,
	})
}
