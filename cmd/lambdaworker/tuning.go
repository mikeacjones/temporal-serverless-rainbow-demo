package main

import (
	"go.temporal.io/sdk/worker"

	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/config"
	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/lambdatune"
)

// unset distinguishes "the environment said nothing, so keep the SDK's own
// default" from a deliberate 0, which the SDK reads as a real value.
const unset = -1

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
	lambdatune.AutoscalePollers(opts, config.EnvInt("WORKER_MAX_POLLERS", 0))

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
