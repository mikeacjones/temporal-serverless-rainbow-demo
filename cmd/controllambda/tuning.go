package main

import (
	"go.temporal.io/sdk/worker"

	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/config"
	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/lambdatune"
	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/traffic"
)

// defaultControlActivities is how many control Activities one invocation runs
// at once.
//
// Far higher than the order worker's, because these are not the same shape of
// work. An order step holds a slot while it sleeps; a control Activity is
// almost entirely network — a burst batch is hundreds of
// StartWorkflowExecution round trips — so the slots exist to keep those in
// flight, not to reserve CPU.
const defaultControlActivities = 100

// tuneControlWorker sizes the control plane's Lambda worker.
func tuneControlWorker(opts *worker.Options) {
	lambdatune.AutoscalePollers(opts, config.EnvInt("CONTROL_MAX_POLLERS", 0))

	opts.MaxConcurrentActivityExecutionSize =
		config.EnvInt("CONTROL_MAX_CONCURRENT_ACTIVITIES", defaultControlActivities)

	// One invocation must be able to hold a whole burst batch, or a batch
	// would be split across invocations by slot pressure rather than by the
	// batching that was asked for.
	if opts.MaxConcurrentActivityExecutionSize < traffic.DefaultConcurrency {
		opts.MaxConcurrentActivityExecutionSize = traffic.DefaultConcurrency
	}
}
