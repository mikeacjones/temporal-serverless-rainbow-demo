package main

import (
	"testing"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/traffic"
)

// lambdaDefaults mirrors the contrib package's applyLambdaWorkerDefaults,
// which runs before our configure callback. The values are copied rather than
// imported because they are unexported.
func lambdaDefaults() worker.Options {
	return worker.Options{
		MaxConcurrentActivityExecutionSize:     2,
		MaxConcurrentWorkflowTaskExecutionSize: 10,
		MaxConcurrentActivityTaskPollers:       1,
		MaxConcurrentWorkflowTaskPollers:       2,
		MaxConcurrentNexusTaskPollers:          1,
		DisableEagerActivities:                 true,
	}
}

// A bad poller config is not a returned error — the SDK panics inside worker
// construction, which on Lambda is a cold start that dies on every
// invocation. So construct the worker for real.
func TestControlWorkerConstructsWithoutPanicking(t *testing.T) {
	opts := lambdaDefaults()
	tuneControlWorker(&opts)

	// Lazy: option validation does not need a server, and this test must not
	// need one either.
	c, err := client.NewLazyClient(client.Options{})
	if err != nil {
		t.Fatalf("lazy client: %v", err)
	}

	defer func() {
		if p := recover(); p != nil {
			t.Fatalf("control worker construction panicked: %v", p)
		}
	}()

	if w := worker.New(c, "control", opts); w == nil {
		t.Fatal("expected a worker")
	}
}

// One invocation has to be able to hold a whole burst batch. Below that, a
// batch is paced by slot pressure on the worker rather than by the batching
// that was actually asked for.
func TestControlSlotsHoldAWholeBurstBatch(t *testing.T) {
	t.Setenv("CONTROL_MAX_CONCURRENT_ACTIVITIES", "4")

	opts := lambdaDefaults()
	tuneControlWorker(&opts)

	if opts.MaxConcurrentActivityExecutionSize < traffic.DefaultConcurrency {
		t.Errorf("activity slots = %d, want at least the batch concurrency of %d",
			opts.MaxConcurrentActivityExecutionSize, traffic.DefaultConcurrency)
	}
}

// And the environment can still raise it.
func TestControlSlotsFollowTheEnvironment(t *testing.T) {
	t.Setenv("CONTROL_MAX_CONCURRENT_ACTIVITIES", "250")

	opts := lambdaDefaults()
	tuneControlWorker(&opts)

	if opts.MaxConcurrentActivityExecutionSize != 250 {
		t.Errorf("activity slots = %d, want 250", opts.MaxConcurrentActivityExecutionSize)
	}
}
