package lambdatune

import (
	"testing"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

// lambdaDefaults mirrors the contrib package's applyLambdaWorkerDefaults, which
// runs before our configure callback. The values are copied rather than
// imported because they are unexported; if the contrib package changes them,
// the counts here drift but the invariants under test do not.
func lambdaDefaults() worker.Options {
	return worker.Options{
		MaxConcurrentActivityExecutionSize:      2,
		MaxConcurrentWorkflowTaskExecutionSize:  10,
		MaxConcurrentLocalActivityExecutionSize: 2,
		MaxConcurrentNexusTaskExecutionSize:     5,
		MaxConcurrentActivityTaskPollers:        1,
		MaxConcurrentWorkflowTaskPollers:        2,
		MaxConcurrentNexusTaskPollers:           1,
		DisableEagerActivities:                  true,
	}
}

func lazyClient(t *testing.T) client.Client {
	t.Helper()
	// Lazy: worker option validation does not need a server, and this test
	// must not need one either.
	c, err := client.NewLazyClient(client.Options{})
	if err != nil {
		t.Fatalf("lazy client: %v", err)
	}
	return c
}

// The one that matters. A bad poller config is not a returned error — the SDK
// panics inside worker construction, which on Lambda is a cold start that dies
// on every invocation. So construct the worker for real.
// The one that matters. A bad poller config is not a returned error — the SDK
// panics inside worker construction, which on Lambda is a cold start that dies
// on every invocation. So construct the worker for real.
func TestTunedWorkerConstructsWithoutPanicking(t *testing.T) {
	opts := lambdaDefaults()
	AutoscalePollers(&opts, 0)

	defer func() {
		if p := recover(); p != nil {
			t.Fatalf("worker construction panicked: %v", p)
		}
	}()

	if w := worker.New(lazyClient(t), "orders", opts); w == nil {
		t.Fatal("expected a worker")
	}
}

// Proves the clearing in tuneWorker is load-bearing rather than superstition:
// setting a behaviour on top of lambdaworker's counts is exactly the panic.
func TestBehaviourWithoutClearingTheCountPanics(t *testing.T) {
	opts := lambdaDefaults()
	opts.ActivityTaskPollerBehavior = behavior(10)

	defer func() {
		if recover() == nil {
			t.Fatal("expected a panic from setting both the count and the behaviour")
		}
	}()

	worker.New(lazyClient(t), "orders", opts)
}

// The counts must be cleared, not merely overwritten, or the mutual-exclusivity
// check fires.
func TestTuningClearsTheFixedPollerCounts(t *testing.T) {
	opts := lambdaDefaults()
	AutoscalePollers(&opts, 0)

	if opts.MaxConcurrentActivityTaskPollers != 0 {
		t.Errorf("activity poller count = %d, want 0", opts.MaxConcurrentActivityTaskPollers)
	}
	if opts.MaxConcurrentWorkflowTaskPollers != 0 {
		t.Errorf("workflow poller count = %d, want 0", opts.MaxConcurrentWorkflowTaskPollers)
	}
	if opts.ActivityTaskPollerBehavior == nil || opts.WorkflowTaskPollerBehavior == nil {
		t.Error("both poller behaviours should be set")
	}
}
