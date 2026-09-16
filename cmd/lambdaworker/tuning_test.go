package main

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
func TestTunedWorkerConstructsWithoutPanicking(t *testing.T) {
	opts := lambdaDefaults()
	tuneWorker(&opts)

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
	opts.ActivityTaskPollerBehavior = autoscalingPollers(10)

	defer func() {
		if recover() == nil {
			t.Fatal("expected a panic from setting both the count and the behaviour")
		}
	}()

	worker.New(lazyClient(t), "orders", opts)
}

// Slots are the SDK's unless the environment overrides them. Defaulting these
// ourselves is how the workflow-task slots ended up at 5, below lambdaworker's
// own 10.
func TestSlotsKeepTheSdkDefaultsWhenUnset(t *testing.T) {
	opts := lambdaDefaults()
	before := opts.MaxConcurrentWorkflowTaskExecutionSize
	tuneWorker(&opts)

	if opts.MaxConcurrentWorkflowTaskExecutionSize != before {
		t.Errorf("workflow task slots = %d, want the untouched default %d",
			opts.MaxConcurrentWorkflowTaskExecutionSize, before)
	}
	if opts.MaxConcurrentActivityExecutionSize != 2 {
		t.Errorf("activity slots = %d, want the untouched default 2",
			opts.MaxConcurrentActivityExecutionSize)
	}
}

func TestSlotsFollowTheEnvironmentWhenSet(t *testing.T) {
	t.Setenv("WORKER_MAX_CONCURRENT_ACTIVITIES", "20")
	t.Setenv("WORKER_MAX_CONCURRENT_WORKFLOW_TASKS", "12")

	opts := lambdaDefaults()
	tuneWorker(&opts)

	if opts.MaxConcurrentActivityExecutionSize != 20 {
		t.Errorf("activity slots = %d, want 20", opts.MaxConcurrentActivityExecutionSize)
	}
	if opts.MaxConcurrentWorkflowTaskExecutionSize != 12 {
		t.Errorf("workflow task slots = %d, want 12", opts.MaxConcurrentWorkflowTaskExecutionSize)
	}
}

// The counts must be cleared, not merely overwritten, or the mutual-exclusivity
// check fires.
func TestTuningClearsTheFixedPollerCounts(t *testing.T) {
	opts := lambdaDefaults()
	tuneWorker(&opts)

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
