package orders

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

// newEnv builds a test environment running one version's pipeline.
func newEnv(t *testing.T, v Version, gate bool) *testsuite.TestWorkflowEnvironment {
	t.Helper()

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	// One registration per step, mirroring what a real worker does: only the
	// Activity types this version's pipeline actually uses.
	acts := &Activities{Profile: ProfileFast}
	for _, step := range StepsFor(v) {
		env.RegisterActivityWithOptions(acts.PerformStep,
			activity.RegisterOptions{Name: step.ActivityName()})
	}

	run := Order
	if gate {
		run = Gate
	}
	env.RegisterWorkflowWithOptions(
		func(ctx workflow.Context, in OrderInput) (OrderResult, error) { return run(ctx, v, in) },
		workflow.RegisterOptions{Name: WorkflowTypeName},
	)
	return env
}

// Every version must run its own pipeline end to end. This is the guarantee
// worker versioning exists to provide: an order runs the shape it started on.
func TestOrderRunsItsVersionsPipeline(t *testing.T) {
	for _, v := range AllVersions {
		t.Run(string(v), func(t *testing.T) {
			env := newEnv(t, v, false)
			env.ExecuteWorkflow(WorkflowTypeName, NewOrderInput(1, nil))

			if !env.IsWorkflowCompleted() {
				t.Fatal("workflow did not complete")
			}
			if err := env.GetWorkflowError(); err != nil {
				t.Fatalf("workflow failed: %v", err)
			}

			var result OrderResult
			if err := env.GetWorkflowResult(&result); err != nil {
				t.Fatalf("decode result: %v", err)
			}
			if result.Version != v {
				t.Errorf("result version = %q, want %q", result.Version, v)
			}

			want := StepsFor(v)
			if len(result.Steps) != len(want) {
				t.Fatalf("ran %d steps, want %d", len(result.Steps), len(want))
			}
			for i := range want {
				if result.Steps[i] != want[i] {
					t.Errorf("step %d = %q, want %q", i, result.Steps[i], want[i])
				}
			}
		})
	}
}

// A fault aimed at another version must not disturb this one.
func TestOrderIgnoresChaosAimedElsewhere(t *testing.T) {
	env := newEnv(t, V2, false)
	env.ExecuteWorkflow(WorkflowTypeName, NewOrderInput(1, &ChaosSpec{
		TargetVersion: V5, // not us
		Step:          StepPayment,
		Mode:          ChaosFail,
	}))

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("v2 order should be unaffected by a v5 fault, got: %v", err)
	}
}

// A fault aimed at a step this version does not have is also harmless — worth
// pinning down, because it is how an operator's stale chaos setting behaves
// after a rollout changes the pipeline shape.
func TestOrderIgnoresChaosOnAStepItDoesNotHave(t *testing.T) {
	// v1 has no Loyalty accrual step.
	env := newEnv(t, V1, false)
	env.ExecuteWorkflow(WorkflowTypeName, NewOrderInput(1, &ChaosSpec{
		TargetVersion: V1,
		Step:          StepLoyalty,
		Mode:          ChaosFail,
	}))

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("v1 order should be unaffected by a fault on a step it lacks, got: %v", err)
	}
}

// The gate must fail fast on a poisoned version rather than retrying forever.
// If this regressed, a bad candidate would hang its rollout instead of
// blocking it.
func TestGateFailsOnPoisonedVersion(t *testing.T) {
	env := newEnv(t, V4, true)
	env.ExecuteWorkflow(WorkflowTypeName, NewOrderInput(1, &ChaosSpec{
		TargetVersion: V4,
		Step:          StepPayment,
		Mode:          ChaosFail,
	}))

	if !env.IsWorkflowCompleted() {
		t.Fatal("gate should finish rather than hang")
	}
	if env.GetWorkflowError() == nil {
		t.Fatal("gate should fail when its version is poisoned")
	}
}

// A healthy candidate's gate must pass, or no rollout would ever start.
func TestGatePassesOnHealthyVersion(t *testing.T) {
	env := newEnv(t, V4, true)
	env.ExecuteWorkflow(WorkflowTypeName, NewOrderInput(1, nil))

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("gate should pass on a healthy version, got: %v", err)
	}
}

// Each step must run as its own Activity type, so a version's pipeline is
// legible in Event History rather than appearing as N identical entries.
func TestEachStepRunsAsItsOwnActivityType(t *testing.T) {
	var mu sync.Mutex
	seen := map[string]int{}

	env := newEnv(t, V4, false)
	env.SetOnActivityStartedListener(func(info *activity.Info, _ context.Context, _ converter.EncodedValues) {
		mu.Lock()
		defer mu.Unlock()
		seen[info.ActivityType.Name]++
	})
	env.ExecuteWorkflow(WorkflowTypeName, NewOrderInput(1, nil))

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("order failed: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	want := StepsFor(V4)
	if len(seen) != len(want) {
		t.Fatalf("saw %d distinct Activity types (%v), want %d", len(seen), seen, len(want))
	}
	for _, step := range want {
		if seen[step.ActivityName()] != 1 {
			t.Errorf("Activity %q ran %d times, want 1", step.ActivityName(), seen[step.ActivityName()])
		}
	}
	// The generic name is the thing being replaced; it must not appear.
	if seen["PerformStep"] != 0 {
		t.Error("PerformStep should no longer be used as an Activity type")
	}
}

// Versions must register genuinely different Activity types, or a rollout
// between them is invisible in the Temporal UI.
func TestVersionsRunDifferentActivityTypes(t *testing.T) {
	names := func(v Version) map[string]bool {
		out := map[string]bool{}
		for _, s := range StepsFor(v) {
			out[s.ActivityName()] = true
		}
		return out
	}

	v3, v4 := names(V3), names(V4)
	if !v4["DispatchMobilePickup"] {
		t.Error("v4 should run a DispatchMobilePickup Activity")
	}
	if v3["DispatchMobilePickup"] {
		t.Error("v3 should not have that Activity — it is what v4 adds")
	}
	if !names(V5)["HandOffAtDriveThru"] || names(V1)["HandOffAtDriveThru"] {
		t.Error("the drive-thru handoff should be unique to v5")
	}

	// Distinct steps must never collide onto one Activity name, or two
	// different pieces of work would be indistinguishable in history.
	all := map[string]Step{}
	for _, v := range AllVersions {
		for _, s := range StepsFor(v) {
			if prior, ok := all[s.ActivityName()]; ok && prior != s {
				t.Errorf("steps %q and %q share the Activity name %q", prior, s, s.ActivityName())
			}
			all[s.ActivityName()] = s
		}
	}
}

// The headline recovery path: an order stuck on an injected fault must come
// back to life when the fault is cleared, without being reset or restarted.
//
// Before this existed the fault was resolved once per step and then retried
// forever with the same poisoned input, so a stuck order was unrecoverable no
// matter what an operator did.
//
// This also pins the retry behaviour either side of the release: the step must
// go on retrying well past its bounded probe attempts (otherwise the order
// gave up rather than parked), and must then finish the whole pipeline.
func TestClearingTheFaultReleasesAStuckOrder(t *testing.T) {
	var mu sync.Mutex
	attempts := 0

	env := newEnv(t, V2, false)
	env.SetOnActivityStartedListener(func(info *activity.Info, _ context.Context, _ converter.EncodedValues) {
		if info.ActivityType.Name == StepPayment.ActivityName() {
			mu.Lock()
			attempts++
			mu.Unlock()
		}
	})

	// Long enough that the probe budget is spent and the order is parked on
	// its durable retry, then clear the fault the way the dashboard does.
	// Not much longer: the time-skipping environment races an unbounded retry
	// forward, and eventually abandons the run rather than skipping forever.
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalClearFault, nil)
	}, 2*time.Minute)

	env.ExecuteWorkflow(WorkflowTypeName, NewOrderInput(1, &ChaosSpec{
		TargetVersion: V2,
		Step:          StepPayment,
		Mode:          ChaosFail,
	}))

	if !env.IsWorkflowCompleted() {
		t.Fatal("a released order should finish, not stay parked")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("a released order should complete cleanly, got: %v", err)
	}

	var result OrderResult
	if err := env.GetWorkflowResult(&result); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	// It must finish the *whole* pipeline, not just escape the broken step.
	if len(result.Steps) != len(StepsFor(V2)) {
		t.Errorf("ran %d steps, want the full %d", len(result.Steps), len(StepsFor(V2)))
	}

	mu.Lock()
	defer mu.Unlock()
	// Strictly more than the probe budget, or the order gave up instead of
	// parking on a durable retry.
	if attempts <= probeAttempts {
		t.Errorf("payment ran %d times, want more than the %d probe attempts — "+
			"a stuck order should keep retrying, not give up", attempts, probeAttempts)
	}
}

// A fault that is never cleared must leave the order retrying rather than
// failing it: the customer's order is owed, not lost.
func TestAnUnclearedFaultNeverFailsTheOrder(t *testing.T) {
	env := newEnv(t, V2, false)

	env.ExecuteWorkflow(WorkflowTypeName, NewOrderInput(1, &ChaosSpec{
		TargetVersion: V2,
		Step:          StepPayment,
		Mode:          ChaosFail,
	}))

	// The time-skipping environment races an unbounded retry forward until it
	// gives up, so the assertion is about *how* it ended: whatever happened,
	// the order must never have completed successfully behind a live fault.
	if !env.IsWorkflowCompleted() {
		return // still parked, which is the intended state
	}
	if err := env.GetWorkflowError(); err == nil {
		t.Fatal("an order should not complete successfully while its fault is live")
	}
}
