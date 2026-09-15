package orders

import (
	"testing"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

// newEnv builds a test environment running one version's pipeline.
func newEnv(t *testing.T, v Version, gate bool) *testsuite.TestWorkflowEnvironment {
	t.Helper()

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	acts := &Activities{Profile: ProfileFast}
	env.RegisterActivityWithOptions(acts.PerformStep, activity.RegisterOptions{Name: PerformStepActivityName})

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
