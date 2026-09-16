package rollout

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/testsuite"

	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/deploy"
	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/metrics"
)

func TestPhaseTerminal(t *testing.T) {
	live := []Phase{PhasePending, PhaseGating, PhaseRamping, PhasePaused, PhasePromoting}
	done := []Phase{PhaseCompleted, PhaseRolledBack, PhaseAborted, PhaseGateFailed}

	for _, p := range live {
		if p.Terminal() {
			t.Errorf("phase %q should not be terminal", p)
		}
	}
	for _, p := range done {
		if !p.Terminal() {
			t.Errorf("phase %q should be terminal", p)
		}
	}
}

func TestFirstStageAbove(t *testing.T) {
	stages := []Stage{{Pct: 1}, {Pct: 5}, {Pct: 25}, {Pct: 50}, {Pct: 100}}

	for _, tc := range []struct {
		pct  float32
		want int
	}{
		{pct: 0, want: 0},
		{pct: 1, want: 1},
		{pct: 30, want: 3},
		{pct: 50, want: 4},
		{pct: 100, want: 5}, // past the end: the plan is finished
	} {
		if got := firstStageAbove(stages, tc.pct); got != tc.want {
			t.Errorf("firstStageAbove(%.0f) = %d, want %d", tc.pct, got, tc.want)
		}
	}
}

// A single bad order must not roll back a small ramp: MinSamples is what stops
// a 1% stage being decided by one unlucky customer.
func TestBreachedRequiresEnoughEvidence(t *testing.T) {
	policy := HealthPolicy{MaxErrorRatePct: 20, MinSamples: 5}

	for _, tc := range []struct {
		name   string
		health metrics.Health
		want   bool
	}{
		{"clean", metrics.Health{Samples: 50, ErrorRatePct: 0}, false},
		{"under threshold", metrics.Health{Samples: 50, ErrorRatePct: 10}, false},
		{"at threshold is not over it", metrics.Health{Samples: 50, ErrorRatePct: 20}, false},
		{"over threshold with evidence", metrics.Health{Samples: 50, ErrorRatePct: 21}, true},
		{"over threshold but too few samples", metrics.Health{Samples: 4, ErrorRatePct: 100}, false},
		{"no samples at all", metrics.Health{}, false},
	} {
		if got := breached(tc.health, policy); got != tc.want {
			t.Errorf("%s: breached = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestWithDefaultsFillsEverything(t *testing.T) {
	in := Input{TargetVersion: "v2"}.WithDefaults()

	if len(in.Stages) == 0 {
		t.Error("stages should default")
	}
	if in.Gate.Orders == 0 || in.Gate.Timeout == 0 {
		t.Error("gate should default")
	}
	if in.Health.MaxErrorRatePct == 0 || in.Health.MinSamples == 0 || in.Health.EvalInterval == 0 {
		t.Error("health policy should default")
	}
}

func TestWithDefaultsKeepsCallerValues(t *testing.T) {
	in := Input{
		TargetVersion: "v2",
		Stages:        []Stage{{Pct: 50, Hold: time.Second}},
		Health:        HealthPolicy{MaxErrorRatePct: 1, MinSamples: 2, EvalInterval: 3 * time.Second},
	}.WithDefaults()

	if len(in.Stages) != 1 || in.Stages[0].Pct != 50 {
		t.Error("caller stages should be preserved")
	}
	if in.Health.MaxErrorRatePct != 1 || in.Health.MinSamples != 2 {
		t.Error("caller health policy should be preserved")
	}
}

// stubs stand in for the real Activities so the coordinator's decisions can be
// tested without a Temporal server.
type stubs struct{}

// The signatures must match the real Activities exactly, context included, or
// the mock expectations will not line up.
func (stubs) ResolveVersion(context.Context, string) (string, error) { return "", nil }
func (stubs) SnapshotRouting(context.Context) (deploy.Routing, error) {
	return deploy.Routing{}, nil
}
func (stubs) SetRamp(context.Context, RampRequest) error           { return nil }
func (stubs) SetCurrent(context.Context, string) error             { return nil }
func (stubs) ClearRamp(context.Context) error                      { return nil }
func (stubs) RestoreRouting(context.Context, deploy.Routing) error { return nil }
func (stubs) RunProbe(context.Context, ProbeRequest) (ProbeResult, error) {
	return ProbeResult{}, nil
}
func (stubs) SampleHealth(context.Context, HealthRequest) (metrics.Health, error) {
	return metrics.Health{}, nil
}

// newRolloutEnv registers the coordinator plus stub Activities.
func newRolloutEnv(t *testing.T) *testsuite.TestWorkflowEnvironment {
	t.Helper()

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(Rollout, workflowRegisterOptions())
	env.RegisterWorkflowWithOptions(GateProbe, gateProbeRegisterOptions())

	s := stubs{}
	register := map[string]any{
		ActivityResolveVersion:  s.ResolveVersion,
		ActivitySnapshotRouting: s.SnapshotRouting,
		ActivitySetRamp:         s.SetRamp,
		ActivitySetCurrent:      s.SetCurrent,
		ActivityClearRamp:       s.ClearRamp,
		ActivityRestoreRouting:  s.RestoreRouting,
		ActivityRunProbe:        s.RunProbe,
		ActivitySampleHealth:    s.SampleHealth,
	}
	for name, fn := range register {
		env.RegisterActivityWithOptions(fn, activityOptions(name))
	}
	return env
}

func testInput(target string) Input {
	return Input{
		TargetVersion:     target,
		Stages:            []Stage{{Pct: 25, Hold: 30 * time.Second}, {Pct: 100, Hold: 10 * time.Second}},
		Gate:              GateConfig{Enabled: true, Orders: 2, Timeout: time.Minute},
		Health:            HealthPolicy{MaxErrorRatePct: 20, MinSamples: 5, EvalInterval: 5 * time.Second},
		AutoPromote:       true,
		RollbackOnFailure: true,
	}
}

// The headline guarantee of the canary gate: a failed gate must move no
// traffic at all. If SetRamp is ever called here, the gate is decorative.
func TestGateFailureMovesNoTraffic(t *testing.T) {
	env := newRolloutEnv(t)

	env.OnActivity(ActivityResolveVersion, mock.Anything, "v4").Return("build-v4", nil)
	env.OnActivity(ActivitySnapshotRouting, mock.Anything).Return(deploy.Routing{
		CurrentBuildID: "build-v1", CurrentLabel: "v1",
	}, nil)
	env.OnActivity(ActivityRunProbe, mock.Anything, mock.Anything).Return(
		func(_ context.Context, req ProbeRequest) (ProbeResult, error) {
			return ProbeResult{}, errors.New("payment always fails")
		})

	env.ExecuteWorkflow(Rollout, testInput("v4"))

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("a failed gate is a normal outcome, not an error: %v", err)
	}

	var state State
	if err := env.GetWorkflowResult(&state); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	if state.Phase != PhaseGateFailed {
		t.Errorf("phase = %q, want %q", state.Phase, PhaseGateFailed)
	}
	if state.CurrentPct != 0 {
		t.Errorf("ramp = %.0f%%, want 0", state.CurrentPct)
	}
	env.AssertNotCalled(t, ActivitySetRamp, mock.Anything, mock.Anything)
	env.AssertNotCalled(t, ActivitySetCurrent, mock.Anything, mock.Anything)
}

// A healthy candidate should walk the whole plan and be promoted.
func TestHealthyRolloutCompletesAndPromotes(t *testing.T) {
	env := newRolloutEnv(t)

	env.OnActivity(ActivityResolveVersion, mock.Anything, "v2").Return("build-v2", nil)
	env.OnActivity(ActivitySnapshotRouting, mock.Anything).Return(deploy.Routing{
		CurrentBuildID: "build-v1", CurrentLabel: "v1",
	}, nil)
	env.OnActivity(ActivityRunProbe, mock.Anything, mock.Anything).Return(
		func(_ context.Context, req ProbeRequest) (ProbeResult, error) {
			return ProbeResult{OrderID: req.OrderID, Version: req.Label}, nil
		})
	env.OnActivity(ActivitySetRamp, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(ActivitySampleHealth, mock.Anything, mock.Anything).Return(metrics.Health{
		Completed: 100, Samples: 100, ErrorRatePct: 0,
	}, nil)
	env.OnActivity(ActivitySetCurrent, mock.Anything, "build-v2").Return(nil)
	env.OnActivity(ActivityClearRamp, mock.Anything).Return(nil)

	env.ExecuteWorkflow(Rollout, testInput("v2"))

	var state State
	if err := env.GetWorkflowResult(&state); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	if state.Phase != PhaseCompleted {
		t.Fatalf("phase = %q (%s), want %q", state.Phase, state.Message, PhaseCompleted)
	}
	if state.CurrentPct != 100 {
		t.Errorf("ramp = %.0f%%, want 100", state.CurrentPct)
	}
	env.AssertCalled(t, ActivitySetCurrent, mock.Anything, "build-v2")
}

// A candidate that goes bad after the gate must be rolled back automatically,
// restoring the routing captured before the rollout began.
func TestUnhealthyRampRollsBack(t *testing.T) {
	env := newRolloutEnv(t)

	previous := deploy.Routing{CurrentBuildID: "build-v1", CurrentLabel: "v1"}

	env.OnActivity(ActivityResolveVersion, mock.Anything, "v5").Return("build-v5", nil)
	env.OnActivity(ActivitySnapshotRouting, mock.Anything).Return(previous, nil)
	env.OnActivity(ActivityRunProbe, mock.Anything, mock.Anything).Return(
		func(_ context.Context, req ProbeRequest) (ProbeResult, error) {
			return ProbeResult{OrderID: req.OrderID, Version: req.Label}, nil
		})
	env.OnActivity(ActivitySetRamp, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(ActivitySampleHealth, mock.Anything, mock.Anything).Return(metrics.Health{
		Degraded: 20, Samples: 20, ErrorRatePct: 100,
	}, nil)
	env.OnActivity(ActivityRestoreRouting, mock.Anything, previous).Return(nil)

	env.ExecuteWorkflow(Rollout, testInput("v5"))

	var state State
	if err := env.GetWorkflowResult(&state); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	if state.Phase != PhaseRolledBack {
		t.Fatalf("phase = %q (%s), want %q", state.Phase, state.Message, PhaseRolledBack)
	}
	env.AssertCalled(t, ActivityRestoreRouting, mock.Anything, previous)
	env.AssertNotCalled(t, ActivitySetCurrent, mock.Anything, mock.Anything)
}

// An unresolvable version means that version's workers are not running. It
// must abort before touching routing, not guess at a Build ID.
func TestUnknownVersionAbortsBeforeTouchingRouting(t *testing.T) {
	env := newRolloutEnv(t)

	env.OnActivity(ActivityResolveVersion, mock.Anything, "v9").Return("", deploy.ErrUnknownVersion)

	env.ExecuteWorkflow(Rollout, testInput("v9"))

	var state State
	if err := env.GetWorkflowResult(&state); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	if state.Phase != PhaseAborted {
		t.Errorf("phase = %q, want %q", state.Phase, PhaseAborted)
	}
	env.AssertNotCalled(t, ActivitySetRamp, mock.Anything, mock.Anything)
}

// The gate must fan out one child Workflow per configured probe, and must wait
// for all of them. A probe whose future is never read is a gate that passes by
// omission, so the count is the assertion that matters here.
func TestGateFansOutOneChildPerProbe(t *testing.T) {
	env := newRolloutEnv(t)

	var mu sync.Mutex
	var seen []string

	env.OnActivity(ActivityResolveVersion, mock.Anything, "v2").Return("build-v2", nil)
	env.OnActivity(ActivitySnapshotRouting, mock.Anything).Return(deploy.Routing{
		CurrentBuildID: "build-v1", CurrentLabel: "v1",
	}, nil)
	env.OnActivity(ActivityRunProbe, mock.Anything, mock.Anything).Return(
		func(_ context.Context, req ProbeRequest) (ProbeResult, error) {
			mu.Lock()
			defer mu.Unlock()
			seen = append(seen, req.OrderID)
			return ProbeResult{OrderID: req.OrderID, Version: req.Label}, nil
		})
	env.OnActivity(ActivitySetRamp, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(ActivitySampleHealth, mock.Anything, mock.Anything).Return(metrics.Health{
		Completed: 100, Samples: 100, ErrorRatePct: 0,
	}, nil)
	env.OnActivity(ActivitySetCurrent, mock.Anything, "build-v2").Return(nil)
	env.OnActivity(ActivityClearRamp, mock.Anything).Return(nil)

	in := testInput("v2")
	in.Gate.Orders = 4
	env.ExecuteWorkflow(Rollout, in)

	var state State
	if err := env.GetWorkflowResult(&state); err != nil {
		t.Fatalf("decode result: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 4 {
		t.Fatalf("ran %d probes (%v), want 4", len(seen), seen)
	}
	// Distinct, and each naming the version under test, so two probes can never
	// dedupe onto one Workflow ID and silently halve the gate.
	unique := map[string]bool{}
	for _, id := range seen {
		if unique[id] {
			t.Errorf("probe ID %q was reused", id)
		}
		unique[id] = true
		if !strings.Contains(id, "v2") {
			t.Errorf("probe ID %q does not name the candidate version", id)
		}
	}
	if state.Gate.Probes != 4 || !state.Gate.Passed {
		t.Errorf("gate = %+v, want 4 passing probes", state.Gate)
	}
}

// One bad probe out of several is still a failed gate. The gate is a
// unanimity check, not a majority vote.
func TestOneFailedProbeFailsTheWholeGate(t *testing.T) {
	env := newRolloutEnv(t)

	env.OnActivity(ActivityResolveVersion, mock.Anything, "v4").Return("build-v4", nil)
	env.OnActivity(ActivitySnapshotRouting, mock.Anything).Return(deploy.Routing{
		CurrentBuildID: "build-v1", CurrentLabel: "v1",
	}, nil)
	env.OnActivity(ActivityRunProbe, mock.Anything, mock.Anything).Return(
		func(_ context.Context, req ProbeRequest) (ProbeResult, error) {
			if strings.HasSuffix(req.OrderID, "-02") {
				return ProbeResult{}, fmt.Errorf("Roast for order %s: roast step never finished", req.OrderID)
			}
			return ProbeResult{OrderID: req.OrderID, Version: req.Label}, nil
		})

	in := testInput("v4")
	in.Gate.Orders = 3
	env.ExecuteWorkflow(Rollout, in)

	var state State
	if err := env.GetWorkflowResult(&state); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	if state.Phase != PhaseGateFailed {
		t.Fatalf("phase = %q (%s), want %q", state.Phase, state.Message, PhaseGateFailed)
	}
	if state.Gate.Failures != 1 {
		t.Errorf("failures = %d, want 1", state.Gate.Failures)
	}
	// The detail is what the dashboard shows, so it must name the failed probe
	// and the real cause — not the SDK's child-Workflow wrapper, which carries
	// run and event IDs that belong in the Temporal UI instead.
	if !strings.Contains(state.Gate.Detail, "roast step never finished") {
		t.Errorf("detail %q should say why the probe failed", state.Gate.Detail)
	}
	if !strings.Contains(state.Gate.Detail, "probe 2 of 3") {
		t.Errorf("detail %q should name which probe failed", state.Gate.Detail)
	}
	for _, noise := range []string{"initiatedEventID", "runID", "child workflow execution error"} {
		if strings.Contains(state.Gate.Detail, noise) {
			t.Errorf("detail %q leaks SDK wrapper detail (%q)", state.Gate.Detail, noise)
		}
	}
	env.AssertNotCalled(t, ActivitySetRamp, mock.Anything, mock.Anything)
}
