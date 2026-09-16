package api

import (
	"strings"
	"testing"

	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/rollout"
)

// A terminated Workflow stays queryable for the namespace's retention period,
// and its handler answers with whatever state it held when it stopped. So a
// rollout terminated mid-ramp keeps reporting "paused" — a live phase — and
// everything downstream believes a rollout is running: the version cards
// disable their deployment buttons, starting a new rollout is refused as a
// duplicate, and every control Update fails because the execution is closed.
//
// The dashboard ends up wedged, insisting on a rollout that cannot be resumed,
// advanced or aborted. This is the reconciliation that unwedges it.
func TestOrphanedRolloutIsReportedAsStopped(t *testing.T) {
	frozen := rollout.State{
		TargetVersion: "v4",
		Phase:         rollout.PhasePaused,
		CurrentPct:    25,
		// A paused rollout holds indefinitely, which is what made this state
		// look permanently live.
		HoldRemainingSec: -1,
	}

	got := orphaned(frozen, false)

	if !got.Phase.Terminal() {
		t.Errorf("phase = %q, which is still live; nothing could start a new rollout", got.Phase)
	}
	if got.HoldRemainingSec != 0 {
		t.Errorf("holdRemainingSec = %d, want 0 for a stopped rollout", got.HoldRemainingSec)
	}
	// The message has to name the phase it replaced, or an operator cannot
	// tell what the rollout was doing when it was stopped.
	if !strings.Contains(got.Message, string(rollout.PhasePaused)) {
		t.Errorf("message does not say it was %q: %q", rollout.PhasePaused, got.Message)
	}
	if !strings.Contains(got.Message, "v4") {
		t.Errorf("message does not name the target version: %q", got.Message)
	}
}

// A coordinator that is genuinely running must be left exactly as it reported
// itself — rewriting a live rollout would strand it mid-ramp with no controls.
func TestRunningRolloutIsLeftAlone(t *testing.T) {
	live := rollout.State{
		TargetVersion:    "v4",
		Phase:            rollout.PhasePaused,
		CurrentPct:       25,
		HoldRemainingSec: -1,
		Message:          "paused at 25%",
	}

	got := orphaned(live, true)
	// rollout.State holds a slice, so compare the fields the reconciliation
	// would have touched rather than the whole struct.
	if got.Phase != live.Phase || got.Message != live.Message ||
		got.HoldRemainingSec != live.HoldRemainingSec {
		t.Errorf("a running rollout was rewritten:\n got  phase=%q msg=%q hold=%d\n want phase=%q msg=%q hold=%d",
			got.Phase, got.Message, got.HoldRemainingSec,
			live.Phase, live.Message, live.HoldRemainingSec)
	}
}

// Terminal phases are never reconciled, so a completed rollout keeps showing
// its real outcome rather than being relabelled as stopped.
func TestTerminalPhasesAreNotRewritten(t *testing.T) {
	for _, phase := range []rollout.Phase{
		rollout.PhaseCompleted, rollout.PhaseRolledBack, rollout.PhaseGateFailed, rollout.PhaseAborted,
	} {
		state := rollout.State{TargetVersion: "v4", Phase: phase, Message: "the real outcome"}
		// The caller only reconciles non-terminal phases; this asserts the
		// guard that makes that safe.
		if !phase.Terminal() {
			t.Fatalf("%q should be terminal", phase)
		}
		if got := orphaned(state, true); got.Message != "the real outcome" {
			t.Errorf("%q: message was rewritten to %q", phase, got.Message)
		}
	}
}
