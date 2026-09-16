package rollout

import (
	"errors"
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/deploy"
	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/metrics"
)

// holdIndefinitely is the hold duration used when an operator has taken manual
// control: stay at this ramp percentage until they say otherwise.
const holdIndefinitely time.Duration = 0

// Rollout moves new-order traffic onto a target version, one stage at a time.
//
// The sequence is: resolve the target, remember the routing we can roll back
// to, run the canary gate, then walk the ramp plan while watching health. An
// operator can intervene at any point without stopping the safety net.
func Rollout(ctx workflow.Context, in Input) (State, error) {
	in = in.WithDefaults()
	logger := workflow.GetLogger(ctx)

	state := &State{
		TargetVersion: in.TargetVersion,
		Phase:         PhasePending,
		Stages:        in.Stages,
		Policy:        in.Health,
		StartedAt:     workflow.Now(ctx),
		UpdatedAt:     workflow.Now(ctx),
	}
	ctrl := &control{}

	if err := register(ctx, state, ctrl); err != nil {
		return *state, err
	}

	acts := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
	})

	// Resolve the friendly label to the Build ID Temporal actually routes on.
	// A label with no registered version means that version's worker is not
	// running, which is worth failing loudly rather than guessing about.
	if err := workflow.ExecuteActivity(acts, ActivityResolveVersion, in.TargetVersion).
		Get(acts, &state.TargetBuildID); err != nil {
		return *finish(ctx, state, PhaseAborted, fmt.Sprintf("cannot resolve version %q: %v", in.TargetVersion, err)), nil
	}

	// Snapshot the routing before touching anything. This is what a rollback
	// restores, and capturing it up front means a rollback does not depend on
	// remembering how we got here.
	var previous deploy.Routing
	if err := workflow.ExecuteActivity(acts, ActivitySnapshotRouting).Get(acts, &previous); err != nil {
		return *finish(ctx, state, PhaseAborted, fmt.Sprintf("cannot read current routing: %v", err)), nil
	}
	state.PreviousCurrentLabel = previous.CurrentLabel
	logger.Info("rollout starting",
		"target", in.TargetVersion, "buildId", state.TargetBuildID, "from", previous.CurrentLabel)

	// The canary gate. Nothing has been routed yet, so a failure here costs
	// zero real orders — which is the entire point of gating before ramping.
	if in.Gate.Enabled {
		touch(ctx, state, PhaseGating, fmt.Sprintf("running %d canary orders on %s", in.Gate.Orders, in.TargetVersion))

		// One child Workflow per probe, all awaited. Their failures are the
		// gate's verdict, so runGate reports rather than fails.
		state.Gate = runGate(ctx, in, state.TargetBuildID)
		if !state.Gate.Passed {
			// No rollback needed: the routing was never changed.
			return *finish(ctx, state, PhaseGateFailed,
				fmt.Sprintf("canary gate failed on %s, no traffic was moved: %s", in.TargetVersion, state.Gate.Detail)), nil
		}
		touch(ctx, state, PhaseGating, fmt.Sprintf("canary gate passed on %s", in.TargetVersion))
	}

	// Walk the ramp plan.
	for state.StageIndex < len(state.Stages) {
		target := state.Stages[state.StageIndex].Pct
		hold := state.Stages[state.StageIndex].Hold

		// A manual ramp overrides the plan, and holds there indefinitely with
		// health monitoring still armed.
		//
		// manual matters below: it moves StageIndex to the next *unrun* stage,
		// whereas a planned stage leaves StageIndex pointing at the stage
		// currently applied. Advancing has to mean different things in the two
		// cases, or handing control back to the plan would skip a stage.
		manual := false
		if pct := ctrl.takeJump(); pct != nil {
			target = *pct
			hold = holdIndefinitely
			manual = true
			state.ManualControl = true
			state.StageIndex = firstStageAbove(state.Stages, target)
		}

		if err := workflow.ExecuteActivity(acts, ActivitySetRamp, RampRequest{
			BuildID: state.TargetBuildID,
			Pct:     target,
		}).Get(acts, nil); err != nil {
			return *rollback(ctx, state, in, previous, fmt.Sprintf("cannot set ramp to %.0f%%: %v", target, err)), nil
		}

		state.CurrentPct = target
		touch(ctx, state, PhaseRamping, fmt.Sprintf("%s taking %.0f%% of new orders", in.TargetVersion, target))
		logger.Info("ramp applied", "version", in.TargetVersion, "pct", target, "manual", state.ManualControl)

		switch outcome := holdStage(ctx, acts, state, ctrl, in, hold); outcome {
		case outcomeAdvance:
			state.ManualControl = false
			if !manual {
				// StageIndex already points past a manual ramp; only a planned
				// stage needs stepping on.
				state.StageIndex++
			}
		case outcomeManual:
			// Loop round and apply the operator's percentage.
			continue
		case outcomeAbort:
			if ctrl.rollbackOnAbort {
				return *rollback(ctx, state, in, previous, "rollout aborted by operator, routing restored"), nil
			}
			return *finish(ctx, state, PhaseAborted,
				fmt.Sprintf("rollout stopped by operator at %.0f%%, routing left as-is", state.CurrentPct)), nil
		case outcomeUnhealthy:
			return *rollback(ctx, state, in, previous, fmt.Sprintf(
				"%s unhealthy at %.0f%%: %.0f%% of %d orders failed or stalled (limit %.0f%%)",
				in.TargetVersion, state.CurrentPct, state.Health.ErrorRatePct,
				state.Health.Samples, in.Health.MaxErrorRatePct)), nil
		}
	}

	// The plan is done: the candidate is taking all new orders but is not yet
	// Current. Promoting makes that permanent and lets the old version drain.
	if in.AutoPromote {
		touch(ctx, state, PhasePromoting, fmt.Sprintf("promoting %s to current", in.TargetVersion))
		if err := workflow.ExecuteActivity(acts, ActivitySetCurrent, state.TargetBuildID).Get(acts, nil); err != nil {
			return *rollback(ctx, state, in, previous, fmt.Sprintf("cannot promote %s: %v", in.TargetVersion, err)), nil
		}
		// Promoting the ramping version normally clears the ramp server-side;
		// clearing it explicitly keeps the routing unambiguous either way.
		_ = workflow.ExecuteActivity(acts, ActivityClearRamp).Get(acts, nil)

		// Current takes every new order, whatever the ramp happened to be.
		state.CurrentPct = 100
		return *finish(ctx, state, PhaseCompleted, fmt.Sprintf("%s is now current", in.TargetVersion)), nil
	}

	return *finish(ctx, state, PhaseCompleted, fmt.Sprintf(
		"%s is taking all new orders but was not promoted", in.TargetVersion)), nil
}

// outcome is why a stage's hold ended.
type outcome int

const (
	// outcomeAdvance: the hold elapsed, or an operator skipped ahead.
	outcomeAdvance outcome = iota
	// outcomeManual: an operator set an explicit ramp percentage.
	outcomeManual
	// outcomeAbort: an operator stopped the rollout.
	outcomeAbort
	// outcomeUnhealthy: the candidate breached the health policy.
	outcomeUnhealthy
)

// holdStage waits out one stage, sampling health as it goes.
//
// A hold of zero means hold indefinitely — the state an operator is in after
// setting a ramp by hand. Health is still sampled and rollback is still armed
// in that state; taking manual control should not mean turning off the alarm.
func holdStage(
	ctx workflow.Context,
	acts workflow.Context,
	state *State,
	ctrl *control,
	in Input,
	hold time.Duration,
) outcome {
	timed := hold > 0
	deadline := workflow.Now(ctx).Add(hold)

	for {
		switch {
		case ctrl.abort:
			return outcomeAbort
		case ctrl.jumpPct != nil:
			return outcomeManual
		case ctrl.advance:
			ctrl.advance = false
			return outcomeAdvance
		}

		// Paused freezes the clock: the ramp stays where it is and the stage
		// does not progress. Health sampling stops too, because the operator
		// has explicitly taken over.
		if ctrl.paused {
			pausedAt := workflow.Now(ctx)
			touch(ctx, state, PhasePaused, fmt.Sprintf("paused at %.0f%%", state.CurrentPct))
			_ = workflow.Await(ctx, func() bool {
				return !ctrl.paused || ctrl.abort || ctrl.jumpPct != nil
			})
			if timed {
				deadline = deadline.Add(workflow.Now(ctx).Sub(pausedAt))
			}
			touch(ctx, state, PhaseRamping, fmt.Sprintf("resumed at %.0f%%", state.CurrentPct))
			continue
		}

		if timed && !workflow.Now(ctx).Before(deadline) {
			return outcomeAdvance
		}

		// Sample health. A read failure is not evidence of ill health, so it
		// is logged and ignored rather than treated as a breach.
		var health metrics.Health
		err := workflow.ExecuteActivity(acts, ActivitySampleHealth, HealthRequest{
			BuildID: state.TargetBuildID,
			Since:   state.StartedAt,
		}).Get(acts, &health)
		if err != nil {
			workflow.GetLogger(ctx).Warn("health sample failed, ignoring", "err", err)
		} else {
			state.Health = health
			state.UpdatedAt = workflow.Now(ctx)
			if breached(health, in.Health) {
				return outcomeUnhealthy
			}
		}

		wait := in.Health.EvalInterval
		if timed {
			if remaining := deadline.Sub(workflow.Now(ctx)); remaining < wait {
				wait = remaining
			}
			state.HoldRemainingSec = int(deadline.Sub(workflow.Now(ctx)).Seconds())
		} else {
			state.HoldRemainingSec = -1 // indefinite
		}
		if wait <= 0 {
			return outcomeAdvance
		}

		// Sleep until the next sample, but wake immediately if an operator
		// intervenes.
		_, _ = workflow.AwaitWithTimeout(ctx, wait, func() bool {
			return ctrl.abort || ctrl.advance || ctrl.paused || ctrl.jumpPct != nil
		})
	}
}

// breached reports whether health has crossed the policy, with enough evidence
// to be worth acting on.
func breached(h metrics.Health, p HealthPolicy) bool {
	return h.Samples >= p.MinSamples && h.ErrorRatePct > p.MaxErrorRatePct
}

// firstStageAbove returns the index of the first planned stage above pct, so
// resuming the plan after a manual ramp continues forwards rather than
// repeating ground already covered.
func firstStageAbove(stages []Stage, pct float32) int {
	for i, s := range stages {
		if s.Pct > pct {
			return i
		}
	}
	return len(stages)
}

// rollback restores the pre-rollout routing and ends the rollout.
//
// The restore runs on a disconnected context so it still completes when the
// rollout itself is being cancelled — a rollback that gets cancelled halfway
// is worse than no rollback at all.
func rollback(ctx workflow.Context, state *State, in Input, previous deploy.Routing, reason string) *State {
	if !in.RollbackOnFailure {
		return finish(ctx, state, PhaseAborted, reason+" (rollback disabled, routing left as-is)")
	}

	disconnected, cancel := workflow.NewDisconnectedContext(ctx)
	defer cancel()
	acts := workflow.WithActivityOptions(disconnected, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 5},
	})

	if err := workflow.ExecuteActivity(acts, ActivityRestoreRouting, previous).Get(acts, nil); err != nil {
		workflow.GetLogger(ctx).Error("rollback failed", "err", err)
		return finish(ctx, state, PhaseAborted, reason+fmt.Sprintf(" — rollback ALSO failed: %v", err))
	}

	state.CurrentPct = 0
	return finish(ctx, state, PhaseRolledBack, reason)
}

// touch records a phase change and message for the UI.
func touch(ctx workflow.Context, state *State, phase Phase, message string) {
	state.Phase = phase
	state.Message = message
	state.UpdatedAt = workflow.Now(ctx)
}

// finish records a terminal phase.
func finish(ctx workflow.Context, state *State, phase Phase, message string) *State {
	touch(ctx, state, phase, message)
	state.HoldRemainingSec = 0
	workflow.GetLogger(ctx).Info("rollout finished", "phase", phase, "message", message)
	return state
}

// errNotRunning rejects control Updates once a rollout is over.
var errNotRunning = errors.New("rollout is no longer running")
