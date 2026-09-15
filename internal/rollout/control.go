package rollout

import (
	"fmt"

	"go.temporal.io/sdk/workflow"
)

// control is the operator's intent, set by Update handlers and read by the
// rollout loop.
//
// It is plain mutable state with no locking, which is safe because Update
// handlers and Workflow code run on the same goroutine — one of the quieter
// conveniences of writing a control plane as a Workflow.
type control struct {
	paused          bool
	advance         bool
	abort           bool
	rollbackOnAbort bool
	jumpPct         *float32
}

// takeJump consumes a pending manual ramp request.
func (c *control) takeJump() *float32 {
	pct := c.jumpPct
	c.jumpPct = nil
	return pct
}

// AbortRequest stops a rollout, optionally restoring the previous routing.
type AbortRequest struct {
	// Rollback restores the pre-rollout routing. Without it the rollout stops
	// where it stands, leaving the candidate on whatever share it had.
	Rollback bool `json:"rollback"`
}

// register wires the Query and the five control Updates.
func register(ctx workflow.Context, state *State, ctrl *control) error {
	if err := workflow.SetQueryHandler(ctx, QueryGetState, func() (State, error) {
		return *state, nil
	}); err != nil {
		return fmt.Errorf("register %s query: %w", QueryGetState, err)
	}

	// Pause freezes the stage clock without changing the traffic split.
	if err := simpleUpdate(ctx, state, UpdatePause,
		func() error {
			if ctrl.paused {
				return fmt.Errorf("rollout is already paused")
			}
			return nil
		},
		func() { ctrl.paused = true },
	); err != nil {
		return err
	}

	// Resume restarts the clock where it left off.
	if err := simpleUpdate(ctx, state, UpdateResume,
		func() error {
			if !ctrl.paused {
				return fmt.Errorf("rollout is not paused")
			}
			return nil
		},
		func() { ctrl.paused = false },
	); err != nil {
		return err
	}

	// Advance skips the rest of the current hold. After a manual ramp it is
	// also how the operator hands control back to the plan.
	if err := simpleUpdate(ctx, state, UpdateAdvance, nil,
		func() {
			ctrl.advance = true
			ctrl.paused = false
		},
	); err != nil {
		return err
	}

	// Abort stops the rollout.
	if err := workflow.SetUpdateHandlerWithOptions(ctx, UpdateAbort,
		func(ctx workflow.Context, req AbortRequest) (State, error) {
			ctrl.abort = true
			ctrl.rollbackOnAbort = req.Rollback
			ctrl.paused = false
			return *state, nil
		},
		workflow.UpdateHandlerOptions{
			Validator: func(ctx workflow.Context, req AbortRequest) error {
				return liveOnly(state)
			},
		},
	); err != nil {
		return fmt.Errorf("register %s update: %w", UpdateAbort, err)
	}

	// SetRamp is the arbitrary-control lever: jump straight to any percentage,
	// forwards or backwards, at any time. The rollout then holds there with
	// health monitoring still armed until the operator advances or aborts.
	if err := workflow.SetUpdateHandlerWithOptions(ctx, UpdateSetRamp,
		func(ctx workflow.Context, pct float32) (State, error) {
			ctrl.jumpPct = &pct
			ctrl.paused = false
			return *state, nil
		},
		workflow.UpdateHandlerOptions{
			Validator: func(ctx workflow.Context, pct float32) error {
				if pct < 0 || pct > 100 {
					return fmt.Errorf("ramp percentage must be between 0 and 100, got %.1f", pct)
				}
				return liveOnly(state)
			},
		},
	); err != nil {
		return fmt.Errorf("register %s update: %w", UpdateSetRamp, err)
	}

	return nil
}

// simpleUpdate registers a no-argument control Update that returns the new
// state. extra runs after the shared "still running" check.
func simpleUpdate(
	ctx workflow.Context,
	state *State,
	name string,
	extra func() error,
	apply func(),
) error {
	err := workflow.SetUpdateHandlerWithOptions(ctx, name,
		func(ctx workflow.Context) (State, error) {
			apply()
			return *state, nil
		},
		workflow.UpdateHandlerOptions{
			Validator: func(ctx workflow.Context) error {
				if err := liveOnly(state); err != nil {
					return err
				}
				if extra != nil {
					return extra()
				}
				return nil
			},
		},
	)
	if err != nil {
		return fmt.Errorf("register %s update: %w", name, err)
	}
	return nil
}

// liveOnly rejects control of a rollout that has already finished, so the UI
// gets a clear reason instead of a silent no-op.
func liveOnly(state *State) error {
	if state.Phase.Terminal() {
		return fmt.Errorf("%w (%s)", errNotRunning, state.Phase)
	}
	return nil
}
