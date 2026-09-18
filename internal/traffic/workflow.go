package traffic

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// ordersPerActivity caps how many orders one Activity starts, so a large spike
// fans out across several Activities running in parallel instead of queueing
// behind a single one.
const ordersPerActivity = 500

// Window is how much time one paced batch covers.
//
// This is the generator's whole clock. There is deliberately no per-order or
// per-second timer in the Workflow: the pacing happens *inside* an Activity,
// which dribbles its orders out over the window. That matters because every
// Workflow task and every timer is a billable action, and a generator that
// ticked every couple of seconds spent tens of thousands of actions a day
// producing nothing at all.
const Window = 30 * time.Second

// historyBudget is when to Continue-As-New.
//
// Measured against actual history length rather than a tick count, so an idle
// generator — which accrues no history — never continues-as-new at all.
const historyBudget = 4000

// Director starts customer orders at a controllable rate until it is stopped.
//
// Its two design rules, both about cost:
//
//   - While idle it does nothing whatsoever: no timer, no polling, no history.
//     It blocks until an operator asks for something, which costs no actions.
//   - While running it holds exactly one paced Activity in flight per window.
//     The Activity does the second-by-second pacing, because an Activity is
//     one action regardless of how long it runs.
func Director(ctx workflow.Context, in Input) error {
	maxRun := in.MaxRun
	if maxRun <= 0 {
		maxRun = DefaultMaxRun
	}

	stopAt := in.StopAt
	if stopAt.IsZero() {
		stopAt = workflow.Now(ctx).Add(maxRun)
	}

	state := &State{
		RatePerMin: in.RatePerMin,
		Chaos:      in.Chaos,
		Split:      in.Split,
		Running:    true,
		Started:    in.Started,
		NextSeq:    in.NextSeq,
		StopAt:     stopAt,
		UpdatedAt:  workflow.Now(ctx),
	}

	// Operator intent arrives on channels rather than as plain flags, so the
	// main loop can block on a Selector and be woken by an Update without
	// burning a timer to go looking.
	ctrl := &control{
		maxRun:  maxRun,
		changes: workflow.NewBufferedChannel(ctx, 16),
	}

	if err := register(ctx, state, ctrl); err != nil {
		return err
	}

	acts := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		// Long enough for a paced batch to run its whole window.
		StartToCloseTimeout: Window + 2*time.Minute,
		HeartbeatTimeout:    30 * time.Second,
		// One retry only: a batch of orders is not worth retrying hard, and a
		// slow retry would distort the rate we are trying to hold.
		RetryPolicy: &temporal.RetryPolicy{MaximumAttempts: 2},
	})

	// carry accumulates the fractional part of each window's order count, so a
	// rate that does not divide evenly still averages out exactly.
	var carry float64

	for {
		if ctrl.stop {
			state.Running = false
			workflow.GetLogger(ctx).Info("traffic generator stopped", "started", state.Started)
			return nil
		}

		// Stop generating if nobody has touched this for a while. The Workflow
		// stays alive so the operator can pick straight back up; it is the
		// traffic that stops, not the generator.
		if state.RatePerMin > 0 && !workflow.Now(ctx).Before(state.StopAt) {
			workflow.GetLogger(ctx).Info("traffic auto-stopped after being left untouched",
				"maxRun", maxRun, "started", state.Started)
			state.RatePerMin = 0
			state.AutoStopped = true
			state.UpdatedAt = workflow.Now(ctx)
		}

		// Nothing to do. Block until an operator asks for something: no timer,
		// no history, no actions.
		//
		// A burst no longer wakes this. Bursts are Standalone Activities
		// started from the client, so an idle generator stays idle through one
		// — which is the point: a burst is not a change to the steady rate.
		if state.RatePerMin == 0 {
			waitForChange(ctx, ctrl)
			continue
		}

		if workflow.GetInfo(ctx).GetCurrentHistoryLength() > historyBudget {
			break
		}

		due := ordersDue(state.RatePerMin, Window, &carry)
		runWindow(ctx, acts, state, ctrl, due)
	}

	// Continue-As-New keeps history bounded across a long session. The carried
	// state makes the restart invisible.
	return workflow.NewContinueAsNewError(ctx, Director, Input{
		RatePerMin: state.RatePerMin,
		Chaos:      state.Chaos,
		Split:      state.Split,
		NextSeq:    state.NextSeq,
		Started:    state.Started,
		MaxRun:     maxRun,
		StopAt:     state.StopAt,
	})
}

// waitForChange blocks until the operator changes a setting.
//
// This is where an idle generator spends all of its time, and it is free: a
// Workflow waiting on a Selector with nothing scheduled generates no history
// and no actions.
func waitForChange(ctx workflow.Context, ctrl *control) {
	selector := workflow.NewSelector(ctx)
	selector.AddReceive(ctrl.changes, func(c workflow.ReceiveChannel, _ bool) {
		c.Receive(ctx, nil)
	})
	selector.Select(ctx)
}

// runWindow starts one window's worth of orders, paced inside the Activity,
// and returns early if the operator intervenes.
func runWindow(
	ctx workflow.Context,
	acts workflow.Context,
	state *State,
	ctrl *control,
	due int,
) {
	pacedCtx, cancelPaced := workflow.WithCancel(acts)
	defer cancelPaced()

	paced := startOrders(pacedCtx, state, StartOrdersRequest{
		Count:      due,
		FirstSeq:   state.NextSeq,
		Chaos:      state.Chaos,
		Split:      state.Split,
		SpreadOver: Window,
	})
	state.NextSeq += due
	state.UpdatedAt = workflow.Now(ctx)

	for {
		var (
			finished bool
			changed  bool
		)

		selector := workflow.NewSelector(ctx)
		for _, future := range paced {
			selector.AddFuture(future, func(f workflow.Future) {
				var result StartOrdersResult
				if err := f.Get(ctx, &result); err == nil {
					state.Started += result.Started
				}
				finished = true
			})
		}
		selector.AddReceive(ctrl.changes, func(c workflow.ReceiveChannel, _ bool) {
			c.Receive(ctx, nil)
			changed = true
		})
		selector.Select(ctx)

		switch {
		case changed:
			// The rate or the fault config moved. Abandon the rest of this
			// window and start a new one from the new settings.
			return
		case finished:
			return
		}
	}
}

// IDs stay unique and deterministic even if a batch is cancelled part way.
func startOrders(ctx workflow.Context, _ *State, req StartOrdersRequest) []workflow.Future {
	var futures []workflow.Future

	seq := req.FirstSeq
	for remaining := req.Count; remaining > 0; {
		chunk := min(remaining, ordersPerActivity)

		futures = append(futures, workflow.ExecuteActivity(ctx, ActivityStartOrders, StartOrdersRequest{
			Count:      chunk,
			FirstSeq:   seq,
			Chaos:      req.Chaos,
			Split:      req.Split,
			SpreadOver: req.SpreadOver,
		}))

		seq += chunk
		remaining -= chunk
	}
	return futures
}

// ordersDue returns how many orders this window owes, carrying the remainder.
func ordersDue(ratePerMin int, window time.Duration, carry *float64) int {
	*carry += float64(ratePerMin) * window.Seconds() / 60
	due := int(*carry)
	*carry -= float64(due)
	return due
}

// control is the operator's intent. The channels let the main loop block on a
// Selector instead of waking on a timer to check flags.
type control struct {
	stop   bool
	maxRun time.Duration

	changes workflow.Channel
}

// touched records that an operator is present, pushing the auto-stop deadline
// out and waking the main loop.
func (c *control) touched(ctx workflow.Context, state *State) {
	state.AutoStopped = false
	state.StopAt = workflow.Now(ctx).Add(c.maxRun)
	state.UpdatedAt = workflow.Now(ctx)
	// Non-blocking: an Update handler must never block, and a full buffer
	// already means the loop is about to wake up anyway.
	c.changes.SendAsync(nil)
}

// register wires the Query and the four control Updates.
func register(ctx workflow.Context, state *State, ctrl *control) error {
	if err := workflow.SetQueryHandler(ctx, QueryGetState, func() (State, error) {
		return *state, nil
	}); err != nil {
		return fmt.Errorf("register %s query: %w", QueryGetState, err)
	}

	// setRate changes the steady-state order rate. Zero is valid and means
	// "hold, but stay ready" — the generator keeps running, doing nothing.
	if err := workflow.SetUpdateHandlerWithOptions(ctx, UpdateSetRate,
		func(ctx workflow.Context, ratePerMin int) (State, error) {
			state.RatePerMin = ratePerMin
			ctrl.touched(ctx, state)
			return *state, nil
		},
		workflow.UpdateHandlerOptions{
			Validator: func(ctx workflow.Context, ratePerMin int) error {
				if ratePerMin < 0 || ratePerMin > MaxRatePerMin {
					return fmt.Errorf("rate must be between 0 and %d orders/min, got %d",
						MaxRatePerMin, ratePerMin)
				}
				return nil
			},
		},
	); err != nil {
		return fmt.Errorf("register %s update: %w", UpdateSetRate, err)
	}

	// setSplit sends new orders to several versions at once. An empty split
	// hands routing back to the deployment's Current/Ramping config.
	if err := workflow.SetUpdateHandlerWithOptions(ctx, UpdateSetSplit,
		func(ctx workflow.Context, split Split) (State, error) {
			state.Split = split
			ctrl.touched(ctx, state)
			return *state, nil
		},
		workflow.UpdateHandlerOptions{
			Validator: func(ctx workflow.Context, split Split) error {
				if len(split) == 0 {
					return nil // clearing is always valid
				}

				seen := map[string]bool{}
				for _, e := range split {
					if e.Pct < 0 || e.Pct > 100 {
						return fmt.Errorf("share for %s must be between 0 and 100, got %.1f", e.Version, e.Pct)
					}
					if seen[e.Version] {
						return fmt.Errorf("%s appears twice in the split", e.Version)
					}
					seen[e.Version] = true
				}

				// Require the shares to add up. Anything else silently drops or
				// double-counts orders, and an operator would have no way to
				// tell from the dashboard which had happened.
				if total := split.Total(); total < 99.5 || total > 100.5 {
					return fmt.Errorf("shares must add up to 100, got %.1f", total)
				}
				return nil
			},
		},
	); err != nil {
		return fmt.Errorf("register %s update: %w", UpdateSetSplit, err)
	}

	// setChaos aims the fault injector at a version and step.
	if err := workflow.SetUpdateHandlerWithOptions(ctx, UpdateSetChaos,
		func(ctx workflow.Context, cfg ChaosConfig) (State, error) {
			state.Chaos = cfg
			ctrl.touched(ctx, state)
			return *state, nil
		},
		workflow.UpdateHandlerOptions{
			Validator: func(ctx workflow.Context, cfg ChaosConfig) error {
				if cfg.Pct < 0 || cfg.Pct > 100 {
					return fmt.Errorf("chaos percentage must be between 0 and 100, got %.1f", cfg.Pct)
				}
				if cfg.Pct > 0 && cfg.Spec == nil {
					return fmt.Errorf("chaos percentage is set but no target version or step was given")
				}
				return nil
			},
		},
	); err != nil {
		return fmt.Errorf("register %s update: %w", UpdateSetChaos, err)
	}

	// stop ends the generator for good; starting traffic again starts a new one.
	if err := workflow.SetUpdateHandlerWithOptions(ctx, UpdateStop,
		func(ctx workflow.Context) (State, error) {
			ctrl.stop = true
			state.Running = false
			state.RatePerMin = 0
			state.UpdatedAt = workflow.Now(ctx)
			ctrl.changes.SendAsync(nil)
			return *state, nil
		},
		workflow.UpdateHandlerOptions{},
	); err != nil {
		return fmt.Errorf("register %s update: %w", UpdateStop, err)
	}

	return nil
}
