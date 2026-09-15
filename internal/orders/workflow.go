package orders

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// QueryGetState is the Query name for a single order's live state.
const QueryGetState = "getState"

// Health values published to the OrderHealth search attribute.
const (
	HealthOK       = "ok"
	HealthDegraded = "degraded"
)

// Search attributes the order publishes about itself.
//
// These exist so the dashboard can read thousands of orders in bulk with
// CountWorkflowExecutions instead of Querying each one. OrderHealth in
// particular is the only way to count stuck orders: an order retrying an
// Activity forever is still Running, so visibility alone cannot see that
// anything is wrong.
var (
	saOrderVersion = temporal.NewSearchAttributeKeyKeyword("OrderVersion")
	saOrderStep    = temporal.NewSearchAttributeKeyKeyword("OrderStep")
	saOrderHealth  = temporal.NewSearchAttributeKeyKeyword("OrderHealth")
)

// Timings for a customer order.
const (
	// stepStartToClose bounds how long one step may spend *executing*.
	//
	// Crucially this does not include time queued: a spike of five thousand
	// orders makes steps wait, but it does not make them slow. Judging a step
	// on schedule-to-completion instead would mark every healthy order as
	// stuck the moment a backlog formed, and a rollout watching that signal
	// would roll itself back during exactly the traffic surge it was meant to
	// survive.
	//
	// 30s comfortably clears the slowest healthy step in any profile (about
	// 17s under "heavy") while still catching a deliberately slowed one.
	stepStartToClose = 30 * time.Second

	// stuckRetryPause is how long a stuck order waits before trying its step
	// again, once Temporal's own Activity retries are exhausted.
	stuckRetryPause = 15 * time.Second
)

// Order runs one customer order through the pipeline of version v.
//
// A worker serves exactly one version, so v is fixed for the life of this
// Workflow — and because every version is Pinned, an order that starts here
// finishes here, on this code, even while a rollout moves new orders elsewhere.
func Order(ctx workflow.Context, v Version, in OrderInput) (OrderResult, error) {
	return run(ctx, v, in, runOptions{
		// A broken step leaves the order stuck and retrying rather than
		// failing outright, which is the honest failure mode: the customer's
		// order is not lost, it just is not progressing. It stays recoverable
		// by resetting it onto a healthy version.
		retryForever: true,
	})
}

// Gate runs the same pipeline as a canary probe against one version.
//
// It differs from a real order in one important way: its Activity retries are
// bounded, so a broken candidate makes the gate *fail* quickly instead of
// stalling forever and hanging the rollout that is waiting on it.
func Gate(ctx workflow.Context, v Version, in OrderInput) (OrderResult, error) {
	return run(ctx, v, in, runOptions{
		// Give up at the first genuinely failing step, so a broken candidate
		// blocks its rollout promptly instead of stalling it.
		retryForever: false,
	})
}

// runOptions is the one difference between a real order and a gate probe:
// whether a failing step is retried forever or gives up.
type runOptions struct {
	retryForever bool
}

// run walks the version's pipeline, one Activity per step.
func run(ctx workflow.Context, v Version, in OrderInput, opts runOptions) (OrderResult, error) {
	state := OrderState{
		OrderID: in.OrderID,
		Version: v,
		Steps:   StepsFor(v),
	}

	if err := workflow.SetQueryHandler(ctx, QueryGetState, func() (OrderState, error) {
		return state, nil
	}); err != nil {
		return OrderResult{}, err
	}

	// Publish who is running this order before doing any work, so the
	// dashboard can attribute it immediately.
	_ = workflow.UpsertTypedSearchAttributes(ctx,
		saOrderVersion.ValueSet(string(v)),
		saOrderHealth.ValueSet(HealthOK),
	)

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: stepStartToClose,
		// No ScheduleToStartTimeout: queuing is not failure. Orders waiting
		// behind a backlog should wait, not time out.
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    2 * time.Second,
			BackoffCoefficient: 1.5,
			MaximumInterval:    10 * time.Second,
			// Bounded on purpose. When these are exhausted the step has really
			// failed — as opposed to merely being slow to get a worker — and
			// the Workflow can say so before deciding what to do next.
			MaximumAttempts: 3,
		},
	})

	for i, step := range state.Steps {
		state.CurrentStep = i
		_ = workflow.UpsertTypedSearchAttributes(ctx, saOrderStep.ValueSet(string(step)))

		if err := runStep(ctx, &state, StepInput{
			OrderID: in.OrderID,
			Version: v,
			Step:    step,
			Fail:    in.Chaos.appliesTo(v, step) && in.Chaos.Mode == ChaosFail,
			Slow:    in.Chaos.appliesTo(v, step) && in.Chaos.Mode == ChaosSlow,
		}, opts); err != nil {
			return OrderResult{}, err
		}
	}

	state.Done = true
	return OrderResult{OrderID: in.OrderID, Version: v, Steps: state.Steps}, nil
}

// runStep executes one step, marking the order stuck if the step genuinely
// fails.
//
// "Genuinely" is the important word. Temporal has already retried the Activity
// several times by the time an error reaches here, and those retries are of
// execution, not of waiting for a worker. So an error here means the step is
// broken, not that the system is busy — which is what makes this a signal a
// rollout can safely act on.
func runStep(ctx workflow.Context, state *OrderState, in StepInput, opts runOptions) error {
	for {
		err := workflow.ExecuteActivity(ctx, PerformStepActivityName, in).Get(ctx, nil)

		if err == nil {
			if state.Degraded {
				// It came good on a retry: stop counting this order as stuck.
				state.Degraded = false
				_ = workflow.UpsertTypedSearchAttributes(ctx, saOrderHealth.ValueSet(HealthOK))
			}
			return nil
		}

		if !opts.retryForever {
			return err
		}

		if !state.Degraded {
			state.Degraded = true
			_ = workflow.UpsertTypedSearchAttributes(ctx, saOrderHealth.ValueSet(HealthDegraded))
			workflow.GetLogger(ctx).Warn("order stuck on step",
				"orderId", in.OrderID, "version", in.Version, "step", in.Step, "err", err)
		}

		// Keep trying. The order is stuck, not lost — and an operator can
		// rescue it onto a healthy version at any point.
		if err := workflow.Sleep(ctx, stuckRetryPause); err != nil {
			return err
		}
	}
}
