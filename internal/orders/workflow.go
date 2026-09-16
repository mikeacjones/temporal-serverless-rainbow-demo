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

	// probeAttempts is how many times a step is tried before the order is
	// called stuck. Bounded, because exhausting them is the signal — and it is
	// a signal about the step's execution, not about how busy the queue is.
	probeAttempts = 3

	// parkBackoffMax caps the wait between attempts once an order is parked.
	// Long enough that a thousand stuck orders are not hammering a broken
	// dependency, short enough that recovery looks prompt.
	parkBackoffMax = 60 * time.Second
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

	runner := &stepRunner{
		probe: workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: stepStartToClose,
			// No ScheduleToStartTimeout: queuing is not failure. Orders waiting
			// behind a backlog should wait, not time out.
			RetryPolicy: &temporal.RetryPolicy{
				InitialInterval:    2 * time.Second,
				BackoffCoefficient: 1.5,
				MaximumInterval:    10 * time.Second,
				MaximumAttempts:    probeAttempts,
			},
		}),
		park: workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: stepStartToClose,
			RetryPolicy: &temporal.RetryPolicy{
				InitialInterval:    5 * time.Second,
				BackoffCoefficient: 2,
				MaximumInterval:    parkBackoffMax,
				// Unlimited. A stuck order is not a lost order: the customer's
				// coffee is still owed, so it waits for the fault to be fixed
				// rather than failing and dropping the work on the floor.
				MaximumAttempts: 0,
			},
		}),
		state: &state,
		chaos: in.Chaos,
		clear: workflow.GetSignalChannel(ctx, SignalClearFault),
		opts:  opts,
	}

	for i, step := range state.Steps {
		state.CurrentStep = i
		_ = workflow.UpsertTypedSearchAttributes(ctx, saOrderStep.ValueSet(string(step)))

		if err := runner.run(step); err != nil {
			return OrderResult{}, err
		}
	}

	state.Done = true
	return OrderResult{OrderID: in.OrderID, Version: v, Steps: state.Steps}, nil
}

// stepRunner holds what every step of one order needs.
//
// The two contexts are the whole idea: a step is first *probed* with bounded
// retries to find out whether it is broken, and only then *parked* on an
// unbounded retry to wait for it to be fixed.
type stepRunner struct {
	// probe retries a few times. Its attempts bound execution, not queue
	// time, so exhausting them means the step is genuinely failing rather
	// than merely waiting for a worker.
	probe workflow.Context
	// park retries forever with a long backoff. An order that reaches here is
	// stuck but not lost, and is waiting to be released.
	park workflow.Context

	state *OrderState
	chaos *ChaosSpec
	// clear carries SignalClearFault, which releases a parked order.
	clear workflow.ReceiveChannel
	opts  runOptions
}

// input builds the Activity payload for a step, resolving the fault *now*.
//
// Resolving per attempt rather than once per order is what makes a stuck
// order recoverable: once the order has been told to drop its fault, the very
// next attempt asks for clean work.
func (r *stepRunner) input(step Step) StepInput {
	faulted := !r.state.FaultCleared && r.chaos.appliesTo(r.state.Version, step)
	return StepInput{
		OrderID: r.state.OrderID,
		Version: r.state.Version,
		Step:    step,
		Fail:    faulted && r.chaos.Mode == ChaosFail,
		Slow:    faulted && r.chaos.Mode == ChaosSlow,
	}
}

// run executes one step, parking the order if the step is genuinely broken.
func (r *stepRunner) run(step Step) error {
	err := workflow.ExecuteActivity(r.probe, step.ActivityName(), r.input(step)).Get(r.probe, nil)
	if err == nil {
		r.healthy()
		return nil
	}

	// A gate probe stops here on purpose: a broken candidate should fail its
	// gate promptly, not park and hang the rollout waiting on it.
	if !r.opts.retryForever {
		return err
	}

	r.stuck(step, err)
	return r.parkUntilFixed(step)
}

// parkUntilFixed retries the step forever, and lets a cleared fault release it.
//
// The Activity retries on its own schedule, so an operator watching the
// Temporal UI sees one Activity with a climbing attempt count and a widening
// backoff — the honest picture of a durable retry. Clearing the fault cancels
// that attempt so the step can be re-run clean immediately, rather than the
// order waiting out whatever backoff it had reached.
func (r *stepRunner) parkUntilFixed(step Step) error {
	for {
		attempt, cancelAttempt := workflow.WithCancel(r.park)
		future := workflow.ExecuteActivity(attempt, step.ActivityName(), r.input(step))

		var err error
		var settled, released bool

		selector := workflow.NewSelector(r.park)
		selector.AddFuture(future, func(f workflow.Future) {
			err = f.Get(r.park, nil)
			settled = true
		})
		selector.AddReceive(r.clear, func(c workflow.ReceiveChannel, _ bool) {
			c.Receive(r.park, nil)
			r.state.FaultCleared = true
			released = true
			// Drop the poisoned attempt rather than waiting out its backoff.
			cancelAttempt()
		})

		for !settled {
			selector.Select(r.park)
		}
		cancelAttempt()

		if err == nil {
			r.healthy()
			return nil
		}
		if released {
			// Cancelled by the operator, not by failure: run it again clean.
			continue
		}
		return err
	}
}

// stuck marks the order degraded, once.
func (r *stepRunner) stuck(step Step, err error) {
	if r.state.Degraded {
		return
	}
	r.state.Degraded = true
	_ = workflow.UpsertTypedSearchAttributes(r.park, saOrderHealth.ValueSet(HealthDegraded))
	workflow.GetLogger(r.park).Warn("order stuck on step",
		"orderId", r.state.OrderID, "version", r.state.Version, "step", step, "err", err)
}

// healthy clears a degraded mark once the order is moving again.
func (r *stepRunner) healthy() {
	if !r.state.Degraded {
		return
	}
	r.state.Degraded = false
	_ = workflow.UpsertTypedSearchAttributes(r.park, saOrderHealth.ValueSet(HealthOK))
}
