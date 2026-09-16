package rollout

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/deploy"
	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/orders"
)

// The canary gate: a handful of probe orders run against the candidate version
// before any real traffic is routed to it.
const (
	// GateProbeWorkflowTypeName is one probe. The coordinator fans these out as
	// child Workflows and awaits them all.
	GateProbeWorkflowTypeName = "GateProbe"

	// ActivityRunProbe starts one probe order and waits for it to finish.
	ActivityRunProbe = "RunProbe"
)

// probeHeartbeat must stay well under the probe Activity's heartbeat timeout: a
// probe order routinely runs for longer than that timeout on its own, so the
// Activity has to keep saying it is alive while it waits.
const probeHeartbeat = 10 * time.Second

// ProbeRequest asks for one canary order against a specific version.
type ProbeRequest struct {
	BuildID string `json:"buildId"`
	Label   string `json:"label"`
	// OrderID is chosen by the coordinator so it is deterministic on replay.
	OrderID string            `json:"orderId"`
	Timeout time.Duration     `json:"timeout"`
	Chaos   *orders.ChaosSpec `json:"chaos,omitempty"`
}

// ProbeResult is one probe's outcome.
type ProbeResult struct {
	OrderID string `json:"orderId"`
	Version string `json:"version"`
}

// GateProbe runs one canary order pinned to the candidate version.
//
// It exists as a Workflow rather than as part of the coordinator so that each
// probe gets its own ID, history and outcome: the rollout shows up in the UI as
// a parent with one child per probe, and a probe that fails is a failed
// Workflow you can open, rather than a line in somebody else's history.
//
// A failing probe fails this Workflow. That is what the coordinator counts, and
// it means the failure is visible where it happened.
func GateProbe(ctx workflow.Context, req ProbeRequest) (ProbeResult, error) {
	acts := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		// The Activity bounds the wait itself; this only has to outlast it.
		StartToCloseTimeout: req.Timeout + time.Minute,
		HeartbeatTimeout:    30 * time.Second,
		// One attempt: a probe that failed has told us what we needed to know,
		// and retrying it would just delay the verdict.
		RetryPolicy: &temporal.RetryPolicy{MaximumAttempts: 1},
	})

	var result ProbeResult
	if err := workflow.ExecuteActivity(acts, ActivityRunProbe, req).Get(acts, &result); err != nil {
		return ProbeResult{OrderID: req.OrderID, Version: req.Label}, err
	}
	return result, nil
}

// runGate fans out the probes as child Workflows and waits for all of them.
//
// Every probe has to be awaited explicitly: a failed child does not fail its
// parent on its own, so an unread future is a gate that passes by omission.
//
// The children run on the coordinator's own task queue, not the order queue.
// The probe *orders* they start are what must land on the candidate version,
// and pinning those needs a versioning override on the start request — which
// the Go SDK exposes only on the raw client call, not on ChildWorkflowOptions
// (checked against v1.48 and v1.49; the wire protocol has the field, the SDK
// does not surface it). So the pinning stays inside the probe's Activity.
func runGate(ctx workflow.Context, in Input, buildID string) GateResult {
	result := GateResult{Ran: true, Probes: in.Gate.Orders}

	// Unique per rollout run, deterministic within one. Deriving from the
	// Workflow ID alone would collide, because the coordinator is a singleton:
	// a second rollout of the same version would dedup onto the first one's
	// long-finished probes and pass without testing anything.
	token := workflow.GetInfo(ctx).WorkflowExecution.RunID
	if len(token) > 8 {
		token = token[:8]
	}

	futures := make([]workflow.ChildWorkflowFuture, 0, in.Gate.Orders)
	for i := 1; i <= in.Gate.Orders; i++ {
		orderID := fmt.Sprintf("gate-%s-%s-%02d", in.TargetVersion, token, i)

		child := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			WorkflowID:         orderID + "-probe",
			TaskQueue:          TaskQueue,
			WorkflowRunTimeout: in.Gate.Timeout + 2*time.Minute,
			// Left at the default TERMINATE on purpose: aborting a rollout
			// should stop the probes it started, not leave them running against
			// a candidate nobody is watching any more.
		})

		futures = append(futures, workflow.ExecuteChildWorkflow(child, GateProbeWorkflowTypeName, ProbeRequest{
			BuildID: buildID,
			Label:   in.TargetVersion,
			OrderID: orderID,
			Timeout: in.Gate.Timeout,
			Chaos:   in.GateChaos,
		}))
		result.OrderIDs = append(result.OrderIDs, orderID)
	}

	for i, future := range futures {
		var probe ProbeResult
		if err := future.Get(ctx, &probe); err != nil {
			result.Failures++
			if result.Detail == "" {
				result.Detail = fmt.Sprintf("probe %d of %d failed: %s", i+1, len(futures), cause(err))
			}
			workflow.GetLogger(ctx).Warn("canary probe failed",
				"orderId", result.OrderIDs[i], "version", in.TargetVersion, "err", err)
		}
	}

	result.Passed = result.Failures == 0
	if result.Passed {
		result.Detail = fmt.Sprintf("%d/%d canary orders completed on %s",
			result.Probes, result.Probes, in.TargetVersion)
	}
	return result
}

// RunProbe starts one probe order pinned to the candidate and waits for it.
//
// Pinning is the whole point: the gate has to exercise the candidate version
// while that version is taking no traffic at all, so the order cannot be left
// to the deployment's routing. The override is only available on the raw start
// request, which is why this is an Activity rather than a child Workflow of
// the probe.
//
// A probe order uses its own Workflow type, so probes never appear in the order
// metrics on the dashboard.
func (a *Activities) RunProbe(ctx context.Context, req ProbeRequest) (ProbeResult, error) {
	in := orders.NewOrderInput(1, req.Chaos)
	in.OrderID = req.OrderID

	runID, err := a.deployment.StartPinned(ctx, deploy.PinnedStart{
		BuildID:      req.BuildID,
		WorkflowID:   in.OrderID,
		WorkflowType: orders.GateWorkflowTypeName,
		TaskQueue:    orders.TaskQueue,
		Arg:          in,
		RunTimeout:   req.Timeout,
	})
	if err != nil {
		// Usually means the candidate has no workers polling, which is a gate
		// failure rather than an infrastructure problem to retry.
		return ProbeResult{}, fmt.Errorf("could not start a canary order on %s: %w", req.Label, err)
	}

	waitCtx, cancel := context.WithTimeout(ctx, req.Timeout)
	defer cancel()

	// Wait in the background so this goroutine can keep heartbeating. A probe
	// order takes longer than the heartbeat timeout, so heartbeating only
	// before and after the wait would have the Activity killed mid-probe.
	done := make(chan error, 1)
	go func() { done <- a.c.GetWorkflow(waitCtx, in.OrderID, runID).Get(waitCtx, nil) }()

	heartbeat := time.NewTicker(probeHeartbeat)
	defer heartbeat.Stop()

	for {
		select {
		case err := <-done:
			if err != nil {
				// Unwrap here as well as in runGate: this message crosses two
				// failure boundaries (Activity, then child Workflow), and each
				// one flattens whatever it is given into a single string. Wrap
				// it twice and the dashboard shows the wrappers, not the fault.
				return ProbeResult{}, errors.New(cause(err))
			}
			a.logger.Info("canary probe passed", "orderId", in.OrderID, "version", req.Label)
			return ProbeResult{OrderID: in.OrderID, Version: req.Label}, nil

		case <-heartbeat.C:
			activity.RecordHeartbeat(ctx, in.OrderID)

		case <-ctx.Done():
			return ProbeResult{}, ctx.Err()
		}
	}
}

// cause reduces a Temporal failure to the message a human needs.
//
// A failed probe arrives wrapped several layers deep — child Workflow error
// around Activity error around the real cause — and the outer layers carry run
// and event IDs that belong in the Temporal UI, not on the dashboard.
func cause(err error) string {
	var app *temporal.ApplicationError
	if errors.As(err, &app) {
		return app.Message()
	}
	return err.Error()
}
