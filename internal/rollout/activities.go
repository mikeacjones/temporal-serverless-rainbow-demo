package rollout

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/deploy"
	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/metrics"
	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/orders"
)

// Activity names. The coordinator refers to them by name so the Workflow
// definition does not need to import anything the Activities depend on.
const (
	ActivityResolveVersion  = "ResolveVersion"
	ActivitySnapshotRouting = "SnapshotRouting"
	ActivitySetRamp         = "SetRamp"
	ActivitySetCurrent      = "SetCurrent"
	ActivityClearRamp       = "ClearRamp"
	ActivityRestoreRouting  = "RestoreRouting"
	ActivityRunGate         = "RunGate"
	ActivitySampleHealth    = "SampleHealth"
)

// gateHeartbeatInterval must stay comfortably under the gate Activity's
// heartbeat timeout, since a canary order routinely runs for longer than that
// timeout on its own.
const gateHeartbeatInterval = 10 * time.Second

// RampRequest sets the ramp percentage for a Build ID.
type RampRequest struct {
	BuildID string  `json:"buildId"`
	Pct     float32 `json:"pct"`
}

// GateRequest asks for a canary run against one version.
type GateRequest struct {
	BuildID string            `json:"buildId"`
	Label   string            `json:"label"`
	Orders  int               `json:"orders"`
	Timeout time.Duration     `json:"timeout"`
	Chaos   *orders.ChaosSpec `json:"chaos,omitempty"`
}

// HealthRequest asks for one version's order outcomes since a point in time.
type HealthRequest struct {
	BuildID string    `json:"buildId"`
	Since   time.Time `json:"since"`
}

// Activities are the side effects a rollout performs: reading and changing
// routing, running canary orders, and sampling health.
type Activities struct {
	deployment *deploy.Client
	reader     *metrics.Reader
	c          client.Client
	logger     *slog.Logger
}

// NewActivities builds the rollout Activities.
func NewActivities(c client.Client, deployment *deploy.Client, reader *metrics.Reader, logger *slog.Logger) *Activities {
	return &Activities{deployment: deployment, reader: reader, c: c, logger: logger}
}

// Register wires the coordinator and its Activities onto a worker.
//
// Note what is absent: any versioning options. This worker is deliberately
// unversioned, because it is the thing that changes versions.
func (a *Activities) Register(w worker.Registry) {
	w.RegisterWorkflowWithOptions(Rollout, workflowRegisterOptions())

	for name, fn := range map[string]any{
		ActivityResolveVersion:  a.ResolveVersion,
		ActivitySnapshotRouting: a.SnapshotRouting,
		ActivitySetRamp:         a.SetRamp,
		ActivitySetCurrent:      a.SetCurrent,
		ActivityClearRamp:       a.ClearRamp,
		ActivityRestoreRouting:  a.RestoreRouting,
		ActivityRunGate:         a.RunGate,
		ActivitySampleHealth:    a.SampleHealth,
	} {
		w.RegisterActivityWithOptions(fn, activityOptions(name))
	}
}

// ResolveVersion maps a friendly label to the Build ID Temporal routes on.
func (a *Activities) ResolveVersion(ctx context.Context, label string) (string, error) {
	return a.deployment.ResolveLabel(ctx, label)
}

// SnapshotRouting captures the routing a rollback would restore.
func (a *Activities) SnapshotRouting(ctx context.Context) (deploy.Routing, error) {
	state, err := a.deployment.Describe(ctx)
	if err != nil {
		return deploy.Routing{}, err
	}
	return state.Routing, nil
}

// SetRamp routes a percentage of new orders to the candidate.
func (a *Activities) SetRamp(ctx context.Context, req RampRequest) error {
	return a.deployment.SetRamp(ctx, req.BuildID, req.Pct)
}

// SetCurrent promotes a Build ID to Current.
func (a *Activities) SetCurrent(ctx context.Context, buildID string) error {
	return a.deployment.SetCurrent(ctx, buildID)
}

// ClearRamp removes any ramp, sending all new orders to Current.
func (a *Activities) ClearRamp(ctx context.Context) error {
	return a.deployment.ClearRamp(ctx)
}

// RestoreRouting puts the routing back the way it was before the rollout.
//
// Order matters: Current is restored first, so that clearing the ramp cannot
// briefly strand traffic on the candidate.
func (a *Activities) RestoreRouting(ctx context.Context, previous deploy.Routing) error {
	if previous.CurrentBuildID != "" {
		if err := a.deployment.SetCurrent(ctx, previous.CurrentBuildID); err != nil {
			return err
		}
	}
	if previous.RampingBuildID == "" || previous.RampingPct == 0 {
		return a.deployment.ClearRamp(ctx)
	}
	// There was already a ramp in flight before this rollout; put it back.
	return a.deployment.SetRamp(ctx, previous.RampingBuildID, previous.RampingPct)
}

// SampleHealth reads the candidate's order outcomes.
func (a *Activities) SampleHealth(ctx context.Context, req HealthRequest) (metrics.Health, error) {
	return a.reader.VersionHealth(ctx, req.BuildID, req.Since)
}

// RunGate runs the canary gate: a small number of orders pinned to the
// candidate version, which must all complete.
//
// The gate runs before any routing change, so a broken candidate is caught at
// a cost of a few synthetic orders and zero real ones. Gate orders use their
// own Workflow type, so they never pollute the order metrics on the dashboard.
//
// A failing gate is a normal outcome, not an error: it returns a result saying
// so, rather than failing the Activity.
func (a *Activities) RunGate(ctx context.Context, req GateRequest) (GateResult, error) {
	result := GateResult{Ran: true, Probes: req.Orders}

	// One unique run of the gate per rollout attempt, so a retried rollout
	// does not collide with Workflow IDs from a previous attempt.
	token := activity.GetInfo(ctx).WorkflowExecution.RunID
	if len(token) > 8 {
		token = token[:8]
	}

	type probe struct{ id, runID string }
	probes := make([]probe, 0, req.Orders)

	for i := range req.Orders {
		in := orders.NewOrderInput(i+1, req.Chaos)
		in.OrderID = fmt.Sprintf("gate-%s-%s-%02d", req.Label, token, i+1)

		runID, err := a.deployment.StartPinned(ctx, deploy.PinnedStart{
			BuildID:      req.BuildID,
			WorkflowID:   in.OrderID,
			WorkflowType: orders.GateWorkflowTypeName,
			TaskQueue:    orders.TaskQueue,
			Arg:          in,
			RunTimeout:   req.Timeout,
		})
		if err != nil {
			// Not being able to start a probe at all is a gate failure: it
			// usually means the candidate has no workers polling.
			result.Failures = req.Orders - len(probes)
			result.Detail = fmt.Sprintf("could not start canary order on %s: %v", req.Label, err)
			return result, nil
		}

		probes = append(probes, probe{id: in.OrderID, runID: runID})
		result.OrderIDs = append(result.OrderIDs, in.OrderID)
	}

	waitCtx, cancel := context.WithTimeout(ctx, req.Timeout)
	defer cancel()

	// Wait on every probe at once, and keep heartbeating while we do.
	//
	// Both halves matter. Waiting concurrently means the gate takes as long as
	// one order rather than all of them. Heartbeating on a timer — rather than
	// once per probe — is what keeps this Activity alive: a probe can easily
	// take longer than the heartbeat timeout, so heartbeating between probes
	// would let a gate time out while the orders it is watching are perfectly
	// healthy.
	type outcome struct {
		id  string
		err error
	}
	outcomes := make(chan outcome, len(probes))

	for _, p := range probes {
		go func(p probe) {
			outcomes <- outcome{id: p.id, err: a.c.GetWorkflow(waitCtx, p.id, p.runID).Get(waitCtx, nil)}
		}(p)
	}

	heartbeat := time.NewTicker(gateHeartbeatInterval)
	defer heartbeat.Stop()

	for remaining := len(probes); remaining > 0; {
		select {
		case done := <-outcomes:
			remaining--
			if done.err != nil {
				result.Failures++
				if result.Detail == "" {
					result.Detail = fmt.Sprintf("%s did not complete: %v", done.id, done.err)
				}
				a.logger.Warn("canary order failed", "orderId", done.id, "version", req.Label, "err", done.err)
			}

		case <-heartbeat.C:
			activity.RecordHeartbeat(ctx, fmt.Sprintf("%d of %d canary orders still running", remaining, len(probes)))

		case <-ctx.Done():
			return result, ctx.Err()
		}
	}

	result.Passed = result.Failures == 0
	if result.Passed {
		result.Detail = fmt.Sprintf("%d/%d canary orders completed on %s", result.Probes, result.Probes, req.Label)
	}
	return result, nil
}
