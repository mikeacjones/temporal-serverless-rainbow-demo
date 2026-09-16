package rollout

import (
	"context"
	"log/slog"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/deploy"
	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/metrics"
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
	ActivitySampleHealth    = "SampleHealth"
)

// RampRequest sets the ramp percentage for a Build ID.
type RampRequest struct {
	BuildID string  `json:"buildId"`
	Pct     float32 `json:"pct"`
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
	w.RegisterWorkflowWithOptions(GateProbe, gateProbeRegisterOptions())

	for name, fn := range map[string]any{
		ActivityResolveVersion:  a.ResolveVersion,
		ActivitySnapshotRouting: a.SnapshotRouting,
		ActivitySetRamp:         a.SetRamp,
		ActivitySetCurrent:      a.SetCurrent,
		ActivityClearRamp:       a.ClearRamp,
		ActivityRestoreRouting:  a.RestoreRouting,
		ActivityRunProbe:        a.RunProbe,
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
