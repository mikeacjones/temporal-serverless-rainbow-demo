// Package deploy wraps Temporal's Worker Deployment API.
//
// It is the only place in the demo that talks to the versioning endpoints, so
// the rollout coordinator, the dashboard and the workers all share one view of
// what a "version" is: a friendly label (v1..v5) that maps to an opaque Build
// ID.
package deploy

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

// MetadataKeyVersion is the Worker Deployment Version metadata key each worker
// writes at startup to publish its friendly label.
//
// Build IDs are opaque — a pod-template hash in Kubernetes, a Lambda alias in
// AWS — so nothing in the UI or the rollout logic should ever parse one.
// Workers publish "I am v3" instead.
const MetadataKeyVersion = "orderVersion"

// Status is a version's routing role, as shown on the dashboard.
type Status string

// Version routing roles.
const (
	// StatusCurrent takes all new orders not claimed by a ramp.
	StatusCurrent Status = "current"
	// StatusRamping takes a percentage of new orders.
	StatusRamping Status = "ramping"
	// StatusDraining takes no new orders but still has some in flight.
	StatusDraining Status = "draining"
	// StatusIdle is registered and polling, but taking no traffic.
	StatusIdle Status = "idle"
)

// Version is one registered Worker Deployment Version.
type Version struct {
	Label      string    `json:"label"`
	BuildID    string    `json:"buildId"`
	Status     Status    `json:"status"`
	TrafficPct float32   `json:"trafficPct"`
	CreateTime time.Time `json:"createTime"`

	// Pollers is how many workers are currently polling for this version.
	// With serverless workers this is the number that matters: an idle
	// version sits at zero until there is work for it.
	Pollers int `json:"pollers"`
}

// Routing is the deployment's live traffic split.
type Routing struct {
	CurrentBuildID string  `json:"currentBuildId"`
	CurrentLabel   string  `json:"currentLabel"`
	RampingBuildID string  `json:"rampingBuildId"`
	RampingLabel   string  `json:"rampingLabel"`
	RampingPct     float32 `json:"rampingPct"`
}

// State is a full snapshot of the deployment.
type State struct {
	DeploymentName string    `json:"deploymentName"`
	Routing        Routing   `json:"routing"`
	Versions       []Version `json:"versions"`
}

// Client is a label-aware view of one Worker Deployment.
type Client struct {
	c         client.Client
	name      string
	namespace string
	logger    *slog.Logger

	// labels caches buildID -> friendly label. Labels never change for a given
	// Build ID, so a hit is permanent; misses are retried, because a freshly
	// started worker may not have published its metadata yet.
	mu     sync.RWMutex
	labels map[string]string
}

// New builds a deployment client for the named Worker Deployment.
func New(c client.Client, name, namespace string, logger *slog.Logger) *Client {
	return &Client{
		c:         c,
		name:      name,
		namespace: namespace,
		logger:    logger,
		labels:    map[string]string{},
	}
}

// Name returns the Worker Deployment name.
func (d *Client) Name() string { return d.name }

// Namespace returns the Temporal namespace.
func (d *Client) Namespace() string { return d.namespace }

// handle returns a handle to this deployment.
func (d *Client) handle() client.WorkerDeploymentHandle {
	return d.c.WorkerDeploymentClient().GetHandle(d.name)
}

// Describe returns a labelled snapshot of the deployment's versions and routing.
func (d *Client) Describe(ctx context.Context) (State, error) {
	resp, err := d.handle().Describe(ctx, client.WorkerDeploymentDescribeOptions{})
	if err != nil {
		return State{}, fmt.Errorf("describe deployment %q: %w", d.name, err)
	}
	info := resp.Info

	routing := Routing{RampingPct: info.RoutingConfig.RampingVersionPercentage}
	if v := info.RoutingConfig.CurrentVersion; v != nil {
		routing.CurrentBuildID = v.BuildID
		routing.CurrentLabel = d.Label(ctx, v.BuildID)
	}
	if v := info.RoutingConfig.RampingVersion; v != nil {
		routing.RampingBuildID = v.BuildID
		routing.RampingLabel = d.Label(ctx, v.BuildID)
	}

	versions := make([]Version, 0, len(info.VersionSummaries))
	for _, s := range info.VersionSummaries {
		buildID := s.Version.BuildID
		v := Version{
			Label:      d.Label(ctx, buildID),
			BuildID:    buildID,
			CreateTime: s.CreateTime,
			Status:     StatusIdle,
		}
		switch {
		case buildID == routing.CurrentBuildID:
			v.Status = StatusCurrent
			// Current keeps whatever the ramp is not taking.
			v.TrafficPct = 100 - routing.RampingPct
		case buildID == routing.RampingBuildID:
			v.Status = StatusRamping
			v.TrafficPct = routing.RampingPct
		case s.DrainageStatus == client.WorkerDeploymentVersionDrainageStatusDraining,
			s.DrainageStatus == client.WorkerDeploymentVersionDrainageStatusDrained:
			// Both map to "draining" on screen: the operator cares that it is
			// no longer taking traffic, not which stage of teardown it is in.
			v.Status = StatusDraining
		}
		versions = append(versions, v)
	}

	// Show versions in label order (v1, v2, ...) so the dashboard is stable
	// regardless of the order Temporal returns them in.
	sort.Slice(versions, func(i, j int) bool {
		if versions[i].Label != versions[j].Label {
			return versions[i].Label < versions[j].Label
		}
		return versions[i].CreateTime.Before(versions[j].CreateTime)
	})

	return State{DeploymentName: d.name, Routing: routing, Versions: versions}, nil
}

// Label returns a Build ID's friendly label, or the Build ID itself when no
// worker has published one yet.
//
// Falling back to the Build ID rather than an empty string keeps the UI
// readable during startup, and is harmless because locally the Build ID *is*
// the label.
func (d *Client) Label(ctx context.Context, buildID string) string {
	if buildID == "" {
		return ""
	}

	d.mu.RLock()
	label, ok := d.labels[buildID]
	d.mu.RUnlock()
	if ok {
		return label
	}

	desc, err := d.handle().DescribeVersion(ctx, client.WorkerDeploymentDescribeVersionOptions{
		BuildID: buildID,
	})
	if err != nil {
		d.logger.Debug("describe version failed, using build ID as label", "buildId", buildID, "err", err)
		return buildID
	}

	label = decodeLabel(desc.Info.Metadata)
	if label == "" {
		return buildID
	}

	d.mu.Lock()
	d.labels[buildID] = label
	d.mu.Unlock()
	return label
}

// ResolveLabel maps a friendly label to its registered Build ID.
func (d *Client) ResolveLabel(ctx context.Context, label string) (string, error) {
	state, err := d.Describe(ctx)
	if err != nil {
		return "", err
	}
	for _, v := range state.Versions {
		if v.Label == label {
			return v.BuildID, nil
		}
	}
	return "", fmt.Errorf("%w: %q", ErrUnknownVersion, label)
}

// SetRamp routes pct% of new orders to a Build ID.
//
// AllowNoPollers and IgnoreMissingTaskQueues are needed because a candidate
// may not have been polled yet when a rollout begins.
func (d *Client) SetRamp(ctx context.Context, buildID string, pct float32) error {
	_, err := d.handle().SetRampingVersion(ctx, client.WorkerDeploymentSetRampingVersionOptions{
		BuildID:                 buildID,
		Percentage:              pct,
		AllowNoPollers:          true,
		IgnoreMissingTaskQueues: true,
	})
	if err != nil {
		return fmt.Errorf("set ramp %q to %.0f%%: %w", buildID, pct, err)
	}
	return nil
}

// ClearRamp removes the ramp so all new orders go to Current.
//
// Percentage must be zero *and* BuildID empty: an empty Build ID on its own
// means "ramp to unversioned workers", not "no ramp".
func (d *Client) ClearRamp(ctx context.Context) error {
	_, err := d.handle().SetRampingVersion(ctx, client.WorkerDeploymentSetRampingVersionOptions{
		BuildID:    "",
		Percentage: 0,
	})
	if err != nil {
		return fmt.Errorf("clear ramp: %w", err)
	}
	return nil
}

// SetCurrent promotes a Build ID to Current, a full cutover for new orders.
func (d *Client) SetCurrent(ctx context.Context, buildID string) error {
	_, err := d.handle().SetCurrentVersion(ctx, client.WorkerDeploymentSetCurrentVersionOptions{
		BuildID:                 buildID,
		AllowNoPollers:          true,
		IgnoreMissingTaskQueues: true,
	})
	if err != nil {
		return fmt.Errorf("set current %q: %w", buildID, err)
	}
	return nil
}

// PublishLabel writes a worker's friendly label into its version metadata.
//
// Called by the worker at startup. Best-effort and retried, because a version
// is not registered until its first poll.
func PublishLabel(ctx context.Context, c client.Client, deploymentName, buildID, label string, logger *slog.Logger) {
	h := c.WorkerDeploymentClient().GetHandle(deploymentName)
	opts := client.WorkerDeploymentUpdateVersionMetadataOptions{
		Version: worker.WorkerDeploymentVersion{DeploymentName: deploymentName, BuildID: buildID},
		MetadataUpdate: client.WorkerDeploymentMetadataUpdate{
			UpsertEntries: map[string]any{MetadataKeyVersion: label},
		},
	}

	for attempt := 1; attempt <= 12; attempt++ {
		if _, err := h.UpdateVersionMetadata(ctx, opts); err == nil {
			logger.Info("published version label", "buildId", buildID, "label", label)
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}
	}
	logger.Warn("gave up publishing version label", "buildId", buildID, "label", label)
}
