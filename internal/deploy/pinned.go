package deploy

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/types/known/durationpb"
)

// PinnedStart describes a Workflow to start against one specific version.
type PinnedStart struct {
	BuildID      string
	WorkflowID   string
	WorkflowType string
	TaskQueue    string
	Arg          any
	// RunTimeout bounds the run. Always set it for a canary gate: a gate that
	// could hang forever would hang the rollout waiting on it.
	RunTimeout time.Duration
}

// StartPinned starts a Workflow pinned to a specific Build ID, bypassing the
// deployment's Current/Ramping routing.
//
// This is how the canary gate works: it must run on the *candidate* version,
// which by definition is not taking any traffic yet.
//
// It goes through the raw gRPC service rather than client.ExecuteWorkflow
// because the SDK's StartWorkflowOptions has no versioning-override field
// (checked against SDK v1.48.0); the override only exists on the wire request.
func (d *Client) StartPinned(ctx context.Context, s PinnedStart) (runID string, err error) {
	input, err := converter.GetDefaultDataConverter().ToPayloads(s.Arg)
	if err != nil {
		return "", fmt.Errorf("encode input: %w", err)
	}

	req := &workflowservice.StartWorkflowExecutionRequest{
		Namespace:    d.namespace,
		WorkflowId:   s.WorkflowID,
		WorkflowType: &commonpb.WorkflowType{Name: s.WorkflowType},
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: s.TaskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Input:              input,
		RequestId:          uuid.NewString(),
		Identity:           "rainbow-rollout",
		WorkflowRunTimeout: durationpb.New(s.RunTimeout),
		VersioningOverride: &workflowpb.VersioningOverride{
			Override: &workflowpb.VersioningOverride_Pinned{
				Pinned: &workflowpb.VersioningOverride_PinnedOverride{
					Behavior: workflowpb.VersioningOverride_PINNED_OVERRIDE_BEHAVIOR_PINNED,
					Version: &deploymentpb.WorkerDeploymentVersion{
						DeploymentName: d.name,
						BuildId:        s.BuildID,
					},
				},
			},
		},
	}

	resp, err := d.c.WorkflowService().StartWorkflowExecution(ctx, req)
	if err != nil {
		return "", fmt.Errorf("start %s pinned to %q: %w", s.WorkflowType, s.BuildID, err)
	}
	return resp.GetRunId(), nil
}
