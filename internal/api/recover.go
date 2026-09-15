package api

import (
	"fmt"
	"net/http"

	"github.com/google/uuid"
	batchpb "go.temporal.io/api/batch/v1"
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/orders"
)

// recoverRatePerSecond throttles the batch so recovery does not itself become
// a traffic spike.
const recoverRatePerSecond = 50

type recoverRequest struct {
	// Version is the version to move orders off.
	Version string `json:"version"`

	// StuckOnly restricts the move to orders already stuck.
	//
	// The default is to move *every* order still running on that version,
	// which is almost always what is wanted: after a rollback, orders that
	// were started on the bad version are still working their way towards the
	// step that breaks. Rescuing only the currently-stuck ones leaves the rest
	// to get stuck a minute later.
	StuckOnly bool `json:"stuckOnly"`
}

type recoverResponse struct {
	JobID   string `json:"jobId"`
	Query   string `json:"query"`
	Version string `json:"version"`
	Target  string `json:"targetVersion"`
	Message string `json:"message"`
}

// handleRecover rescues every order stranded on a bad version.
//
// Rolling back caps the damage but does not undo it: orders already stuck on
// the bad version stay stuck, retrying forever. Recovery resets each one to
// its first Workflow Task and pins the new run to the healthy Current build,
// so it starts again from the top and runs the good pipeline.
//
// This is a *server-side* batch: one call resets every matching order, rather
// than the backend looping over thousands of them. It is durable, throttled,
// and survives the backend restarting halfway through.
func (s *Server) handleRecover(w http.ResponseWriter, r *http.Request) {
	var req recoverRequest
	if err := decode(r, &req); err != nil {
		s.writeError(w, http.StatusBadRequest, err)
		return
	}
	if _, ok := orders.ParseVersion(req.Version); !ok {
		s.writeError(w, http.StatusBadRequest, fmt.Errorf("unknown version %q", req.Version))
		return
	}

	state, err := s.deployment.Describe(r.Context())
	if err != nil {
		s.writeError(w, http.StatusBadGateway, err)
		return
	}

	good := state.Routing.CurrentBuildID
	if good == "" {
		s.writeError(w, http.StatusConflict,
			fmt.Errorf("no current version to recover onto: promote a healthy version first"))
		return
	}
	if state.Routing.CurrentLabel == req.Version {
		s.writeError(w, http.StatusConflict, fmt.Errorf(
			"%s is the current version: roll back to a healthy version before recovering", req.Version))
		return
	}

	bad, err := s.deployment.ResolveLabel(r.Context(), req.Version)
	if err != nil {
		s.writeError(w, statusForVersionError(err), err)
		return
	}

	query := fmt.Sprintf(
		`WorkflowType = %q AND ExecutionStatus = "Running" AND TemporalWorkerDeploymentVersion = %q`,
		orders.WorkflowTypeName, s.deployment.Name()+":"+bad)
	if req.StuckOnly {
		query += fmt.Sprintf(` AND OrderHealth = %q`, orders.HealthDegraded)
	}

	jobID := "recover-" + req.Version + "-" + uuid.NewString()

	_, err = s.rollouts.c.WorkflowService().StartBatchOperation(r.Context(), &workflowservice.StartBatchOperationRequest{
		Namespace:              s.deployment.Namespace(),
		JobId:                  jobID,
		VisibilityQuery:        query,
		Reason:                 fmt.Sprintf("move orders off %s onto %s", req.Version, state.Routing.CurrentLabel),
		MaxOperationsPerSecond: recoverRatePerSecond,
		Operation: &workflowservice.StartBatchOperationRequest_ResetOperation{
			ResetOperation: &batchpb.BatchOperationReset{
				Identity: "rainbow-backend",
				Options: &commonpb.ResetOptions{
					// Back to the very start: the bad version may have taken a
					// different path through the pipeline, so replaying from
					// any later point is not meaningful.
					Target: &commonpb.ResetOptions_FirstWorkflowTask{FirstWorkflowTask: &emptypb.Empty{}},
					// Keep Signals that arrived before the reset.
					ResetReapplyType: enumspb.RESET_REAPPLY_TYPE_SIGNAL,
				},
				// Pin each new run to the healthy build, so recovery cannot
				// land the order back on the version that broke it.
				PostResetOperations: []*workflowpb.PostResetOperation{{
					Variant: &workflowpb.PostResetOperation_UpdateWorkflowOptions_{
						UpdateWorkflowOptions: &workflowpb.PostResetOperation_UpdateWorkflowOptions{
							WorkflowExecutionOptions: &workflowpb.WorkflowExecutionOptions{
								VersioningOverride: &workflowpb.VersioningOverride{
									Override: &workflowpb.VersioningOverride_Pinned{
										Pinned: &workflowpb.VersioningOverride_PinnedOverride{
											Behavior: workflowpb.VersioningOverride_PINNED_OVERRIDE_BEHAVIOR_PINNED,
											Version: &deploymentpb.WorkerDeploymentVersion{
												DeploymentName: s.deployment.Name(),
												BuildId:        good,
											},
										},
									},
								},
							},
							UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"versioning_override"}},
						},
					},
				}},
			},
		},
	})
	if err != nil {
		s.writeError(w, http.StatusBadGateway, fmt.Errorf("start recovery batch: %w", err))
		return
	}

	s.logger.Info("recovery batch started",
		"jobId", jobID, "from", req.Version, "onto", state.Routing.CurrentLabel)

	scope := "every order still running on"
	if req.StuckOnly {
		scope = "the stuck orders on"
	}

	writeJSON(w, http.StatusAccepted, recoverResponse{
		JobID:   jobID,
		Query:   query,
		Version: req.Version,
		Target:  state.Routing.CurrentLabel,
		Message: fmt.Sprintf("Moving %s %s to %s", scope, req.Version, state.Routing.CurrentLabel),
	})
}
