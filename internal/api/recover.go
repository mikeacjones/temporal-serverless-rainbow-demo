package api

import (
	"context"
	"fmt"
	"net/http"
	"time"

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

// batchIdentity is who the server-side batch operations act as.
const batchIdentity = "rainbow-backend"

// releaseRatePerSecond throttles the release batch. Higher than the recovery
// rate because a Signal is far cheaper than a reset: it appends one event and
// wakes the Workflow, rather than replaying it from the start.
const releaseRatePerSecond = 200

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
				Identity: batchIdentity,
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

// releaseStuckOrders tells every parked order to drop the fault it carries.
//
// Orders are parked on an unbounded retry of a step that was told to fail, and
// the instruction travels in their own input — so no amount of reconfiguring
// the injector reaches them. A batch Signal does: each order sets its fault
// aside, cancels the attempt in flight, and re-runs the step clean.
//
// This is the counterpart to handleRecover, and the gentler one. Recovery
// resets orders onto a different version because their *code* was wrong;
// this leaves them exactly where they are, because only the injected fault
// was.
//
// The batch is started even when nothing is parked; the visibility query
// simply matches nothing, which costs one call and avoids having to count
// stuck orders first just to decide whether to bother.
func (s *Server) releaseStuckOrders(ctx context.Context) (string, error) {
	// Every running order, not just the ones already stuck.
	//
	// Restricting this to OrderHealth = "degraded" looks like the obvious
	// optimisation and is wrong: the fault travels in each order's input, so an
	// order that started before the fault was cleared but has not yet reached
	// the broken step is still carrying it. It is healthy when the batch runs,
	// gets stuck a few seconds later, and by then nothing is coming to release
	// it. Observed exactly that way — clearing a fault left 32 orders stuck in
	// a contiguous block, all of them started before the clear and broken
	// after it.
	//
	// Signalling a healthy order is harmless: it sets aside a fault it may not
	// even have, and an order with no fault was never going to apply one.
	query := fmt.Sprintf(
		`WorkflowType = %q AND ExecutionStatus = "Running"`,
		orders.WorkflowTypeName)

	jobID := "release-" + uuid.NewString()

	_, err := s.rollouts.c.WorkflowService().StartBatchOperation(ctx, &workflowservice.StartBatchOperationRequest{
		Namespace:       s.deployment.Namespace(),
		JobId:           jobID,
		VisibilityQuery: query,
		Reason:          "fault cleared from the dashboard",
		// Throttled for the same reason recovery is: releasing ten thousand
		// orders at once would put every one of them back on the task queue in
		// the same instant.
		MaxOperationsPerSecond: releaseRatePerSecond,
		Operation: &workflowservice.StartBatchOperationRequest_SignalOperation{
			SignalOperation: &batchpb.BatchOperationSignal{
				Signal:   orders.SignalClearFault,
				Identity: batchIdentity,
			},
		},
	})
	if err != nil {
		return "", fmt.Errorf("start release batch: %w", err)
	}
	return jobID, nil
}

// reconcileInterval is the longest a parked order should wait to be released
// after its fault has gone. Frequent enough to look immediate on stage,
// infrequent enough that a genuinely broken version is not batch-signalled
// every second.
const reconcileInterval = 20 * time.Second

// reconcileFaults enforces one invariant: no order stays parked on a fault
// that is no longer configured.
//
// Clearing the injector fires a release batch immediately, but that batch is a
// snapshot. Orders started just before the clear still carry the fault in
// their input and have not reached the broken step yet — they are healthy when
// the batch runs and get stuck seconds later, with nothing left coming for
// them. Measured on the local stack: a cohort of 46 orders in a contiguous ID
// block, stuck indefinitely after a clear that had already released 145
// others.
//
// The generator is what makes the gap unavoidable at the source. It plans a
// window of orders at a time and stamps the fault in when the window is
// planned, so for up to one window after a clear it is still starting orders
// that carry the old spec. Rather than chase that timing, this re-checks the
// invariant and repairs it.
//
// Signalling an order that has no fault is harmless, which is what makes a
// repeating repair safe: a genuinely broken version keeps failing and stays
// degraded, because the Signal only sets aside an *injected* fault.
func (s *Server) reconcileFaults(ctx context.Context, snapshot *Snapshot) {
	if snapshot == nil || snapshot.Traffic == nil {
		return
	}
	if !needsFaultRepair(snapshot.Traffic.Chaos.Pct, snapshot.Totals.Degraded) {
		return
	}
	if time.Since(s.lastReconcile) < reconcileInterval {
		return
	}
	s.lastReconcile = time.Now()

	job, err := s.releaseStuckOrders(ctx)
	if err != nil {
		s.logger.Warn("cannot release orders parked on a cleared fault", "err", err)
		return
	}
	s.logger.Info("releasing orders parked on a cleared fault",
		"jobId", job, "stuck", snapshot.Totals.Degraded)
}

// needsFaultRepair reports whether anything is parked on a fault that is no
// longer configured.
//
// Pure and separate so the condition can be tested: this gates a batch
// operation over every running order, so firing it when a fault is still
// aimed — when parked orders are the *expected* outcome — would mean
// continuously releasing orders the operator is deliberately breaking.
func needsFaultRepair(chaosPct float64, degraded int64) bool {
	return chaosPct == 0 && degraded > 0
}
