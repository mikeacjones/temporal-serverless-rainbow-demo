package api

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"

	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/orders"
	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/rollout"
	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/traffic"
)

// trafficController drives the generator Workflow on the dashboard's behalf.
type trafficController struct {
	c      client.Client
	logger *slog.Logger
	// maxRun is how long traffic may flow untouched before stopping itself.
	maxRun time.Duration
}

// state returns the generator's live state, or nil when it is not running.
func (t *trafficController) state(ctx context.Context) *traffic.State {
	var state traffic.State
	value, err := t.c.QueryWorkflow(ctx, traffic.WorkflowID, "", traffic.QueryGetState)
	if err != nil {
		// Not running is the normal case before anyone presses play.
		return nil
	}
	if err := value.Get(&state); err != nil {
		t.logger.Debug("cannot decode traffic state", "err", err)
		return nil
	}

	// Same trap as the rollout: a terminated generator keeps answering with
	// Running: true, which would leave the dashboard showing traffic that
	// cannot be changed. Saying it is not running lets the next action start a
	// fresh one, which is what ensure() is for.
	if state.Running && !t.running(ctx) {
		state.Running = false
		state.RatePerMin = 0
	}

	return &state
}

// running reports whether the generator is still executing. An error counts as
// running, so a read failure cannot cause a second generator to be started
// alongside a healthy one.
func (t *trafficController) running(ctx context.Context) bool {
	desc, err := t.c.DescribeWorkflowExecution(ctx, traffic.WorkflowID, "")
	if err != nil {
		t.logger.Debug("cannot describe traffic execution", "err", err)
		return true
	}
	return desc.GetWorkflowExecutionInfo().GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
}

// chaos returns the live fault-injection setting, which the canary gate needs
// so that a sabotaged candidate fails its probes.
func (t *trafficController) chaos(ctx context.Context) *orders.ChaosSpec {
	state := t.state(ctx)
	if state == nil || state.Chaos.Pct <= 0 {
		return nil
	}
	return state.Chaos.Spec
}

// ensure starts the generator if it is not already running.
//
// WorkflowIDConflictPolicy: UseExisting makes this idempotent, so the
// dashboard can call it before any control action without racing itself.
func (t *trafficController) ensure(ctx context.Context) error {
	_, err := t.c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                       traffic.WorkflowID,
		TaskQueue:                traffic.TaskQueue,
		WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
	}, traffic.WorkflowTypeName, traffic.Input{MaxRun: t.maxRun})
	if err != nil {
		return fmt.Errorf("start traffic generator: %w", err)
	}
	return nil
}

// update sends one control Update to the generator, starting it first if need be.
func (t *trafficController) update(ctx context.Context, name string, args ...any) (traffic.State, error) {
	if err := t.ensure(ctx); err != nil {
		return traffic.State{}, controlPlaneError(err)
	}

	handle, err := t.c.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID:   traffic.WorkflowID,
		UpdateName:   name,
		Args:         args,
		WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	if err != nil {
		return traffic.State{}, controlPlaneError(fmt.Errorf("traffic %s: %w", name, err))
	}

	var state traffic.State
	if err := handle.Get(ctx, &state); err != nil {
		return traffic.State{}, controlPlaneError(fmt.Errorf("traffic %s: %w", name, err))
	}
	return state, nil
}

// controlPlaneError adds the likely cause to an Update failure.
//
// An Update to a control-plane Workflow can only complete if a control worker
// is polling. When one is not — during startup, or because that container is
// down — the underlying error is a bare deadline or unavailable, which tells an
// operator nothing about what to fix.
func controlPlaneError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w — is the control plane running? (docker compose ps control)", err)
}

// rolloutController drives the rollout coordinator Workflow.
type rolloutController struct {
	c         client.Client
	taskQueue string
	logger    *slog.Logger
}

// ErrRolloutRunning is returned when a rollout is asked for while one is live.
var ErrRolloutRunning = errors.New("a rollout is already in progress")

// rolloutMaxDuration caps a rollout, so one left paused cannot block the next
// one indefinitely. Comfortably longer than any plan a demo would use.
const rolloutMaxDuration = 2 * time.Hour

// state returns the last rollout's state, or nil when none is worth showing.
//
// How it reads that state depends on whether the coordinator is still running,
// and the distinction is not cosmetic.
//
// Querying a *closed* Workflow makes a worker replay its entire history to
// rebuild the state the handler would answer from. So any change to the
// coordinator's command sequence turns every dashboard poll into a replay
// failure on the worker — a hot loop of panics against a Workflow that
// finished successfully hours ago. A completed rollout is therefore read from
// its result, which is a recorded event: no replay, no worker involved, and
// nothing that a later code change can invalidate.
//
// A closed rollout that did not complete has no result to read. It reports as
// nothing running, which is also the honest answer: it cannot be resumed,
// advanced or aborted, so presenting it as live would only wedge the dashboard
// into refusing to start the next one.
func (r *rolloutController) state(ctx context.Context) *rollout.State {
	desc, err := r.c.DescribeWorkflowExecution(ctx, rollout.WorkflowID, "")
	if err != nil {
		// No execution yet, or a read problem. Either way there is nothing to
		// show; a rollout that does exist will appear on the next poll.
		return nil
	}

	switch readFor(desc.GetWorkflowExecutionInfo().GetStatus()) {
	case readQuery:
		return r.liveState(ctx)
	case readResult:
		return r.finishedState(ctx)
	default:
		return nil
	}
}

// rolloutRead is how a rollout's state may be obtained for a given execution
// status.
type rolloutRead int

const (
	// readNothing: there is no state worth showing, and nothing safe to read.
	readNothing rolloutRead = iota
	// readQuery: ask the running coordinator.
	readQuery
	// readResult: take the state the coordinator returned, from history.
	readResult
)

// readFor maps an execution status to how its state may be read.
//
// Pure and separate because the rule it encodes is easy to get wrong and
// expensive when wrong: only a RUNNING execution may be queried. Querying a
// closed one replays its whole history on a worker, so a coordinator whose
// command sequence has changed since that history was written panics on every
// poll.
func readFor(status enumspb.WorkflowExecutionStatus) rolloutRead {
	switch status {
	case enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING:
		return readQuery
	case enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED:
		return readResult
	default:
		// Terminated, failed, timed out, cancelled or continued-as-new: no
		// result to decode, and querying would mean replaying.
		return readNothing
	}
}

// liveState queries the running coordinator.
func (r *rolloutController) liveState(ctx context.Context) *rollout.State {
	value, err := r.c.QueryWorkflow(ctx, rollout.WorkflowID, "", rollout.QueryGetState)
	if err != nil {
		// Includes the narrow race where the rollout closed between the
		// Describe above and this Query.
		r.logger.Debug("cannot query rollout state", "err", err)
		return nil
	}

	var state rollout.State
	if err := value.Get(&state); err != nil {
		r.logger.Debug("cannot decode rollout state", "err", err)
		return nil
	}
	return &state
}

// finishedState reads a completed rollout's returned state from history.
func (r *rolloutController) finishedState(ctx context.Context) *rollout.State {
	var state rollout.State
	if err := r.c.GetWorkflow(ctx, rollout.WorkflowID, "").Get(ctx, &state); err != nil {
		r.logger.Debug("cannot read finished rollout result", "err", err)
		return nil
	}
	return &state
}

// running reports whether the rollout coordinator is still executing.
//
// Treats an error as "still running": a failed Describe is a read problem, and
// wrongly declaring a live rollout dead would let a second one start alongside
// it.
func (r *rolloutController) running(ctx context.Context) bool {
	desc, err := r.c.DescribeWorkflowExecution(ctx, rollout.WorkflowID, "")
	if err != nil {
		r.logger.Debug("cannot describe rollout execution", "err", err)
		return true
	}
	return desc.GetWorkflowExecutionInfo().GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
}

// start begins a rollout, refusing to start a second one alongside a live one.
func (r *rolloutController) start(ctx context.Context, in rollout.Input) (rollout.State, error) {
	if current := r.state(ctx); current != nil && !current.Phase.Terminal() {
		return rollout.State{}, fmt.Errorf("%w (%s to %s)",
			ErrRolloutRunning, current.Phase, current.TargetVersion)
	}

	_, err := r.c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        rollout.WorkflowID,
		TaskQueue: r.taskQueue,
		// A rollout must not outlive the presentation it belongs to, and a
		// coordinator left running would block the next one.
		WorkflowExecutionTimeout: rolloutMaxDuration,
		// Fail rather than terminate: if a rollout is somehow still live, the
		// right answer is to say so, not to kill it mid-ramp.
		WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
	}, rollout.WorkflowTypeName, in)
	if err != nil {
		var alreadyStarted *serviceerror.WorkflowExecutionAlreadyStarted
		if errors.As(err, &alreadyStarted) {
			return rollout.State{}, ErrRolloutRunning
		}
		return rollout.State{}, fmt.Errorf("start rollout: %w", err)
	}

	// The Workflow sets its own initial state; return what it reports.
	if state := r.state(ctx); state != nil {
		return *state, nil
	}
	return rollout.State{TargetVersion: in.TargetVersion, Phase: rollout.PhasePending}, nil
}

// update sends one control Update to the live rollout.
func (r *rolloutController) update(ctx context.Context, name string, args ...any) (rollout.State, error) {
	handle, err := r.c.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID:   rollout.WorkflowID,
		UpdateName:   name,
		Args:         args,
		WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	if err != nil {
		return rollout.State{}, fmt.Errorf("rollout %s: %w", name, err)
	}

	var state rollout.State
	if err := handle.Get(ctx, &state); err != nil {
		return rollout.State{}, fmt.Errorf("rollout %s: %w", name, err)
	}
	return state, nil
}
