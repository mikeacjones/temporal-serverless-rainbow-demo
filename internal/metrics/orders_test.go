package metrics

import (
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func record(status enumspb.WorkflowExecutionStatus) *workflowpb.WorkflowExecutionInfo {
	return &workflowpb.WorkflowExecutionInfo{
		Execution: &commonpb.WorkflowExecution{WorkflowId: "ord-000001"},
		Status:    status,
		StartTime: timestamppb.New(time.Now().Add(-10 * time.Second)),
	}
}

// Every closed order must report Done, not just the ones that succeeded.
//
// The dashboard retires finished orders from its live strip using this flag.
// When it keyed off Status == "Completed" instead, a terminated order was
// never retired and sat there looking live indefinitely.
func TestEveryClosedOrderIsDone(t *testing.T) {
	closed := []enumspb.WorkflowExecutionStatus{
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
		enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT,
		enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED,
		enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
	}
	for _, status := range closed {
		if got := liveOrder(record(status), time.Now()); !got.Done {
			t.Errorf("status %v reported Done=false; it would sit on the rail as live", status)
		}
	}
}

// A running order must not be retired, or live orders would vanish.
func TestARunningOrderIsNotDone(t *testing.T) {
	got := liveOrder(record(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING), time.Now())
	if got.Done {
		t.Error("a running order should not be Done")
	}
	if got.Status != "Running" {
		t.Errorf("status = %q, want %q", got.Status, "Running")
	}
}

// Done and "succeeded" must stay distinguishable: order-duration figures use
// the status, because a terminated order has no meaningful duration.
func TestTerminatedIsDoneButNotCompleted(t *testing.T) {
	got := liveOrder(record(enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED), time.Now())
	if !got.Done {
		t.Error("a terminated order is finished")
	}
	if got.Status == "Completed" {
		t.Error("a terminated order must not be reported as Completed")
	}
	if got.Status != "Terminated" {
		t.Errorf("status = %q, want %q", got.Status, "Terminated")
	}
}

// A closed order's elapsed time is measured to its close, not to now, or every
// finished order would appear to keep ageing on the rail.
func TestClosedOrdersStopAgeing(t *testing.T) {
	start := time.Now().Add(-time.Hour)
	e := &workflowpb.WorkflowExecutionInfo{
		Execution: &commonpb.WorkflowExecution{WorkflowId: "ord-000002"},
		Status:    enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		StartTime: timestamppb.New(start),
		CloseTime: timestamppb.New(start.Add(8 * time.Second)),
	}

	if got := liveOrder(e, time.Now()).ElapsedSec; got != 8 {
		t.Errorf("elapsed = %ds, want 8s measured to the close time", got)
	}
}
