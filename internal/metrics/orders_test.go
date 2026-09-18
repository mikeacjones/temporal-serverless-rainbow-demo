package metrics

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/orders"
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

// Each version's order sample must be scoped to that version, and to running
// orders only.
//
// The version scope is what stops one version's burst emptying another's
// column: with a single shared window, 250 orders dumped on v3 straight after
// 250 on v2 left the sample holding 104 v3 rows and no v2 rows at all, while
// 500 orders were running. The Running scope is what stops a column filling
// with orders that have already finished, since at any real order rate most
// recent orders have.
func TestOrderSampleIsScopedPerVersionAndToRunningOrders(t *testing.T) {
	for _, version := range []string{"v1", "v5"} {
		q := recentOrdersQuery(version)

		if !strings.Contains(q, `OrderVersion = "`+version+`"`) {
			t.Errorf("query for %s does not scope to that version: %s", version, q)
		}
		if !strings.Contains(q, `ExecutionStatus = "Running"`) {
			t.Errorf("query for %s does not scope to running orders: %s", version, q)
		}
		if !strings.Contains(q, orders.WorkflowTypeName) {
			t.Errorf("query for %s does not scope to orders: %s", version, q)
		}
	}

	// Two versions must not produce the same query, or they would share a
	// window again by a different route.
	if recentOrdersQuery("v1") == recentOrdersQuery("v2") {
		t.Error("every version produced the same query")
	}
}

// An empty sample must encode as [] rather than null.
//
// The snapshot is serialised straight to the dashboard, and a null where an
// array is expected is the kind of thing that survives until something
// downstream iterates it.
func TestAnEmptySampleIsAnEmptyList(t *testing.T) {
	r := &Reader{}
	for _, tc := range []struct {
		name     string
		versions []string
		per      int
	}{
		{"no versions", nil, 40},
		{"no rows asked for", []string{"v1"}, 0},
	} {
		got, err := r.RecentOrders(context.Background(), tc.versions, tc.per)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if got == nil {
			t.Errorf("%s: returned nil, which encodes as null", tc.name)
			continue
		}
		encoded, err := json.Marshal(got)
		if err != nil {
			t.Fatalf("%s: marshal: %v", tc.name, err)
		}
		if string(encoded) != "[]" {
			t.Errorf("%s: encoded as %s, want []", tc.name, encoded)
		}
	}
}

// Resetting the counters must narrow the query, not delete anything.
//
// The point of the reset is that a demo given twice in a morning can have
// clean numbers the second time without destroying the evidence from the
// first, so the scope is the only thing that may change.
func TestCounterResetNarrowsTheQueryByStartTime(t *testing.T) {
	since := time.Date(2026, 9, 18, 13, 0, 0, 0, time.UTC)

	all := totalsQuery(time.Time{})
	from := totalsQuery(since)

	if strings.Contains(all, "StartTime") {
		t.Errorf("the default scope is time-limited: %s", all)
	}
	if !strings.Contains(from, `StartTime > "2026-09-18T13:00:00Z"`) {
		t.Errorf("reset scope does not filter on the baseline: %s", from)
	}
	// Both must still be scoped to orders, or a reset would start counting
	// gate probes and anything else in the namespace.
	for name, q := range map[string]string{"all-time": all, "since reset": from} {
		if !strings.Contains(q, orders.WorkflowTypeName) {
			t.Errorf("%s scope is not limited to orders: %s", name, q)
		}
	}
}
