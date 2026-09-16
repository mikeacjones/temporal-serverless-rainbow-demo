package api

import (
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
)

// The invariant that matters: only a RUNNING rollout may be queried.
//
// Querying a closed Workflow makes a worker replay its entire history to
// rebuild the queried state. The coordinator is unversioned and its command
// sequence changes whenever the rollout logic does, so a query against a
// rollout recorded under older code panics on the worker — once per dashboard
// poll, against a Workflow that finished successfully long ago. This is the
// rule that keeps the dashboard from generating that loop.
func TestOnlyARunningRolloutIsEverQueried(t *testing.T) {
	for status, name := range enumspb.WorkflowExecutionStatus_name {
		s := enumspb.WorkflowExecutionStatus(status)
		if s == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
			continue
		}
		if got := readFor(s); got == readQuery {
			t.Errorf("status %s would be queried; only RUNNING may be", name)
		}
	}

	if got := readFor(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING); got != readQuery {
		t.Errorf("a running rollout should be queried, got %v", got)
	}
}

// A completed rollout is still shown, so the panel keeps the outcome of the
// last deploy rather than blanking the moment it finishes.
func TestCompletedRolloutIsReadFromItsResult(t *testing.T) {
	if got := readFor(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED); got != readResult {
		t.Errorf("completed rollout read = %v, want readResult", got)
	}
}

// A rollout that was terminated or failed cannot be resumed, advanced or
// aborted. Reporting it as live is what previously wedged the dashboard into
// refusing to start the next one, so it must report as nothing running.
func TestStoppedRolloutReportsNothing(t *testing.T) {
	for _, status := range []enumspb.WorkflowExecutionStatus{
		enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
		enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT,
		enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED,
	} {
		if got := readFor(status); got != readNothing {
			t.Errorf("status %v read = %v, want readNothing", status, got)
		}
	}
}

// The fault repair must fire exactly when an order is parked on a fault that
// no longer exists — and stay quiet otherwise, because it batch-signals every
// running order.
func TestFaultRepairFiresOnlyWhenNeeded(t *testing.T) {
	for _, tc := range []struct {
		name     string
		chaosPct float64
		degraded int64
		want     bool
	}{
		{"parked with no fault configured", 0, 46, true},
		{"nothing parked", 0, 0, false},
		{"fault still aimed, so parking is expected", 100, 46, false},
		{"fault aimed and nothing parked yet", 100, 0, false},
	} {
		got := needsFaultRepair(tc.chaosPct, tc.degraded)
		if got != tc.want {
			t.Errorf("%s: repair = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// The sparklines must cover long enough to show a spike's tail.
//
// History used to be sampled once per poll, so at a 1s poll the graphs held
// two minutes — less than the tail of a large spike, which made the backlog
// graph look empty minutes after a peak that had simply scrolled off.
func TestHistoryWindowOutlivesASpike(t *testing.T) {
	window := time.Duration(historyLength) * historySample
	if window < 8*time.Minute {
		t.Errorf("history covers only %s; a spike's tail outlives that", window)
	}
}
