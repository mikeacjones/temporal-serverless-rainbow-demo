package metrics

import (
	"context"
	"fmt"
	"sync"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/converter"

	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/orders"
)

// LiveOrder is one order as the dashboard shows it.
//
// Every field comes from the visibility record — no per-order Query. That is
// what makes this affordable at a thousand orders a minute.
type LiveOrder struct {
	OrderID    string `json:"orderId"`
	Version    string `json:"version"`
	Step       string `json:"step"`
	Status     string `json:"status"`
	Degraded   bool   `json:"degraded"`
	ElapsedSec int    `json:"elapsedSec"`

	// StartedAt is when the order was started, to the nanosecond.
	//
	// The dashboard stacks each version's orders oldest-first and needs a
	// stable key to do it. ElapsedSec cannot be that key — it is whole
	// seconds, so dozens of orders tie and the tie-break churns as the sample
	// slides. The order ID cannot be it either, now that bursts allocate IDs
	// independently of the steady stream: two callers cannot share a counter
	// without coordinating, and coordinating is what starting a burst as a
	// Standalone Activity is meant to avoid.
	StartedAt time.Time `json:"startedAt"`

	// Done reports that the order is closed, for any reason.
	//
	// Deliberately not the same question as Status == "Completed". An order
	// that was terminated, failed or timed out is equally finished, and a
	// dashboard that retires only the *completed* ones leaves every terminated
	// order sitting on the rail looking live. Successful completion is still
	// distinguishable via Status, which is what order-duration figures should
	// use — a terminated order has no meaningful duration.
	Done bool `json:"done"`
}

// RecentOrders samples each version's live orders separately.
//
// One shared sample does not work once the orders are shown per version. The
// visibility list returns the newest executions, so dumping 250 orders onto v3
// straight after 250 onto v2 pushed every v2 order out of a 150-row window —
// 104 v3 rows and not one v2 row, with 500 orders running. The v2 column
// emptied on screen while its orders were still working.
//
// A window per version cannot be crowded out by another version's burst. It
// costs one list call per version instead of one in total, but they run
// together and each asks for far fewer rows.
//
// Only Running executions are sampled. Completed orders would otherwise fill
// most of every window — at any real order rate the great majority of recent
// orders have already finished — and the dashboard shows work in flight.
func (r *Reader) RecentOrders(ctx context.Context, versions []string, perVersion int) ([]LiveOrder, error) {
	// Always a slice, never nil: this is serialised straight to the dashboard,
	// and an empty list must encode as [] rather than null.
	out := []LiveOrder{}
	if perVersion <= 0 || len(versions) == 0 {
		return out, nil
	}

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		firstErr error
	)

	now := time.Now()
	for _, version := range versions {
		wg.Add(1)
		go func() {
			defer wg.Done()

			resp, err := r.c.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
				Namespace: r.namespace,
				PageSize:  int32(perVersion),
				// No ORDER BY: the visibility store rejects it outright
				// ("operation is not supported"), and it is unnecessary —
				// executions already come back newest-first.
				Query: recentOrdersQuery(version),
			})
			if err != nil {
				mu.Lock()
				defer mu.Unlock()
				if firstErr == nil {
					firstErr = fmt.Errorf("list %s orders: %w", version, err)
				}
				return
			}

			mu.Lock()
			defer mu.Unlock()
			for _, e := range resp.GetExecutions() {
				out = append(out, liveOrder(e, now))
			}
		}()
	}
	wg.Wait()

	if firstErr != nil {
		return nil, firstErr
	}
	return out, nil
}

// recentOrdersQuery scopes a sample to one version's running orders.
//
// Both halves are load-bearing. Without the version filter the windows are
// shared, and one version's burst evicts another's orders from it. Without the
// Running filter the window fills with completed orders — at any real order
// rate most recent orders have already finished — and the column shows work
// that is already done.
func recentOrdersQuery(version string) string {
	return fmt.Sprintf(
		`WorkflowType = %q AND ExecutionStatus = "Running" AND OrderVersion = %q`,
		orders.WorkflowTypeName, version)
}

// liveOrder maps one visibility record to what the dashboard shows.
//
// Separate from the paging above so the status semantics can be tested: which
// executions count as finished is the part that is easy to get subtly wrong,
// and getting it wrong is invisible until a terminated order sits on the rail
// pretending to be live.
func liveOrder(e *workflowpb.WorkflowExecutionInfo, now time.Time) LiveOrder {
	fields := e.GetSearchAttributes().GetIndexedFields()

	order := LiveOrder{
		OrderID:   e.GetExecution().GetWorkflowId(),
		StartedAt: e.GetStartTime().AsTime(),
		Version:   keyword(fields["OrderVersion"]),
		Step:      keyword(fields["OrderStep"]),
		Status:    statusName(e.GetStatus().String()),
		Degraded:  keyword(fields["OrderHealth"]) == orders.HealthDegraded,
		Done:      e.GetStatus() != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	}

	if start := e.GetStartTime().AsTime(); !start.IsZero() {
		end := now
		if closed := e.GetCloseTime().AsTime(); e.GetCloseTime() != nil && !closed.IsZero() {
			end = closed
		}
		order.ElapsedSec = int(end.Sub(start).Seconds())
	}
	return order
}

// keyword decodes a Keyword search-attribute payload to a plain string.
func keyword(p *commonPayload) string {
	if p == nil {
		return ""
	}
	var s string
	if err := converter.GetDefaultDataConverter().FromPayload(p, &s); err != nil {
		return ""
	}
	return s
}

// statusName trims Temporal's WORKFLOW_EXECUTION_STATUS_ prefix, e.g.
// "WORKFLOW_EXECUTION_STATUS_RUNNING" becomes "Running".
func statusName(raw string) string {
	const prefix = "WORKFLOW_EXECUTION_STATUS_"
	if len(raw) > len(prefix) {
		raw = raw[len(prefix):]
	}
	if raw == "" {
		return ""
	}
	return string(raw[0]) + lower(raw[1:])
}

func lower(s string) string {
	out := make([]byte, len(s))
	for i := range len(s) {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		out[i] = c
	}
	return string(out)
}
