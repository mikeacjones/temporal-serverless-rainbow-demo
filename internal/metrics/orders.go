package metrics

import (
	"context"
	"fmt"
	"time"

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
}

// RecentOrders returns a sample of the most recently started orders.
//
// A sample, not the full set: at demo volumes there can be tens of thousands of
// open orders, and the point of the live strip is to show the texture of
// traffic, not to enumerate it.
func (r *Reader) RecentOrders(ctx context.Context, limit int) ([]LiveOrder, error) {
	resp, err := r.c.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: r.namespace,
		PageSize:  int32(limit),
		// No ORDER BY: the visibility store rejects it outright ("operation is
		// not supported") and it is unnecessary — open executions already come
		// back newest-first.
		Query: fmt.Sprintf(`WorkflowType = %q`, orders.WorkflowTypeName),
	})
	if err != nil {
		return nil, fmt.Errorf("list recent orders: %w", err)
	}

	out := make([]LiveOrder, 0, len(resp.GetExecutions()))
	for _, e := range resp.GetExecutions() {
		fields := e.GetSearchAttributes().GetIndexedFields()

		order := LiveOrder{
			OrderID:  e.GetExecution().GetWorkflowId(),
			Version:  keyword(fields["OrderVersion"]),
			Step:     keyword(fields["OrderStep"]),
			Status:   statusName(e.GetStatus().String()),
			Degraded: keyword(fields["OrderHealth"]) == orders.HealthDegraded,
		}

		if start := e.GetStartTime().AsTime(); !start.IsZero() {
			end := time.Now()
			if closed := e.GetCloseTime().AsTime(); e.GetCloseTime() != nil && !closed.IsZero() {
				end = closed
			}
			order.ElapsedSec = int(end.Sub(start).Seconds())
		}

		out = append(out, order)
	}
	return out, nil
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
