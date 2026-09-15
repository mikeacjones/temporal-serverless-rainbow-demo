package metrics

import (
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/converter"
)

// commonPayload aliases the protobuf payload type, so the decoding helpers
// read cleanly at their call sites.
type commonPayload = commonpb.Payload

// groupValue decodes the first value of a GROUP BY aggregation group, e.g. the
// execution status a count belongs to.
func groupValue(g *workflowservice.CountWorkflowExecutionsResponse_AggregationGroup) string {
	values := g.GetGroupValues()
	if len(values) == 0 {
		return ""
	}
	var s string
	if err := converter.GetDefaultDataConverter().FromPayload(values[0], &s); err != nil {
		return ""
	}
	return s
}
