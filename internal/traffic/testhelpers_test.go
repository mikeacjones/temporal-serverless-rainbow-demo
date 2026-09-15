package traffic

import (
	"testing"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
)

// newTestEnv builds a test environment with a stub order starter, so the
// generator's pacing decisions can be tested without a Temporal server.
func newTestEnv(t *testing.T) *testsuite.TestWorkflowEnvironment {
	t.Helper()

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterActivityWithOptions(
		func(req StartOrdersRequest) (StartOrdersResult, error) {
			return StartOrdersResult{Started: req.Count}, nil
		},
		activity.RegisterOptions{Name: ActivityStartOrders},
	)
	return env
}
