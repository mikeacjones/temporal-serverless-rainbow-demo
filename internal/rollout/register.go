package rollout

import "go.temporal.io/sdk/workflow"

// workflowRegisterOptions names the coordinator Workflow.
//
// No versioning behaviour is set: the control plane runs on unversioned
// workers, deliberately outside the deployment it manages.
func workflowRegisterOptions() workflow.RegisterOptions {
	return workflow.RegisterOptions{Name: WorkflowTypeName}
}
