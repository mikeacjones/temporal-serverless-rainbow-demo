package rollout

import "go.temporal.io/sdk/workflow"

// workflowRegisterOptions names the coordinator Workflow.
//
// No versioning behaviour is set: the control plane runs on unversioned
// workers, deliberately outside the deployment it manages.
func workflowRegisterOptions() workflow.RegisterOptions {
	return workflow.RegisterOptions{Name: WorkflowTypeName}
}

// gateProbeRegisterOptions names the canary probe Workflow.
//
// Unversioned for the same reason as the coordinator: a probe decides whether a
// version is fit to take traffic, so it must not be routed by that decision.
func gateProbeRegisterOptions() workflow.RegisterOptions {
	return workflow.RegisterOptions{Name: GateProbeWorkflowTypeName}
}
