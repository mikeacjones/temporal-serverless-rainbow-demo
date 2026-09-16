package metrics

import (
	"testing"

	deploymentpb "go.temporal.io/api/deployment/v1"
	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/api/workflowservice/v1"
)

func version(build string) *deploymentpb.WorkerDeploymentVersion {
	return &deploymentpb.WorkerDeploymentVersion{BuildId: build}
}

// The response carries the same workers in two shapes. Reading both would
// double every number on the dashboard, which is the kind of wrong that looks
// plausible — so this asserts only one shape is counted.
func TestWorkersAreCountedOnceWhenBothShapesArePresent(t *testing.T) {
	resp := &workflowservice.ListWorkersResponse{
		Workers: []*workerpb.WorkerListInfo{
			{DeploymentVersion: version("v1")},
			{DeploymentVersion: version("v1")},
		},
		WorkersInfo: []*workerpb.WorkerInfo{
			{WorkerHeartbeat: &workerpb.WorkerHeartbeat{DeploymentVersion: version("v1")}},
			{WorkerHeartbeat: &workerpb.WorkerHeartbeat{DeploymentVersion: version("v1")}},
		},
	}

	got := WorkerCount{ByBuild: map[string]int{}}
	countPage(resp, &got)

	if got.Total != 2 {
		t.Errorf("total = %d, want 2 (the same workers were counted twice)", got.Total)
	}
	if got.ByBuild["v1"] != 2 {
		t.Errorf("v1 = %d, want 2", got.ByBuild["v1"])
	}
}

// An older server populates only the heartbeat list.
func TestWorkersAreCountedFromTheHeartbeatShapeAlone(t *testing.T) {
	resp := &workflowservice.ListWorkersResponse{
		WorkersInfo: []*workerpb.WorkerInfo{
			{WorkerHeartbeat: &workerpb.WorkerHeartbeat{DeploymentVersion: version("v3")}},
			{WorkerHeartbeat: &workerpb.WorkerHeartbeat{DeploymentVersion: version("v4")}},
		},
	}

	got := WorkerCount{ByBuild: map[string]int{}}
	countPage(resp, &got)

	if got.Total != 2 || got.ByBuild["v3"] != 1 || got.ByBuild["v4"] != 1 {
		t.Errorf("got %+v, want one worker each on v3 and v4", got)
	}
}

// Paging accumulates rather than replacing: a fleet larger than one page must
// not report only its last page.
func TestPagesAccumulate(t *testing.T) {
	got := WorkerCount{ByBuild: map[string]int{}}
	for range 3 {
		countPage(&workflowservice.ListWorkersResponse{
			Workers: []*workerpb.WorkerListInfo{{DeploymentVersion: version("v2")}},
		}, &got)
	}

	if got.Total != 3 || got.ByBuild["v2"] != 3 {
		t.Errorf("got %+v, want 3 workers on v2", got)
	}
}

// A worker that reported no Build ID is bucketed, not attributed to a version
// it never claimed — the control-plane workers are deliberately unversioned.
func TestUnversionedWorkersAreBucketedSeparately(t *testing.T) {
	resp := &workflowservice.ListWorkersResponse{
		Workers: []*workerpb.WorkerListInfo{
			{DeploymentVersion: version("v1")},
			{}, // no deployment version at all
			{DeploymentVersion: version("")},
		},
	}

	got := WorkerCount{ByBuild: map[string]int{}}
	countPage(resp, &got)

	if got.ByBuild[UnversionedBuild] != 2 {
		t.Errorf("%s = %d, want 2", UnversionedBuild, got.ByBuild[UnversionedBuild])
	}
	if got.ByBuild["v1"] != 1 {
		t.Errorf("v1 = %d, want 1", got.ByBuild["v1"])
	}
}
