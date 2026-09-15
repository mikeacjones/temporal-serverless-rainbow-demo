package rollout

import "go.temporal.io/sdk/activity"

// activityOptions names an Activity for registration. It exists so tests can
// register stand-ins under the same names the coordinator calls.
func activityOptions(name string) activity.RegisterOptions {
	return activity.RegisterOptions{Name: name}
}
