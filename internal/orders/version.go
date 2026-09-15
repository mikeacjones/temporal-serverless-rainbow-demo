package orders

// Version identifies one shape of the customer-order pipeline.
//
// Every version is a separate Worker Deployment Version in Temporal, but they
// all register the same Workflow type (WorkflowTypeName). A worker process
// serves exactly one version, chosen at startup, and publishes its label as
// Worker Deployment Version metadata so the dashboard never has to decode
// opaque Build IDs.
type Version string

// The five pipeline shapes shipped by this demo.
const (
	V1 Version = "v1"
	V2 Version = "v2"
	V3 Version = "v3"
	V4 Version = "v4"
	V5 Version = "v5"
)

// AllVersions lists every version this build knows how to serve, in order.
var AllVersions = []Version{V1, V2, V3, V4, V5}

// Step is one stage of an order's journey. Steps are the unit of work: each one
// becomes a single Activity execution.
type Step string

// The steps that appear across the five pipelines.
const (
	StepReceived       Step = "Received"
	StepFraudCheck     Step = "Fraud check"
	StepPayment        Step = "Payment"
	StepLoyalty        Step = "Loyalty accrual"
	StepPrep           Step = "Prep"
	StepMobileDispatch Step = "Mobile pickup dispatch"
	StepDriveThru      Step = "Drive-thru handoff"
	StepHandoff        Step = "Handoff"
)

// StepsFor returns the ordered pipeline for a version.
//
// The differences between versions are deliberately varied, so a rollout
// between any two of them is a genuinely different change:
//
//	v1 -> v2   appends a step
//	v2 -> v3   inserts a step before an existing one
//	v3 -> v4   appends a step late in the pipeline
//	v4 -> v5   restructures: drops two steps and swaps the handoff
func StepsFor(v Version) []Step {
	switch v {
	case V1:
		return []Step{StepReceived, StepPayment, StepPrep, StepHandoff}
	case V2:
		return []Step{StepReceived, StepPayment, StepLoyalty, StepPrep, StepHandoff}
	case V3:
		return []Step{StepReceived, StepFraudCheck, StepPayment, StepLoyalty, StepPrep, StepHandoff}
	case V4:
		return []Step{
			StepReceived, StepFraudCheck, StepPayment, StepLoyalty,
			StepPrep, StepMobileDispatch, StepHandoff,
		}
	case V5:
		return []Step{StepReceived, StepPayment, StepLoyalty, StepPrep, StepDriveThru}
	default:
		return nil
	}
}

// ParseVersion validates a version label, e.g. from the ORDER_VERSION env var.
func ParseVersion(s string) (Version, bool) {
	for _, v := range AllVersions {
		if Version(s) == v {
			return v, true
		}
	}
	return "", false
}
