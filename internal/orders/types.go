package orders

// Shared names. The order Workflow type is the same across every version:
// versioning routes a new order to a Build ID, not to a different type.
const (
	// TaskQueue is polled by every versioned order worker.
	TaskQueue = "orders"

	// WorkflowTypeName is the one order Workflow type, served by all versions.
	WorkflowTypeName = "CustomerOrder"

	// GateWorkflowTypeName is the canary gate. It runs on the same task queue
	// as orders, because the whole point is for it to execute on the candidate
	// version's worker.
	GateWorkflowTypeName = "OrderGate"

	// SignalClearFault tells a stuck order to stop applying the fault it was
	// started with.
	//
	// The fault travels with the order, which is what lets any version be the
	// bad one without shared mutable state — but it also meant a stuck order
	// could never recover, because every retry re-read the same poisoned
	// input. This signal is the way back: clearing the fault in the dashboard
	// sends it to every order still carrying one, and their next attempt runs
	// clean.
	SignalClearFault = "clearFault"
)

// ChaosMode is how an injected fault behaves.
type ChaosMode string

// Fault behaviours. Both are recoverable: neither fails the order outright,
// because a stuck-but-retrying order is the more honest failure to show.
const (
	// ChaosFail makes the step's Activity error, so Temporal retries it
	// durably. The order goes red and stalls rather than failing.
	ChaosFail ChaosMode = "fail"

	// ChaosSlow makes the step take far longer than normal, so the version
	// looks healthy but drags — the subtler regression to catch.
	ChaosSlow ChaosMode = "slow"
)

// ChaosSpec marks one order as a planned casualty.
//
// It is stamped into the order's input at start time by whoever starts the
// order, and is only acted on by the version it names. This is what lets any
// version be the bad one:
//
//   - no global mutable state, which matters because workers may be Lambdas
//     with nothing shared between invocations;
//   - no randomness inside Workflow code, so replay stays deterministic;
//   - the fault travels with the order, so the canary gate inherits it and a
//     poisoned candidate fails its gate before any real traffic moves.
type ChaosSpec struct {
	// TargetVersion is the only version that reacts to this spec.
	TargetVersion Version `json:"targetVersion"`
	// Step is where the fault fires. A step the target version does not have
	// means nothing happens — which is itself a useful thing to demonstrate.
	Step Step `json:"step"`
	// Mode is how the fault behaves.
	Mode ChaosMode `json:"mode"`
}

// appliesTo reports whether this spec targets the given version and step.
func (c *ChaosSpec) appliesTo(v Version, s Step) bool {
	return c != nil && c.TargetVersion == v && c.Step == s
}

// OrderInput starts one customer order.
type OrderInput struct {
	OrderID string `json:"orderId"`
	Store   string `json:"store"`
	Items   []Item `json:"items"`
	// Chaos is set on only the sampled fraction of orders.
	Chaos *ChaosSpec `json:"chaos,omitempty"`
}

// Item is one line on the order.
type Item struct {
	Name string `json:"name"`
	Qty  int    `json:"qty"`
}

// OrderResult is what a completed order reports back.
type OrderResult struct {
	OrderID string  `json:"orderId"`
	Version Version `json:"version"`
	// Steps is the pipeline this order actually ran, which is the pipeline of
	// the version it was pinned to for its whole life.
	Steps []Step `json:"steps"`
}

// OrderState is the live view of an order, served by the getState Query.
//
// The dashboard does not poll this per order at demo scale — it reads search
// attributes in bulk instead. The Query exists for drilling into a single
// order, and for the Temporal UI.
type OrderState struct {
	OrderID     string  `json:"orderId"`
	Version     Version `json:"version"`
	Steps       []Step  `json:"steps"`
	CurrentStep int     `json:"currentStep"`
	// Degraded is set once a step has genuinely failed rather than merely
	// being slow to find a worker.
	Degraded bool `json:"degraded"`
	// FaultCleared records that this order was told to drop its fault.
	FaultCleared bool `json:"faultCleared,omitempty"`
	Done         bool `json:"done"`
}

// StepInput is the Activity payload for one step.
type StepInput struct {
	OrderID string  `json:"orderId"`
	Version Version `json:"version"`
	Step    Step    `json:"step"`
	// Fail and Slow are resolved by the Workflow from the order's ChaosSpec on
	// every attempt, so the Activity needs no knowledge of chaos
	// configuration — and a fault cleared mid-flight takes effect on the next
	// attempt rather than being baked in for the order's lifetime.
	Fail bool `json:"fail,omitempty"`
	Slow bool `json:"slow,omitempty"`
}
