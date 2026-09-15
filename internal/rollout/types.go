// Package rollout coordinates moving order traffic from one worker version to
// another, safely and without a human watching every step.
//
// The coordinator is itself a Temporal Workflow. That is deliberate: a rollout
// is a long-running, stateful, interruptible process that must survive the
// failure of whatever started it — which is exactly what Workflows are for.
// It runs on its own task queue with unversioned workers, because it cannot be
// pinned to the deployment whose routing it is changing.
package rollout

import (
	"time"

	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/metrics"
	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/orders"
)

// Names used by the coordinator.
const (
	// TaskQueue is polled by the unversioned control-plane worker.
	TaskQueue = "control"

	// WorkflowTypeName is the rollout coordinator Workflow.
	WorkflowTypeName = "VersionRollout"

	// WorkflowID makes the rollout a singleton: one rollout per deployment at
	// a time, so a second attempt is rejected cleanly instead of racing.
	WorkflowID = "rollout"
)

// The control surface. These are Updates rather than Signals so the dashboard
// gets a synchronous accept or reject — pressing Pause on a finished rollout
// should say so, not quietly do nothing.
const (
	UpdatePause   = "pause"
	UpdateResume  = "resume"
	UpdateSetRamp = "setRamp"
	UpdateAdvance = "advance"
	UpdateAbort   = "abort"

	QueryGetState = "getState"
)

// Stage is one step of the ramp plan: send Pct of new orders to the candidate,
// then hold there for Hold while watching health.
type Stage struct {
	Pct  float32       `json:"pct"`
	Hold time.Duration `json:"hold"`
}

// DefaultStages is the built-in ramp plan. It starts deliberately small: 1% of
// a thousand orders a minute is still ten orders a minute, which is plenty to
// judge a version on while capping the blast radius at one order in a hundred.
func DefaultStages() []Stage {
	return []Stage{
		{Pct: 1, Hold: 45 * time.Second},
		{Pct: 5, Hold: 45 * time.Second},
		{Pct: 25, Hold: 60 * time.Second},
		{Pct: 50, Hold: 60 * time.Second},
		{Pct: 100, Hold: 30 * time.Second},
	}
}

// GateConfig is the canary gate: a handful of orders sent to the candidate
// version, pinned, before any real traffic is allowed near it.
type GateConfig struct {
	Enabled bool          `json:"enabled"`
	Orders  int           `json:"orders"`
	Timeout time.Duration `json:"timeout"`
}

// DefaultGate is enough probes to catch a broken build without delaying a good
// one for long.
func DefaultGate() GateConfig {
	return GateConfig{Enabled: true, Orders: 3, Timeout: 90 * time.Second}
}

// HealthPolicy decides when a ramp stage has gone wrong.
type HealthPolicy struct {
	// MaxErrorRatePct is the failed-or-stuck percentage that trips a rollback.
	MaxErrorRatePct float64 `json:"maxErrorRatePct"`
	// MinSamples stops a single unlucky order from rolling back a 1% ramp.
	MinSamples int64 `json:"minSamples"`
	// EvalInterval is how often health is sampled during a hold.
	EvalInterval time.Duration `json:"evalInterval"`
}

// DefaultHealthPolicy is tuned for a live demo: strict enough that an injected
// fault trips it within one stage, loose enough that normal variance does not.
func DefaultHealthPolicy() HealthPolicy {
	return HealthPolicy{MaxErrorRatePct: 20, MinSamples: 5, EvalInterval: 10 * time.Second}
}

// Input starts a rollout.
type Input struct {
	// TargetVersion is the friendly label to roll out, e.g. "v4". Any
	// registered version is valid, including one older than the current — a
	// deliberate downgrade is just a rollout in the other direction.
	TargetVersion string `json:"targetVersion"`

	Stages []Stage      `json:"stages"`
	Gate   GateConfig   `json:"gate"`
	Health HealthPolicy `json:"health"`

	// AutoPromote makes the candidate Current once it holds at 100%. Without
	// it the rollout finishes with the candidate taking all new traffic but
	// not yet promoted, which is a real state operators use.
	AutoPromote bool `json:"autoPromote"`

	// GateChaos is the live fault-injection config, stamped in by whoever
	// starts the rollout. It is what makes the gate meaningful: a candidate
	// that has been sabotaged fails its canary orders and never ramps.
	GateChaos *orders.ChaosSpec `json:"gateChaos,omitempty"`

	// RollbackOnFailure restores the pre-rollout routing when health trips or
	// the gate fails. With it off, a failed rollout stops where it is and
	// waits for a human.
	RollbackOnFailure bool `json:"rollbackOnFailure"`
}

// WithDefaults fills in anything the caller left empty.
func (in Input) WithDefaults() Input {
	if len(in.Stages) == 0 {
		in.Stages = DefaultStages()
	}
	if in.Gate.Orders == 0 {
		in.Gate.Orders = DefaultGate().Orders
	}
	if in.Gate.Timeout == 0 {
		in.Gate.Timeout = DefaultGate().Timeout
	}
	if in.Health.MaxErrorRatePct == 0 {
		in.Health.MaxErrorRatePct = DefaultHealthPolicy().MaxErrorRatePct
	}
	if in.Health.MinSamples == 0 {
		in.Health.MinSamples = DefaultHealthPolicy().MinSamples
	}
	if in.Health.EvalInterval == 0 {
		in.Health.EvalInterval = DefaultHealthPolicy().EvalInterval
	}
	return in
}

// Phase is where a rollout has got to.
type Phase string

// Rollout phases. The first four are live; the last four are terminal.
const (
	PhasePending   Phase = "pending"
	PhaseGating    Phase = "gating"
	PhaseRamping   Phase = "ramping"
	PhasePaused    Phase = "paused"
	PhasePromoting Phase = "promoting"

	PhaseCompleted  Phase = "completed"
	PhaseRolledBack Phase = "rolledBack"
	PhaseAborted    Phase = "aborted"
	PhaseGateFailed Phase = "gateFailed"
)

// Terminal reports whether no further progress will happen.
func (p Phase) Terminal() bool {
	switch p {
	case PhaseCompleted, PhaseRolledBack, PhaseAborted, PhaseGateFailed:
		return true
	default:
		return false
	}
}

// GateResult is the outcome of the canary gate.
type GateResult struct {
	Ran      bool     `json:"ran"`
	Passed   bool     `json:"passed"`
	Probes   int      `json:"probes"`
	Failures int      `json:"failures"`
	OrderIDs []string `json:"orderIds,omitempty"`
	Detail   string   `json:"detail,omitempty"`
}

// State is the rollout's live state, served by the getState Query and rendered
// straight onto the dashboard.
type State struct {
	TargetVersion string `json:"targetVersion"`
	TargetBuildID string `json:"targetBuildId"`
	Phase         Phase  `json:"phase"`

	Stages     []Stage `json:"stages"`
	StageIndex int     `json:"stageIndex"`
	CurrentPct float32 `json:"currentPct"`
	// HoldRemainingSec counts down the current stage's hold, so the UI can
	// show progress rather than an opaque wait.
	HoldRemainingSec int `json:"holdRemainingSec"`

	// ManualControl is set once an operator overrides the ramp by hand. The
	// plan stops advancing on its own until they resume.
	ManualControl bool `json:"manualControl"`

	Gate   GateResult     `json:"gate"`
	Health metrics.Health `json:"health"`
	Policy HealthPolicy   `json:"policy"`

	// PreviousRouting is what the routing looked like before this rollout,
	// which is what a rollback restores.
	PreviousCurrentLabel string `json:"previousCurrentLabel"`

	Message   string    `json:"message"`
	StartedAt time.Time `json:"startedAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}
