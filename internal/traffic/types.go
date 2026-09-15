// Package traffic generates customer orders at a controlled rate.
//
// The generator is a long-running Workflow rather than a loop in the backend,
// for three reasons: it survives a backend restart in the middle of a
// presentation, its rate schedule is itself durable state anyone can inspect,
// and it is the single source of truth for the fault-injection config that
// both the traffic and the canary gate read.
package traffic

import (
	"time"

	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/orders"
)

// Names used by the generator.
const (
	// TaskQueue is the same unversioned control-plane queue the rollout
	// coordinator uses.
	TaskQueue = "control"

	WorkflowTypeName = "TrafficDirector"

	// WorkflowID is fixed: there is one traffic generator, and asking for it
	// by name is how the backend and the rollout both find it.
	WorkflowID = "traffic-director"

	ActivityStartOrders = "StartOrders"
)

// The control surface.
const (
	UpdateSetSplit    = "setSplit"
	UpdateSpikePinned = "spikePinned"
	UpdateSetRate     = "setRate"
	UpdateSpike       = "spike"
	UpdateSetChaos    = "setChaos"
	UpdateStop        = "stop"

	QueryGetState = "getState"
)

// Limits, generous enough for a demo and low enough to avoid an accident.
const (
	MaxRatePerMin = 20000
	MaxSpike      = 20000
)

// ChaosConfig is the live fault-injection setting.
type ChaosConfig struct {
	// Spec names the version and step to break. Nil means no faults.
	Spec *orders.ChaosSpec `json:"spec,omitempty"`
	// Pct is the percentage of new orders that carry the fault. Sabotaging
	// every order is rarely the interesting case; a partial rate is what a
	// real bad deploy looks like.
	Pct float64 `json:"pct"`
}

// DefaultMaxRun is how long traffic keeps flowing without anyone touching it.
//
// It exists because this demo can point at Temporal Cloud with workers on AWS
// Lambda, where traffic left running overnight quietly bills for every order
// and every worker invocation. Any operator action pushes the deadline out, so
// it only ever fires when the demo has actually been abandoned.
const DefaultMaxRun = 20 * time.Minute

// SplitEntry sends a share of new orders to one version.
type SplitEntry struct {
	Version string  `json:"version"`
	Pct     float64 `json:"pct"`
}

// Split routes new orders across several versions at once — a rainbow
// deployment, with v3, v4 and v5 all serving live traffic simultaneously.
//
// This cannot be done with a ramp, and the reason is worth understanding: a
// Worker Deployment's routing config holds exactly one Current version and one
// Ramping version with one percentage. A ramp is a two-way split by
// construction, so no sequence of SetRampingVersion calls can put three
// versions in service at once.
//
// So a split does not use deployment routing at all. Each order is started
// with a **pinned versioning override** naming the version it should run on,
// chosen per order against these weights. That is a documented Temporal
// capability rather than a trick — it is the same mechanism you would use to
// route one customer segment to a specific build — and because the override is
// Pinned, every order still runs its whole life on one version, so the
// in-flight guarantee is unchanged.
//
// The trade-off to be honest about on stage: while a split is active, the
// deployment's Current and Ramping settings no longer decide where generated
// orders go. The client does.
type Split []SplitEntry

// Total returns the sum of every share.
func (s Split) Total() float64 {
	var total float64
	for _, e := range s {
		total += e.Pct
	}
	return total
}

// Active reports whether this split should override deployment routing.
func (s Split) Active() bool {
	return len(s) > 0 && s.Total() > 0
}

// Input starts or continues the generator.
type Input struct {
	RatePerMin int         `json:"ratePerMin"`
	Chaos      ChaosConfig `json:"chaos"`

	// Split spreads new orders across several versions at once. Empty means
	// orders are routed by the deployment's own Current/Ramping config.
	Split Split `json:"split,omitempty"`

	// MaxRun is how long traffic may flow untouched before stopping itself.
	// Zero means DefaultMaxRun.
	MaxRun time.Duration `json:"maxRun"`
	// StopAt carries the deadline across Continue-As-New so a restart does not
	// silently reset the clock.
	StopAt time.Time `json:"stopAt"`
	// NextSeq carries the order numbering across Continue-As-New so order IDs
	// stay unique and monotonic for the whole demo.
	NextSeq int `json:"nextSeq"`
	// Started carries the cumulative count across Continue-As-New.
	Started int `json:"started"`
}

// State is the generator's live state, served by the getState Query.
type State struct {
	RatePerMin int         `json:"ratePerMin"`
	Chaos      ChaosConfig `json:"chaos"`
	Running    bool        `json:"running"`
	// Started is the cumulative number of orders this generator has started.
	Started int `json:"started"`
	// NextSeq is the next order number to be used.
	NextSeq int `json:"nextSeq"`
	// PendingSpike is a burst that has been requested but not yet fired.
	PendingSpike int `json:"pendingSpike"`
	LastSpike    int `json:"lastSpike"`
	// LastSpikeVersion names the version the last burst was pinned to, if any.
	LastSpikeVersion string    `json:"lastSpikeVersion,omitempty"`
	UpdatedAt        time.Time `json:"updatedAt"`

	// Split is the active multi-version split, if any.
	Split Split `json:"split,omitempty"`

	// StopAt is when traffic will stop itself if nobody touches it.
	StopAt time.Time `json:"stopAt"`
	// AutoStopped records that the deadline is why the rate is zero, so the
	// dashboard can say so rather than looking as though someone stopped it.
	AutoStopped bool `json:"autoStopped"`
}

// SpikeRequest dumps a burst of orders.
type SpikeRequest struct {
	Count int `json:"count"`
	// Version pins every order in the burst to one version, bypassing
	// deployment routing. Empty lets routing decide, as a normal spike does.
	//
	// Pinning a burst is the quickest way to show a specific version's workers
	// come to life: an idle version has no Lambdas running at all, so dumping
	// orders on it makes Temporal start them from nothing while you watch.
	Version string `json:"version,omitempty"`
}

// StartOrdersRequest asks the Activity to start a batch of orders.
type StartOrdersRequest struct {
	Count    int         `json:"count"`
	FirstSeq int         `json:"firstSeq"`
	Chaos    ChaosConfig `json:"chaos"`

	// SpreadOver paces the batch across this duration instead of starting it
	// all at once. This is where steady-state pacing lives: doing it here
	// costs one action for the whole window, where a Workflow timer per
	// interval costs one per tick.
	//
	// Zero means start everything as fast as the fanout allows, which is what
	// a spike wants.
	SpreadOver time.Duration `json:"spreadOver"`

	// Split, when set, assigns each order a version and starts it pinned to
	// that version instead of letting deployment routing decide.
	Split Split `json:"split,omitempty"`
}

// StartOrdersResult reports how a batch went. Failures are counted rather than
// raised: losing a few starts in a burst should not fail the generator.
type StartOrdersResult struct {
	Started int    `json:"started"`
	Failed  int    `json:"failed"`
	Detail  string `json:"detail,omitempty"`
}
