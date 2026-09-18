package traffic

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/deploy"
	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/orders"
)

// DefaultConcurrency is how many order starts are in flight at once inside one
// batch Activity.
//
// Starting orders is a network round trip each, so concurrency is what makes a
// spike sharp. It is capped because Temporal Cloud applies namespace-level
// rate limits, and a burst that trips them is slower than one that does not.
const DefaultConcurrency = 50

// Activities starts orders on behalf of the generator Workflow.
type Activities struct {
	c           client.Client
	deployment  *deploy.Client
	logger      *slog.Logger
	concurrency int
}

// NewActivities builds the generator Activities. A concurrency of zero uses
// DefaultConcurrency.
//
// The deployment client is needed only for splits, which have to turn a
// friendly version label into the Build ID a pinned override names.
func NewActivities(c client.Client, deployment *deploy.Client, concurrency int, logger *slog.Logger) *Activities {
	if concurrency <= 0 {
		concurrency = DefaultConcurrency
	}
	return &Activities{c: c, deployment: deployment, logger: logger, concurrency: concurrency}
}

// Register wires the generator and its Activity onto a worker.
func (a *Activities) Register(w worker.Registry) {
	w.RegisterWorkflowWithOptions(Director, workflow.RegisterOptions{Name: WorkflowTypeName})
	w.RegisterActivityWithOptions(a.StartOrders, activity.RegisterOptions{Name: ActivityStartOrders})
}

// StartOrders starts one batch of customer orders.
//
// Orders are started with no versioning override, which is the important part:
// Temporal routes each one according to the deployment's Current and Ramping
// config. That is what makes a ramp actually mean something — the generator
// has no idea which version any given order will land on, and does not care.
func (a *Activities) StartOrders(ctx context.Context, req StartOrdersRequest) (StartOrdersResult, error) {
	var (
		result StartOrdersResult
		mu     sync.Mutex
		wg     sync.WaitGroup
	)

	slots := make(chan struct{}, a.concurrency)

	// A split needs Build IDs, and resolving them is one call rather than one
	// per order. Nil when no split is active, which is the common case.
	builds, err := a.resolveSplit(ctx, req.Split)
	if err != nil {
		return StartOrdersResult{}, err
	}

	// Pacing lives here rather than in Workflow timers. A steady rate needs
	// orders spread through the window, and spacing them inside one Activity
	// costs a single action for the whole window — where a Workflow timer per
	// interval costs an action every time it fires.
	var gap time.Duration
	if req.SpreadOver > 0 && req.Count > 1 {
		gap = req.SpreadOver / time.Duration(req.Count)
	}

	// Heartbeat for the whole batch, from its own goroutine.
	//
	// This used to be a non-blocking ticker check inside the spawn loop, which
	// meant it stopped the moment the last order was *spawned* — and a spike
	// spends nearly all its time after that, waiting on wg.Wait(). A batch
	// that took longer than the heartbeat timeout was killed mid-flight, and
	// every start still in flight failed with "context deadline exceeded".
	// Observed on the cloud environment: 26 to 46 of every 500 orders lost,
	// with the server reporting a p99 StartWorkflowExecution latency of 44ms
	// and no rate limiting at all.
	heartbeatDone := make(chan struct{})
	defer close(heartbeatDone)
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-heartbeatDone:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				mu.Lock()
				n := result.Started
				mu.Unlock()
				activity.RecordHeartbeat(ctx, n)
			}
		}
	}()

	for i := range req.Count {
		seq := req.FirstSeq + i

		if gap > 0 && i > 0 {
			select {
			case <-time.After(gap):
			case <-ctx.Done():
				// Cancelled or timed out: stop here and report what went out.
				wg.Wait()
				return result, nil
			}
		}

		wg.Add(1)
		go func() {
			defer wg.Done()

			slots <- struct{}{}
			defer func() { <-slots }()

			in := orders.NewOrderInput(seq, sampleChaos(req.Chaos))
			if req.IDPrefix != "" {
				in.OrderID = fmt.Sprintf("%s-%06d", req.IDPrefix, seq)
			}
			err := a.startOne(ctx, in, builds)

			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				result.Failed++
				if result.Detail == "" {
					result.Detail = fmt.Sprintf("%s: %v", in.OrderID, err)
				}
				return
			}
			result.Started++
		}()
	}

	wg.Wait()

	activity.GetLogger(ctx).Debug("order batch started",
		"requested", req.Count, "started", result.Started,
		"failed", result.Failed, "spreadOver", req.SpreadOver)
	return result, nil
}

// weightedBuild is one version's share of a split, with its Build ID resolved.
type weightedBuild struct {
	label   string
	buildID string
	// upTo is the cumulative share, so picking a version is one comparison
	// against a single random draw.
	upTo float64
}

// resolveSplit turns version labels into Build IDs once per batch.
//
// A label that is not registered is a hard error rather than something to skip:
// silently dropping a version would leave the dashboard showing a split that
// is not the one being served.
func (a *Activities) resolveSplit(ctx context.Context, split Split) ([]weightedBuild, error) {
	if !split.Active() {
		return nil, nil
	}
	if a.deployment == nil {
		return nil, fmt.Errorf("a traffic split needs a deployment client to resolve versions")
	}

	out := make([]weightedBuild, 0, len(split))
	var cumulative float64
	for _, e := range split {
		if e.Pct <= 0 {
			continue
		}
		buildID, err := a.deployment.ResolveLabel(ctx, e.Version)
		if err != nil {
			return nil, fmt.Errorf("split targets %s: %w", e.Version, err)
		}
		cumulative += e.Pct
		out = append(out, weightedBuild{label: e.Version, buildID: buildID, upTo: cumulative})
	}
	if len(out) == 0 {
		return nil, nil
	}

	// Normalise to the actual total so rounding in the UI cannot bias the last
	// version or leave a sliver unassigned.
	for i := range out {
		out[i].upTo = out[i].upTo / cumulative * 100
	}
	return out, nil
}

// startOne starts a single order, either by deployment routing or pinned to a
// version chosen from the split.
func (a *Activities) startOne(ctx context.Context, in orders.OrderInput, builds []weightedBuild) error {
	if len(builds) == 0 {
		// No split: start it unpinned and let the deployment's Current and
		// Ramping config decide. This is what makes a ramp mean anything.
		_, err := a.c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
			ID:        in.OrderID,
			TaskQueue: orders.TaskQueue,
		}, orders.WorkflowTypeName, in)
		return err
	}

	chosen := pickBuild(builds, rand.Float64()*100)
	_, err := a.deployment.StartPinned(ctx, deploy.PinnedStart{
		BuildID:      chosen.buildID,
		WorkflowID:   in.OrderID,
		WorkflowType: orders.WorkflowTypeName,
		TaskQueue:    orders.TaskQueue,
		Arg:          in,
		// Orders are short; this only stops a stuck one living forever.
		RunTimeout: 2 * time.Hour,
	})
	return err
}

// pickBuild selects the version whose cumulative share covers draw.
func pickBuild(builds []weightedBuild, draw float64) weightedBuild {
	for _, b := range builds {
		if draw < b.upTo {
			return b
		}
	}
	return builds[len(builds)-1]
}

// sampleChaos decides whether this order carries the injected fault.
//
// The dice are rolled here, in an Activity, rather than in the Workflow: a
// Workflow must be deterministic on replay, and an order that was sabotaged
// once must stay sabotaged in exactly the same way.
func sampleChaos(cfg ChaosConfig) *orders.ChaosSpec {
	if cfg.Spec == nil || cfg.Pct <= 0 {
		return nil
	}
	if rand.Float64()*100 >= cfg.Pct {
		return nil
	}
	return cfg.Spec
}
