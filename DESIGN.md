# Rainbow Deploys — design

A single demo that combines **elastic scale** (from
`lainecsmith/temporal-serverless-no-roads`) with **safe versioned
deploys** (from `temporal-sa/temporal-versioning-demo`), themed
around a quick-service coffee-and-baked-goods ordering system.

Two headline additions over the source demos:

1. **Any-to-any automated rollouts.** Pick any registered version,
   hit *Start deployment*, and a Temporal Workflow coordinates the
   whole rollout — canary gate, staged ramp, health validation,
   auto-promote, auto-rollback — while you retain manual control
   (pause, resume, jump to an arbitrary ramp %, abort).
2. **Traffic shaping.** Sustained steady-state order rate (e.g.
   1,000 orders/min) plus arbitrary spikes (dump 5,000 at once),
   driven by a long-running Workflow.

---

## Decisions taken

| Decision | Choice |
| --- | --- |
| Runtime | Hybrid: local dev-server for iteration, Temporal Cloud + Lambda Serverless Workers for the live run. Same binaries, env-switched. |
| Canary gate | A gate Workflow **pinned to the candidate Build ID**, mirroring temporal-worker-controller's `gate`. Ramping cannot begin unless it succeeds. |
| Traffic engine | `TrafficDirectorWorkflow` — a long-running Workflow holding target rate as mutable state, driven by Updates. |
| Bad versions | Per-order **chaos injection stamped at start time**, targetable at any version. Any version can be the villain. |
| Language / stack | Go, single module. **Backend is a pure JSON + SSE API and renders no HTML**; the dashboard is a standalone static frontend in its own nginx container. Requested explicitly, and a better split than the versioning demo's server-rendered HTMX fragments, which put markup inside Go. |
| Packaging | Everything containerised from the start, with Kubernetes manifests, so the move to k8s is a deploy rather than a port. |

---

## Why versions are never "shipped" during the demo

The versioning demo ships a version by swapping a Kustomize image
tag, so the only reachable path is v1 → v2 → v3 in order. That is
exactly what blocks arbitrary version selection.

Here, **every version is registered and polling from the moment the
demo starts**. All N versioned workers run simultaneously (N local
processes, or N Lambda aliases wired as N Worker Deployment
Versions). *Start deployment* is then purely a **routing**
operation — `SetRampingVersion` / `SetCurrentVersion` — which is
what makes v1 → v4, v4 → v2, and backwards rollouts all equally
possible.

Each worker publishes its friendly label (`v1`…`v5`) as Worker
Deployment Version **metadata** under the `orderVersion` key, so
the UI and the rollout coordinator never have to decode opaque
Build IDs. (Pattern lifted directly from the versioning demo's
`pizzaVersion`.)

---

## Order pipeline versions

One Workflow type, `CustomerOrder`, five shapes, all
`VersioningBehaviorPinned`:

| Version | Pipeline | What it demonstrates |
| --- | --- | --- |
| `v1` | Received → Payment → Prep → Handoff | Baseline, 4 steps |
| `v2` | Received → Payment → **Loyalty accrual** → Prep → Handoff | Additive step |
| `v3` | Received → **Fraud check** → Payment → Loyalty accrual → Prep → Handoff | Step inserted *before* payment |
| `v4` | Received → Fraud check → Payment → Loyalty accrual → Prep → **Mobile pickup dispatch** → Handoff | 7 steps |
| `v5` | Received → Payment → Loyalty accrual → Prep → **Drive-thru handoff** | Restructured, not just additive |

In-flight orders stay pinned to the shape they started on, so a
rollout never changes an order already being made.

### Work-duration profiles

Throughput and worker-occupancy are in tension: 1,000 orders/min
at ~8s of activity occupancy per order needs ~130 concurrent
activity slots. So an order's duration is a profile, not a constant.

It is an **end-to-end budget** divided across the version's steps,
rather than a per-step duration, so every version's order takes about
the same time. Pipelines differ in length by nearly two to one, and
without this a rollout would change how long customers wait as well as
what the order does — conflating two things the demo wants to show
separately.

| Profile | Whole order | Use |
| --- | --- | --- |
| `fast` | 1 s | Local. High order rates with a few worker processes. |
| `demo` | 8 s | The presentation setting. |
| `heavy` | 40 s | The no-roads setting; saturates deliberately. |

`TestOrdersTakeTheSameTimeWhateverTheirShape` pins the property.

---

## Chaos injection (how a version goes bad)

No global mutable state — which matters because the workers are
Lambdas with no shared memory, and because Workflow code must stay
deterministic.

Instead the **traffic engine rolls the dice and stamps the order**:

```go
type ChaosSpec struct {
    TargetVersion string // "v4" — only this shape reacts
    Step          string // "payment" | "fraudCheck" | "handoff" | ...
    Mode          string // "fail" (retry forever) | "slow" | "reject"
}

type OrderInput struct {
    OrderID int
    Items   []Item
    Chaos   *ChaosSpec // non-nil on the sampled fraction only
}
```

Each version's Workflow checks `Chaos.TargetVersion == myVersion`
and, at the matching step, passes `failMe: true` to the activity.
The activity errors, Temporal retries durably, the order goes red
and stalls — the pizza-demo v3 behaviour, but aimable at any
version and switchable mid-presentation.

Consequences that make the demo work:

- The **canary gate** starts its gate Workflow with the live chaos
  config applied to the candidate, so a poisoned candidate fails
  its gate and the rollout never ramps at all.
- You can roll out v4 cleanly, then re-roll v4 with chaos on, in
  the same session.

### Making stalls countable

A stalled order stays `Running` with an activity retrying, which
visibility cannot see. So the order Workflow **upserts a
search attribute** (`OrderHealth = "degraded"`) once a step really
fails, giving the rollout coordinator a countable stall signal.

**This is where the original plan was wrong, and it matters.** The first
implementation timed each step and flagged any that ran longer than a
threshold. That conflates two unrelated things:

- a step that is *broken*, and
- a step that is *waiting for a worker*.

Under the spike this demo exists to show, every healthy order breached
the threshold, the candidate version read as 100% unhealthy, and the
rollout rolled itself back during exactly the traffic surge it was
supposed to survive. Verified live: a 2,000-order spike produced 27,190
"stuck" orders and a spurious rollback.

The fix is to judge a step on **execution** time, never on
schedule-to-completion:

- `StartToCloseTimeout` (30s) bounds how long a step may *run*. Queue
  time is excluded, so a backlog cannot trip it.
- No `ScheduleToStartTimeout` at all: queuing is not failure.
- Activity retries are **bounded** (3). When they are exhausted the step
  has genuinely failed, and only then is the order marked degraded — after
  which the Workflow keeps retrying it on its own, so the order stalls
  rather than failing.

A slowed step is therefore caught by exceeding its 30s execution budget,
which is why `ChaosSlow` uses an absolute 45s rather than a multiple of
the profile — a multiplier would trip under `heavy` and go unnoticed
under `fast`.

`TestStepBudgetSeparatesBrokenFromBusy` pins the invariant: the budget
must sit above the slowest healthy step in every profile and below a
deliberately slowed one.

---

## RolloutWorkflow

Runs on a **separate, unversioned task queue** (`control`) — it
cannot be pinned to the deployment it is mutating.

```go
type RolloutInput struct {
    TargetVersion     string        // friendly label, e.g. "v4"
    Stages            []Stage       // {Pct, Hold} — defaults 1/5/25/50/100
    Gate              GateConfig    // Enabled, Orders, Timeout
    Health            HealthPolicy  // ErrorRatePct, StallPct, MinSamples, EvalInterval
    AutoPromote       bool          // SetCurrentVersion on reaching 100%
    RollbackOnFailure bool
}
```

States: `Pending → Gating → Ramping(stage i) → Promoting →
Completed`, with `Paused`, `RolledBack`, `Aborted` as exits.

**Control surface** — Updates (synchronous accept/reject, so the UI
can show a rejection reason) plus one Query:

| Handler | Effect |
| --- | --- |
| `pause` / `resume` | Freeze at the current ramp % — traffic keeps flowing, the clock stops |
| `setStage(pct)` | Jump to an arbitrary ramp %, forwards or backwards |
| `advance` | Skip the remaining hold, go to the next stage now |
| `abort(rollback bool)` | Stop; optionally restore the pre-rollout routing |
| `updatePolicy` | Retune health thresholds mid-flight |
| `getState` (Query) | Full state for the UI |

Cancellation runs its rollback in a `workflow.NewDisconnectedContext`
so the restore completes even on cancel.

Singleton per deployment: Workflow ID `rollout-<deployment>` with
`WorkflowIDConflictPolicy: Fail`, so a second *Start deployment*
returns a clean "rollout already in progress" instead of racing.

### Activities

| Activity | Temporal API |
| --- | --- |
| `SnapshotRouting` | `DescribeWorkerDeployment` — captures pre-rollout Current/Ramping for rollback |
| `ResolveVersion(label)` | `DescribeVersion` metadata → Build ID |
| `RunGate(buildID, n, timeout)` | Starts *n* gate Workflows **pinned** to the candidate, waits for success |
| `SetRamp(buildID, pct)` | `SetRampingVersion` (`AllowNoPollers`, `IgnoreMissingTaskQueues`) |
| `SetCurrent(buildID)` | `SetCurrentVersion` |
| `ClearRamp` | `SetRampingVersion` with empty Build ID + `Percentage: 0` |
| `SampleHealth(buildID, window)` | `CountWorkflowExecutions` over the versioning search attributes |

### Pinning the gate Workflow — verified mechanism

The Go SDK's `client.StartWorkflowOptions` has **no**
`VersioningOverride` field (checked against SDK v1.48.0). The raw
gRPC request does:
`StartWorkflowExecutionRequest.VersioningOverride` (field 25) with
`VersioningOverride_Pinned`. So `internal/deploy` wraps
`client.WorkflowService().StartWorkflowExecution` with a small
`StartPinned()` helper that encodes payloads through the data
converter. This is the same override the versioning demo applies
*post-reset*; here it is applied *at start*.

### Health sampling — verified search attributes

Confirmed present on server 1.31.2:

- `TemporalWorkerDeploymentVersion` (Keyword) — **`<deployment>:<buildID>`**,
  with a colon. Verified empirically: the proto comment and the
  `versioningInfo.version` field both use a *dot*, but the search
  attribute does not, and the dot form silently matches zero rows.
- `TemporalUsedWorkerDeploymentVersions` (KeywordList)
- `TemporalWorkflowVersioningBehavior` (Keyword)
- plus our custom `OrderHealth` (Keyword)

Health per stage is `CountWorkflowExecutions` for failed / degraded
/ completed counts filtered to the candidate version, gated on
`MinSamples` so a 1% ramp does not roll back on a single blip.

---

## TrafficDirectorWorkflow

Singleton Workflow ID `traffic-director`, also on the `control`
queue.

- Ticks on a short timer; each tick starts
  `rate × tick / 60` orders via a `StartOrderBatch` activity that
  fans out the actual starts with bounded concurrency.
- **Updates**: `setRate(n)`, `spike(n)`, `setChaos(cfg)`, `stop`.
- A 5,000-order spike fans out across several parallel activities
  so the burst is sharp; fanout concurrency is configurable
  because Temporal Cloud applies namespace-level rate limits.
- Continue-As-New on a tick budget to keep history bounded.

Why a Workflow rather than a goroutine: it survives a backend
restart mid-presentation, and the rate schedule itself becomes
demo material.

---

## Task queues

| Queue | Workers | Versioned |
| --- | --- | --- |
| `orders` | All N order workers (v1…v5) + the gate Workflow type | Yes — Pinned |
| `control` | Rollout coordinator, traffic director, their activities | No |

---

## Dashboard

Built on the versioning demo's Go backend: server-rendered HTMX
fragments pushed over SSE.

**Read path had to change for scale.** The versioning demo
`getState`-Queries every open Workflow, which dies at 1,000
orders/min. Here, the order Workflow upserts search attributes
(version, step, health) so that:

- **aggregates** come from `CountWorkflowExecutions` (grouped), and
- the **live order strip** is a *sampled* `ListWorkflowExecutions`
  page (most recent ~40), not every open order.

Panels (as rebuilt on the `ui-redesign` branch):

1. **Top strip** — orders/min in, served/min, in flight, queued, median
   order time, and stuck (which appears only when something is stuck).
2. **Order routing** — the hero. Three segmented bars: where new orders
   are sent, where orders actually are, and where the workers actually
   are. The gap between the first two is the pinning guarantee; the third
   is empty at rest and fills per version as work arrives. A live
   deployment's controls sit in this panel's header, beside the bars they
   move, with its stage rail, health readout and canary result below.
3. **Order flow** — steady rate with presets, and burst triggers.
4. **Worker fleet** — workers running, sync match rate, backlog task
   queue, longest wait, and a peak-worker sparkline.
5. **Break a version** — target version, step, mode and blast radius.
6. **Versions & pipelines** — a card per version: routing role, share of
   new orders, its own live worker count, in-flight and served counts,
   its pipeline as a ladder, and buttons to start a deployment, send
   orders straight to it, or rescue orders stranded on it.
7. **On the rail** — sampled live orders, **oldest first**, each drawing
   its journey as one dot per step of *its* version's pipeline.

Recovery is a **server-side batch**: one `StartBatchOperation` with a
`BatchOperationReset` whose `PostResetOperations` pin each new run to the
healthy Current build. That is better than the per-order loop originally
planned — durable, throttled, surviving a backend restart, and one API
call rather than thousands. Verified live: 37 stranded orders recovered
to zero.

### The rail has to be followable

Two properties, and both took a deliberate fix:

**Stable order.** Visibility returns orders newest-first, so every
ticket moved every second and no single order could be watched. The rail
is ordered oldest-first, which turns it into a conveyor: an order joins
at the end, rises as those ahead complete and drop off, and leaves from
the front.

**Stable identity.** Rebuilding the list each tick destroyed and
recreated every ticket, restarting the pulse on the live step dot — the
one thing showing an order is alive. Tickets are now reconciled by order
ID, and existing nodes are handed back to `replaceChildren`, which moves
them rather than recreating them. Measured at 240 orders/min: 93% of
tickets persist between consecutive frames.

The sample size (60) follows from this. Visibility returns the newest N,
so too small an N at a high order rate drops an order out of the window
before it finishes, and it appears to vanish halfway through.

---

## Repo layout

```
cmd/backend/         dashboard + API + SSE
cmd/worker/          versioned order worker (ORDER_VERSION selects the shape)
cmd/controlworker/   unversioned control-plane worker (rollout + traffic)
cmd/lambdaworker/    Lambda entrypoint around the versioned worker
internal/orders/     the five Workflow shapes, activities, chaos, types
internal/rollout/    RolloutWorkflow, gate Workflow, activities
internal/traffic/    TrafficDirectorWorkflow, StartOrderBatch
internal/deploy/     Worker Deployment API wrapper, label resolver, StartPinned
internal/dashboard/  state model, poller, SSE hub, render, actions, server
internal/config/     env + Temporal client options (local / Cloud API key)
frontend/            embedded SPA
deploy/local/        run N versioned workers locally
deploy/aws/          Lambda build + per-version alias wiring, CFN roles
```

---

## Open items

- **Branding.** Built as a generic coffee-and-baked-goods QSR
  theme with Temporal styling — no third-party logos or
  trademarks. Adjustable on request.
- **Cloud cost/limits.** 1,000 orders/min for a 30-minute session
  is ~30k Workflows; namespace rate limits apply to the 5,000-order
  spike. Worth a dry run against the real namespace.


---

## Corrections found while building

Recorded because each one was a real trap, and each is easy to
reintroduce.

**The search attribute uses a colon.** Temporal's proto comments describe
the pinned version as `<deployment>.<build_id>`, and a Workflow's
`versioningInfo.version` field does use a dot — but the
`TemporalWorkerDeploymentVersion` search attribute is
`<deployment>:<build_id>`. The dot form is not an error; it silently
matches zero rows, so every health check would read as "perfectly
healthy".

**`ORDER BY` is rejected outright.** `ListWorkflowExecutions` with an
`ORDER BY` clause fails with *"operation is not supported"* on the dev
server, and is restricted on Cloud. It is also unnecessary: open
executions already come back newest-first.

**The SDK cannot start a pinned Workflow.** `client.StartWorkflowOptions`
has no versioning-override field (checked against SDK v1.48.0), so the
canary gate goes through the raw
`StartWorkflowExecutionRequest.VersioningOverride` (field 25). Without
this the gate could not run on a candidate that is taking no traffic —
which is the whole point of a gate.

**Heartbeat on a timer, not per unit of work.** The gate Activity
originally heartbeated once before waiting on each canary order. A canary
order takes longer than the heartbeat timeout, so the Activity died of a
heartbeat timeout while the orders it was watching completed
successfully — blocking a *healthy* deploy. It now waits on all probes
concurrently and heartbeats on a ticker.

**Eager activity dispatch hides the whole scale story.** With it on,
Activity tasks go straight to the worker that completed the Workflow
task instead of through the queue, so `TasksAddRate` and
`TasksDispatchRate` stay at zero — and backlog and sync-match rate read
as "nothing is happening" no matter how much load is applied. Both
workers set `DisableEagerActivities: true`.

**Advancing after a manual ramp skipped a stage.** A manual ramp moves
`StageIndex` to the next *unrun* stage, whereas a planned stage leaves it
pointing at the stage currently applied. Incrementing in both cases meant
"jump to 40%, then continue" finished the rollout at 40% without ever
applying 100%.

**Take the narrowest interface.** `orders.Register` originally took
`worker.Registry`, which a Lambda serverless worker does not satisfy even
though it has the registration methods. It now declares its own
two-method `orders.Registry`, so both host kinds can run the same
pipeline.

**"Sync match rate" was measuring the wrong thing.** Inherited from the
no-roads demo, it was derived from whether the backlog was growing:

```
1 - (addRate - dispatchRate) / addRate
```

Clamped to [0, 100], that reads **100% whenever the backlog is
shrinking** — so a queue of 324 tasks draining steadily displayed as
"100% handed straight to a worker", which is the opposite of the truth.
It measured the backlog's *direction* and presented it as the backlog's
*absence*.

Temporal exposes no true sync-match counter, but `TaskQueueStats` does
carry `ApproximateBacklogAge` — how long the oldest queued task has been
waiting. That is both honest and a better thing to show: "orders are
waiting 40 seconds for a worker" lands harder than any percentage. The
add and dispatch rates are now shown as themselves, as a plain
"falling behind / keeping up / catching up" line, rather than being
folded into a single misleading number.


**Sync match is real, but it is not in the task-queue API.** Having
removed the derived figure above, the honest version was still worth
showing, because it is the signal Temporal scales serverless workers on.
It comes from the *server's* Prometheus endpoint as two cumulative
counters:

```
poll_success       every task delivered to a poller
poll_success_sync  those delivered by a direct match
```

(Newer servers report the same things under a `pri_` prefix, from the
priority matcher, so both families are summed.) The backend scrapes them
and takes the rate over the interval between two polls — a lifetime
average would be dominated by whatever happened first in the session.

It needs `--metrics-port` on the dev server and `TEMPORAL_METRICS_URL`
on the backend; without them the gauge says it is unavailable rather
than showing a confident zero. Verified live: ~100% while workers idle,
collapsing to 35–70% under a 1,500-order spike, where the old derived
figure sat at 100% throughout.

One operational footgun found on the way: recreating only the `temporal`
container (`docker compose up -d temporal`) wipes the dev server's
in-memory state, which unregisters the Worker Deployment. Orders then
queue with nothing to match them, and because the backend bootstraps a
Current version only at *its* startup, nothing repromotes v1. Restart
the whole stack rather than that one service.

**Serverless workers do not idle at zero.** Measured on the deployed
sa-demo environment: with traffic stopped and every order finished,
about 18 Lambda workers stayed alive — and CloudWatch showed the
invocations were **all on the Current version** (18 in six minutes on
v3; exactly zero on v1, v2, v4 and v5). Temporal keeps a warm pool on
whichever version is taking traffic so that live orders never pay a cold
start, and lets the idle versions go fully cold.

Two consequences worth knowing before quoting numbers on stage:

- "Scales to zero" is true of *versions not taking traffic*, and of
  burst capacity after a spike. It is not true of the Current version
  while it is registered.
- Lowering the Lambda timeout does not shrink that pool. A worker polls
  until its invocation deadline and then exits (it does not exit early
  when idle), so a shorter timeout means more, shorter invocations for
  roughly the same GB-seconds. It does make *burst* capacity drain much
  faster, which is the part that shows on the dashboard — 600s was a
  ten-minute wait after a spike, 120s is about two.

What actually removes the standing cost is tearing the Lambdas down
between demos (`make lambda-teardown`), which is why that script exists.

## The generator was burning actions for nothing

The first traffic generator ticked on a two-second timer regardless of
whether any traffic was wanted. Measured on the deployed namespace: **640
history events in four minutes while completely idle** — about 160
events a minute — and a Continue-As-New every ten minutes, all afternoon,
producing zero orders. In Temporal Cloud terms that is roughly 86,000
actions a day to do nothing.

Two things were wrong, and they have different fixes.

**Idle should cost nothing.** The generator now blocks on a Selector over
two channels (spikes, and everything else) fed by its Update handlers. A
Workflow waiting with nothing scheduled generates no history and no
actions at all. Verified: **0 events over three minutes idle**, against
~480 before.

**Pacing belongs in an Activity, not in Workflow timers.** A Workflow
timer costs an action every time it fires; an Activity costs one action
however long it runs. So the Workflow now holds a single paced Activity
in flight per 30-second window, and the Activity dribbles its orders out
across that window. Verified: **30 events per two minutes of running
traffic**, against ~320 before — about ten times cheaper — while still
holding the rate exactly (90 orders per 30s window at 180/min).

Responsiveness was kept deliberately: a spike fires a second Activity
immediately alongside the paced one rather than waiting for the window to
end, and a rate change cancels the in-flight window so the new rate takes
effect at once.

### It does not run on Lambda

Worth stating plainly, because it is easy to assume otherwise: the
traffic generator and the rollout coordinator run on the **`control`**
task queue, served by an ordinary always-on worker (a pod in the
cluster), registered as UNVERSIONED. The Lambdas only poll **`orders`**.
So the generator's action burn was never what kept serverless workers
warm — it could not be, because it puts nothing on the orders queue. The
warm workers were Temporal's own pool on the Current version.

### Changing control-plane code breaks running control-plane Workflows

Rewriting the generator produced exactly the failure this demo is about:

```
[TMPRL1100] During replay, a matching Timer command was expected in
history event position 5. However, the replayed code did not produce that.
```

The old run's history contains a timer the new code never creates. The
control plane is unversioned on purpose — it must not be pinned to the
versions it manages — so it gets none of the protection the order
Workflows have. Its singleton Workflows hold no state worth keeping, so
the answer is to terminate them and let fresh runs start:

```bash
make reset-control
```

## Two mistakes in the serverless deployment

**Reserved concurrency is a throttle, not a guardrail.** Capping each
worker function at 100 concurrent executions — added to protect a shared
AWS account — pinned a 5,000-order burst at 695 orders/min and produced
**1,495 Lambda throttles against 189 successful invocations**. Temporal
scales serverless workers out on backlog; AWS was refusing, so the
backlog simply sat there. Removing the reservation took the same burst to
4,400 orders/min immediately.

It is worth being precise about why the instinct was wrong. Reserved
concurrency does not limit *total* usage, it *reserves* a slice of the
account pool for one function and forbids that function from exceeding
it. For five functions where only one takes traffic at a time, that
divides the pool five ways and caps the one version doing the work at a
fifth of the account. The account limit was already the guardrail.

Separately, the Lambda entrypoint defaulted to five concurrent
Activities per worker, so draining a burst needed hundreds of concurrent
invocations — which is itself what triggers throttling. Twenty slots does
the same work with roughly a fifth of the workers. The steps only sleep,
so they do not compete for CPU.

**A metrics line's last field is not its value.** Prometheus text allows
an optional trailing timestamp, and Temporal Cloud's OpenMetrics output
includes one:

```
temporal_cloud_v1_poll_success_count{…} 242.783 1789489980
```

Reading the last whitespace-separated field works perfectly against a
self-hosted server, which omits the timestamp, and silently returns the
timestamp on Cloud. Because every counter in a scrape shares the same
timestamp, the sync match rate came out as exactly **100.0%** — a
confident, entirely wrong number, which is the worst kind. The value is
the first field after the closing brace.
`TestParseSampleIgnoresTrailingTimestamp` pins it.

## Two dashboard bugs that only showed up during a rollout

**The Deployment panel flickered to "No deployment running".** The
snapshot builder read everything *sequentially* inside a five-second
budget: describe the deployment, count totals, describe two task queues,
list recent orders, then five versions × four counts each — roughly
thirty cross-region round trips, because the backend runs in us-west-1
and the namespace lives in ca-central-1. The rollout Query was dead last
in that queue, so it was the first thing to be starved when the budget
ran out — and a failed Query rendered as "no rollout", which during a
rollout is not a cosmetic glitch but an outright lie.

Fixed at both ends:

- every read is now issued **concurrently**, so the budget is
  per-round-trip rather than per-call;
- the four counts inside each version's health run together, turning
  twenty round trips into one, and the result is cached for three
  seconds because those numbers move slowly;
- a failed read **carries forward the previous snapshot's value** instead
  of zeroing it. A Query that timed out does not mean the rollout went
  away.

Verified: 0 blank renders across 30 polls at three-second intervals
during a live rollout.

**The stage rail disagreed with every other number on the panel.** It
showed "50% holding" while the ramp, the readout and the version card all
said 25%. The rail was keyed off `stageIndex`, and after an operator sets
a share by hand that index points at the next *unrun* stage — the same
semantic subtlety that earlier caused `advance` to skip a stage.

The rail now keys off the percentage actually in force: stages below it
read as done, the one equal to it is live, and a hand-set share that
matches no planned stop gets its own dashed "by hand" marker rather than
highlighting nothing.

## Rainbow deployments: several versions live at once

A ramp cannot do this, and the reason is structural rather than a
limitation to work around. A Worker Deployment's routing config holds
exactly one `CurrentVersion` and one `RampingVersion` with one
percentage, and `SetRampingVersion` replaces whatever was there. A ramp
is a **two-way** split by construction, so no sequence of calls puts
three versions in service simultaneously.

Both features here therefore route per order instead, using a **pinned
versioning override** on the start request — the same documented
mechanism the canary gate uses, and the one you would reach for to send a
particular customer segment to a particular build. Because the override
is Pinned, each order still runs its whole life on one version, so the
in-flight guarantee is untouched.

**Send orders to a version** (`POST /api/versions/dump`, and the control
under each version card) dumps a burst pinned to one version. This is the
sharpest way to show serverless workers: an idle version has *no workers
at all*, so the burst makes Temporal start them from nothing while you
watch. Measured on the deployed environment, from a standing start of
zero workers everywhere:

| | v1 | v2 | v3 | v4 | v5 |
| --- | --- | --- | --- | --- | --- |
| after dumping on v1 | **97** | 0 | 0 | 0 | 0 |
| after also dumping on v2, v4, v5 | 101 | **60** | 0 | **56** | **58** |

Four independent worker populations, each scaled up from nothing, each
serving only its own orders — with v3 (the Current version) still at zero
because nothing was routed to it.

**Split traffic across versions** (`POST /api/traffic/split`) does the
same thing for sustained traffic: give it `v3: 25, v4: 25, v5: 50` and
every generated order is assigned a version against those weights. The
shares must add to 100, because anything else silently drops or
double-counts orders with no way to tell from the dashboard which
happened. It is refused while a rollout is running: the coordinator would
be adjusting a ramp that no longer decides anything.

What to say on stage, because it matters: while a split or a pinned dump
is in play, the deployment's Current and Ramping settings are **not** what
decides where those orders go. The client is. The automated rollout demo
is the one that shows Temporal's own routing.

### Showing it

Per-version worker counts come free. Every `PollerInfo` in a
`DescribeTaskQueue` response carries the `DeploymentOptions` its worker
reported, including the Build ID, so grouping the poller list by version
is a regrouping of a response already being fetched rather than five more
calls. The dashboard uses it twice: a live worker count on each version
card that sits at zero until that version is used, and a third hero bar
showing workers by version — empty at rest, filling per version as work
arrives.
