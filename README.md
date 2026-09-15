# Rainbow deploys

A live demo of two things at once: an order system that absorbs sudden
traffic without provisioning for the peak, and worker versions that can
be rolled out — automatically, to any version, forwards or backwards —
without disturbing a single order already in flight.

It combines [temporal-serverless-no-roads][no-roads] (elastic scale)
with [temporal-versioning-demo][versioning] (safe versioned deploys),
themed as a quick-service coffee shop taking online orders.

[no-roads]: https://github.com/lainecsmith/temporal-serverless-no-roads
[versioning]: https://github.com/temporal-sa/temporal-versioning-demo

```bash
make up          # the whole demo in containers
open http://localhost:3000
```

---

## What it demonstrates

**Orders never move version.** Every order is pinned to the version
that took it. A deploy changes only where *new* orders go — the two
bars at the top of the dashboard show exactly that: the top one shifts
as a rollout ramps, the bottom one lags behind as in-flight orders
finish on the version they started on.

**Any version to any version.** Every version is registered and polling
from the moment the demo starts, so *Start deployment* is purely a
routing change. v1 to v4, v4 to v2, backwards — all the same operation.
Nothing is built, shipped or restarted mid-demo.

**A gate that actually blocks.** Before any traffic moves, the rollout
sends a handful of canary orders pinned to the candidate version. If
they do not all complete, the rollout stops and the routing is never
touched. Zero real orders reach a broken build.

**Automated ramp with a safety net.** 1% to 5% to 25% to 50% to 100%,
holding at each stage while watching failed and stuck orders. Breach the
threshold and it rolls back to where it started, by itself.

**Manual control at any moment.** Pause, continue, skip to the next
stage, or jump to any percentage you like — and the health safety net
stays armed while you do.

**Traffic you can shape.** Hold a steady rate (say 1,000 orders a
minute) and dump arbitrary spikes on top (5,000 at once) to show backlog
building and worker capacity following it.

**Rainbow deployments — several versions serving at once.** Send a burst
straight to any version from its card, or split sustained traffic across
versions (`v3: 25, v4: 25, v5: 50`). Each version's workers scale up
from nothing independently, so you can have four live worker populations
side by side while a fifth sits at zero.

A ramp cannot express that: a deployment's routing config holds one
Current and one Ramping version, so a ramp is a two-way split by
construction. These route per order instead, with a **pinned versioning
override** on each start — the same mechanism the canary gate uses, and
still Pinned, so every order runs its whole life on one version. The
trade-off worth saying out loud on stage: while a split or a pinned dump
is in play, the deployment's own Current/Ramping settings are not what
decides where those orders go.

**Recovery, not just rollback.** Rolling back caps the damage; it does
not heal the orders already stuck. One click resets every stranded
order onto the healthy version, in a single server-side batch.

---

## The five versions

One Workflow type, `CustomerOrder`, five pipeline shapes, all Pinned.
The differences are deliberately varied, so a deploy between any two of
them is a genuinely different change:

| Version | Pipeline | The change |
| --- | --- | --- |
| `v1` | Received, Payment, Prep, Handoff | baseline |
| `v2` | + **Loyalty accrual** after Payment | a step appended |
| `v3` | + **Fraud check** before Payment | a step inserted mid-pipeline |
| `v4` | + **Mobile pickup dispatch** before Handoff | a step added late |
| `v5` | Payment, Loyalty, Prep, **Drive-thru handoff** | restructured, not additive |

The dashboard draws each version's pipeline as a ladder, so the shape
difference is visible rather than implied.

---

## The demo script

**1. Steady state.** Set the rate to 600 a minute. Orders stream in on
v1. The top bar is entirely v1.

**2. A spike.** Dump 5,000 orders. Backlog climbs, the longest wait
rises from "none" into minutes, and **sync match collapses** from 100%
to somewhere in the 30s — that last one is the signal Temporal scales
serverless workers on. Nothing fails: the orders queue and drain. *This
is the elastic-scale half.*

**3. Deploy v3.** Press **Start deployment** on v3. The canary check
runs first, then the ramp begins: 1%, 5%, 25%, 50%, 100%. Watch the two
hero bars diverge — new orders move to v3 while v1's in-flight orders
keep their 4-step journey to the end.

**4. Take manual control.** Mid-ramp, press **Pause**, then set the
share to something arbitrary like 40%. The rollout holds there. Press
**Next stage** to hand it back to the plan.

**5. Break v4, then try to deploy it.** In *Break a version*, set v4's
Payment step to fail. Press **Start deployment** on v4. The canary gate
fails and the rollout stops — **the routing is never touched**. Show
that the routing panel still says v3, 100%.

**6. Break it after the gate instead.** Clear the fault, start the v4
deployment, and once it is past the gate and ramping, break v4. Stuck
orders appear, the error rate crosses the threshold, and the rollout
rolls itself back to v3 without anyone touching it.

**7. Rescue the casualties.** The v4 station shows its stuck orders.
Press **Rescue** — every stranded order is reset and re-pinned to v3,
and completes cleanly.

Each step is also a `make` target, if driving it from a terminal narrates
better: `make rate N=600`, `make spike N=5000`,
`make deploy-version V=v3`, `make break-version V=v4 STEP=Payment`,
`make rescue V=v4`, `make status`.

---

## How it is put together

```
                      ┌─────────────┐
  browser ──HTTP/SSE──│  dashboard  │  nginx: static files + API proxy
                      └──────┬──────┘
                             │
                      ┌──────┴──────┐
                      │   backend   │  JSON + SSE over Temporal. No HTML,
                      └──────┬──────┘  no database, no state of its own.
                             │
      ┌──────────────────────┼──────────────────────┐
      │                 Temporal                    │
      └──────┬───────────────────────────┬──────────┘
             │                           │
    ┌────────┴────────┐        ┌─────────┴──────────┐
    │  order workers  │        │   control plane    │
    │  v1 v2 v3 v4 v5 │        │  rollout + traffic │
    │  task queue:    │        │  task queue:       │
    │  orders         │        │  control           │
    │  VERSIONED      │        │  UNVERSIONED       │
    └─────────────────┘        └────────────────────┘
```

Three processes, cleanly separated:

| Piece | What it is | Notes |
| --- | --- | --- |
| `cmd/worker` | one version of the order pipeline | 100 lines. Serves exactly one version, chosen by `ORDER_VERSION`. All five run at once. |
| `cmd/controlworker` | the rollout coordinator and traffic generator | Unversioned: a Workflow that changes which version takes traffic must not be pinned to one of them. |
| `cmd/backend` | JSON and SSE API | Stateless. Renders no HTML. |
| `frontend/` | the dashboard | Plain HTML, CSS and JavaScript. No build step, no npm, no framework. Served by nginx, which also proxies the API so the browser sees one origin. |
| `cmd/lambdaworker` | the order worker as a Lambda | The same pipeline, hosted as a Temporal serverless worker. |

### The rollout coordinator is a Workflow

A rollout is long-running, stateful, interruptible, and must survive the
thing that started it — so it is a Temporal Workflow, not a loop in the
backend. Its control surface is Updates rather than Signals, so pressing
**Pause** on a finished rollout says so instead of quietly doing
nothing.

Internals are in [`internal/rollout`](internal/rollout); the full design
notes, including the things that turned out to be subtle, are in
[DESIGN.md](DESIGN.md).

### How a version goes bad

No global switch and no randomness inside Workflow code. The traffic
generator rolls the dice per order and stamps the fault into the order's
input:

```go
type ChaosSpec struct {
    TargetVersion Version  // only this version reacts
    Step          Step     // where it breaks
    Mode          ChaosMode // fails and retries, or runs very slowly
}
```

Only the named version acts on it, so any version can be the broken one
and the others are unaffected. The fault travels with the order, which
is what lets the canary gate inherit it and catch a sabotaged candidate
before any real traffic moves.

### Counting stuck orders

An order retrying a broken step forever is still `Running`, so
visibility alone cannot see that anything is wrong. The order Workflow
therefore publishes its own health as a search attribute once a step
genuinely fails, which gives the coordinator something countable.

"Genuinely" is doing real work there. An earlier version of this timed
each step and flagged slow ones — which meant that during a traffic
spike, healthy-but-queued orders were counted as stuck and rollouts
rolled themselves back during exactly the surge they were meant to ride
out. Steps are now judged on *execution* time, which a backlog does not
affect. See `TestStepBudgetSeparatesBrokenFromBusy`.

---

## Running it

### Containers (what to use for a demo)

```bash
make up      # Temporal, five workers, control plane, backend, dashboard
make down    # stop and delete the data
make logs
```

- Dashboard — <http://localhost:3000>
- Temporal UI — <http://localhost:8233>
- Backend API — <http://localhost:8088>

### On the host (for iterating on code)

```bash
make dev
```

Runs everything as host processes with logs under `.run-logs/`. The
dashboard is served as static files, so it needs the API's address:
<http://localhost:3000/?api=http://localhost:8088>.

### Go

```bash
make build   # every binary into ./bin
make test    # go test -race -shuffle=on ./...
make lint
```

---

## Temporal Cloud and Lambda

The same binaries run against Temporal Cloud; set `TEMPORAL_ADDRESS`,
`TEMPORAL_NAMESPACE` and `TEMPORAL_API_KEY` and TLS turns itself on.

To host the order workers as serverless workers, one Lambda per version:

```bash
make lambda-build
EXECUTION_ROLE=arn:aws:iam::<account>:role/<role> \
TEMPORAL_ADDRESS=<ns>.<account>.tmprl.cloud:7233 \
TEMPORAL_NAMESPACE=<ns>.<account> \
TEMPORAL_API_KEY=<key> \
  make lambda-deploy
```

Then register each function as a worker deployment version in the
Temporal Cloud UI (**Workers**, then **Serverless**), using the version
label as the Build ID. Create all five before demoing: a rollout can
only move traffic between versions that exist.

The backend and control plane are not serverless — only the order
workers are. Run those on Kubernetes or anywhere else, pointed at the
same namespace.

### Kubernetes

```bash
kubectl -n rainbow create secret generic temporal --from-literal=api-key=<key>
make k8s-apply
```

Edit the image references and the Temporal address in
[`deploy/k8s/kustomization.yaml`](deploy/k8s/kustomization.yaml) first.
The dashboard image needs no change between Docker Compose and
Kubernetes: its nginx proxies to `http://backend:8080`, which resolves
to the backend Service in the same namespace.

---

## Configuration

| Variable | Default | Applies to |
| --- | --- | --- |
| `TEMPORAL_ADDRESS` | `localhost:7233` | all |
| `TEMPORAL_NAMESPACE` | `default` | all |
| `TEMPORAL_API_KEY` | — | all; its presence switches on TLS |
| `TEMPORAL_DEPLOYMENT_NAME` | `rainbow-orders` | all |
| `ORDER_VERSION` | `v1` | order worker; which pipeline it serves |
| `ORDER_PROFILE` | `demo` | order worker; `fast`, `demo` or `heavy` |
| `TEMPORAL_WORKER_BUILD_ID` | the version label | order worker |
| `WORKER_MAX_CONCURRENT_ACTIVITIES` | `20` | order worker |
| `PORT` | `8080` | backend |
| `POLL_INTERVAL` | `1s` | backend |
| `TEMPORAL_METRICS_URL` | — | backend; where to read the sync match rate. Self-hosted: the server's own metrics port. Temporal Cloud: `https://metrics.temporal.io/v1/metrics`. Without it the gauge reports itself unavailable. |
| `TEMPORAL_METRICS_API_KEY` | — | backend; required for Temporal Cloud's metrics endpoint, from a service account with the **Metrics Read-Only** role. Unused self-hosted. |
| `TRAFFIC_CONCURRENCY` | `50` | control plane; how sharp a spike can be |
| `TRAFFIC_MAX_RUN` | `20m` | backend; how long traffic flows untouched before stopping itself |
| `LOG_FORMAT` | `json` | all; `text` for humans |

### Speed profiles

Throughput and worker occupancy pull against each other: 1,000 orders a
minute at 8 seconds of work each needs about 130 concurrent activity
slots. So the duration of an order is a deployment choice.

It is set as an **end-to-end budget**, not a per-step duration, and each
version divides its budget across however many steps it has. So every
version's order takes about the same time, and a rollout reads as a
change in an order's *shape* rather than in how long a customer waits.

| Profile | Whole order | Use |
| --- | --- | --- |
| `fast` | 1s | a laptop sustaining a high order rate |
| `demo` | **8s** | the presentation setting: an order finishes while you talk about it, and a spike still builds a visible queue |
| `heavy` | 40s | saturates workers deliberately |

A `demo`-profile order is about 8 seconds whether it runs v1's four
steps (2s each) or v4's seven (1.1s each).

### What the capacity panel shows

| Gauge | Source | Means |
| --- | --- | --- |
| backlog task queue | `ApproximateBacklogCount` | tasks written to the backlog, waiting for a worker |
| workers polling | live poller count | with serverless workers, one poller per invocation |
| longest a task has waited | `ApproximateBacklogAge` | the delay a customer is actually experiencing |
| handed straight to a waiting worker | server metrics | the **sync match rate** — the signal Temporal scales on |

The first three come from `DescribeTaskQueue`. Sync match does not: it is
a *server* metric, and the server reports it in one of two shapes.

**Self-hosted**, including the local dev server, exposes raw Prometheus
text on its own metrics port as cumulative counters (`poll_success` and
`poll_success_sync`), so the backend takes the rate over the interval
between scrapes — a lifetime average would be dominated by whatever
happened first.

**Temporal Cloud** exposes its OpenMetrics endpoint instead, at
`https://metrics.temporal.io/v1/metrics`, authenticated with an API key
from a **Metrics Read-Only** service account. Those values
(`temporal_cloud_v1_poll_success_count` and `…_sync_count`) are already
per-second rates, so they divide directly. They also carry a
`temporal_task_queue` label, so the figure is specific to the orders
queue rather than the whole namespace.

One reading is reused for 15 seconds: the Cloud endpoint returns every
task queue in the account — several megabytes — so scraping it on the
dashboard's one-second loop would be wasteful for a number that is
already smoothed.

### Search attributes

The order Workflow publishes `OrderVersion`, `OrderStep` and
`OrderHealth` about itself, so the dashboard can read thousands of
orders with counts instead of querying each one. The container and local
dev servers register them automatically; on Temporal Cloud, create them
once:

```bash
temporal operator search-attribute create --name OrderVersion --type Keyword
temporal operator search-attribute create --name OrderStep    --type Keyword
temporal operator search-attribute create --name OrderHealth  --type Keyword
```

---

## Things worth knowing

**Sub-1% ramps are real.** At 1,000 orders a minute, 1% is still ten
orders a minute — enough to judge a version on while capping the blast
radius at one order in a hundred.

**Cost, and why it does not fall to zero.** 1,000 orders a minute for a
half-hour session is about 30,000 Workflows. More importantly, with
serverless workers the deployment does **not** idle at zero: Temporal
keeps a warm pool of Lambda workers on whichever version is Current, so
live orders never pay a cold start. Measured on the sa-demo deployment:
about 18 warm workers on the Current version, with the other four
versions genuinely at zero invocations.

At 512MB that warm pool is roughly **$13/day** if left registered. The
levers, in order of effect:

| Lever | Effect |
| --- | --- |
| `make lambda-teardown` between demos | Removes the standing cost entirely; `lambda-deploy` rebuilds in a couple of minutes |
| Do **not** set `RESERVED_CONCURRENCY` | See below: it throttles rather than protects |
| `MEMORY=256` on deploy | Halves the warm-pool cost; slightly less CPU per worker, so cold starts and TLS setup are a little slower |
| `TIMEOUT` (default 120s) | Does *not* shrink the warm pool — Temporal just re-invokes more often. It does make burst capacity drain about 5x faster after a spike, which is what you see on the dashboard |
| Traffic auto-stop (built in) | Stops order volume after 20 minutes untouched, so an abandoned demo stops generating work |

A Lambda worker polls until its invocation deadline and then exits — it
does not exit early when idle — so the function timeout is exactly how
long an idle worker keeps billing.

**The dashboard shows the last known value, not a blank.** Every read in
a snapshot is issued concurrently and is best-effort; where one fails,
the previous value is carried forward. This matters because the backend
and the namespace can be in different regions, and a timed-out Query
against the rollout Workflow must not render as "no deployment running".

**The generator costs nothing while idle.** It blocks on an Update
rather than polling a timer, and paces each window inside an Activity
instead of ticking. Idle is 0 history events; running is about 15 events
a minute. If you change control-plane code while it is running, run
`make reset-control` — that plane is unversioned, so a live run cannot
replay against new code.

**Do not cap Lambda concurrency per function.** Reserved concurrency
looks like a safety guardrail and behaves like a throttle. Reserving 100
per function pinned a 5,000-order burst at **695 orders/min** and
produced **1,495 Lambda throttles against 189 successful invocations** —
Temporal kept asking to scale out, AWS kept refusing, and the demo looked
like Temporal could not scale. Removing it took the same burst to
**4,400 orders/min**. The account's own concurrency limit is the real
guardrail, and only one version takes traffic at a time, so the five
functions sharing one pool is the right shape.

**Give each worker enough Activity slots.** `WORKER_ACTIVITIES` defaults
to 20. The Lambda entrypoint's own default is 5, which needs hundreds of
concurrent invocations to drain a burst — and asking for hundreds of
invocations is how you get throttled. The steps only sleep, so they are
not competing for CPU.

**Rate limits.** A 5,000-order spike is a burst of Workflow starts;
Temporal Cloud applies namespace-level limits. `TRAFFIC_CONCURRENCY`
tunes how hard the generator pushes.

**Fonts.** The dashboard asks Google Fonts for Barlow and falls back to
the system sans if the network is unavailable. Nothing else is fetched
from the internet.
