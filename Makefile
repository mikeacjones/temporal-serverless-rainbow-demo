# Rainbow deploys — order ops demo.
#
# Two ways to run it:
#   make up          everything in containers (what you want for a demo)
#   make dev         Temporal plus every process on the host (for iterating)

SHELL := /bin/bash
.DEFAULT_GOAL := help

DASHBOARD ?= http://localhost:3000
API       ?= $(DASHBOARD)/api
VERSIONS  := v1 v2 v3 v4 v5
BIN       := ./bin

# Host-mode defaults. The containers get these from compose.yaml instead.
export TEMPORAL_ADDRESS         ?= localhost:7233
export TEMPORAL_NAMESPACE       ?= default
export TEMPORAL_DEPLOYMENT_NAME ?= rainbow-orders
export ORDER_PROFILE            ?= demo
export LOG_FORMAT               ?= text

## help: list every target
.PHONY: help
help:
	@echo "Rainbow deploys — order ops demo"
	@echo
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/## /  /' | column -t -s ':'

# --- Containers -------------------------------------------------------------

## up: build and start the whole demo in containers
.PHONY: up
up:
	docker compose up --build -d
	@echo
	@echo -n "Waiting for the stack to be ready"
	@# The backend answers before the control plane is polling, and a rate
	@# change cannot be applied until it is — so wait for the thing that
	@# actually needs to work, not just for the HTTP port to open.
	@for i in $$(seq 1 60); do 		if curl -fsS -X POST $(API)/traffic/rate -H 'Content-Type: application/json' 			-d '{"ratePerMin":0}' >/dev/null 2>&1; then echo " ready"; break; fi; 		echo -n "."; sleep 2; 	done
	@echo
	@echo "Dashboard    $(DASHBOARD)"
	@echo "Temporal UI  http://localhost:8233"
	@echo "Backend API  http://localhost:8088"

## down: stop the demo and delete its data
.PHONY: down
down:
	docker compose down -v

## logs: follow the logs of every container
.PHONY: logs
logs:
	docker compose logs -f

## ps: show what is running
.PHONY: ps
ps:
	docker compose ps

# --- Host mode --------------------------------------------------------------

## dev: run Temporal in Docker and every process on the host
.PHONY: dev
dev: build
	./deploy/local/run.sh

## dev-server: start only a local Temporal dev server
.PHONY: dev-server
dev-server:
	./deploy/local/dev-server.sh

# --- Go ---------------------------------------------------------------------

## build: compile every binary into ./bin
.PHONY: build
build:
	@mkdir -p $(BIN)
	go build -o $(BIN)/ ./cmd/...
	@ls $(BIN)

## test: run the tests
.PHONY: test
test:
	go test -race -shuffle=on ./...

## fmt: format and tidy
.PHONY: fmt
fmt:
	gofmt -w cmd internal
	go mod tidy

## lint: vet everything
.PHONY: lint
lint:
	go vet ./...

# --- Demo controls ----------------------------------------------------------
#
# Everything here is also a button on the dashboard. These exist so the demo
# can be driven from a terminal when that is easier to narrate.

## rate: set the steady order rate, e.g. make rate N=1000
.PHONY: rate
rate:
	@curl -fsS -X POST $(API)/traffic/rate -H 'Content-Type: application/json' \
		-d '{"ratePerMin":$(or $(N),600)}' | python3 -m json.tool

## spike: dump a burst of orders, e.g. make spike N=5000
.PHONY: spike
spike:
	@curl -fsS -X POST $(API)/traffic/spike -H 'Content-Type: application/json' \
		-d '{"count":$(or $(N),5000)}' | python3 -m json.tool

## deploy-version: roll out a version, e.g. make deploy-version V=v4
.PHONY: deploy-version
deploy-version:
	@test -n "$(V)" || { echo "set V, e.g. make deploy-version V=v4"; exit 1; }
	@curl -fsS -X POST $(API)/rollout -H 'Content-Type: application/json' \
		-d '{"targetVersion":"$(V)"}' | python3 -m json.tool

## break-version: sabotage a version, e.g. make break-version V=v4 STEP=Payment
.PHONY: break-version
break-version:
	@test -n "$(V)" || { echo "set V, e.g. make break-version V=v4"; exit 1; }
	@curl -fsS -X POST $(API)/traffic/chaos -H 'Content-Type: application/json' \
		-d '{"version":"$(V)","step":"$(or $(STEP),Payment)","mode":"fail","pct":$(or $(PCT),100)}' \
		| python3 -m json.tool

## fix-version: clear any injected fault
.PHONY: fix-version
fix-version:
	@curl -fsS -X POST $(API)/traffic/chaos -H 'Content-Type: application/json' \
		-d '{"pct":0}' | python3 -m json.tool

## rescue: restart orders stranded on a version, e.g. make rescue V=v4
.PHONY: rescue
rescue:
	@test -n "$(V)" || { echo "set V, e.g. make rescue V=v4"; exit 1; }
	@curl -fsS -X POST $(API)/orders/recover -H 'Content-Type: application/json' \
		-d '{"version":"$(V)"}' | python3 -m json.tool

## reset-control: terminate the control-plane workflows after changing their code
.PHONY: reset-control
reset-control:
	@# The control plane is deliberately unversioned, so a running traffic
	@# director cannot replay against changed code. Terminating the singleton
	@# lets a fresh run start on the new build; it holds no state worth keeping.
	@for id in traffic-director rollout; do \
		printf '  %s: ' "$$id"; \
		temporal workflow terminate --workflow-id "$$id" \
			--reason "control-plane code changed" 2>&1 | tail -1; \
	done

## status: print the current routing, traffic and deployment state
.PHONY: status
status:
	@curl -fsS $(API)/state | python3 -c "$$STATUS_PY"

# --- AWS Lambda -------------------------------------------------------------

## lambda-build: cross-compile the Lambda worker for every version
.PHONY: lambda-build
lambda-build:
	./deploy/aws/build.sh

## lambda-deploy: create or update one Lambda per version
.PHONY: lambda-deploy
lambda-deploy:
	./deploy/aws/deploy.sh

## lambda-teardown: delete the Lambdas and their IAM roles (add ALL=1 for versions)
.PHONY: lambda-teardown
lambda-teardown:
	./deploy/aws/teardown.sh $(if $(ALL),--all,)

# --- Kubernetes -------------------------------------------------------------

## k8s-apply: apply the backend and dashboard manifests
.PHONY: k8s-apply
k8s-apply:
	kubectl apply -k deploy/k8s

## k8s-delete: remove them again
.PHONY: k8s-delete
k8s-delete:
	kubectl delete -k deploy/k8s

define STATUS_PY
import json,sys
s=json.load(sys.stdin)
r=s["deployment"]["routing"]
print("routing   current=%s ramping=%s %s%%" % (r["currentLabel"] or "-", r["rampingLabel"] or "-", int(r["rampingPct"])))
print("versions  " + "  ".join("%s:%s(%d%%)" % (v["label"], v["status"], v["trafficPct"]) for v in s["deployment"]["versions"]))
t=s["traffic"] or {}
print("traffic   rate=%s/min running=%s started=%s" % (t.get("ratePerMin","-"), t.get("running",False), t.get("started","-")))
print("orders    inflight=%d served=%d stuck=%d" % (s["totals"]["running"], s["totals"]["completed"], s["totals"]["degraded"]))
c=s["capacity"]
print("capacity  backlog=%d workers=%d longest-wait=%ds arriving=%.1f/s taken=%.1f/s" % (c["backlogDepth"], c["pollers"], round(c["oldestWaitSec"]), c["addedPerSec"], c["dispatchedPerSec"]))
d=s["rollout"]
print("rollout   %s" % (("%s %s at %d%% — %s" % (d["targetVersion"], d["phase"], d["currentPct"], d["message"])) if d and d.get("targetVersion") else "none"))
endef
export STATUS_PY
