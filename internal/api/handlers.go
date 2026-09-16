package api

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/deploy"
	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/orders"
	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/rollout"
	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/traffic"
)

// --- Traffic shaping -------------------------------------------------------

type rateRequest struct {
	RatePerMin int `json:"ratePerMin"`
}

// handleTrafficRate sets the steady-state order rate.
func (s *Server) handleTrafficRate(w http.ResponseWriter, r *http.Request) {
	var req rateRequest
	if err := decode(r, &req); err != nil {
		s.writeError(w, http.StatusBadRequest, err)
		return
	}

	state, err := s.traffic.update(r.Context(), traffic.UpdateSetRate, req.RatePerMin)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, state)
}

type spikeRequest struct {
	Count int `json:"count"`
}

// handleTrafficSpike dumps a one-off burst of orders on top of the steady rate.
func (s *Server) handleTrafficSpike(w http.ResponseWriter, r *http.Request) {
	var req spikeRequest
	if err := decode(r, &req); err != nil {
		s.writeError(w, http.StatusBadRequest, err)
		return
	}

	state, err := s.traffic.update(r.Context(), traffic.UpdateSpike, req.Count)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, state)
}

type splitRequest struct {
	// Entries maps versions to their share of new orders. An empty list hands
	// routing back to the deployment's Current/Ramping config.
	Entries []struct {
		Version string  `json:"version"`
		Pct     float64 `json:"pct"`
	} `json:"entries"`
}

// handleTrafficSplit sends new orders to several versions at once.
//
// This is the rainbow deployment: v3, v4 and v5 all serving live traffic
// together. It cannot be a ramp — a deployment's routing config holds one
// Current and one Ramping version, so a ramp is a two-way split by
// construction — so each order is instead started with a pinned versioning
// override naming its version.
//
// Because that bypasses deployment routing, it is refused while a rollout is
// running: the coordinator would be adjusting a ramp that no longer decides
// anything, and the dashboard would show two different stories at once.
func (s *Server) handleTrafficSplit(w http.ResponseWriter, r *http.Request) {
	var req splitRequest
	if err := decode(r, &req); err != nil {
		s.writeError(w, http.StatusBadRequest, err)
		return
	}

	split := make(traffic.Split, 0, len(req.Entries))
	for _, e := range req.Entries {
		if _, ok := orders.ParseVersion(e.Version); !ok {
			s.writeError(w, http.StatusBadRequest, fmt.Errorf("unknown version %q", e.Version))
			return
		}
		split = append(split, traffic.SplitEntry{Version: e.Version, Pct: e.Pct})
	}

	if split.Active() {
		if current := s.rollouts.state(r.Context()); current != nil && !current.Phase.Terminal() {
			s.writeError(w, http.StatusConflict, fmt.Errorf(
				"a deployment of %s is running (%s): stop it before splitting traffic by hand, "+
					"because a split routes each order itself and would override the ramp",
				current.TargetVersion, current.Phase))
			return
		}
	}

	state, err := s.traffic.update(r.Context(), traffic.UpdateSetSplit, split)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, state)
}

type chaosRequest struct {
	Version string  `json:"version"`
	Step    string  `json:"step"`
	Mode    string  `json:"mode"`
	Pct     float64 `json:"pct"`
}

// handleTrafficChaos aims the fault injector, or clears it when pct is zero.
func (s *Server) handleTrafficChaos(w http.ResponseWriter, r *http.Request) {
	var req chaosRequest
	if err := decode(r, &req); err != nil {
		s.writeError(w, http.StatusBadRequest, err)
		return
	}

	cfg := traffic.ChaosConfig{Pct: req.Pct}

	if req.Pct > 0 {
		version, ok := orders.ParseVersion(req.Version)
		if !ok {
			s.writeError(w, http.StatusBadRequest, fmt.Errorf("unknown version %q", req.Version))
			return
		}
		mode, err := parseChaosMode(req.Mode)
		if err != nil {
			s.writeError(w, http.StatusBadRequest, err)
			return
		}
		step, err := parseStep(version, req.Step)
		if err != nil {
			s.writeError(w, http.StatusBadRequest, err)
			return
		}
		cfg.Spec = &orders.ChaosSpec{TargetVersion: version, Step: step, Mode: mode}
	}

	state, err := s.traffic.update(r.Context(), traffic.UpdateSetChaos, cfg)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, err)
		return
	}

	// Clearing the fault has to reach the orders already carrying it.
	//
	// The spec travels with each order, so stopping the injector only spares
	// orders not yet started — the ones already parked on a broken step would
	// stay parked forever. Releasing them is a separate, deliberate act.
	if req.Pct == 0 {
		if job, err := s.releaseStuckOrders(r.Context()); err != nil {
			// The injector *was* cleared, so this is a partial success and
			// must not read as a failure to turn the fault off.
			s.logger.Warn("fault cleared, but stuck orders could not be released", "err", err)
		} else {
			s.logger.Info("releasing stuck orders", "jobId", job)
		}
	}

	writeJSON(w, http.StatusOK, state)
}

// handleTrafficStop ends the generator.
func (s *Server) handleTrafficStop(w http.ResponseWriter, r *http.Request) {
	state, err := s.traffic.update(r.Context(), traffic.UpdateStop)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, state)
}

// parseChaosMode validates a fault mode.
func parseChaosMode(mode string) (orders.ChaosMode, error) {
	switch orders.ChaosMode(mode) {
	case orders.ChaosFail:
		return orders.ChaosFail, nil
	case orders.ChaosSlow:
		return orders.ChaosSlow, nil
	case "":
		// Failing is the more useful default: it is what a bad deploy looks like.
		return orders.ChaosFail, nil
	default:
		return "", fmt.Errorf("unknown chaos mode %q, want %q or %q", mode, orders.ChaosFail, orders.ChaosSlow)
	}
}

// parseStep checks that a step actually exists in that version's pipeline.
//
// Worth rejecting rather than accepting silently: a fault aimed at a step the
// version does not have would simply never fire, which looks like a bug in the
// demo rather than a typo in the request.
func parseStep(version orders.Version, step string) (orders.Step, error) {
	available := orders.StepsFor(version)
	if step == "" {
		// Default to something every version has, after the order is taken.
		return available[1], nil
	}
	for _, s := range available {
		if string(s) == step {
			return s, nil
		}
	}
	return "", fmt.Errorf("version %s has no step %q", version, step)
}

// --- Automated rollouts ----------------------------------------------------

// startRolloutRequest is the dashboard's view of a rollout plan. Durations are
// in seconds here, rather than Go's nanoseconds, so the API is pleasant to
// call by hand.
type startRolloutRequest struct {
	TargetVersion string `json:"targetVersion"`

	Stages []struct {
		Pct     float32 `json:"pct"`
		HoldSec int     `json:"holdSec"`
	} `json:"stages"`

	Gate *struct {
		Enabled    bool `json:"enabled"`
		Orders     int  `json:"orders"`
		TimeoutSec int  `json:"timeoutSec"`
	} `json:"gate"`

	Health *struct {
		MaxErrorRatePct float64 `json:"maxErrorRatePct"`
		MinSamples      int64   `json:"minSamples"`
		EvalIntervalSec int     `json:"evalIntervalSec"`
	} `json:"health"`

	AutoPromote       *bool `json:"autoPromote"`
	RollbackOnFailure *bool `json:"rollbackOnFailure"`
}

// toInput converts the request into the coordinator's Input, applying the
// demo's defaults for anything omitted.
func (req startRolloutRequest) toInput() rollout.Input {
	in := rollout.Input{
		TargetVersion: req.TargetVersion,
		// Safe defaults: gate first, roll back automatically, promote on success.
		Gate:              rollout.DefaultGate(),
		Health:            rollout.DefaultHealthPolicy(),
		AutoPromote:       true,
		RollbackOnFailure: true,
	}

	for _, stage := range req.Stages {
		in.Stages = append(in.Stages, rollout.Stage{
			Pct:  stage.Pct,
			Hold: time.Duration(stage.HoldSec) * time.Second,
		})
	}

	if g := req.Gate; g != nil {
		in.Gate.Enabled = g.Enabled
		if g.Orders > 0 {
			in.Gate.Orders = g.Orders
		}
		if g.TimeoutSec > 0 {
			in.Gate.Timeout = time.Duration(g.TimeoutSec) * time.Second
		}
	}

	if h := req.Health; h != nil {
		if h.MaxErrorRatePct > 0 {
			in.Health.MaxErrorRatePct = h.MaxErrorRatePct
		}
		if h.MinSamples > 0 {
			in.Health.MinSamples = h.MinSamples
		}
		if h.EvalIntervalSec > 0 {
			in.Health.EvalInterval = time.Duration(h.EvalIntervalSec) * time.Second
		}
	}

	if req.AutoPromote != nil {
		in.AutoPromote = *req.AutoPromote
	}
	if req.RollbackOnFailure != nil {
		in.RollbackOnFailure = *req.RollbackOnFailure
	}

	return in
}

// handleRolloutStart begins an automated rollout to any registered version.
func (s *Server) handleRolloutStart(w http.ResponseWriter, r *http.Request) {
	var req startRolloutRequest
	if err := decode(r, &req); err != nil {
		s.writeError(w, http.StatusBadRequest, err)
		return
	}
	if _, ok := orders.ParseVersion(req.TargetVersion); !ok {
		s.writeError(w, http.StatusBadRequest, fmt.Errorf("unknown version %q", req.TargetVersion))
		return
	}

	in := req.toInput()

	// Stamp in the live fault-injection config so the canary gate exercises
	// the same conditions real traffic would meet. This is what makes the gate
	// catch a sabotaged candidate.
	in.GateChaos = s.traffic.chaos(r.Context())

	state, err := s.rollouts.start(r.Context(), in)
	if err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, ErrRolloutRunning) {
			status = http.StatusConflict
		}
		s.writeError(w, status, err)
		return
	}
	writeJSON(w, http.StatusAccepted, state)
}

func (s *Server) handleRolloutPause(w http.ResponseWriter, r *http.Request) {
	s.rolloutUpdate(w, r, rollout.UpdatePause)
}

func (s *Server) handleRolloutResume(w http.ResponseWriter, r *http.Request) {
	s.rolloutUpdate(w, r, rollout.UpdateResume)
}

func (s *Server) handleRolloutAdvance(w http.ResponseWriter, r *http.Request) {
	s.rolloutUpdate(w, r, rollout.UpdateAdvance)
}

type rampRequest struct {
	Pct float32 `json:"pct"`
}

// handleRolloutRamp jumps the live rollout to an arbitrary percentage.
func (s *Server) handleRolloutRamp(w http.ResponseWriter, r *http.Request) {
	var req rampRequest
	if err := decode(r, &req); err != nil {
		s.writeError(w, http.StatusBadRequest, err)
		return
	}
	s.rolloutUpdate(w, r, rollout.UpdateSetRamp, req.Pct)
}

// handleRolloutAbort stops the live rollout, rolling back unless told not to.
func (s *Server) handleRolloutAbort(w http.ResponseWriter, r *http.Request) {
	req := rollout.AbortRequest{Rollback: true}
	if err := decode(r, &req); err != nil {
		s.writeError(w, http.StatusBadRequest, err)
		return
	}
	s.rolloutUpdate(w, r, rollout.UpdateAbort, req)
}

// rolloutUpdate applies one control Update and returns the new state.
func (s *Server) rolloutUpdate(w http.ResponseWriter, r *http.Request, name string, args ...any) {
	state, err := s.rollouts.update(r.Context(), name, args...)
	if err != nil {
		s.writeError(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusOK, state)
}

// defaultDumpCount is the burst size when none is given.
const defaultDumpCount = 250

// --- Manual routing --------------------------------------------------------

type versionRampRequest struct {
	Version string  `json:"version"`
	Pct     float32 `json:"pct"`
}

// handleVersionRamp ramps a version by hand, with no coordinator involved.
//
// Kept alongside the automated path on purpose: it is how you show what the
// coordinator is doing for you, and the fallback if a demo goes sideways.
func (s *Server) handleVersionRamp(w http.ResponseWriter, r *http.Request) {
	var req versionRampRequest
	if err := decode(r, &req); err != nil {
		s.writeError(w, http.StatusBadRequest, err)
		return
	}
	if req.Pct < 0 || req.Pct > 100 {
		s.writeError(w, http.StatusBadRequest, fmt.Errorf("ramp percentage must be between 0 and 100"))
		return
	}

	buildID, err := s.deployment.ResolveLabel(r.Context(), req.Version)
	if err != nil {
		s.writeError(w, statusForVersionError(err), err)
		return
	}
	if err := s.deployment.SetRamp(r.Context(), buildID, req.Pct); err != nil {
		s.writeError(w, http.StatusBadGateway, err)
		return
	}
	s.writeDeployment(w, r)
}

type dumpRequest struct {
	Version string `json:"version"`
	Count   int    `json:"count"`
}

// handleVersionDump dumps a burst of orders onto one specific version.
//
// Unlike a normal spike, every order is started with a pinned versioning
// override naming that version, so the deployment's routing is bypassed
// entirely. That is the point: an idle version has no workers running at all,
// so dumping orders on it makes Temporal start them from nothing — which is
// the clearest way to show serverless workers appearing on demand, and lets
// several versions be put to work at once.
func (s *Server) handleVersionDump(w http.ResponseWriter, r *http.Request) {
	req := dumpRequest{Count: defaultDumpCount}
	if err := decode(r, &req); err != nil {
		s.writeError(w, http.StatusBadRequest, err)
		return
	}
	if _, ok := orders.ParseVersion(req.Version); !ok {
		s.writeError(w, http.StatusBadRequest, fmt.Errorf("unknown version %q", req.Version))
		return
	}

	// Fail here rather than deep inside the Activity, so an operator who has
	// not started that version's worker gets told plainly.
	if _, err := s.deployment.ResolveLabel(r.Context(), req.Version); err != nil {
		s.writeError(w, statusForVersionError(err), err)
		return
	}

	state, err := s.traffic.update(r.Context(), traffic.UpdateSpikePinned,
		traffic.SpikeRequest{Count: req.Count, Version: req.Version})
	if err != nil {
		s.writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, state)
}

type versionRequest struct {
	Version string `json:"version"`
}

// handleVersionPromote makes a version Current immediately, skipping any ramp.
func (s *Server) handleVersionPromote(w http.ResponseWriter, r *http.Request) {
	var req versionRequest
	if err := decode(r, &req); err != nil {
		s.writeError(w, http.StatusBadRequest, err)
		return
	}

	buildID, err := s.deployment.ResolveLabel(r.Context(), req.Version)
	if err != nil {
		s.writeError(w, statusForVersionError(err), err)
		return
	}
	if err := s.deployment.SetCurrent(r.Context(), buildID); err != nil {
		s.writeError(w, http.StatusBadGateway, err)
		return
	}
	s.writeDeployment(w, r)
}

// handleVersionRollback clears the ramp so all new orders go to Current.
func (s *Server) handleVersionRollback(w http.ResponseWriter, r *http.Request) {
	if err := s.deployment.ClearRamp(r.Context()); err != nil {
		s.writeError(w, http.StatusBadGateway, err)
		return
	}
	s.writeDeployment(w, r)
}

// writeDeployment returns the routing as it now stands.
func (s *Server) writeDeployment(w http.ResponseWriter, r *http.Request) {
	state, err := s.deployment.Describe(r.Context())
	if err != nil {
		s.writeError(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, http.StatusOK, state)
}

// statusForVersionError maps an unknown version to 404, which usually means
// that version's workers are not running.
func statusForVersionError(err error) int {
	if errors.Is(err, deploy.ErrUnknownVersion) {
		return http.StatusNotFound
	}
	return http.StatusBadGateway
}
