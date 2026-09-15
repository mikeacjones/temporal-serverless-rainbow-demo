package orders

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"go.temporal.io/sdk/activity"
)

// Profile controls how long a whole order takes.
//
// It is expressed as an end-to-end budget rather than a per-step duration, so
// that every version's order takes about the same time however many steps its
// pipeline has. That keeps the versions comparable on screen: a rollout should
// look like a change in an order's *shape*, not a change in how long customers
// wait.
//
// Throughput and worker occupancy pull against each other — 1,000 orders a
// minute at 8 seconds of work each needs about 130 concurrent activity slots —
// so the budget is a deployment choice.
type Profile string

// The three speed profiles.
const (
	// ProfileFast keeps orders around a second, so a laptop with a handful of
	// worker processes can still sustain a high order rate.
	ProfileFast Profile = "fast"

	// ProfileDemo is the presentation setting: quick enough that an order
	// completes while you are talking about it, slow enough that a spike
	// still builds a visible queue.
	ProfileDemo Profile = "demo"

	// ProfileHeavy saturates workers deliberately, to force queueing.
	ProfileHeavy Profile = "heavy"
)

// orderWork is how long a whole order takes under this profile, spread across
// however many steps its version has.
func (p Profile) orderWork() time.Duration {
	switch p {
	case ProfileFast:
		return time.Second
	case ProfileHeavy:
		return 40 * time.Second
	default:
		return 8 * time.Second
	}
}

// ParseProfile validates a profile name, e.g. from the ORDER_PROFILE env var.
func ParseProfile(s string) (Profile, bool) {
	switch Profile(s) {
	case ProfileFast, ProfileDemo, ProfileHeavy:
		return Profile(s), true
	default:
		return "", false
	}
}

// slowStepWork is how long a deliberately slowed step takes.
//
// An absolute duration rather than a multiple of the profile: it has to exceed
// the Workflow's 30s execution budget for the step in *every* profile, so a
// slowed step reliably times out and is reported as trouble. A multiplier
// would trip under "heavy" and go unnoticed under "fast".
const slowStepWork = 45 * time.Second

// ErrStepFailed is returned by an injected fault. Temporal retries it, so the
// order stalls and goes red rather than failing outright.
var ErrStepFailed = errors.New("step failed")

// Activities implements the one Activity every order step goes through.
//
// There is deliberately only one: the interesting differences between versions
// live in which steps they run and in what order, not in what a step does.
type Activities struct {
	Profile Profile
}

// PerformStep does the work for one step of one order.
func (a *Activities) PerformStep(ctx context.Context, in StepInput) (string, error) {
	logger := activity.GetLogger(ctx)
	attempt := activity.GetInfo(ctx).Attempt

	if in.Fail {
		logger.Warn("step failing (injected fault)",
			"orderId", in.OrderID, "version", in.Version, "step", in.Step, "attempt", attempt)
		return "", fmt.Errorf("%s for order %s: %w", in.Step, in.OrderID, ErrStepFailed)
	}

	work := a.stepDuration(in.Version, in.Slow)
	logger.Info("step started",
		"orderId", in.OrderID, "version", in.Version, "step", in.Step, "work", work)

	// Sleeping here — rather than in the Workflow — is the point: it holds a
	// worker slot for the duration, which is what makes backlog and scale-out
	// visible. A Workflow timer would release the worker immediately.
	select {
	case <-time.After(work):
		return fmt.Sprintf("%s complete for order %s", in.Step, in.OrderID), nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

// stepDuration is this step's share of the order's budget, with up to 40%
// jitter so orders do not advance in lockstep and the dashboard looks alive.
//
// The share is derived from the version's own pipeline length, which the
// Activity knows from its input — so a four-step order and a seven-step order
// both finish in about the same time.
func (a *Activities) stepDuration(version Version, slow bool) time.Duration {
	if slow {
		return slowStepWork
	}

	steps := len(StepsFor(version))
	if steps == 0 {
		steps = 1
	}

	share := a.Profile.orderWork() / time.Duration(steps)
	return time.Duration(float64(share) * (0.8 + 0.4*rand.Float64()))
}
