package orders

import (
	"testing"
	"time"
)

func TestStepsForCoversEveryVersion(t *testing.T) {
	for _, v := range AllVersions {
		steps := StepsFor(v)
		if len(steps) == 0 {
			t.Fatalf("version %s has no pipeline", v)
		}
		if steps[0] != StepReceived {
			t.Errorf("version %s starts with %q, want %q", v, steps[0], StepReceived)
		}
		seen := map[Step]bool{}
		for _, s := range steps {
			if seen[s] {
				t.Errorf("version %s repeats step %q", v, s)
			}
			seen[s] = true
		}
	}
}

func TestStepsForUnknownVersion(t *testing.T) {
	if steps := StepsFor(Version("v99")); steps != nil {
		t.Errorf("got %v, want nil for an unknown version", steps)
	}
}

// Each version must be a genuinely different shape, or a rollout between two
// of them demonstrates nothing.
func TestVersionsAreDistinctShapes(t *testing.T) {
	shapes := map[string]Version{}
	for _, v := range AllVersions {
		key := ""
		for _, s := range StepsFor(v) {
			key += string(s) + "|"
		}
		if other, dup := shapes[key]; dup {
			t.Errorf("versions %s and %s have identical pipelines", other, v)
		}
		shapes[key] = v
	}
}

func TestParseVersion(t *testing.T) {
	if v, ok := ParseVersion("v3"); !ok || v != V3 {
		t.Errorf("ParseVersion(v3) = %q, %v", v, ok)
	}
	if _, ok := ParseVersion("v6"); ok {
		t.Error("ParseVersion(v6) should not be valid")
	}
	if _, ok := ParseVersion(""); ok {
		t.Error("ParseVersion(empty) should not be valid")
	}
}

func TestParseProfile(t *testing.T) {
	for _, name := range []string{"fast", "demo", "heavy"} {
		if _, ok := ParseProfile(name); !ok {
			t.Errorf("profile %q should be valid", name)
		}
	}
	if _, ok := ParseProfile("turbo"); ok {
		t.Error("profile turbo should not be valid")
	}
}

// Chaos must only bite the version it names: this is what lets the demo aim a
// fault at any one version while the others stay healthy.
func TestChaosAppliesToOnlyTargetVersionAndStep(t *testing.T) {
	spec := &ChaosSpec{TargetVersion: V4, Step: StepPayment, Mode: ChaosFail}

	if !spec.appliesTo(V4, StepPayment) {
		t.Error("should apply to its own version and step")
	}
	if spec.appliesTo(V3, StepPayment) {
		t.Error("should not apply to a different version")
	}
	if spec.appliesTo(V4, StepPrep) {
		t.Error("should not apply to a different step")
	}

	var none *ChaosSpec
	if none.appliesTo(V4, StepPayment) {
		t.Error("a nil spec should never apply")
	}
}

func TestNewOrderInputIsDeterministicAndUnique(t *testing.T) {
	a := NewOrderInput(7, nil)
	if a.OrderID != NewOrderInput(7, nil).OrderID {
		t.Error("the same sequence number should produce the same order ID")
	}
	if a.OrderID == NewOrderInput(8, nil).OrderID {
		t.Error("different sequence numbers should produce different order IDs")
	}
	if len(a.Items) == 0 {
		t.Error("an order should have at least one item")
	}
	if a.Store == "" {
		t.Error("an order should have a store")
	}
	for _, item := range a.Items {
		if item.Qty < 1 {
			t.Errorf("item %q has quantity %d", item.Name, item.Qty)
		}
	}
}

// The step execution budget has to sit between "the slowest healthy step" and
// "a deliberately slowed step", in every profile.
//
// This guards a subtle failure that is easy to reintroduce: if the budget can
// be breached by a healthy step, then a busy system looks like a broken one,
// and a rollout watching for stuck orders rolls itself back during precisely
// the traffic surge it was supposed to ride out.
func TestStepBudgetSeparatesBrokenFromBusy(t *testing.T) {
	// The longest a single healthy step can take is the order budget divided
	// by the *shortest* pipeline, plus the 20% jitter ceiling.
	fewestSteps := len(StepsFor(AllVersions[0]))
	for _, v := range AllVersions {
		if n := len(StepsFor(v)); n < fewestSteps {
			fewestSteps = n
		}
	}

	for _, profile := range []Profile{ProfileFast, ProfileDemo, ProfileHeavy} {
		share := profile.orderWork() / time.Duration(fewestSteps)
		worstHealthy := time.Duration(float64(share) * 1.2)

		if worstHealthy >= stepStartToClose {
			t.Errorf("profile %q: a healthy step can take %s, which breaches the %s budget",
				profile, worstHealthy, stepStartToClose)
		}
	}

	if slowStepWork <= stepStartToClose {
		t.Errorf("a slowed step takes %s, which is inside the %s budget, so it would never be flagged",
			slowStepWork, stepStartToClose)
	}
}

// Every version's order should take about the same time, so that a rollout
// reads as a change in an order's shape rather than in how long a customer
// waits. Pipelines differ in length by nearly two to one, so this only holds
// because the profile is an end-to-end budget rather than a per-step duration.
func TestOrdersTakeTheSameTimeWhateverTheirShape(t *testing.T) {
	const profile = ProfileDemo
	budget := profile.orderWork()

	for _, v := range AllVersions {
		steps := len(StepsFor(v))
		// Jitter is symmetric around the share, so the expected total is the
		// budget itself; the worst case is 20% either side.
		lowest := time.Duration(float64(budget) * 0.8)
		highest := time.Duration(float64(budget) * 1.2)

		acts := &Activities{Profile: profile}
		var total time.Duration
		for range steps {
			total += acts.stepDuration(v, false)
		}

		if total < lowest || total > highest {
			t.Errorf("%s (%d steps) takes %s, outside the %s–%s range around the %s budget",
				v, steps, total, lowest, highest, budget)
		}
	}
}
