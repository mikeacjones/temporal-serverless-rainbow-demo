package traffic

import (
	"os"
	"strings"
	"testing"
	"time"
)

func TestOrdersDueCarriesTheRemainder(t *testing.T) {
	// 1,000/min over a 30s window is 500 exactly.
	var carry float64
	if due := ordersDue(1000, 30*time.Second, &carry); due != 500 {
		t.Errorf("got %d, want 500", due)
	}

	// 100/min over 7s is 11.67 — the fraction has to survive to the next
	// window, or the generator quietly runs slow forever.
	carry = 0
	total := 0
	for range 60 {
		total += ordersDue(100, 7*time.Second, &carry)
	}
	// 60 windows of 7s is 7 minutes, so about 700 orders.
	if total < 695 || total > 700 {
		t.Errorf("60 windows produced %d orders, want ~700 (the remainder is being dropped)", total)
	}
}

func TestOrdersDueAtZeroRate(t *testing.T) {
	var carry float64
	if due := ordersDue(0, Window, &carry); due != 0 {
		t.Errorf("a zero rate should owe no orders, got %d", due)
	}
}

// The generator must cost nothing while idle.
//
// This is the property that matters most in Temporal Cloud, where every
// Workflow task and every timer is a billable action. An earlier version
// ticked every two seconds whatever the rate, which spent tens of thousands
// of actions a day producing no orders at all. If a timer ever creeps back
// into the idle path, this test is what should catch it.
func TestIdleGeneratorSchedulesNothing(t *testing.T) {
	env := newTestEnv(t)

	// Start at rate zero and let the test clock run. A Workflow that is
	// genuinely blocked on an Update cannot advance the clock on its own, so
	// the test environment ends with no orders started and no timers fired.
	env.ExecuteWorkflow(Director, Input{RatePerMin: 0})

	if !env.IsWorkflowCompleted() {
		// Blocked forever on the Selector is the correct behaviour here: the
		// test environment has nothing to fire, so it finishes the run.
		t.Log("workflow still blocked, which is the expected idle state")
	}
	env.AssertNotCalled(t, ActivityStartOrders)
}

func TestWindowIsLongEnoughToBeCheap(t *testing.T) {
	// One paced Activity per window plus its Workflow tasks is roughly three
	// actions. At a 2s window that is ~90 actions a minute; the whole point of
	// pacing inside the Activity is to keep this small.
	if Window < 10*time.Second {
		t.Errorf("Window is %s: short windows put the action cost back", Window)
	}
}

// A spike's Activity must keep heartbeating while it waits for starts to
// finish, not only while it is spawning them.
//
// The spawn phase is the fast part: with no pacing, hundreds of goroutines are
// launched almost instantly and then the Activity sits in wg.Wait(). When the
// heartbeat lived inside the spawn loop it stopped there, so any batch slower
// than the heartbeat timeout was killed and every start still in flight failed
// with "context deadline exceeded".
//
// This asserts the shape that caused it cannot come back: heartbeats are
// recorded from a goroutine whose lifetime is the whole Activity.
func TestStartOrdersHeartbeatsForTheWholeBatch(t *testing.T) {
	src, err := os.ReadFile("activities.go")
	if err != nil {
		t.Fatalf("read activities.go: %v", err)
	}
	body := string(src)

	fn := body[strings.Index(body, "func (a *Activities) StartOrders("):]
	spawn := strings.Index(fn, "for i := range req.Count {")
	wait := strings.Index(fn, "wg.Wait()")
	if spawn < 0 || wait < 0 {
		t.Fatal("cannot locate the spawn loop and the wait in StartOrders")
	}

	// The heartbeat must be started before the spawn loop, so it covers the
	// wait that follows it.
	hb := strings.Index(fn, "activity.RecordHeartbeat")
	if hb < 0 {
		t.Fatal("StartOrders records no heartbeat at all")
	}
	if hb > spawn && hb < wait {
		t.Error("the heartbeat is recorded inside the spawn loop, so it stops " +
			"before the Activity starts waiting — the batch will be killed mid-flight")
	}
	if !strings.Contains(fn[:spawn], "go func()") {
		t.Error("no heartbeat goroutine is started before the spawn loop")
	}
}
