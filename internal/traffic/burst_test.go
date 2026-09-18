package traffic

import (
	"strings"
	"testing"
)

// Every order in a burst must be accounted for exactly once.
//
// Batches carry slices of one sequence, so an off-by-one either drops orders
// or makes two batches write the same order IDs — and duplicates are rejected
// as already started, which looks like a burst that silently came up short.
func TestBurstBatchesCoverEveryOrderExactlyOnce(t *testing.T) {
	for _, count := range []int{1, 250, 500, 501, 5000, 12345} {
		batches := burstBatches("burst-1", BurstRequest{Count: count})

		total := 0
		seen := map[int]bool{}
		for _, b := range batches {
			total += b.Count
			for i := range b.Count {
				seq := b.FirstSeq + i
				if seen[seq] {
					t.Errorf("count=%d: sequence %d appears in two batches", count, seq)
				}
				seen[seq] = true
			}
		}

		if total != count {
			t.Errorf("count=%d: batches total %d orders", count, total)
		}
		if len(seen) != count {
			t.Errorf("count=%d: %d distinct sequences, want %d", count, len(seen), count)
		}
		// Contiguous from 1, so the IDs a burst writes are predictable.
		for seq := 1; seq <= count; seq++ {
			if !seen[seq] {
				t.Errorf("count=%d: sequence %d was never queued", count, seq)
				break
			}
		}
	}
}

// No batch may exceed the per-Activity cap, or one Activity would hold the
// whole burst and lose the concurrency that makes it sharp.
func TestBurstBatchesRespectThePerActivityCap(t *testing.T) {
	batches := burstBatches("burst-1", BurstRequest{Count: 5000})

	if len(batches) != 10 {
		t.Errorf("5000 orders split into %d batches, want 10 of %d", len(batches), ordersPerActivity)
	}
	for i, b := range batches {
		if b.Count > ordersPerActivity {
			t.Errorf("batch %d holds %d orders, over the cap of %d", i, b.Count, ordersPerActivity)
		}
	}
}

// Every batch must carry the same prefix, because that is what separates a
// burst's order IDs from the steady stream's — the two allocate independently
// and share no counter.
func TestEveryBatchCarriesTheBurstPrefix(t *testing.T) {
	batches := burstBatches("burst-1789736379", BurstRequest{Count: 1200})

	for i, b := range batches {
		if b.IDPrefix != "burst-1789736379" {
			t.Errorf("batch %d has prefix %q", i, b.IDPrefix)
		}
	}
	if !strings.HasPrefix(batchID("burst-1789736379", 3), "burst-1789736379-batch-") {
		t.Errorf("batch ID %q does not name its burst", batchID("burst-1789736379", 3))
	}
}

// A burst is meant to arrive at once, so it must never be paced. Pacing is for
// the steady stream.
func TestBurstBatchesAreNotPaced(t *testing.T) {
	for _, b := range burstBatches("burst-1", BurstRequest{Count: 900}) {
		if b.SpreadOver != 0 {
			t.Errorf("a burst batch is paced over %s", b.SpreadOver)
		}
	}
}

// Pinning a burst to one version is a 100% split to it, which is what bypasses
// deployment routing and lets an idle version be woken on demand.
func TestPinnedBurstBecomesAFullSplit(t *testing.T) {
	batches := burstBatches("burst-1", BurstRequest{Count: 600, Version: "v3"})
	if len(batches) < 2 {
		t.Fatalf("expected more than one batch, got %d", len(batches))
	}
	for i, b := range batches {
		if !b.Split.Active() {
			t.Errorf("batch %d has no split, so it would follow deployment routing", i)
			continue
		}
		if len(b.Split) != 1 || b.Split[0].Version != "v3" || b.Split[0].Pct != 100 {
			t.Errorf("batch %d split = %+v, want v3 at 100%%", i, b.Split)
		}
	}

	// An unpinned burst must stay unpinned, or a ramp would mean nothing.
	for i, b := range burstBatches("burst-1", BurstRequest{Count: 600}) {
		if b.Split.Active() {
			t.Errorf("batch %d of an unpinned burst carries a split", i)
		}
	}
}

// The live fault config travels with the burst, so a burst is subject to
// whatever fault is currently aimed.
func TestBurstCarriesTheFaultConfig(t *testing.T) {
	chaos := ChaosConfig{Pct: 100}
	for i, b := range burstBatches("burst-1", BurstRequest{Count: 700, Chaos: chaos}) {
		if b.Chaos.Pct != 100 {
			t.Errorf("batch %d lost the fault config: %+v", i, b.Chaos)
		}
	}
}
