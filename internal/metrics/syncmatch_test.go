package metrics

import "testing"

// Prometheus text lines may carry an optional trailing timestamp, and Temporal
// Cloud's OpenMetrics output does. Taking the last field on the line therefore
// reads the timestamp instead of the value — and because every counter on a
// scrape shares that timestamp, the sync match rate comes out as a confident,
// entirely wrong 100%.
func TestParseSampleIgnoresTrailingTimestamp(t *testing.T) {
	for _, tc := range []struct {
		name   string
		line   string
		want   float64
		metric string
	}{
		{
			name:   "cloud, with timestamp",
			line:   `temporal_cloud_v1_poll_success_count{task_type="Activity",temporal_task_queue="orders"} 242.783 1789489980`,
			want:   242.783,
			metric: "temporal_cloud_v1_poll_success_count",
		},
		{
			name:   "self-hosted, no timestamp",
			line:   `poll_success{namespace="default",taskqueue="orders"} 4211`,
			want:   4211,
			metric: "poll_success",
		},
		{
			name:   "value with a brace-free suffix",
			line:   `temporal_cloud_v1_poll_success_sync_count{temporal_task_queue="orders"} 0 1789489980`,
			want:   0,
			metric: "temporal_cloud_v1_poll_success_sync_count",
		},
	} {
		got, value, ok := parseSample(tc.line)
		if !ok {
			t.Fatalf("%s: could not parse", tc.name)
		}
		if got != tc.metric {
			t.Errorf("%s: name = %q, want %q", tc.name, got, tc.metric)
		}
		if value != tc.want {
			t.Errorf("%s: value = %v, want %v", tc.name, value, tc.want)
		}
	}
}

func TestParseSampleRejectsRubbish(t *testing.T) {
	for _, line := range []string{
		"# a comment",
		"no_labels_here 42",
		`bad{labels="x"} not-a-number`,
		"",
	} {
		if _, _, ok := parseSample(line); ok {
			t.Errorf("should not have parsed %q", line)
		}
	}
}

// A rate-shaped reading divides directly; a counter-shaped one needs a delta.
func TestComputeHandlesBothShapes(t *testing.T) {
	r := &SyncMatchReader{}

	// Cloud: already per-second rates.
	if got := r.computeLocked(100, 90, true); got.RatePct != 90 {
		t.Errorf("rate shape: got %.1f%%, want 90%%", got.RatePct)
	}

	// Self-hosted: cumulative counters. The first reading has no baseline.
	c := &SyncMatchReader{}
	if got := c.computeLocked(1000, 900, false); got.RatePct != -1 {
		t.Errorf("first counter reading should be unknown, got %.1f", got.RatePct)
	}
	// Second reading: 200 more delivered, 150 of them sync matched.
	if got := c.computeLocked(1200, 1050, false); got.RatePct != 75 {
		t.Errorf("counter shape: got %.1f%%, want 75%%", got.RatePct)
	}
}
