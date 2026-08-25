package columnarhead

import (
	"fmt"
	"runtime"
	"testing"
)

func TestHeadDedupesTargetsAndSeries(t *testing.T) {
	h := NewHead(10, 10, 10)
	tgt := TargetLabels{
		Cluster: "eks-prod-1", Namespace: "ns-7", Pod: "payments-api-abc123",
		Container: "app", Node: "ip-10-1-2-3", Job: "cadvisor",
	}

	ref1, err := h.GetOrCreateSeries(tgt, "cpu_seconds_total", "")
	if err != nil {
		t.Fatalf("GetOrCreateSeries: %v", err)
	}
	ref2, err := h.GetOrCreateSeries(tgt, "cpu_seconds_total", "")
	if err != nil {
		t.Fatalf("GetOrCreateSeries: %v", err)
	}
	if ref1 != ref2 {
		t.Fatalf("identical (target, metric, local) got different refs: %d, %d - not deduplicated", ref1, ref2)
	}
	if h.NumSeries() != 1 {
		t.Fatalf("NumSeries() = %d, want 1", h.NumSeries())
	}
	if h.NumTargets() != 1 {
		t.Fatalf("NumTargets() = %d, want 1", h.NumTargets())
	}

	// A different metric on the SAME target must share the target but get a new series.
	ref3, err := h.GetOrCreateSeries(tgt, "memory_bytes", "")
	if err != nil {
		t.Fatalf("GetOrCreateSeries: %v", err)
	}
	if ref3 == ref1 {
		t.Fatal("different metric names on the same target got the same series ref")
	}
	if h.NumTargets() != 1 {
		t.Fatalf("NumTargets() = %d after a second metric on the same target, want 1 (target unchanged)", h.NumTargets())
	}
	if h.NumSeries() != 2 {
		t.Fatalf("NumSeries() = %d, want 2", h.NumSeries())
	}

	// A histogram bucket (different localLabel) is a distinct series from the same
	// metric name with no local label.
	ref4, err := h.GetOrCreateSeries(tgt, "request_duration_bucket", "0.1")
	if err != nil {
		t.Fatalf("GetOrCreateSeries: %v", err)
	}
	ref5, err := h.GetOrCreateSeries(tgt, "request_duration_bucket", "0.5")
	if err != nil {
		t.Fatalf("GetOrCreateSeries: %v", err)
	}
	if ref4 == ref5 {
		t.Fatal("different le values got the same series ref")
	}

	// A different target (different pod) must get its own target record, even with
	// otherwise-identical metric/local-label.
	tgt2 := tgt
	tgt2.Pod = "payments-api-def456"
	ref6, err := h.GetOrCreateSeries(tgt2, "cpu_seconds_total", "")
	if err != nil {
		t.Fatalf("GetOrCreateSeries: %v", err)
	}
	if ref6 == ref1 {
		t.Fatal("different pods got the same series ref")
	}
	if h.NumTargets() != 2 {
		t.Fatalf("NumTargets() = %d after a second pod, want 2", h.NumTargets())
	}
}

func TestHeadAppendAndIterate(t *testing.T) {
	h := NewHead(1, 1, 1)
	tgt := TargetLabels{Cluster: "c", Namespace: "n", Pod: "p", Container: "co", Node: "no", Job: "j"}
	ref, err := h.GetOrCreateSeries(tgt, "up", "")
	if err != nil {
		t.Fatalf("GetOrCreateSeries: %v", err)
	}

	want := []sample{{1700000000000, 1}, {1700000015000, 1}, {1700000030000, 0}}
	for _, sm := range want {
		if err := h.Append(ref, sm.ts, sm.v); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	it := h.Iterator(ref)
	var got []sample
	for it.Next() {
		ts, v := it.At()
		got = append(got, sample{ts, v})
	}
	assertSamplesEqual(t, got, want)
}

// TestHeadAtScale measures the real, honest end-to-end memory cost of the actual
// ingest path - Head, not just SeriesStore's own footprint - on the same k8s-shaped
// workload used throughout CHECKLIST.md (25,000 pods x 200 series/pod = 5M series
// worth of target sharing shape, scaled down to 500k series to match the other
// per-package tests). This is the first real measurement of what a live head actually
// costs INCLUDING the Go map-based dedup indexes (targetIndex, seriesIndex,
// liveInterner's index) that a live system needs but the static MPHF/SymbolTable
// don't - see Head's and liveInterner's doc comments for why those aren't used here.
func TestHeadAtScale(t *testing.T) {
	if testing.Short() {
		t.Skip("500k-series head build; skipped in -short")
	}
	const (
		n            = 500_000
		seriesPerTgt = 200
		numTargets   = n / seriesPerTgt
		numMetrics   = 400
	)

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	h := NewHead(n, numTargets, numMetrics+numTargets*1+16)
	les := []string{"0.005", "0.01", "0.025", "0.05", "0.1", "0.25", "0.5", "1", "2.5", "5", "10", "+Inf"}
	refs := make([]uint32, n)
	ts := int64(1700000000000)
	for i := 0; i < n; i++ {
		tgt := TargetLabels{
			Cluster:   "eks-prod-1",
			Namespace: "ns-7",
			Pod:       fmt.Sprintf("payments-api-7d9f8b6c4-%06x", i/seriesPerTgt),
			Container: "app",
			Node:      "ip-10-1-2-3.ec2.internal",
			Job:       "cadvisor",
		}
		metric := fmt.Sprintf("container_metric_name_number_%03d_total", i%numMetrics)
		local := ""
		if i%20 < 6 { // roughly matches the histogram-bucket share used elsewhere
			local = les[i%len(les)]
		}
		ref, err := h.GetOrCreateSeries(tgt, metric, local)
		if err != nil {
			t.Fatalf("series %d: %v", i, err)
		}
		refs[i] = ref
		if err := h.Append(ref, ts, float64(i%2)); err != nil {
			t.Fatalf("series %d: Append: %v", i, err)
		}
	}
	ts += 15000
	for round := 1; round < 8; round++ {
		for i, ref := range refs {
			if err := h.Append(ref, ts, float64((i+round)%2)); err != nil {
				t.Fatalf("round %d, series %d: %v", round, i, err)
			}
		}
		ts += 15000
	}

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(h)
	runtime.KeepAlive(refs)

	heapBytes := after.HeapAlloc - before.HeapAlloc
	t.Logf("Head at scale: %d series, %d targets, %d symbols", h.NumSeries(), h.NumTargets(), h.NumSymbols())
	t.Logf("REAL total heap (series+targets+symbols+all live dedup maps): %.1f MB (%.1f B/series)",
		float64(heapBytes)/1e6, float64(heapBytes)/n)
	t.Logf("target sharing ratio achieved: %.0f series/target (design doc measured 200:1 on a "+
		"comparable workload)", float64(h.NumSeries())/float64(h.NumTargets()))
	t.Logf("component sizes (excluding live map overhead, which isn't separable from heap "+
		"totals without an unsafe per-object accounting trick): symbols blob %d B, targets %d B",
		h.symbols.BlobBytes(), h.targets.SizeBytes())

	// Correctness spot-check at scale, not just the small hand-written cases above.
	for i := 0; i < n; i += 50_000 {
		it := h.Iterator(refs[i])
		count := 0
		for it.Next() {
			count++
		}
		if count != 8 {
			t.Fatalf("series %d: decoded %d samples, want 8", i, count)
		}
	}
}
