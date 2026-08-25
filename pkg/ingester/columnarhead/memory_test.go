package columnarhead

import (
	"runtime"
	"testing"
)

// TestMemoryPerSeries measures real heap cost against the same 500k-series, 8-round
// workload used throughout CHECKLIST.md (bench/00_baseline, bench/04_tight_slab,
// bench/05_compact_arena), now through the actual package rather than benchmark code,
// and now including timestamps - which none of those earlier measurements modeled.
func TestMemoryPerSeries(t *testing.T) {
	if testing.Short() {
		t.Skip("500k-series heap measurement; skipped in -short")
	}
	const (
		n            = 500_000
		seriesPerTgt = 200
		rounds       = 8
	)

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	s := NewSeriesStore(n)
	refs := make([]uint32, n)
	for i := 0; i < n; i++ {
		refs[i] = s.Create(uint16(i/seriesPerTgt), uint16(i%400), uint16(i%12))
	}

	ts := int64(1700000000000)
	for round := 0; round < rounds; round++ {
		for i, ref := range refs {
			v := valueFor(i, round)
			if err := s.Append(ref, ts, v); err != nil {
				t.Fatalf("round %d, series %d: %v", round, i, err)
			}
		}
		ts += 15000
	}

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	// GC above must run while s/refs are still live in the compiler's liveness
	// analysis, or it may collect them before ReadMemStats sees their footprint.
	runtime.KeepAlive(s)
	runtime.KeepAlive(refs)

	heapBytes := after.HeapAlloc - before.HeapAlloc
	bPerSeries := float64(heapBytes) / n
	t.Logf("heap: %.1f MB (%.1f B/series) for %d series x %d rounds, incl. timestamps, "+
		"growable slots (initial %d B, geometric doubling)", float64(heapBytes)/1e6, bPerSeries, n, rounds, initialSlotBytes)

	var usedBits, liveCapBytes uint64
	for i, off := range s.bitOff {
		usedBits += uint64(off)
		liveCapBytes += uint64(s.slotCap[i])
	}
	usedBPerSeries := float64(usedBits) / 8 / n
	liveCapBPerSeries := float64(liveCapBytes) / n
	totalArenaBPerSeries := float64(len(s.arena)) / n
	t.Logf("arena: %.2f B/series actually used, %.2f B/series in the current live slot, "+
		"%.2f B/series total arena consumed (len(arena)/n) - the gap between live-slot and "+
		"total is abandoned regions from earlier grow events, permanently unreclaimed",
		usedBPerSeries, liveCapBPerSeries, totalArenaBPerSeries)

	// Loose bounds, not a tight assertion: catches gross regressions without being
	// brittle to GC noise. Growable slots without reclaim are NOT assumed to beat a
	// fixed slot here - see the arena log line above for why (abandoned-region waste).
	if bPerSeries < 20 || bPerSeries > 250 {
		t.Errorf("B/series = %.1f, expected roughly 20-250 for this workload", bPerSeries)
	}
}

func valueFor(i, round int) float64 {
	switch i % 20 {
	case 0, 1, 2, 3, 4, 5:
		return float64(i % 2)
	case 6, 7, 8, 9, 10, 11, 12:
		return float64(1000 + round*3)
	case 13, 14, 15, 16:
		return float64(1 << 20 * (i%7 + 1))
	}
	return 1e6 + float64(round)*40
}
