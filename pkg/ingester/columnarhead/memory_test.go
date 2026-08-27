package columnarhead

import (
	"fmt"
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
		refs[i] = s.Create(uint32(i/seriesPerTgt), uint16(i%400), nil)
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
	avoidedBPerSeries := float64(s.AllocBytesRequested-uint64(len(s.arena))) / n
	hitRate := float64(s.AllocHits) / float64(s.AllocHits+s.AllocMisses) * 100
	t.Logf("free list: %d hits, %d misses (%.1f%% hit rate), %.2f B/series of fresh arena "+
		"growth avoided by reuse - this workload creates all series before any of them "+
		"append, so every series grows through the same size class in lockstep and there's "+
		"rarely a freed region of the right size available yet; see the staggered variant",
		s.AllocHits, s.AllocMisses, hitRate, avoidedBPerSeries)

	// Loose bounds, not a tight assertion: catches gross regressions without being
	// brittle to GC noise. Growable slots without reclaim are NOT assumed to beat a
	// fixed slot here - see the arena log line above for why (abandoned-region waste).
	if bPerSeries < 20 || bPerSeries > 250 {
		t.Errorf("B/series = %.1f, expected roughly 20-250 for this workload", bPerSeries)
	}
}

// TestMemoryPerSeries_Staggered checks the free list under a shape closer to real
// ingestion than TestMemoryPerSeries: series arrive continuously (new pods/targets
// showing up over time) instead of all 500k existing before any of them ever appends a
// sample. TestMemoryPerSeries's create-everything-then-append-in-lockstep shape means
// every series grows through the same size class at the same moment, so nothing is ever
// mid-grow (and freeing) while something else is mid-Create (and allocating) - the free
// list can't help there no matter how correct it is. This interleaves creation and
// appends across waves to give reuse an actual chance, and reports the hit rate
// directly instead of inferring it from B/series alone.
func TestMemoryPerSeries_Staggered(t *testing.T) {
	if testing.Short() {
		t.Skip("100k-series heap measurement; skipped in -short")
	}
	const (
		totalSeries  = 100_000
		waves        = 20
		perWave      = totalSeries / waves
		seriesPerTgt = 200
		roundsAfter  = 8 // rounds each series gets appended to after it arrives
	)

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	s := NewSeriesStore(totalSeries)
	var refs []uint32
	ts := int64(1700000000000)
	for wave := 0; wave < waves; wave++ {
		for w := 0; w < perWave; w++ {
			i := wave*perWave + w
			ref := s.Create(uint32(i/seriesPerTgt), uint16(i%400), nil)
			refs = append(refs, ref)
		}
		// Every series alive so far gets appended to this wave, same as real ingestion
		// where all active series get a sample each scrape regardless of when they
		// first appeared.
		for i, ref := range refs {
			if err := s.Append(ref, ts, valueFor(i, wave)); err != nil {
				t.Fatalf("wave %d, series %d: %v", wave, i, err)
			}
		}
		ts += 15000
	}
	// A few more rounds so later-arriving series also get to grow past their first slot.
	for r := 0; r < roundsAfter; r++ {
		for i, ref := range refs {
			if err := s.Append(ref, ts, valueFor(i, waves+r)); err != nil {
				t.Fatalf("post-wave round %d, series %d: %v", r, i, err)
			}
		}
		ts += 15000
	}

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(s)
	runtime.KeepAlive(refs)

	heapBytes := after.HeapAlloc - before.HeapAlloc
	bPerSeries := float64(heapBytes) / totalSeries
	totalArenaBPerSeries := float64(len(s.arena)) / totalSeries

	hitRate := float64(s.AllocHits) / float64(s.AllocHits+s.AllocMisses) * 100
	avoidedBPerSeries := float64(s.AllocBytesRequested-uint64(len(s.arena))) / totalSeries
	t.Logf("heap: %.1f MB (%.1f B/series), total arena %.2f B/series, alloc hits=%d misses=%d "+
		"(%.1f%% hit rate), %.2f B/series of fresh arena growth avoided by reuse",
		float64(heapBytes)/1e6, bPerSeries, totalArenaBPerSeries, s.AllocHits, s.AllocMisses, hitRate, avoidedBPerSeries)
}

// TestShardedFreeListDensityCost measures, not assumes, the real per-series arena
// cost of Phase A's locking design: splitting one shared free list into N
// independent per-shard free lists (see seriesShard's doc comment in head.go for
// why sharding requires this - reuse is confined to within a shard purely as a
// byte-addressing fact). A freed region in one shard can never satisfy an alloc()
// in another, so for the identical workload, more shards can only ever reduce the
// free list's effective hit rate, never improve it - this reports the real delta
// across shard counts instead of assuming a number, per CHECKLIST.md's locked-down
// design ("start modest, measure contention/density rather than guess").
//
// Uses the same staggered create/append shape as TestMemoryPerSeries_Staggered -
// TestMemoryPerSeries's create-everything-then-append-in-lockstep shape never gives
// any free list (sharded or not) a chance to matter, so it wouldn't show a real
// delta here even if one exists. Goes through the real Head/GetOrCreateSeries/
// Append API (not raw SeriesStore.Create) since the sharded density cost is a
// property of Head's ref-to-shard distribution, not of SeriesStore in isolation.
func TestShardedFreeListDensityCost(t *testing.T) {
	if testing.Short() {
		t.Skip("100k-series heap measurement; skipped in -short")
	}
	for _, numShards := range []int{1, 8, defaultNumShards} {
		t.Run(fmt.Sprintf("shards=%d", numShards), func(t *testing.T) {
			measureShardedStaggered(t, numShards)
		})
	}
}

func measureShardedStaggered(t *testing.T, numShards int) {
	const (
		totalSeries  = 100_000
		waves        = 20
		perWave      = totalSeries / waves
		seriesPerTgt = 200
		roundsAfter  = 8
	)

	h := NewHeadWithShards(totalSeries, totalSeries/seriesPerTgt, totalSeries+100, numShards)
	tgtFor := func(i int) TargetLabels {
		return TargetLabels{Cluster: "c", Namespace: "n", Pod: fmt.Sprintf("p%d", i/seriesPerTgt), Container: "co", Node: "no", Job: "j"}
	}

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	var refs []uint32
	ts := int64(1700000000000)
	for wave := 0; wave < waves; wave++ {
		for w := 0; w < perWave; w++ {
			i := wave*perWave + w
			// (targetID, nameID) alone must be unique per series - i%400 cycles
			// through 400 distinct metric name strings, and since seriesPerTgt
			// (200) divides evenly into that cycle, every series within one
			// target's 200-series block gets a distinct metric name, guaranteeing
			// a genuinely new series every call (this test needs 100k DISTINCT
			// series, not incidental dedup hits) without needing a local label -
			// which would need 100k distinct symbol strings and overflow
			// SeriesStore's uint16 localRef field (see ErrTooManySymbols).
			ref, err := h.GetOrCreateSeries(tgtFor(i), fmt.Sprintf("metric_%d", i%400))
			if err != nil {
				t.Fatalf("wave %d, series %d: GetOrCreateSeries: %v", wave, w, err)
			}
			refs = append(refs, ref)
		}
		for i, ref := range refs {
			if err := h.Append(ref, ts, valueFor(i, wave)); err != nil {
				t.Fatalf("wave %d, series %d: Append: %v", wave, i, err)
			}
		}
		ts += 15000
	}
	for r := 0; r < roundsAfter; r++ {
		for i, ref := range refs {
			if err := h.Append(ref, ts, valueFor(i, waves+r)); err != nil {
				t.Fatalf("post-wave round %d, series %d: Append: %v", r, i, err)
			}
		}
		ts += 15000
	}

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(h)
	runtime.KeepAlive(refs)

	var totalArenaBytes, allocHits, allocMisses uint64
	for _, shard := range h.shards {
		totalArenaBytes += uint64(len(shard.series.arena))
		allocHits += shard.series.AllocHits
		allocMisses += shard.series.AllocMisses
	}
	heapBytes := after.HeapAlloc - before.HeapAlloc
	hitRate := float64(allocHits) / float64(allocHits+allocMisses) * 100
	t.Logf("shards=%d: heap %.1f MB (%.1f B/series), total arena %.2f B/series, "+
		"alloc hits=%d misses=%d (%.1f%% hit rate)",
		numShards, float64(heapBytes)/1e6, float64(heapBytes)/totalSeries,
		float64(totalArenaBytes)/totalSeries, allocHits, allocMisses, hitRate)
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
