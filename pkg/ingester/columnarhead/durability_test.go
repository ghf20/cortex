package columnarhead

import (
	"context"
	"fmt"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

// TestDurableHeadSurvivesSimulatedCrash is the decisive test for the WAL-as-arena
// spike: build a durable head, append samples, Flush, append MORE samples, then
// Close WITHOUT flushing again (simulating a crash) - reload from disk and confirm
// everything up to the last Flush survived bit-identically, and everything appended
// after it is correctly absent, not silently recovered by accident.
func TestDurableHeadSurvivesSimulatedCrash(t *testing.T) {
	dir := t.TempDir()
	dh, err := CreateDurableHead(dir, 8, 4, 32)
	if err != nil {
		t.Fatalf("CreateDurableHead: %v", err)
	}

	const numSeries = 6
	const beforeFlush = 40
	const afterFlush = 15

	labelsFor := func(i int) labels.Labels {
		return labels.FromStrings(
			labels.MetricName, fmt.Sprintf("series_%d", i),
			"cluster", "c", "namespace", "n", "pod", fmt.Sprintf("p%d", i), "container", "co", "node", "no", "job", "j",
		)
	}

	app := dh.Appender(context.Background())
	want := make([][]sample, numSeries)
	base := int64(1700000000000)
	for i := 0; i < numSeries; i++ {
		l := labelsFor(i)
		for s := 0; s < beforeFlush; s++ {
			ts := base + int64(s)*15000
			v := float64(i*1000 + s)
			if _, err := app.Append(0, l, ts, v); err != nil {
				t.Fatalf("Append(series %d, sample %d): %v", i, s, err)
			}
			want[i] = append(want[i], sample{ts, v})
		}
	}

	stats, err := dh.Flush()
	if err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if stats.NewArenaBytes == 0 {
		t.Fatal("Flush reported 0 new arena bytes after real appends - flush isn't seeing the writes")
	}
	t.Logf("first flush: %+v", stats)

	// Appended AFTER the flush - these must NOT survive the simulated crash below.
	var lostSamples int
	for i := 0; i < numSeries; i++ {
		l := labelsFor(i)
		for s := 0; s < afterFlush; s++ {
			ts := base + int64(beforeFlush+s)*15000
			v := float64(i*1000 + beforeFlush + s)
			if _, err := app.Append(0, l, ts, v); err != nil {
				t.Fatalf("Append(series %d, post-flush sample %d): %v", i, s, err)
			}
			lostSamples++
		}
	}

	// Simulate a crash: close file handles without flushing the post-flush writes.
	if err := dh.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reloaded, err := LoadDurableHead(dir)
	if err != nil {
		t.Fatalf("LoadDurableHead: %v", err)
	}
	defer reloaded.Close()

	if reloaded.NumSeries() != numSeries {
		t.Fatalf("reloaded NumSeries() = %d, want %d", reloaded.NumSeries(), numSeries)
	}

	// Explicit Close (not deferred to function exit) - the querier's read lock
	// must be released before the post-reload append below takes the write lock,
	// exactly the "un-closed querier wedges every future write" hazard Head's own
	// doc comment warns about (see Head.Querier). A deferred Close here would
	// still run eventually, but only at function exit - after the write below
	// already deadlocked waiting for it.
	func() {
		q, err := reloaded.Querier(math.MinInt64, math.MaxInt64)
		if err != nil {
			t.Fatalf("Querier: %v", err)
		}
		defer q.Close()

		for i := 0; i < numSeries; i++ {
			m := labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, fmt.Sprintf("series_%d", i))
			ss := q.Select(context.Background(), false, nil, m)
			if !ss.Next() {
				t.Fatalf("series %d: not found after reload", i)
			}
			it := ss.At().Iterator(nil)
			var got []sample
			for it.Next() == chunkenc.ValFloat {
				ts, v := it.At()
				got = append(got, sample{ts, v})
			}
			assertSamplesEqual(t, got, want[i])
			if ss.Next() {
				t.Fatalf("series %d: matcher unexpectedly matched more than one series", i)
			}
		}
	}()

	// The reloaded head must still work as a normal, live head - append more and
	// confirm dedup/continuity, not just read-only replay.
	app2 := reloaded.Appender(context.Background())
	extraTS := base + int64(beforeFlush+100)*15000
	if _, err := app2.Append(0, labelsFor(0), extraTS, 999); err != nil {
		t.Fatalf("Append after reload: %v", err)
	}
	if reloaded.NumSeries() != numSeries {
		t.Fatalf("NumSeries() after post-reload append = %d, want %d (dedup broken)", reloaded.NumSeries(), numSeries)
	}

	t.Logf("simulated crash correctly lost %d post-flush samples across %d series", lostSamples, numSeries)
}

// TestDurableHeadFlushIsIncremental is the other half of the "no redundant WAL copy"
// claim: a second Flush after more appends should write roughly the new data's size,
// not the whole live head's size again.
func TestDurableHeadFlushIsIncremental(t *testing.T) {
	dir := t.TempDir()
	dh, err := CreateDurableHead(dir, 4, 2, 16)
	if err != nil {
		t.Fatalf("CreateDurableHead: %v", err)
	}
	defer dh.Close()

	l := labels.FromStrings(
		labels.MetricName, "up",
		"cluster", "c", "namespace", "n", "pod", "p", "container", "co", "node", "no", "job", "j",
	)
	app := dh.Appender(context.Background())
	base := int64(1700000000000)
	for s := 0; s < 20; s++ {
		if _, err := app.Append(0, l, base+int64(s)*15000, float64(s)); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	first, err := dh.Flush()
	if err != nil {
		t.Fatalf("first Flush: %v", err)
	}

	for s := 20; s < 25; s++ {
		if _, err := app.Append(0, l, base+int64(s)*15000, float64(s)); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	second, err := dh.Flush()
	if err != nil {
		t.Fatalf("second Flush: %v", err)
	}

	if second.NewArenaBytes == 0 {
		t.Fatal("second Flush wrote 0 new arena bytes despite 5 new samples")
	}
	if second.NewArenaBytes >= first.NewArenaBytes {
		t.Fatalf("second Flush wrote %d new arena bytes (5 samples) vs first Flush's %d (20 samples) - not incremental", second.NewArenaBytes, first.NewArenaBytes)
	}
	t.Logf("first flush (20 samples): %+v; second flush (5 more): %+v", first, second)
}

// TestDurableHeadUsesNormalSeriesStore is a small guard against regressing back to
// disabling reuse (see durability.go's package doc comment for why that was tried,
// measured at +14.6% arena size, and retracted): a durable head's SeriesStore must
// behave identically to an ordinary one's, including real free-list reuse.
func TestDurableHeadUsesNormalSeriesStore(t *testing.T) {
	dir := t.TempDir()
	// Single shard: reuse is confined to within a shard (see seriesShard's doc
	// comment in head.go), so both series below must land in the same shard's
	// arena for this test's premise (second series reuses the first's freed
	// slot) to hold at all.
	dh, err := CreateDurableHeadWithShards(dir, 4, 1, 8, 1)
	if err != nil {
		t.Fatalf("CreateDurableHead: %v", err)
	}
	defer dh.Close()

	l := labels.FromStrings(labels.MetricName, "up", "cluster", "c", "namespace", "n", "pod", "p", "container", "co", "node", "no", "job", "j")
	app := dh.Appender(context.Background())
	base := int64(1700000000000)
	for s := 0; s < 20; s++ {
		if _, err := app.Append(0, l, base+int64(s)*15000, float64(s)*1.7); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	// Forces at least one growSlot, which frees the original 16-byte slot; a
	// second series landing there is the ordinary reuse path.
	l2 := labels.FromStrings(labels.MetricName, "down", "cluster", "c", "namespace", "n", "pod", "p2", "container", "co", "node", "no", "job", "j")
	if _, err := app.Append(0, l2, base, 1); err != nil {
		t.Fatalf("Append(second series): %v", err)
	}
	shard, _ := dh.shardFor(0)
	if shard.series.AllocHits == 0 {
		t.Fatal("AllocHits = 0 - durable head's SeriesStore isn't reusing freed regions like an ordinary one would")
	}
}

// TestDurableHeadTruncateThenFlush is the decisive check for the Truncate/Flush
// interaction: SeriesStore.Truncate rewrites a series' bytes from scratch AT THE
// SAME slotOff, which a byte-count-only comparison cannot distinguish from "nothing
// changed" (found by probing this interaction directly - it wasn't obvious from
// either mechanism's own tests in isolation). Flush after Truncate must durably
// reflect the truncated (shorter) state, not silently keep serving the stale,
// pre-truncation bytes.
func TestDurableHeadTruncateThenFlush(t *testing.T) {
	dir := t.TempDir()
	dh, err := CreateDurableHead(dir, 2, 1, 8)
	if err != nil {
		t.Fatalf("CreateDurableHead: %v", err)
	}
	defer dh.Close()

	l := labels.FromStrings(labels.MetricName, "up", "cluster", "c", "namespace", "n", "pod", "p", "container", "co", "node", "no", "job", "j")
	app := dh.Appender(context.Background())
	base := int64(1700000000000)
	var all []sample
	for s := 0; s < 10; s++ {
		ts := base + int64(s)*15000
		v := float64(s)
		if _, err := app.Append(0, l, ts, v); err != nil {
			t.Fatalf("Append: %v", err)
		}
		all = append(all, sample{ts, v})
	}
	if _, err := dh.Flush(); err != nil {
		t.Fatalf("first Flush: %v", err)
	}

	// Truncate drops the first 5 samples, rewriting the series' arena bytes in
	// place at the SAME slotOff - the case a naive byte-count comparison misses.
	dh.Truncate(base + 5*15000)
	want := all[5:]

	if _, err := dh.Flush(); err != nil {
		t.Fatalf("post-truncate Flush: %v", err)
	}
	if err := dh.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reloaded, err := LoadDurableHead(dir)
	if err != nil {
		t.Fatalf("LoadDurableHead: %v", err)
	}
	defer reloaded.Close()

	q, err := reloaded.Querier(math.MinInt64, math.MaxInt64)
	if err != nil {
		t.Fatalf("Querier: %v", err)
	}
	defer q.Close()
	m := labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "up")
	ss := q.Select(context.Background(), false, nil, m)
	if !ss.Next() {
		t.Fatal("series not found")
	}
	it := ss.At().Iterator(nil)
	var got []sample
	for it.Next() == chunkenc.ValFloat {
		ts, v := it.At()
		got = append(got, sample{ts, v})
	}
	assertSamplesEqual(t, got, want)
}

// TestDurableHeadReloadDoesNotResurrectDeletedSeries confirms a series
// Truncate logically removed (Head.Truncate's own doc comment: no retained
// float/histogram/OOO data left) does NOT come back as discoverable after a
// Flush+reload, even though its SeriesStore array slot is still on disk
// (refs are never reused/compacted away). Without decodeHistogramStore/the
// reload loop's own NumSamples/Has filter, the blind "every ref 0..nextRef is
// live" reconstruction would resurrect it - silently undoing PostDeletion's
// counter decrements the moment the process restarts, the exact drift this
// mechanism exists to prevent.
func TestDurableHeadReloadDoesNotResurrectDeletedSeries(t *testing.T) {
	dir := t.TempDir()
	dh, err := CreateDurableHead(dir, 2, 1, 8)
	if err != nil {
		t.Fatalf("CreateDurableHead: %v", err)
	}

	l := labels.FromStrings(labels.MetricName, "up", "cluster", "c", "namespace", "n", "pod", "p", "container", "co", "node", "no", "job", "j")
	app := dh.Appender(context.Background())
	base := int64(1700000000000)
	oldRef, err := app.Append(0, l, base, 1)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if _, err := dh.Flush(); err != nil {
		t.Fatalf("first Flush: %v", err)
	}

	dh.Truncate(base + 1) // ages out the only sample - the series should be deleted
	if got := dh.NumLiveSeries(); got != 0 {
		t.Fatalf("NumLiveSeries() after Truncate = %d, want 0", got)
	}
	if _, err := dh.Flush(); err != nil {
		t.Fatalf("post-truncate Flush: %v", err)
	}
	if err := dh.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reloaded, err := LoadDurableHead(dir)
	if err != nil {
		t.Fatalf("LoadDurableHead: %v", err)
	}
	defer reloaded.Close()

	if got := reloaded.NumLiveSeries(); got != 0 {
		t.Fatalf("NumLiveSeries() after reload = %d, want 0 (the deleted series must not be resurrected)", got)
	}
	if refs, ok := reloaded.SeriesRefsForName("up"); ok && len(refs) != 0 {
		t.Fatalf("SeriesRefsForName(\"up\") after reload = %v, want empty or not-found", refs)
	}

	// A new sample for the same target/metric after reload must get a
	// genuinely new ref, not resurrect the old (still on-disk, but
	// deliberately orphaned) one.
	app2 := reloaded.Appender(context.Background())
	newRef, err := app2.Append(0, l, base+30000, 2)
	if err != nil {
		t.Fatalf("Append after reload: %v", err)
	}
	if newRef == oldRef {
		t.Fatalf("Append after reload reused the old (deleted) ref %d instead of allocating a new one", oldRef)
	}
	if got := reloaded.NumLiveSeries(); got != 1 {
		t.Fatalf("NumLiveSeries() after re-appending = %d, want 1", got)
	}
}

// TestDurableHeadSurvivesCrossSeriesReuse confirms per-series flush tracking
// (flushedSlotOff/flushedBytes/flushedGeneration) makes free-list reuse safe for
// durability on its own, without disabling reuse (see durability.go's package doc
// comment for the full history) - a real, different question from whether reuse is
// safe for CONCURRENT reads (which it isn't, independent of this - see Head's
// locking doc comment; that's why Flush and Append share the same lock).
//
// Forces the exact adversarial sequence: series A grows past its initial 16-byte
// slot (freeing it) and gets flushed at its NEW location; series B is then created
// and its own Create() (which always allocates a fresh initialSlotBytes-sized
// region) reuses A's just-freed 16-byte region, writing DIFFERENT content there.
// If per-series tracking had a gap, the file would still show A's stale bytes at
// that offset instead of B's real data after the next Flush. (This particular test
// only exercises a brand-new reusing series; TestDurableHeadSurvivesChainedReuse
// covers the sharper case of an already-flushed series moving into a reused slot.)
func TestDurableHeadSurvivesCrossSeriesReuse(t *testing.T) {
	dir := t.TempDir()
	// Single shard: see TestDurableHeadUsesNormalSeriesStore's comment - A and B
	// must share a shard's arena for B to reuse A's freed slot at all.
	dh, err := CreateDurableHeadWithShards(dir, 4, 1, 8, 1)
	if err != nil {
		t.Fatalf("CreateDurableHead: %v", err)
	}
	defer dh.Close()

	lA := labels.FromStrings(labels.MetricName, "series_a", "cluster", "c", "namespace", "n", "pod", "pa", "container", "co", "node", "no", "job", "j")
	lB := labels.FromStrings(labels.MetricName, "series_b", "cluster", "c", "namespace", "n", "pod", "pb", "container", "co", "node", "no", "job", "j")

	app := dh.Appender(context.Background())
	base := int64(1700000000000)

	// A: append enough irregular-delta samples to force at least one real growSlot
	// (past the 16-byte initial slot), freeing A's original region.
	var wantA []sample
	for s := 0; s < 20; s++ {
		ts := base + int64(s)*15000
		v := float64(s) * 1.7
		if _, err := app.Append(0, lA, ts, v); err != nil {
			t.Fatalf("Append(A): %v", err)
		}
		wantA = append(wantA, sample{ts, v})
	}
	refA, ok := dh.SeriesRefsForName("series_a")
	if !ok || len(refA) != 1 {
		t.Fatalf("series_a not found as expected: %v %v", refA, ok)
	}
	shardA, localA := dh.shardFor(refA[0])
	if shardA.series.slotCap[localA] <= initialSlotBytes {
		t.Fatalf("series A slotCap = %d, expected growth past %d - test didn't force a real growSlot", shardA.series.slotCap[localA], initialSlotBytes)
	}
	if _, err := dh.Flush(); err != nil {
		t.Fatalf("Flush after A's growth: %v", err)
	}

	// B: created AFTER A's original 16-byte slot was freed - B's own Create()
	// allocates initialSlotBytes and should reuse exactly that freed region.
	wantB := []sample{{base, 999}}
	if _, err := app.Append(0, lB, base, 999); err != nil {
		t.Fatalf("Append(B): %v", err)
	}
	refB, ok := dh.SeriesRefsForName("series_b")
	if !ok || len(refB) != 1 {
		t.Fatalf("series_b not found as expected: %v %v", refB, ok)
	}

	shardB, localB := dh.shardFor(refB[0])
	if shardB.series.AllocHits == 0 {
		t.Fatal("AllocHits = 0 - B never actually reused A's freed region, this test proves nothing")
	}
	t.Logf("confirmed real reuse: B's slotOff=%d, A's current (grown) slotOff=%d, AllocHits=%d", shardB.series.slotOff[localB], shardA.series.slotOff[localA], shardB.series.AllocHits)

	if _, err := dh.Flush(); err != nil {
		t.Fatalf("Flush after B: %v", err)
	}
	if err := dh.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reloaded, err := LoadDurableHead(dir)
	if err != nil {
		t.Fatalf("LoadDurableHead: %v", err)
	}
	defer reloaded.Close()

	check := func(name string, want []sample) {
		t.Helper()
		q, err := reloaded.Querier(math.MinInt64, math.MaxInt64)
		if err != nil {
			t.Fatalf("Querier: %v", err)
		}
		defer q.Close()
		m := labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, name)
		ss := q.Select(context.Background(), false, nil, m)
		if !ss.Next() {
			t.Fatalf("%s: not found after reload", name)
		}
		it := ss.At().Iterator(nil)
		var got []sample
		for it.Next() == chunkenc.ValFloat {
			ts, v := it.At()
			got = append(got, sample{ts, v})
		}
		assertSamplesEqual(t, got, want)
	}
	check("series_a", wantA)
	check("series_b", wantB)
}

// TestDurableHeadSurvivesChainedReuse goes further than
// TestDurableHeadSurvivesCrossSeriesReuse: that test's reusing series (B) was
// brand new, so its own flushedBytes tracking started at 0 regardless of whether
// the slotOff/generation mismatch check fired - it didn't actually exercise the
// check's role in the reuse case (confirmed by mutation-testing: disabling the
// check entirely still passed that test). This one forces the sharper case: an
// EXISTING, already-flushed series (B) later GROWS into a slot some OTHER series
// previously vacated - B's own flushedBytes is nonzero and tied to its OLD slotOff
// before the move, which is exactly what the mismatch check must catch.
func TestDurableHeadSurvivesChainedReuse(t *testing.T) {
	dir := t.TempDir()
	// Single shard: A, B, and C must all share one shard's arena for the chained
	// reuse (A's slot -> B -> C) this test forces to be possible at all.
	dh, err := CreateDurableHeadWithShards(dir, 4, 1, 8, 1)
	if err != nil {
		t.Fatalf("CreateDurableHead: %v", err)
	}
	defer dh.Close()

	label := func(name, pod string) labels.Labels {
		return labels.FromStrings(labels.MetricName, name, "cluster", "c", "namespace", "n", "pod", pod, "container", "co", "node", "no", "job", "j")
	}
	app := dh.Appender(context.Background())
	base := int64(1700000000000)
	irregular := func(s int) float64 { return float64(s) * 1.7 }

	// A grows past 16 bytes, freeing its original slot.
	var wantA []sample
	for s := 0; s < 20; s++ {
		ts := base + int64(s)*15000
		v := irregular(s)
		if _, err := app.Append(0, label("series_a", "pa"), ts, v); err != nil {
			t.Fatalf("Append(A): %v", err)
		}
		wantA = append(wantA, sample{ts, v})
	}
	if _, err := dh.Flush(); err != nil {
		t.Fatalf("Flush 1: %v", err)
	}

	// B is created (reusing A's freed 16-byte slot) with exactly ONE sample -
	// a full 16-byte slot (firstSampleBits=128 bits), no growth yet - then
	// flushed, so flushedBytes[B] becomes genuinely nonzero AT THIS (reused)
	// slotOff before B ever moves.
	wantB := []sample{{base, 100 + irregular(0)}}
	if _, err := app.Append(0, label("series_b", "pb"), base, wantB[0].v); err != nil {
		t.Fatalf("Append(B) initial: %v", err)
	}
	if _, err := dh.Flush(); err != nil {
		t.Fatalf("Flush 2: %v", err)
	}
	refB, ok := dh.SeriesRefsForName("series_b")
	if !ok || len(refB) != 1 {
		t.Fatalf("series_b not found: %v %v", refB, ok)
	}
	shardB, localB := dh.shardFor(refB[0])
	bSlotBeforeGrowth := shardB.series.slotOff[localB]
	if shardB.series.slotCap[localB] != initialSlotBytes {
		t.Fatalf("series B already grown past its initial slot before the check below - test setup assumption broken (slotCap=%d)", shardB.series.slotCap[localB])
	}

	// ...then B itself grows past ITS slot, moving away and freeing that same
	// physical region YET AGAIN - the case the earlier test never actually hit:
	// B was ALREADY flushed with nonzero flushedBytes at bSlotBeforeGrowth, and
	// now genuinely moves.
	for s := 1; s < 20; s++ {
		ts := base + int64(s)*15000
		v := 100 + irregular(s)
		if _, err := app.Append(0, label("series_b", "pb"), ts, v); err != nil {
			t.Fatalf("Append(B) growth: %v", err)
		}
		wantB = append(wantB, sample{ts, v})
	}
	if shardB.series.slotOff[localB] == bSlotBeforeGrowth {
		t.Fatal("series B never actually moved - test didn't force the growth it needs")
	}
	if _, err := dh.Flush(); err != nil {
		t.Fatalf("Flush 3: %v", err)
	}

	// C is created after B vacated that region a second time - C's Create()
	// should land there too (third occupant of the same physical bytes).
	wantC := []sample{{base, 555}}
	if _, err := app.Append(0, label("series_c", "pc"), base, 555); err != nil {
		t.Fatalf("Append(C): %v", err)
	}
	refC, ok := dh.SeriesRefsForName("series_c")
	if !ok || len(refC) != 1 {
		t.Fatalf("series_c not found: %v %v", refC, ok)
	}
	shardC, localC := dh.shardFor(refC[0])
	if shardC.series.slotOff[localC] != bSlotBeforeGrowth {
		t.Fatalf("series C's slotOff = %d, want %d (B's vacated slot) - chained reuse wasn't forced as intended", shardC.series.slotOff[localC], bSlotBeforeGrowth)
	}
	t.Logf("confirmed chained reuse: A's original slot -> B -> C, all at offset %d", bSlotBeforeGrowth)

	if _, err := dh.Flush(); err != nil {
		t.Fatalf("Flush 4: %v", err)
	}
	if err := dh.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reloaded, err := LoadDurableHead(dir)
	if err != nil {
		t.Fatalf("LoadDurableHead: %v", err)
	}
	defer reloaded.Close()

	check := func(name string, want []sample) {
		t.Helper()
		q, err := reloaded.Querier(math.MinInt64, math.MaxInt64)
		if err != nil {
			t.Fatalf("Querier: %v", err)
		}
		defer q.Close()
		m := labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, name)
		ss := q.Select(context.Background(), false, nil, m)
		if !ss.Next() {
			t.Fatalf("%s: not found after reload", name)
		}
		it := ss.At().Iterator(nil)
		var got []sample
		for it.Next() == chunkenc.ValFloat {
			ts, v := it.At()
			got = append(got, sample{ts, v})
		}
		assertSamplesEqual(t, got, want)
	}
	check("series_a", wantA)
	check("series_b", wantB)
	check("series_c", wantC)
}

// TestDurableHeadAutoFlush is the decisive test for StartAutoFlush: real samples
// appended while a background flush loop is running must show up durably (survive
// a simulated crash) without the test ever calling Flush itself, and samples
// appended AFTER stop() must not.
func TestDurableHeadAutoFlush(t *testing.T) {
	dir := t.TempDir()
	dh, err := CreateDurableHead(dir, 2, 1, 8)
	if err != nil {
		t.Fatalf("CreateDurableHead: %v", err)
	}

	flushed := make(chan FlushStats, 64)
	stop := dh.StartAutoFlush(10*time.Millisecond, func(stats FlushStats, err error) {
		if err != nil {
			t.Errorf("auto-flush: %v", err)
			return
		}
		flushed <- stats
	})

	l := labels.FromStrings(labels.MetricName, "up", "cluster", "c", "namespace", "n", "pod", "p", "container", "co", "node", "no", "job", "j")
	app := dh.Appender(context.Background())
	base := int64(1700000000000)
	want := []sample{{base, 1}}
	if _, err := app.Append(0, l, base, 1); err != nil {
		t.Fatalf("Append: %v", err)
	}

	// Wait for at least one real auto-flush to have written this sample, rather
	// than sleeping a fixed guess.
	deadline := time.After(2 * time.Second)
	sawNonEmptyFlush := false
	for !sawNonEmptyFlush {
		select {
		case stats := <-flushed:
			if stats.NewArenaBytes > 0 {
				sawNonEmptyFlush = true
			}
		case <-deadline:
			t.Fatal("timed out waiting for auto-flush to pick up the appended sample")
		}
	}

	stop()

	// Appended AFTER stop - must NOT survive the simulated crash below.
	if _, err := app.Append(0, l, base+15000, 2); err != nil {
		t.Fatalf("Append after stop: %v", err)
	}

	if err := dh.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reloaded, err := LoadDurableHead(dir)
	if err != nil {
		t.Fatalf("LoadDurableHead: %v", err)
	}
	defer reloaded.Close()

	q, err := reloaded.Querier(math.MinInt64, math.MaxInt64)
	if err != nil {
		t.Fatalf("Querier: %v", err)
	}
	defer q.Close()
	m := labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "up")
	ss := q.Select(context.Background(), false, nil, m)
	if !ss.Next() {
		t.Fatal("series not found after reload")
	}
	it := ss.At().Iterator(nil)
	var got []sample
	for it.Next() == chunkenc.ValFloat {
		ts, v := it.At()
		got = append(got, sample{ts, v})
	}
	assertSamplesEqual(t, got, want)
}

// TestFlushBlocksAppendersUnderLoad measures, not assumes, how long a single Flush
// call blocks concurrent Appenders at a realistic data scale - the real cost behind
// StartAutoFlush's interval tradeoff (see its doc comment) and Phase 4's flagged
// "one coarse lock" limitation. Not a pass/fail correctness test - reports real
// numbers via t.Logf for the record.
func TestFlushBlocksAppendersUnderLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("scale measurement; skipped in -short")
	}
	dir := t.TempDir()
	dh, err := CreateDurableHead(dir, 5000, 500, 64)
	if err != nil {
		t.Fatalf("CreateDurableHead: %v", err)
	}
	defer dh.Close()

	const numSeries = 5000
	const samplesPerSeriesBeforeFlush = 100

	app := dh.Appender(context.Background())
	base := int64(1700000000000)
	for i := 0; i < numSeries; i++ {
		l := labels.FromStrings(
			labels.MetricName, fmt.Sprintf("series_%d", i%200),
			"cluster", "c", "namespace", "n", "pod", fmt.Sprintf("p%d", i), "container", "co", "node", "no", "job", "j",
		)
		for s := 0; s < samplesPerSeriesBeforeFlush; s++ {
			if _, err := app.Append(0, l, base+int64(s)*15000, float64(s)*1.7); err != nil {
				t.Fatalf("Append: %v", err)
			}
		}
	}

	// A backlog of unflushed data (numSeries * samplesPerSeriesBeforeFlush) is now
	// sitting in memory - this is what the upcoming Flush call has to write out,
	// mimicking a real system's first flush after a burst of ingest.
	var wg sync.WaitGroup
	stopAppending := make(chan struct{})
	var appendCount int64
	var maxAppendLatency int64 // nanoseconds, via atomic
	wg.Add(1)
	go func() {
		defer wg.Done()
		l := labels.FromStrings(labels.MetricName, "concurrent_writer", "cluster", "c", "namespace", "n", "pod", "pw", "container", "co", "node", "no", "job", "j")
		s := 0
		for {
			select {
			case <-stopAppending:
				return
			default:
			}
			start := time.Now()
			if _, err := app.Append(0, l, base+int64(samplesPerSeriesBeforeFlush+s)*15000, float64(s)); err != nil {
				t.Errorf("concurrent Append: %v", err)
				return
			}
			if elapsed := time.Since(start).Nanoseconds(); elapsed > atomic.LoadInt64(&maxAppendLatency) {
				atomic.StoreInt64(&maxAppendLatency, elapsed)
			}
			atomic.AddInt64(&appendCount, 1)
			s++
		}
	}()

	flushStart := time.Now()
	stats, err := dh.Flush()
	flushDuration := time.Since(flushStart)
	if err != nil {
		t.Fatalf("Flush: %v", err)
	}

	close(stopAppending)
	wg.Wait()

	t.Logf("Flush of %d series' backlog (%d new arena bytes, %.1f B/series) took %v",
		numSeries, stats.NewArenaBytes, float64(stats.NewArenaBytes)/numSeries, flushDuration)
	t.Logf("during that window, %d concurrent appends completed, max single-append latency %v (the append-side view of the same lock contention)",
		atomic.LoadInt64(&appendCount), time.Duration(atomic.LoadInt64(&maxAppendLatency)))
}

// TestDurableHeadCompactShrinksFile is the decisive test for Compact: after
// Truncate drops old samples, the durable arena FILE must actually shrink (not
// just the live in-memory arena), and the retained data must still survive a
// simulated crash and reload correctly.
func TestDurableHeadCompactShrinksFile(t *testing.T) {
	dir := t.TempDir()
	dh, err := CreateDurableHead(dir, 4, 1, 8)
	if err != nil {
		t.Fatalf("CreateDurableHead: %v", err)
	}

	l := labels.FromStrings(labels.MetricName, "up", "cluster", "c", "namespace", "n", "pod", "p", "container", "co", "node", "no", "job", "j")
	app := dh.Appender(context.Background())
	base := int64(1700000000000)
	const total = 500
	var all []sample
	for s := 0; s < total; s++ {
		ts := base + int64(s)*15000
		v := float64(s) * 1.7
		if _, err := app.Append(0, l, ts, v); err != nil {
			t.Fatalf("Append: %v", err)
		}
		all = append(all, sample{ts, v})
	}
	if _, err := dh.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// The lone series ("up") always lands in shard 0 (ref 0 -> shard 0 regardless
	// of shard count), so shard 0's arena file is the one that should shrink.
	arenaPath := dir + "/" + shardFileName(fileArena, 0)
	sizeBefore := fileSize(t, arenaPath)

	// Drop the first 90% of samples - a realistic post-compaction truncation.
	cutoff := base + int64(total-total/10)*15000
	dh.Truncate(cutoff)
	want := all[total-total/10:]

	stats, err := dh.Compact()
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	t.Logf("Compact stats: %+v", stats)

	sizeAfter := fileSize(t, arenaPath)
	if sizeAfter >= sizeBefore {
		t.Fatalf("arena file size after Compact = %d, want smaller than before (%d) - Compact didn't reclaim space", sizeAfter, sizeBefore)
	}
	t.Logf("arena file shrank from %d to %d bytes after Truncate+Compact", sizeBefore, sizeAfter)

	if err := dh.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reloaded, err := LoadDurableHead(dir)
	if err != nil {
		t.Fatalf("LoadDurableHead: %v", err)
	}
	defer reloaded.Close()

	// Scoped, not deferred to function exit - the read lock must be released
	// before the post-reload append below takes the write lock (see the same
	// hazard noted in TestDurableHeadSurvivesSimulatedCrash).
	func() {
		q, err := reloaded.Querier(math.MinInt64, math.MaxInt64)
		if err != nil {
			t.Fatalf("Querier: %v", err)
		}
		defer q.Close()
		m := labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "up")
		ss := q.Select(context.Background(), false, nil, m)
		if !ss.Next() {
			t.Fatal("series not found after reload")
		}
		it := ss.At().Iterator(nil)
		var got []sample
		for it.Next() == chunkenc.ValFloat {
			ts, v := it.At()
			got = append(got, sample{ts, v})
		}
		assertSamplesEqual(t, got, want)
	}()

	// Still a normal, live, appendable head after Compact + reload.
	app2 := reloaded.Appender(context.Background())
	if _, err := app2.Append(0, l, base+int64(total+1)*15000, 999); err != nil {
		t.Fatalf("Append after reload: %v", err)
	}
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat(%s): %v", path, err)
	}
	return fi.Size()
}

// TestDurableHeadPersistsMetadata is the decisive test for metadata persistence:
// UpdateMetadata via the real Appender, Flush, simulate a crash, reload, and
// confirm the metadata survives - via Head.Metadata, the same accessor real
// callers would use, not by poking internal fields.
func TestDurableHeadPersistsMetadata(t *testing.T) {
	dir := t.TempDir()
	dh, err := CreateDurableHead(dir, 2, 1, 8)
	if err != nil {
		t.Fatalf("CreateDurableHead: %v", err)
	}

	l := labels.FromStrings(labels.MetricName, "up", "cluster", "c", "namespace", "n", "pod", "p", "container", "co", "node", "no", "job", "j")
	app := dh.Appender(context.Background())
	if _, err := app.Append(0, l, 1700000000000, 1); err != nil {
		t.Fatalf("Append: %v", err)
	}
	want := metadata.Metadata{Type: model.MetricTypeGauge, Unit: "seconds", Help: "a test gauge with a reasonably long help string to exercise real byte lengths"}
	if _, err := app.UpdateMetadata(0, l, want); err != nil {
		t.Fatalf("UpdateMetadata: %v", err)
	}

	stats, err := dh.Flush()
	if err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if stats.MetadataBytes == 0 {
		t.Fatal("Flush reported 0 metadata bytes after a real UpdateMetadata call")
	}

	// Update it again to something SHORTER, then flush again - the case that
	// needs metadataFile.Truncate (encoded size can shrink, unlike series_meta.bin).
	shorter := metadata.Metadata{Type: model.MetricTypeGauge, Unit: "s", Help: "short"}
	if _, err := app.UpdateMetadata(0, l, shorter); err != nil {
		t.Fatalf("UpdateMetadata (shorter): %v", err)
	}
	if _, err := dh.Flush(); err != nil {
		t.Fatalf("second Flush: %v", err)
	}
	want = shorter

	if err := dh.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reloaded, err := LoadDurableHead(dir)
	if err != nil {
		t.Fatalf("LoadDurableHead: %v", err)
	}
	defer reloaded.Close()

	refs, ok := reloaded.SeriesRefsForName("up")
	if !ok || len(refs) != 1 {
		t.Fatalf("series 'up' not found as expected: %v %v", refs, ok)
	}
	got, ok := reloaded.Metadata(refs[0])
	if !ok {
		t.Fatal("Metadata not found after reload")
	}
	if got != want {
		t.Fatalf("Metadata after reload = %+v, want %+v", got, want)
	}
}

// TestDurableHeadPersistsExemplars is the decisive test for exemplar persistence:
// AppendExemplar via the real Appender, Flush, simulate a crash, reload, and
// confirm the exemplar survives via Head.Exemplars, including its labels (the
// variable-length part encodeExemplarStorage has to get right).
func TestDurableHeadPersistsExemplars(t *testing.T) {
	dir := t.TempDir()
	dh, err := CreateDurableHead(dir, 2, 1, 8)
	if err != nil {
		t.Fatalf("CreateDurableHead: %v", err)
	}

	l := labels.FromStrings(labels.MetricName, "requests_total", "cluster", "c", "namespace", "n", "pod", "p", "container", "co", "node", "no", "job", "j")
	app := dh.Appender(context.Background())
	if _, err := app.Append(0, l, 1700000000000, 1); err != nil {
		t.Fatalf("Append: %v", err)
	}
	traceLabels := labels.FromStrings("trace_id", "abc123", "span_id", "def456")
	want := exemplar.Exemplar{Labels: traceLabels, Value: 42.5, Ts: 1700000000000, HasTs: true}
	if _, err := app.AppendExemplar(0, l, want); err != nil {
		t.Fatalf("AppendExemplar: %v", err)
	}

	stats, err := dh.Flush()
	if err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if stats.ExemplarBytes == 0 {
		t.Fatal("Flush reported 0 exemplar bytes after a real AppendExemplar call")
	}

	if err := dh.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reloaded, err := LoadDurableHead(dir)
	if err != nil {
		t.Fatalf("LoadDurableHead: %v", err)
	}
	defer reloaded.Close()

	refs, ok := reloaded.SeriesRefsForName("requests_total")
	if !ok || len(refs) != 1 {
		t.Fatalf("series 'requests_total' not found as expected: %v %v", refs, ok)
	}
	got := reloaded.Exemplars(refs[0])
	if len(got) != 1 {
		t.Fatalf("Exemplars after reload = %v, want exactly 1", got)
	}
	if got[0].ts != want.Ts || got[0].value != want.Value {
		t.Fatalf("Exemplars[0] = {ts:%d value:%v}, want {ts:%d value:%v}", got[0].ts, got[0].value, want.Ts, want.Value)
	}
	if got[0].labels["trace_id"] != "abc123" || got[0].labels["span_id"] != "def456" || len(got[0].labels) != 2 {
		t.Fatalf("Exemplars[0].labels = %v, want trace_id=abc123, span_id=def456", got[0].labels)
	}
}

// TestDurableHeadPersistsHistograms is the decisive test for histogram
// persistence: AppendHistogram via the real Appender, Flush, simulate a crash,
// reload, and confirm the histogram survives via Head.HistogramIterator - the
// real accessor, checked with the same histEqual helper histogram_test.go itself
// uses, not a hand-rolled comparison.
func TestDurableHeadPersistsHistograms(t *testing.T) {
	dir := t.TempDir()
	dh, err := CreateDurableHead(dir, 2, 1, 8)
	if err != nil {
		t.Fatalf("CreateDurableHead: %v", err)
	}

	l := labels.FromStrings(labels.MetricName, "request_duration_seconds", "cluster", "c", "namespace", "n", "pod", "p", "container", "co", "node", "no", "job", "j")
	app := dh.Appender(context.Background())
	hists := []*histogram.Histogram{
		{
			Schema: 0, ZeroThreshold: 0.001, ZeroCount: 2, Count: 10, Sum: 42.5,
			PositiveSpans: []histogram.Span{{Offset: 0, Length: 2}}, PositiveBuckets: []int64{3, 1},
		},
		{
			Schema: 0, ZeroThreshold: 0.001, ZeroCount: 3, Count: 15, Sum: 50.0,
			PositiveSpans: []histogram.Span{{Offset: 0, Length: 2}}, PositiveBuckets: []int64{1, 1},
		},
	}
	base := int64(1700000000000)
	for i, h := range hists {
		if _, err := app.AppendHistogram(0, l, base+int64(i)*15000, h, nil); err != nil {
			t.Fatalf("AppendHistogram %d: %v", i, err)
		}
	}

	stats, err := dh.Flush()
	if err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if stats.HistogramBytes == 0 {
		t.Fatal("Flush reported 0 histogram bytes after real AppendHistogram calls")
	}

	if err := dh.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reloaded, err := LoadDurableHead(dir)
	if err != nil {
		t.Fatalf("LoadDurableHead: %v", err)
	}
	defer reloaded.Close()

	refs, ok := reloaded.SeriesRefsForName("request_duration_seconds")
	if !ok || len(refs) != 1 {
		t.Fatalf("series not found as expected: %v %v", refs, ok)
	}
	it := reloaded.HistogramIterator(refs[0])
	for i, want := range hists {
		if !it.Next() {
			t.Fatalf("HistogramIterator.Next() = false at sample %d, want true", i)
		}
		gotTS, gotH := it.At()
		if gotTS != base+int64(i)*15000 {
			t.Fatalf("sample %d: ts = %d, want %d", i, gotTS, base+int64(i)*15000)
		}
		histEqual(t, gotH, want)
	}
	if it.Next() {
		t.Fatal("HistogramIterator has more samples than expected after reload")
	}
}

// TestDurableHeadHistogramTruncateThenFlush is the decisive check for the
// histogram Truncate/Flush interaction: HistogramStore.Truncate DELETES and
// recreates a series' *histoSeries (unlike SeriesStore.Truncate's in-place
// rewrite), so a full-rewrite-per-Flush approach must correctly reflect that
// recreation - not, say, keep serving a stale copy from before the delete.
func TestDurableHeadHistogramTruncateThenFlush(t *testing.T) {
	dir := t.TempDir()
	dh, err := CreateDurableHead(dir, 2, 1, 8)
	if err != nil {
		t.Fatalf("CreateDurableHead: %v", err)
	}

	l := labels.FromStrings(labels.MetricName, "request_duration_seconds", "cluster", "c", "namespace", "n", "pod", "p", "container", "co", "node", "no", "job", "j")
	app := dh.Appender(context.Background())
	base := int64(1700000000000)
	all := []*histogram.Histogram{
		{Schema: 0, Count: 1, Sum: 1, PositiveSpans: []histogram.Span{{Offset: 0, Length: 1}}, PositiveBuckets: []int64{1}},
		{Schema: 0, Count: 2, Sum: 2, PositiveSpans: []histogram.Span{{Offset: 0, Length: 1}}, PositiveBuckets: []int64{1}},
		{Schema: 0, Count: 3, Sum: 3, PositiveSpans: []histogram.Span{{Offset: 0, Length: 1}}, PositiveBuckets: []int64{1}},
	}
	for i, h := range all {
		if _, err := app.AppendHistogram(0, l, base+int64(i)*15000, h, nil); err != nil {
			t.Fatalf("AppendHistogram %d: %v", i, err)
		}
	}
	if _, err := dh.Flush(); err != nil {
		t.Fatalf("first Flush: %v", err)
	}

	dh.Truncate(base + 1*15000) // drops the first sample only
	want := all[1:]
	wantTS := []int64{base + 15000, base + 30000}

	if _, err := dh.Flush(); err != nil {
		t.Fatalf("post-truncate Flush: %v", err)
	}
	if err := dh.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reloaded, err := LoadDurableHead(dir)
	if err != nil {
		t.Fatalf("LoadDurableHead: %v", err)
	}
	defer reloaded.Close()

	refs, ok := reloaded.SeriesRefsForName("request_duration_seconds")
	if !ok || len(refs) != 1 {
		t.Fatalf("series not found as expected: %v %v", refs, ok)
	}
	it := reloaded.HistogramIterator(refs[0])
	for i, wantH := range want {
		if !it.Next() {
			t.Fatalf("HistogramIterator.Next() = false at sample %d, want true", i)
		}
		gotTS, gotH := it.At()
		if gotTS != wantTS[i] {
			t.Fatalf("sample %d: ts = %d, want %d", i, gotTS, wantTS[i])
		}
		histEqual(t, gotH, wantH)
	}
	if it.Next() {
		t.Fatal("HistogramIterator has more samples than expected after reload - truncation wasn't reflected")
	}
}

// TestDurableHeadPersistsFloatHistograms closes a real, previously-latent gap in
// encodeHistogramStore/decodeHistogramStore (found while reworking the format for
// multi-segment layout support, CHECKLIST.md's Phase 3): the format never
// serialized isFloat or any float-path field at all, so a FloatHistogram-typed
// series decoded from a reload as int-typed (the zero value) with its
// gorilla-XOR-encoded arena bytes misinterpreted as varbit-delta-encoded ones - no
// existing durability test exercised a FloatHistogram series, so this went
// uncaught since FloatHistogram support was first built. Mirrors
// TestDurableHeadPersistsHistograms exactly, just with AppendHistogram's fh
// argument instead of h.
func TestDurableHeadPersistsFloatHistograms(t *testing.T) {
	dir := t.TempDir()
	dh, err := CreateDurableHead(dir, 2, 1, 8)
	if err != nil {
		t.Fatalf("CreateDurableHead: %v", err)
	}

	l := labels.FromStrings(labels.MetricName, "request_duration_seconds", "cluster", "c", "namespace", "n", "pod", "p", "container", "co", "node", "no", "job", "j")
	app := dh.Appender(context.Background())
	hists := []*histogram.FloatHistogram{
		{
			Schema: 0, ZeroThreshold: 0.001, ZeroCount: 2, Count: 10, Sum: 42.5,
			PositiveSpans: []histogram.Span{{Offset: 0, Length: 2}}, PositiveBuckets: []float64{3, 4},
		},
		{
			Schema: 0, ZeroThreshold: 0.001, ZeroCount: 3, Count: 15, Sum: 50.0,
			PositiveSpans: []histogram.Span{{Offset: 0, Length: 2}}, PositiveBuckets: []float64{4, 5},
		},
	}
	base := int64(1700000000000)
	for i, h := range hists {
		if _, err := app.AppendHistogram(0, l, base+int64(i)*15000, nil, h); err != nil {
			t.Fatalf("AppendHistogram %d: %v", i, err)
		}
	}

	stats, err := dh.Flush()
	if err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if stats.HistogramBytes == 0 {
		t.Fatal("Flush reported 0 histogram bytes after real AppendHistogram calls")
	}

	if err := dh.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reloaded, err := LoadDurableHead(dir)
	if err != nil {
		t.Fatalf("LoadDurableHead: %v", err)
	}
	defer reloaded.Close()

	refs, ok := reloaded.SeriesRefsForName("request_duration_seconds")
	if !ok || len(refs) != 1 {
		t.Fatalf("series not found as expected: %v %v", refs, ok)
	}
	if !reloaded.HasFloatHistogram(refs[0]) {
		t.Fatal("reloaded series lost its float-histogram type - decoded as int-typed")
	}
	it := reloaded.HistogramIterator(refs[0])
	for i, want := range hists {
		if !it.Next() {
			t.Fatalf("HistogramIterator.Next() = false at sample %d, want true", i)
		}
		gotTS, gotH := it.AtFloat()
		if gotTS != base+int64(i)*15000 {
			t.Fatalf("sample %d: ts = %d, want %d", i, gotTS, base+int64(i)*15000)
		}
		floatHistEqual(t, gotH, want)
	}
	if it.Next() {
		t.Fatal("HistogramIterator has more samples than expected after reload")
	}
}

// TestDurableHeadPersistsHistogramLayoutChange confirms a multi-segment histogram
// series (histoSegment's own doc comment - a genuine schema/zero-threshold/span
// change mid-series) survives a Flush+reload with every segment intact, not just a
// single-layout series - the durability format's own multi-segment encoding
// (encodeHistogramStore/decodeHistogramStore), exercised for real rather than only
// unit-tested against HistogramStore directly (TestHistogramStoreLayoutChange
// StartsNewSegment).
func TestDurableHeadPersistsHistogramLayoutChange(t *testing.T) {
	dir := t.TempDir()
	dh, err := CreateDurableHead(dir, 2, 1, 8)
	if err != nil {
		t.Fatalf("CreateDurableHead: %v", err)
	}

	l := labels.FromStrings(labels.MetricName, "request_duration_seconds", "cluster", "c", "namespace", "n", "pod", "p", "container", "co", "node", "no", "job", "j")
	app := dh.Appender(context.Background())
	base := int64(1700000000000)
	hists := []*histogram.Histogram{
		{Schema: 0, PositiveSpans: []histogram.Span{{Offset: 0, Length: 2}}, PositiveBuckets: []int64{1, 1}},
		{Schema: 1, PositiveSpans: []histogram.Span{{Offset: 0, Length: 3}}, PositiveBuckets: []int64{2, 1, 1}}, // schema+span change -> new segment
	}
	for i, h := range hists {
		if _, err := app.AppendHistogram(0, l, base+int64(i)*15000, h, nil); err != nil {
			t.Fatalf("AppendHistogram %d: %v", i, err)
		}
	}

	if _, err := dh.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := dh.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reloaded, err := LoadDurableHead(dir)
	if err != nil {
		t.Fatalf("LoadDurableHead: %v", err)
	}
	defer reloaded.Close()

	refs, ok := reloaded.SeriesRefsForName("request_duration_seconds")
	if !ok || len(refs) != 1 {
		t.Fatalf("series not found as expected: %v %v", refs, ok)
	}
	it := reloaded.HistogramIterator(refs[0])
	for i, want := range hists {
		if !it.Next() {
			t.Fatalf("HistogramIterator.Next() = false at sample %d, want true", i)
		}
		gotTS, gotH := it.At()
		if gotTS != base+int64(i)*15000 {
			t.Fatalf("sample %d: ts = %d, want %d", i, gotTS, base+int64(i)*15000)
		}
		histEqual(t, gotH, want)
	}
	if it.Next() {
		t.Fatal("HistogramIterator has more samples than expected after reload")
	}
}

// TestDurableHeadPersistsMinMaxTime confirms Head.MinTime/MaxTime survive a
// simulated crash and reload (headtimes.bin) - added alongside OOO support,
// since MinTime/MaxTime are also what OOO's window check depends on. Also
// confirms a durable head that crashes before its FIRST Flush ever ran doesn't
// error on reload (decodeHeadTimes' empty-buf case).
func TestDurableHeadPersistsMinMaxTime(t *testing.T) {
	dir := t.TempDir()
	dh, err := CreateDurableHead(dir, 2, 1, 8)
	if err != nil {
		t.Fatalf("CreateDurableHead: %v", err)
	}

	l := labels.FromStrings(labels.MetricName, "up", "cluster", "c", "namespace", "n", "pod", "p", "container", "co", "node", "no", "job", "j")
	app := dh.Appender(context.Background())
	base := int64(1700000000000)
	if _, err := app.Append(0, l, base, 1); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if _, err := app.Append(0, l, base+30000, 2); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if dh.MinTime() != base || dh.MaxTime() != base+30000 {
		t.Fatalf("before flush: MinTime/MaxTime = %d/%d, want %d/%d", dh.MinTime(), dh.MaxTime(), base, base+30000)
	}

	if _, err := dh.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := dh.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reloaded, err := LoadDurableHead(dir)
	if err != nil {
		t.Fatalf("LoadDurableHead: %v", err)
	}
	defer reloaded.Close()

	if reloaded.MinTime() != base {
		t.Fatalf("reloaded MinTime() = %d, want %d", reloaded.MinTime(), base)
	}
	if reloaded.MaxTime() != base+30000 {
		t.Fatalf("reloaded MaxTime() = %d, want %d", reloaded.MaxTime(), base+30000)
	}
}

// TestDurableHeadEmptyHeadTimesFile confirms a durable head that crashes before
// its first Flush ever ran reloads correctly (headtimes.bin is 0 bytes), rather
// than erroring - matching how the other never-flushed files (exemplars.bin,
// histograms.bin) already handle this.
func TestDurableHeadEmptyHeadTimesFile(t *testing.T) {
	dir := t.TempDir()
	dh, err := CreateDurableHead(dir, 1, 1, 1)
	if err != nil {
		t.Fatalf("CreateDurableHead: %v", err)
	}
	if err := dh.Close(); err != nil { // crash before any Flush
		t.Fatalf("Close: %v", err)
	}

	reloaded, err := LoadDurableHead(dir)
	if err != nil {
		t.Fatalf("LoadDurableHead: %v", err)
	}
	defer reloaded.Close()

	if reloaded.MinTime() != math.MaxInt64 {
		t.Fatalf("MinTime() = %d, want math.MaxInt64 (sentinel for never-flushed)", reloaded.MinTime())
	}
	if reloaded.MaxTime() != math.MinInt64 {
		t.Fatalf("MaxTime() = %d, want math.MinInt64 (sentinel for never-flushed)", reloaded.MaxTime())
	}
}
