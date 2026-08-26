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

	"github.com/prometheus/prometheus/model/labels"
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
	dh, err := CreateDurableHead(dir, 4, 1, 8)
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
	if dh.series.AllocHits == 0 {
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
	dh, err := CreateDurableHead(dir, 4, 1, 8)
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
	if dh.series.slotCap[refA[0]] <= initialSlotBytes {
		t.Fatalf("series A slotCap = %d, expected growth past %d - test didn't force a real growSlot", dh.series.slotCap[refA[0]], initialSlotBytes)
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

	if dh.series.AllocHits == 0 {
		t.Fatal("AllocHits = 0 - B never actually reused A's freed region, this test proves nothing")
	}
	t.Logf("confirmed real reuse: B's slotOff=%d, A's current (grown) slotOff=%d, AllocHits=%d", dh.series.slotOff[refB[0]], dh.series.slotOff[refA[0]], dh.series.AllocHits)

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
	dh, err := CreateDurableHead(dir, 4, 1, 8)
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
	bSlotBeforeGrowth := dh.series.slotOff[refB[0]]
	if dh.series.slotCap[refB[0]] != initialSlotBytes {
		t.Fatalf("series B already grown past its initial slot before the check below - test setup assumption broken (slotCap=%d)", dh.series.slotCap[refB[0]])
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
	if dh.series.slotOff[refB[0]] == bSlotBeforeGrowth {
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
	if dh.series.slotOff[refC[0]] != bSlotBeforeGrowth {
		t.Fatalf("series C's slotOff = %d, want %d (B's vacated slot) - chained reuse wasn't forced as intended", dh.series.slotOff[refC[0]], bSlotBeforeGrowth)
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

	arenaPath := dir + "/" + fileArena
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
