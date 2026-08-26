package columnarhead

import (
	"context"
	"fmt"
	"math"
	"testing"

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

// TestDisableReuseArenaCost measures the real, stated cost of NewDurableSeriesStore's
// disableReuse tradeoff (see its doc comment and durability.go's package doc comment
// on why durability needs it): running the IDENTICAL append workload through a normal
// SeriesStore (free-list reuse enabled) and a durable one (disabled), and comparing
// final arena size - not assumed, measured. The workload appends enough samples per
// series to trigger several real growSlot events each (the case where reuse actually
// matters), not just one or two.
func TestDisableReuseArenaCost(t *testing.T) {
	const numSeries = 2000
	const samplesPerSeries = 200

	build := func(store *SeriesStore) int {
		refs := make([]uint32, numSeries)
		for i := 0; i < numSeries; i++ {
			refs[i] = store.Create(uint32(i), uint16(i%50), 0, 0, false)
		}
		ts := int64(1700000000000)
		for s := 0; s < samplesPerSeries; s++ {
			ts += 15000
			for i := 0; i < numSeries; i++ {
				v := float64(i)*1.7 + float64(s) // irregular deltas, avoids the cheap 1-bit "unchanged" path
				if err := store.Append(refs[i], ts, v); err != nil {
					t.Fatalf("Append: %v", err)
				}
			}
		}
		return len(store.arena)
	}

	withReuse := build(NewSeriesStore(numSeries))
	withoutReuse := build(NewDurableSeriesStore(numSeries))

	bWithReuse := float64(withReuse) / numSeries
	bWithoutReuse := float64(withoutReuse) / numSeries
	t.Logf("arena size: with reuse = %d bytes (%.1f B/series), without reuse (durable) = %d bytes (%.1f B/series), +%.1f%%",
		withReuse, bWithReuse, withoutReuse, bWithoutReuse, (bWithoutReuse/bWithReuse-1)*100)

	if withoutReuse < withReuse {
		t.Fatalf("disabling reuse produced a SMALLER arena (%d) than reuse enabled (%d) - measurement is backwards", withoutReuse, withReuse)
	}
}
