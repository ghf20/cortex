package columnarhead

import (
	"context"
	"log/slog"
	"math"
	"os"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

func testLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

// TestCompactHeadRoundTrip is the decisive check: compact a real columnarhead.Head
// into a block, then read that block back with Prometheus's OWN tsdb.OpenBlock and
// tsdb.NewBlockQuerier - not anything from this package - proving CompactHead
// produces a genuinely valid TSDB block, not merely something that looks right from
// the inside.
func TestCompactHeadRoundTrip(t *testing.T) {
	h := NewHead(2, 2, 2)
	tgt := TargetLabels{Cluster: "c", Namespace: "n", Pod: "p", Container: "co", Node: "no", Job: "j"}

	upRef, err := h.GetOrCreateSeries(tgt, "up", "", "")
	if err != nil {
		t.Fatalf("GetOrCreateSeries(up): %v", err)
	}
	up := []sample{{1700000000000, 1}, {1700000015000, 1}, {1700000030000, 0}}
	for _, sm := range up {
		if err := h.Append(upRef, sm.ts, sm.v); err != nil {
			t.Fatalf("Append(up): %v", err)
		}
	}

	bucketRef, err := h.GetOrCreateSeries(tgt, "request_duration_bucket", "le", "0.1")
	if err != nil {
		t.Fatalf("GetOrCreateSeries(bucket): %v", err)
	}
	bucket := []sample{{1700000000000, 3}, {1700000015000, 5}}
	for _, sm := range bucket {
		if err := h.Append(bucketRef, sm.ts, sm.v); err != nil {
			t.Fatalf("Append(bucket): %v", err)
		}
	}

	dir := t.TempDir()
	blockDir, err := CompactHead(h, math.MinInt64, math.MaxInt64, dir, 2*60*60*1000, testLogger())
	if err != nil {
		t.Fatalf("CompactHead: %v", err)
	}
	if blockDir == "" {
		t.Fatal("CompactHead returned an empty block dir for a head with real samples")
	}

	block, err := tsdb.OpenBlock(testLogger(), blockDir, chunkenc.NewPool(), nil)
	if err != nil {
		t.Fatalf("tsdb.OpenBlock: %v", err)
	}
	defer block.Close()

	if block.MinTime() != 1700000000000 || block.MaxTime() != 1700000030000+1 {
		t.Fatalf("block MinTime/MaxTime = %d/%d, want %d/%d", block.MinTime(), block.MaxTime(), 1700000000000, 1700000030000+1)
	}

	bq, err := tsdb.NewBlockQuerier(block, math.MinInt64, math.MaxInt64)
	if err != nil {
		t.Fatalf("tsdb.NewBlockQuerier: %v", err)
	}
	defer bq.Close()

	got := map[string][]sample{}
	ss := bq.Select(context.Background(), false, nil, labels.MustNewMatcher(labels.MatchRegexp, labels.MetricName, ".+"))
	for ss.Next() {
		s := ss.At()
		name := s.Labels().Get(labels.MetricName)
		it := s.Iterator(nil)
		for it.Next() == chunkenc.ValFloat {
			ts, v := it.At()
			got[name] = append(got[name], sample{ts, v})
		}
		if err := it.Err(); err != nil {
			t.Fatalf("real chunkenc iterator error for %q: %v", name, err)
		}
	}
	if err := ss.Err(); err != nil {
		t.Fatalf("real BlockQuerier SeriesSet.Err(): %v", err)
	}

	assertSamplesEqual(t, got["up"], up)
	assertSamplesEqual(t, got["request_duration_bucket"], bucket)
	if len(got) != 2 {
		t.Fatalf("block contains %d distinct metric names, want 2 (got: %v)", len(got), got)
	}
}

// TestCompactHeadIncludesOOOSamples is the decisive check behind CHECKLIST.md's
// Phase 7 finding that CompactOOOHead can be a genuine no-op for a
// columnarhead-backed tsdbStore, not a stub: unlike real Prometheus (which stores
// OOO samples in a separate OOOHeadChunkReader needing its own compaction pass -
// see tsdb.NewOOOCompactionHead), CompactHead already goes through Head.Querier,
// and Querier's own headSeries.Iterator already merges a series' OOO buffer into
// its in-order stream via mergedIterator (see ooo.go, querier.go) - so OOO samples
// were ALREADY included in every compacted block this package has ever produced,
// whether anyone verified it or not. This test verifies it directly: append
// samples out of order (within the OOO window), compact, and confirm the real,
// independently-read block contains everything in correct timestamp order.
func TestCompactHeadIncludesOOOSamples(t *testing.T) {
	h := NewHead(1, 1, 1)
	h.SetOOOTimeWindow(60_000)
	tgt := TargetLabels{Cluster: "c", Namespace: "n", Pod: "p", Container: "co", Node: "no", Job: "j"}
	ref, err := h.GetOrCreateSeries(tgt, "up", "", "")
	if err != nil {
		t.Fatalf("GetOrCreateSeries: %v", err)
	}

	base := int64(1700000000000)
	// Append in-order first (0, 30s), then an OOO sample landing at 15s - within
	// the 60s window, so it's accepted into the OOO buffer, not rejected.
	if err := h.Append(ref, base, 1); err != nil {
		t.Fatalf("Append(0s): %v", err)
	}
	if err := h.Append(ref, base+30000, 3); err != nil {
		t.Fatalf("Append(30s): %v", err)
	}
	if err := h.Append(ref, base+15000, 2); err != nil {
		t.Fatalf("Append(15s, OOO): %v", err)
	}
	if got := h.OOOSamples(ref); len(got) != 1 {
		t.Fatalf("test setup broken: OOOSamples(ref) = %v, want exactly 1 buffered OOO sample", got)
	}
	want := []sample{{base, 1}, {base + 15000, 2}, {base + 30000, 3}}

	dir := t.TempDir()
	blockDir, err := CompactHead(h, math.MinInt64, math.MaxInt64, dir, 2*60*60*1000, testLogger())
	if err != nil {
		t.Fatalf("CompactHead: %v", err)
	}
	if blockDir == "" {
		t.Fatal("CompactHead returned an empty block dir for a head with real samples")
	}

	block, err := tsdb.OpenBlock(testLogger(), blockDir, chunkenc.NewPool(), nil)
	if err != nil {
		t.Fatalf("tsdb.OpenBlock: %v", err)
	}
	defer block.Close()

	bq, err := tsdb.NewBlockQuerier(block, math.MinInt64, math.MaxInt64)
	if err != nil {
		t.Fatalf("tsdb.NewBlockQuerier: %v", err)
	}
	defer bq.Close()

	ss := bq.Select(context.Background(), false, nil, labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "up"))
	if !ss.Next() {
		t.Fatal("series not found in compacted block")
	}
	var got []sample
	it := ss.At().Iterator(nil)
	for it.Next() == chunkenc.ValFloat {
		ts, v := it.At()
		got = append(got, sample{ts, v})
	}
	if err := it.Err(); err != nil {
		t.Fatalf("real chunkenc iterator error: %v", err)
	}
	if ss.Next() {
		t.Fatal("matcher unexpectedly matched more than one series")
	}
	assertSamplesEqual(t, got, want)
}

// TestCompactHeadRespectsTimeRange checks that CompactHead's mint/maxt bounds are
// genuinely applied, not silently ignored in favor of the head's full range: a
// sample past maxt must not appear in the resulting block.
func TestCompactHeadRespectsTimeRange(t *testing.T) {
	h := NewHead(1, 1, 1)
	tgt := TargetLabels{Cluster: "c", Namespace: "n", Pod: "p", Container: "co", Node: "no", Job: "j"}
	ref, err := h.GetOrCreateSeries(tgt, "up", "", "")
	if err != nil {
		t.Fatalf("GetOrCreateSeries: %v", err)
	}
	inRange := []sample{{1700000000000, 1}, {1700000015000, 1}}
	outOfRange := sample{1800000000000, 1}
	for _, sm := range append(append([]sample{}, inRange...), outOfRange) {
		if err := h.Append(ref, sm.ts, sm.v); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	dir := t.TempDir()
	blockDir, err := CompactHead(h, math.MinInt64, 1700000015000, dir, 2*60*60*1000, testLogger())
	if err != nil {
		t.Fatalf("CompactHead: %v", err)
	}
	if blockDir == "" {
		t.Fatal("CompactHead returned an empty block dir")
	}

	block, err := tsdb.OpenBlock(testLogger(), blockDir, chunkenc.NewPool(), nil)
	if err != nil {
		t.Fatalf("tsdb.OpenBlock: %v", err)
	}
	defer block.Close()
	if block.MaxTime() > outOfRange.ts {
		t.Fatalf("block MaxTime = %d, must not reach the out-of-range sample at %d", block.MaxTime(), outOfRange.ts)
	}

	bq, err := tsdb.NewBlockQuerier(block, math.MinInt64, math.MaxInt64)
	if err != nil {
		t.Fatalf("tsdb.NewBlockQuerier: %v", err)
	}
	defer bq.Close()
	ss := bq.Select(context.Background(), false, nil, labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "up"))
	if !ss.Next() {
		t.Fatal("block has no \"up\" series")
	}
	it := ss.At().Iterator(nil)
	var got []sample
	for it.Next() == chunkenc.ValFloat {
		ts, v := it.At()
		got = append(got, sample{ts, v})
	}
	assertSamplesEqual(t, got, inRange)
}

// TestCompactHeadToleratesUnsortedInput is the decisive check behind CompactHead's
// choice to call Select with sortSeries=false (see its doc comment): series are
// created in reverse label-sorted order (so ref/creation order is the opposite of
// labels.Compare order, the most adversarial case), yet the resulting block still
// contains every series correctly - because LeveledCompactor re-derives sorted order
// from the scratch head's own index (AllSortedPostings) rather than trusting
// CreateBlock's input order.
func TestCompactHeadToleratesUnsortedInput(t *testing.T) {
	h := NewHead(1, 1, 1)
	tgt := TargetLabels{Cluster: "c", Namespace: "n", Pod: "p", Container: "co", Node: "no", Job: "j"}
	names := []string{"zzz_metric", "yyy_metric", "mmm_metric", "aaa_metric"}
	for _, name := range names {
		ref, err := h.GetOrCreateSeries(tgt, name, "", "")
		if err != nil {
			t.Fatalf("GetOrCreateSeries(%s): %v", name, err)
		}
		if err := h.Append(ref, 1700000000000, 1); err != nil {
			t.Fatalf("Append(%s): %v", name, err)
		}
	}

	dir := t.TempDir()
	blockDir, err := CompactHead(h, math.MinInt64, math.MaxInt64, dir, 2*60*60*1000, testLogger())
	if err != nil {
		t.Fatalf("CompactHead: %v", err)
	}
	if blockDir == "" {
		t.Fatal("CompactHead returned an empty block dir")
	}

	block, err := tsdb.OpenBlock(testLogger(), blockDir, chunkenc.NewPool(), nil)
	if err != nil {
		t.Fatalf("tsdb.OpenBlock: %v", err)
	}
	defer block.Close()
	bq, err := tsdb.NewBlockQuerier(block, math.MinInt64, math.MaxInt64)
	if err != nil {
		t.Fatalf("tsdb.NewBlockQuerier: %v", err)
	}
	defer bq.Close()

	ss := bq.Select(context.Background(), false, nil, labels.MustNewMatcher(labels.MatchRegexp, labels.MetricName, ".+"))
	got := map[string]bool{}
	for ss.Next() {
		got[ss.At().Labels().Get(labels.MetricName)] = true
	}
	if err := ss.Err(); err != nil {
		t.Fatalf("SeriesSet.Err(): %v", err)
	}
	if len(got) != len(names) {
		t.Fatalf("got %d distinct series back, want %d (got: %v)", len(got), len(names), got)
	}
	for _, n := range names {
		if !got[n] {
			t.Fatalf("missing series %q in output block", n)
		}
	}
}

// TestCompactHeadThenTruncateClosesTheLoop is the end-to-end story Phase 5a set out
// to validate: compact a head's current range to a durable block, then truncate the
// live head for that same range, and confirm the live head's in-memory data is
// genuinely gone while the block (checked independently via the real tsdb.OpenBlock
// path) still has it.
// TestCompactHeadEmptyRangeReturnsEmptyString is the decisive test for a
// previously latent gap: every OTHER test in this file compacts a non-empty
// range, so none of them ever exercised whether CompactHead's own documented
// "" return actually happens on an empty one. tsdb.CreateBlock's real
// BlockWriter.Flush returns a ZERO ulid.ULID (not an error, and NOT an empty
// path - filepath.Join(dir, ulid.ULID{}.String()) is a real, syntactically valid
// path to a block that was never written) when nothing was written -
// CompactHead must translate that into "" itself, not hand back a dangling path.
func TestCompactHeadEmptyRangeReturnsEmptyString(t *testing.T) {
	h := NewHead(1, 1, 1)
	tgt := TargetLabels{Cluster: "c", Namespace: "n", Pod: "p", Container: "co", Node: "no", Job: "j"}
	ref, err := h.GetOrCreateSeries(tgt, "up", "", "")
	if err != nil {
		t.Fatalf("GetOrCreateSeries: %v", err)
	}
	if err := h.Append(ref, 1700000000000, 1); err != nil {
		t.Fatalf("Append: %v", err)
	}

	dir := t.TempDir()
	// A range that excludes the only sample entirely - CompactHead must find
	// nothing to write.
	blockDir, err := CompactHead(h, 1800000000000, 1900000000000, dir, 2*60*60*1000, testLogger())
	if err != nil {
		t.Fatalf("CompactHead: %v", err)
	}
	if blockDir != "" {
		t.Fatalf("CompactHead on an empty range returned %q, want \"\"", blockDir)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("CompactHead left %d entries on disk for an empty range, want 0: %v", len(entries), entries)
	}
}

func TestCompactHeadThenTruncateClosesTheLoop(t *testing.T) {
	h := NewHead(1, 1, 1)
	tgt := TargetLabels{Cluster: "c", Namespace: "n", Pod: "p", Container: "co", Node: "no", Job: "j"}
	ref, err := h.GetOrCreateSeries(tgt, "up", "", "")
	if err != nil {
		t.Fatalf("GetOrCreateSeries: %v", err)
	}
	want := []sample{{1700000000000, 1}, {1700000015000, 1}, {1700000030000, 0}}
	for _, sm := range want {
		if err := h.Append(ref, sm.ts, sm.v); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	dir := t.TempDir()
	blockMaxt := int64(1700000030000)
	blockDir, err := CompactHead(h, math.MinInt64, blockMaxt, dir, 2*60*60*1000, testLogger())
	if err != nil {
		t.Fatalf("CompactHead: %v", err)
	}
	if blockDir == "" {
		t.Fatal("CompactHead returned an empty block dir")
	}

	h.Truncate(blockMaxt + 1)

	shard, localIdx := h.shardFor(ref)
	if got := decodeAll(t, shard.series, localIdx); len(got) != 0 {
		t.Fatalf("live head still has %d samples after Truncate, want 0", len(got))
	}

	block, err := tsdb.OpenBlock(testLogger(), blockDir, chunkenc.NewPool(), nil)
	if err != nil {
		t.Fatalf("tsdb.OpenBlock: %v", err)
	}
	defer block.Close()
	bq, err := tsdb.NewBlockQuerier(block, math.MinInt64, math.MaxInt64)
	if err != nil {
		t.Fatalf("tsdb.NewBlockQuerier: %v", err)
	}
	defer bq.Close()

	ss := bq.Select(context.Background(), false, nil, labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "up"))
	if !ss.Next() {
		t.Fatal("block has no \"up\" series after the live head was truncated - data was never durably written")
	}
	it := ss.At().Iterator(nil)
	var got []sample
	for it.Next() == chunkenc.ValFloat {
		ts, v := it.At()
		got = append(got, sample{ts, v})
	}
	assertSamplesEqual(t, got, want)
}
