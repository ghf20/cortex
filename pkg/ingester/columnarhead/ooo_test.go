package columnarhead

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

func buildOOOHead(t *testing.T) (h *Head, ref uint32) {
	t.Helper()
	h = NewHead(1, 1, 1)
	tgt := TargetLabels{Cluster: "c", Namespace: "n", Pod: "p", Container: "co", Node: "no", Job: "j"}
	ref, err := h.GetOrCreateSeries(tgt, "up")
	if err != nil {
		t.Fatalf("GetOrCreateSeries: %v", err)
	}
	return h, ref
}

// TestHeadRejectsOutOfOrderSampleByDefault confirms OOO is disabled (oooTimeWindow
// == 0, real Prometheus's own default) unless SetOOOTimeWindow is called - a
// sample older than the series' last in-order timestamp is rejected with the real
// storage.ErrOutOfOrderSample sentinel, not silently corrupted into the stream.
func TestHeadRejectsOutOfOrderSampleByDefault(t *testing.T) {
	h, ref := buildOOOHead(t)
	base := int64(1700000000000)
	if err := h.Append(ref, base, 1); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := h.Append(ref, base+30000, 2); err != nil {
		t.Fatalf("Append: %v", err)
	}
	err := h.Append(ref, base+15000, 99) // between the two - out of order
	if !errors.Is(err, storage.ErrOutOfOrderSample) {
		t.Fatalf("Append(OOO sample, window disabled) = %v, want storage.ErrOutOfOrderSample", err)
	}

	// Rejected sample must not have corrupted the stream - only the two
	// original, correctly-ordered samples should decode back out.
	it := h.Iterator(ref)
	var got []sample
	for it.Next() {
		ts, v := it.At()
		got = append(got, sample{ts, v})
	}
	assertSamplesEqual(t, got, []sample{{base, 1}, {base + 30000, 2}})
}

// TestHeadDuplicateTimestampSameValueIsSilentNoOp matches real Prometheus's own
// stated behavior (appendable's comment in head_append.go): an exact (ts, value)
// duplicate of the last in-order sample is a real, valid case (federation,
// retries) and must be accepted as a no-op, not an error.
func TestHeadDuplicateTimestampSameValueIsSilentNoOp(t *testing.T) {
	h, ref := buildOOOHead(t)
	base := int64(1700000000000)
	if err := h.Append(ref, base, 1); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := h.Append(ref, base, 1); err != nil { // exact duplicate
		t.Fatalf("Append(identical duplicate) = %v, want nil", err)
	}
	it := h.Iterator(ref)
	var got []sample
	for it.Next() {
		ts, v := it.At()
		got = append(got, sample{ts, v})
	}
	assertSamplesEqual(t, got, []sample{{base, 1}}) // still exactly one sample, not two
}

// TestHeadDuplicateTimestampDifferentValueIsRejected: a different value at the
// SAME timestamp as the last in-order sample must be rejected with the real
// storage.ErrDuplicateSampleForTimestamp sentinel (errors.Is-comparable, matching
// how real callers - the PromQL engine, remote-write handlers - detect it),
// rather than silently overwriting or corrupting the stream.
func TestHeadDuplicateTimestampDifferentValueIsRejected(t *testing.T) {
	h, ref := buildOOOHead(t)
	base := int64(1700000000000)
	if err := h.Append(ref, base, 1); err != nil {
		t.Fatalf("Append: %v", err)
	}
	err := h.Append(ref, base, 2) // same ts, different value
	if !errors.Is(err, storage.ErrDuplicateSampleForTimestamp) {
		t.Fatalf("Append(duplicate ts, different value) = %v, want storage.ErrDuplicateSampleForTimestamp", err)
	}
	it := h.Iterator(ref)
	var got []sample
	for it.Next() {
		ts, v := it.At()
		got = append(got, sample{ts, v})
	}
	assertSamplesEqual(t, got, []sample{{base, 1}}) // original value preserved, not overwritten
}

// TestHeadAcceptsOOOWithinWindow is the decisive test for the OOO accept path:
// with a positive time window, a sample landing behind the series' last in-order
// timestamp (but within the window of the HEAD's current max time) is accepted
// into the OOO buffer, and reads back in correct sorted order merged with the
// in-order stream - verified through the real chunkenc.Iterator interface
// (headSeries.Iterator), the same decisive-check discipline used throughout this
// package, not by inspecting internal buffer state directly.
func TestHeadAcceptsOOOWithinWindow(t *testing.T) {
	h, ref := buildOOOHead(t)
	h.SetOOOTimeWindow(60_000) // 60s window
	base := int64(1700000000000)

	for _, s := range []sample{{base, 1}, {base + 30000, 3}, {base + 60000, 5}} {
		if err := h.Append(ref, s.ts, s.v); err != nil {
			t.Fatalf("Append(%v): %v", s, err)
		}
	}
	// OOO: lands between the first and second in-order samples, well within the
	// 60s window of the head's current max (base+60000).
	if err := h.Append(ref, base+15000, 2); err != nil {
		t.Fatalf("Append(OOO): %v", err)
	}
	// A second OOO sample, landing between the second and third.
	if err := h.Append(ref, base+45000, 4); err != nil {
		t.Fatalf("Append(OOO): %v", err)
	}

	q, err := h.Querier(math.MinInt64, math.MaxInt64)
	if err != nil {
		t.Fatalf("Querier: %v", err)
	}
	defer q.Close()
	m := labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "up")
	ss := q.Select(context.Background(), false, nil, m)
	if !ss.Next() {
		t.Fatal("Select found no series")
	}
	it := ss.At().Iterator(nil)
	var got []sample
	for it.Next() == chunkenc.ValFloat {
		ts, v := it.At()
		got = append(got, sample{ts, v})
	}
	if err := it.Err(); err != nil {
		t.Fatalf("real chunkenc iterator error: %v", err)
	}
	assertSamplesEqual(t, got, []sample{
		{base, 1}, {base + 15000, 2}, {base + 30000, 3}, {base + 45000, 4}, {base + 60000, 5},
	})
}

// TestHeadRejectsOOOSampleOlderThanWindow confirms the window boundary is
// enforced, not just "OOO always accepted once enabled": a sample older than
// maxTime-oooTimeWindow is rejected with storage.ErrTooOldSample.
func TestHeadRejectsOOOSampleOlderThanWindow(t *testing.T) {
	h, ref := buildOOOHead(t)
	h.SetOOOTimeWindow(10_000) // 10s window - deliberately narrow
	base := int64(1700000000000)
	if err := h.Append(ref, base, 1); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := h.Append(ref, base+60000, 2); err != nil { // advances maxTime to base+60000
		t.Fatalf("Append: %v", err)
	}
	// base+30000 is 30s behind the new maxTime - outside the 10s window.
	err := h.Append(ref, base+30000, 99)
	if !errors.Is(err, storage.ErrTooOldSample) {
		t.Fatalf("Append(sample outside OOO window) = %v, want storage.ErrTooOldSample", err)
	}
}

// TestHeadNewSeriesFirstSampleRespectsChunkRangeCushion ports real Prometheus's
// TestOOOAppendWithNoSeries (tsdb/head_test.go): a brand-new series' first
// sample - no prior history at all - still needs SOME admission bound, or an
// arbitrarily old first sample would silently land as if it were the most
// recent thing ever seen. Real Prometheus gates this on
// minValidTime = headMaxt - chunkRange/2 before falling through to the same
// too-old/OOO logic an existing series' backward timestamp hits - mirrored here
// via Head.chunkRange/SetChunkRange/appendableNewSeries.
//
// Found while investigating this gap: a first fix attempt comparing directly
// against h.maxTime.Load() with NO cushion regressed ~10 other tests (loading
// independent new series with ordinary small differences in their first
// timestamps is common and legitimate) - see CHECKLIST.md and
// appendableNewSeries' own doc comment for the full story. chunkRange defaults
// to 0 (no cushion, old behavior) unless SetChunkRange is called, which is why
// this test calls it explicitly while the rest of the suite is unaffected.
func TestHeadNewSeriesFirstSampleRespectsChunkRangeCushion(t *testing.T) {
	tgt := TargetLabels{Cluster: "c", Namespace: "n", Pod: "p", Container: "co", Node: "no", Job: "j"}
	h := NewHead(6, 1, 1)
	h.SetOOOTimeWindow(120 * 60 * 1000) // 120 minutes, matching the real test
	h.SetChunkRange(120 * 60 * 1000)    // 2h, matching real DefaultBlockDuration -> cushion = 60m

	minute := int64(60 * 1000)
	newSeries := func(name string) uint32 {
		ref, err := h.GetOrCreateSeries(tgt, name)
		if err != nil {
			t.Fatalf("GetOrCreateSeries(%s): %v", name, err)
		}
		return ref
	}

	s1 := newSeries("s1")
	if err := h.Append(s1, 300*minute, 1); err != nil {
		t.Fatalf("s1@300m (first-ever head sample, always in-order): %v", err)
	}

	// 61m behind the new maxTime (300m), outside the 60m cushion but within the
	// 120m OOO window - must land OOO, not in-order and not rejected.
	s2 := newSeries("s2")
	if err := h.Append(s2, 239*minute, 2); err != nil {
		t.Fatalf("s2@239m (should be OOO): %v", err)
	}
	if len(h.OOOSamples(s2)) != 1 {
		t.Fatalf("s2@239m: OOOSamples = %d, want 1", len(h.OOOSamples(s2)))
	}

	// Exactly at the OOO window boundary (120m behind maxTime) - still OOO.
	s3 := newSeries("s3")
	if err := h.Append(s3, 180*minute, 3); err != nil {
		t.Fatalf("s3@180m (window boundary, should be OOO): %v", err)
	}
	if len(h.OOOSamples(s3)) != 1 {
		t.Fatalf("s3@180m: OOOSamples = %d, want 1", len(h.OOOSamples(s3)))
	}

	// 1 minute past the OOO window - rejected outright.
	s4 := newSeries("s4")
	if err := h.Append(s4, 179*minute, 4); !errors.Is(err, storage.ErrTooOldSample) {
		t.Fatalf("s4@179m (outside window) = %v, want storage.ErrTooOldSample", err)
	}

	// 60m behind maxTime - within the chunkRange/2 cushion, so still in-order,
	// NOT OOO. This is the case a cushion-free maxTime comparison gets wrong.
	s5 := newSeries("s5")
	if err := h.Append(s5, 240*minute, 5); err != nil {
		t.Fatalf("s5@240m (within cushion, should be in-order): %v", err)
	}
	if len(h.OOOSamples(s5)) != 0 {
		t.Fatalf("s5@240m: OOOSamples = %d, want 0 (should be in-order)", len(h.OOOSamples(s5)))
	}
}

// TestHeadNewHistogramSeriesFirstSampleRespectsChunkRangeCushion is
// TestHeadNewSeriesFirstSampleRespectsChunkRangeCushion's histogram
// counterpart. Histogram OOO isn't built here (ooo.go's own "floats only
// first" scope), so there's no OOO landing spot for an accepted-but-behind
// sample - a new histogram series' first sample outside the cushion is
// rejected outright instead.
func TestHeadNewHistogramSeriesFirstSampleRespectsChunkRangeCushion(t *testing.T) {
	tgt := TargetLabels{Cluster: "c", Namespace: "n", Pod: "p", Container: "co", Node: "no", Job: "j"}
	h := NewHead(2, 1, 1)
	h.SetChunkRange(120 * 60 * 1000) // 2h -> cushion = 60m

	minute := int64(60 * 1000)
	newSeries := func(name string) uint32 {
		ref, err := h.GetOrCreateSeries(tgt, name)
		if err != nil {
			t.Fatalf("GetOrCreateSeries(%s): %v", name, err)
		}
		return ref
	}

	s1 := newSeries("s1")
	if err := h.AppendHistogram(s1, 300*minute, &histogram.Histogram{Schema: 0, Sum: 1, Count: 1}); err != nil {
		t.Fatalf("s1@300m (first-ever head sample): %v", err)
	}

	// 61m behind maxTime, outside the 60m cushion, no OOO to fall back to.
	s2 := newSeries("s2")
	if err := h.AppendHistogram(s2, 239*minute, &histogram.Histogram{Schema: 0, Sum: 2, Count: 1}); !errors.Is(err, storage.ErrOutOfOrderSample) {
		t.Fatalf("s2@239m (outside cushion) = %v, want storage.ErrOutOfOrderSample", err)
	}

	// 60m behind maxTime, within the cushion - in-order.
	s3 := newSeries("s3")
	if err := h.AppendHistogram(s3, 240*minute, &histogram.Histogram{Schema: 0, Sum: 3, Count: 1}); err != nil {
		t.Fatalf("s3@240m (within cushion): %v", err)
	}
}

// TestOOOSeriesBufferRejectsDuplicateWithinBuffer is a focused unit check on
// oooSeriesBuffer.insert directly, mirroring real Prometheus's own OOOChunk.Insert
// behavior: an exact-timestamp duplicate within the OOO buffer itself (not just
// against the in-order stream) is rejected.
func TestOOOSeriesBufferRejectsDuplicateWithinBuffer(t *testing.T) {
	var b oooSeriesBuffer
	if !b.insert(100, 1) {
		t.Fatal("first insert should succeed")
	}
	if !b.insert(200, 2) {
		t.Fatal("second insert (later ts) should succeed")
	}
	if !b.insert(150, 1.5) {
		t.Fatal("third insert (ts between existing entries) should succeed")
	}
	if b.insert(150, 999) {
		t.Fatal("duplicate-timestamp insert should be rejected (return false)")
	}
	want := []oooSample{{100, 1}, {150, 1.5}, {200, 2}}
	if len(b.samples) != len(want) {
		t.Fatalf("buffer has %d samples, want %d: %v", len(b.samples), len(want), b.samples)
	}
	for i, s := range want {
		if b.samples[i] != s {
			t.Fatalf("sample %d = %v, want %v", i, b.samples[i], s)
		}
	}
}

// TestHeadMinMaxTime confirms MinTime/MaxTime are tracked incrementally across
// float, histogram, and ST-zero-sample appends - not just the float OOO path
// this file otherwise focuses on.
func TestHeadMinMaxTime(t *testing.T) {
	h := NewHead(2, 1, 8)
	if got := h.MinTime(); got != math.MaxInt64 {
		t.Fatalf("MinTime() on empty head = %d, want math.MaxInt64", got)
	}
	if got := h.MaxTime(); got != math.MinInt64 {
		t.Fatalf("MaxTime() on empty head = %d, want math.MinInt64", got)
	}

	tgt := TargetLabels{Cluster: "c", Namespace: "n", Pod: "p", Container: "co", Node: "no", Job: "j"}
	refA, err := h.GetOrCreateSeries(tgt, "up")
	if err != nil {
		t.Fatalf("GetOrCreateSeries: %v", err)
	}
	// SetSTZeroSample must precede its paired real sample (its own documented
	// contract) - st=1700000010000 becomes the new MinTime, t=1700000030000 the
	// running MaxTime.
	if err := h.SetSTZeroSample(refA, 1700000030000, 1700000010000); err != nil {
		t.Fatalf("SetSTZeroSample: %v", err)
	}
	if err := h.Append(refA, 1700000030000, 1); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if h.MinTime() != 1700000010000 {
		t.Fatalf("MinTime() = %d, want 1700000010000 (from SetSTZeroSample)", h.MinTime())
	}
	if h.MaxTime() != 1700000030000 {
		t.Fatalf("MaxTime() = %d, want 1700000030000", h.MaxTime())
	}

	// A histogram sample on a DIFFERENT series, later in time, must extend
	// MaxTime too - not just the float path.
	refB, err := h.GetOrCreateSeries(tgt, "request_latency")
	if err != nil {
		t.Fatalf("GetOrCreateSeries: %v", err)
	}
	hg := &histogram.Histogram{Schema: 0, Count: 1, Sum: 1, PositiveSpans: []histogram.Span{{Offset: 0, Length: 1}}, PositiveBuckets: []int64{1}}
	if err := h.AppendHistogram(refB, 1700000060000, hg); err != nil {
		t.Fatalf("AppendHistogram: %v", err)
	}
	if h.MaxTime() != 1700000060000 {
		t.Fatalf("MaxTime() after histogram append = %d, want 1700000060000", h.MaxTime())
	}
	if h.MinTime() != 1700000010000 {
		t.Fatalf("MinTime() after histogram append = %d, want unchanged 1700000010000", h.MinTime())
	}
}
