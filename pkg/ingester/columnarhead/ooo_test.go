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
	ref, err := h.GetOrCreateSeries(tgt, "up", "", "")
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
	refA, err := h.GetOrCreateSeries(tgt, "up", "", "")
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
	refB, err := h.GetOrCreateSeries(tgt, "request_latency", "", "")
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
