package columnarhead

import (
	"context"
	"math"
	"testing"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

func TestChunkQuerierRoundTrip(t *testing.T) {
	h := NewHead(1, 1, 1)
	app := h.Appender(context.Background())
	l := labels.FromStrings(
		labels.MetricName, "up",
		"cluster", "c", "namespace", "n", "pod", "p", "container", "co", "node", "no", "job", "j",
	)
	want := []sample{{1700000000000, 1}, {1700000015000, 0}, {1700000030000, 1}}
	for _, sm := range want {
		if _, err := app.Append(0, l, sm.ts, sm.v); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	cq, err := h.ChunkQuerier(math.MinInt64, math.MaxInt64)
	if err != nil {
		t.Fatalf("ChunkQuerier: %v", err)
	}
	defer cq.Close()

	css := cq.Select(context.Background(), false, nil, labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "up"))
	if !css.Next() {
		t.Fatal("Select found no series")
	}
	series := css.At()
	if got := series.Labels().Get(labels.MetricName); got != "up" {
		t.Fatalf("series labels __name__=%q, want up", got)
	}

	cit := series.Iterator(nil)
	if !cit.Next() {
		t.Fatal("chunks.Iterator.Next() = false, want a chunk")
	}
	meta := cit.At()
	if meta.MinTime != want[0].ts || meta.MaxTime != want[len(want)-1].ts {
		t.Fatalf("chunk MinTime/MaxTime = %d/%d, want %d/%d", meta.MinTime, meta.MaxTime, want[0].ts, want[len(want)-1].ts)
	}

	// The real, decisive check: decode the chunk via Prometheus's OWN chunkenc
	// iterator, not anything from this package - proving the produced chunk is a
	// genuinely valid, bit-for-bit correct chunkenc.XORChunk, not merely something
	// that looks right from the inside.
	sit := meta.Chunk.Iterator(nil)
	var got []sample
	for sit.Next() == chunkenc.ValFloat {
		ts, v := sit.At()
		got = append(got, sample{ts, v})
	}
	if err := sit.Err(); err != nil {
		t.Fatalf("real chunkenc iterator error: %v", err)
	}
	assertSamplesEqual(t, got, want)

	if cit.Next() {
		t.Fatal("expected exactly one chunk per series (stated simplification - see Head.ChunkQuerier doc comment)")
	}
	if css.Next() {
		t.Fatal("expected exactly one series")
	}
}

func TestChunkQuerierTimeRangeFiltering(t *testing.T) {
	h := NewHead(1, 1, 1)
	app := h.Appender(context.Background())
	l := labels.FromStrings(
		labels.MetricName, "up",
		"cluster", "c", "namespace", "n", "pod", "p", "container", "co", "node", "no", "job", "j",
	)
	ts := []int64{1700000000000, 1700000015000, 1700000030000, 1700000045000}
	for _, t0 := range ts {
		if _, err := app.Append(0, l, t0, 1); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	cq, err := h.ChunkQuerier(1700000015000, 1700000030000)
	if err != nil {
		t.Fatalf("ChunkQuerier: %v", err)
	}
	defer cq.Close()
	css := cq.Select(context.Background(), false, nil, labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "up"))
	if !css.Next() {
		t.Fatal("Select found no series")
	}
	cit := css.At().Iterator(nil)
	if !cit.Next() {
		t.Fatal("expected a chunk covering the bounded range")
	}
	meta := cit.At()
	if meta.MinTime != 1700000015000 || meta.MaxTime != 1700000030000 {
		t.Fatalf("chunk MinTime/MaxTime = %d/%d, want 1700000015000/1700000030000", meta.MinTime, meta.MaxTime)
	}
}

func TestChunkQuerierNoSamplesInRange(t *testing.T) {
	h := NewHead(1, 1, 1)
	app := h.Appender(context.Background())
	l := labels.FromStrings(
		labels.MetricName, "up",
		"cluster", "c", "namespace", "n", "pod", "p", "container", "co", "node", "no", "job", "j",
	)
	if _, err := app.Append(0, l, 1700000000000, 1); err != nil {
		t.Fatalf("Append: %v", err)
	}

	cq, err := h.ChunkQuerier(1800000000000, 1900000000000) // window with no samples
	if err != nil {
		t.Fatalf("ChunkQuerier: %v", err)
	}
	defer cq.Close()
	css := cq.Select(context.Background(), false, nil, labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "up"))
	if !css.Next() {
		t.Fatal("Select found no series (series should still be returned - see Head.Querier's doc comment on matcher-hit-but-empty-range behavior)")
	}
	cit := css.At().Iterator(nil)
	if cit.Next() {
		t.Fatal("expected no chunks for a series with zero samples in the requested range")
	}
}

// TestChunkQuerierHistogramSeriesRoundTrip is the decisive test for real
// histogram chunk encoding (see Head.ChunkQuerier's doc comment for why this
// used to be a stated gap): a stable-layout, no-counter-reset sequence must
// round-trip through ONE real chunkenc.HistogramChunk, decodable by real
// Prometheus code, bit-exact via histEqual - not just "doesn't return an empty
// iterator anymore."
func TestChunkQuerierHistogramSeriesRoundTrip(t *testing.T) {
	h := NewHead(1, 1, 1)
	app := h.Appender(context.Background())
	l := labels.FromStrings(
		labels.MetricName, "request_latency",
		"cluster", "c", "namespace", "n", "pod", "p", "container", "co", "node", "no", "job", "j",
	)
	base := int64(1700000000000)
	samples := []*histogram.Histogram{
		{Schema: 0, Count: 1, Sum: 1, PositiveSpans: []histogram.Span{{Offset: 0, Length: 1}}, PositiveBuckets: []int64{1}},
		{Schema: 0, Count: 3, Sum: 4, PositiveSpans: []histogram.Span{{Offset: 0, Length: 1}}, PositiveBuckets: []int64{3}},
		{Schema: 0, Count: 6, Sum: 9, PositiveSpans: []histogram.Span{{Offset: 0, Length: 1}}, PositiveBuckets: []int64{6}},
	}
	for i, hg := range samples {
		if _, err := app.AppendHistogram(0, l, base+int64(i)*15000, hg, nil); err != nil {
			t.Fatalf("AppendHistogram %d: %v", i, err)
		}
	}

	cq, err := h.ChunkQuerier(math.MinInt64, math.MaxInt64)
	if err != nil {
		t.Fatalf("ChunkQuerier: %v", err)
	}
	defer cq.Close()
	css := cq.Select(context.Background(), false, nil, labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "request_latency"))
	if !css.Next() {
		t.Fatal("Select found no series")
	}
	cit := css.At().Iterator(nil)
	if !cit.Next() {
		t.Fatalf("chunks.Iterator returned no chunks: %v", cit.Err())
	}
	meta := cit.At()
	if cit.Next() {
		t.Fatal("expected exactly one chunk for a stable-layout, no-counter-reset sequence")
	}
	if err := cit.Err(); err != nil {
		t.Fatalf("chunks.Iterator error: %v", err)
	}
	wantMinTime, wantMaxTime := base, base+int64(len(samples)-1)*15000
	if meta.MinTime != wantMinTime || meta.MaxTime != wantMaxTime {
		t.Fatalf("meta.MinTime/MaxTime = %d/%d, want %d/%d", meta.MinTime, meta.MaxTime, wantMinTime, wantMaxTime)
	}

	it := meta.Chunk.Iterator(nil)
	for i, want := range samples {
		if it.Next() != chunkenc.ValHistogram {
			t.Fatalf("sample %d: real chunk iterator exhausted early", i)
		}
		gotTS, got := it.AtHistogram(nil)
		wantTS := base + int64(i)*15000
		if gotTS != wantTS {
			t.Fatalf("sample %d: ts = %d, want %d", i, gotTS, wantTS)
		}
		histEqual(t, got, want)
	}
	if it.Next() != chunkenc.ValNone {
		t.Fatal("real chunk iterator has more samples than expected")
	}
	if it.Err() != nil {
		t.Fatalf("real chunk iterator error: %v", it.Err())
	}
}

// TestChunkQuerierHistogramSeriesCounterResetSplitsChunk confirms the
// multi-chunk case Head.ChunkQuerier's doc comment describes actually happens:
// HistogramStore itself never detects or rejects a counter reset (a histogram
// whose bucket counts shrink relative to the previous sample - see its doc
// comment), so real chunkenc.HistogramAppender must be the one to catch it when
// building the real chunk, splitting into a new chunk rather than silently
// encoding an inconsistent one.
func TestChunkQuerierHistogramSeriesCounterResetSplitsChunk(t *testing.T) {
	h := NewHead(1, 1, 1)
	app := h.Appender(context.Background())
	l := labels.FromStrings(
		labels.MetricName, "request_latency",
		"cluster", "c", "namespace", "n", "pod", "p", "container", "co", "node", "no", "job", "j",
	)
	base := int64(1700000000000)
	big := &histogram.Histogram{Schema: 0, Count: 100, Sum: 500, PositiveSpans: []histogram.Span{{Offset: 0, Length: 1}}, PositiveBuckets: []int64{100}}
	// A real counter reset: bucket count and total Count both drop sharply,
	// exactly what a process restart looks like to a native histogram.
	small := &histogram.Histogram{Schema: 0, Count: 1, Sum: 2, PositiveSpans: []histogram.Span{{Offset: 0, Length: 1}}, PositiveBuckets: []int64{1}}

	if _, err := app.AppendHistogram(0, l, base, big, nil); err != nil {
		t.Fatalf("AppendHistogram(big): %v", err)
	}
	if _, err := app.AppendHistogram(0, l, base+15000, small, nil); err != nil {
		t.Fatalf("AppendHistogram(small): %v", err)
	}

	cq, err := h.ChunkQuerier(math.MinInt64, math.MaxInt64)
	if err != nil {
		t.Fatalf("ChunkQuerier: %v", err)
	}
	defer cq.Close()
	css := cq.Select(context.Background(), false, nil, labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "request_latency"))
	if !css.Next() {
		t.Fatal("Select found no series")
	}
	cit := css.At().Iterator(nil)

	var metas []struct {
		ts int64
		h  *histogram.Histogram
	}
	for cit.Next() {
		meta := cit.At()
		it := meta.Chunk.Iterator(nil)
		if it.Next() != chunkenc.ValHistogram {
			t.Fatalf("chunk has no samples: %v", it.Err())
		}
		ts, hg := it.AtHistogram(nil)
		metas = append(metas, struct {
			ts int64
			h  *histogram.Histogram
		}{ts, hg})
		if it.Next() != chunkenc.ValNone {
			t.Fatal("expected exactly one sample per chunk in this test's setup")
		}
	}
	if err := cit.Err(); err != nil {
		t.Fatalf("chunks.Iterator error: %v", err)
	}
	if len(metas) != 2 {
		t.Fatalf("got %d chunks, want 2 (the counter reset should have split into a new chunk)", len(metas))
	}
	if metas[0].ts != base || metas[1].ts != base+15000 {
		t.Fatalf("chunk timestamps = [%d, %d], want [%d, %d]", metas[0].ts, metas[1].ts, base, base+15000)
	}
	histEqual(t, metas[0].h, big)
	histEqual(t, metas[1].h, small)
}

// TestChunkQuerierHistogramSeriesLayoutChangeSplitsChunk confirms a genuine
// schema/zero-threshold/span change (histoSegment's own doc comment, CHECKLIST.md's
// Phase 3 mid-stream-layout-change work) reaches the real gRPC chunks path
// correctly: HistogramStore now stores this as two segments instead of erroring,
// HistogramIterator walks both transparently, and chunkenc.HistogramAppender.
// AppendHistogram (called exactly the same way for every decoded sample,
// layout-changed or not) detects the incompatible layout on its own and starts a
// genuinely new chunk - no special-casing needed anywhere in this file for the
// reason the split happened.
func TestChunkQuerierHistogramSeriesLayoutChangeSplitsChunk(t *testing.T) {
	h := NewHead(1, 1, 1)
	app := h.Appender(context.Background())
	l := labels.FromStrings(
		labels.MetricName, "request_latency",
		"cluster", "c", "namespace", "n", "pod", "p", "container", "co", "node", "no", "job", "j",
	)
	base := int64(1700000000000)
	h1 := &histogram.Histogram{Schema: 0, Count: 1, Sum: 1, PositiveSpans: []histogram.Span{{Offset: 0, Length: 1}}, PositiveBuckets: []int64{1}}
	// A genuine layout change - different schema AND span shape, not a counter
	// reset (Count/Sum still increase).
	h2 := &histogram.Histogram{Schema: 1, Count: 5, Sum: 9, PositiveSpans: []histogram.Span{{Offset: 0, Length: 2}}, PositiveBuckets: []int64{3, 2}}

	if _, err := app.AppendHistogram(0, l, base, h1, nil); err != nil {
		t.Fatalf("AppendHistogram(h1): %v", err)
	}
	if _, err := app.AppendHistogram(0, l, base+15000, h2, nil); err != nil {
		t.Fatalf("AppendHistogram(h2): %v", err)
	}

	cq, err := h.ChunkQuerier(math.MinInt64, math.MaxInt64)
	if err != nil {
		t.Fatalf("ChunkQuerier: %v", err)
	}
	defer cq.Close()
	css := cq.Select(context.Background(), false, nil, labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "request_latency"))
	if !css.Next() {
		t.Fatal("Select found no series")
	}
	cit := css.At().Iterator(nil)

	var metas []struct {
		ts int64
		h  *histogram.Histogram
	}
	for cit.Next() {
		meta := cit.At()
		it := meta.Chunk.Iterator(nil)
		if it.Next() != chunkenc.ValHistogram {
			t.Fatalf("chunk has no samples: %v", it.Err())
		}
		ts, hg := it.AtHistogram(nil)
		metas = append(metas, struct {
			ts int64
			h  *histogram.Histogram
		}{ts, hg})
		if it.Next() != chunkenc.ValNone {
			t.Fatal("expected exactly one sample per chunk in this test's setup")
		}
	}
	if err := cit.Err(); err != nil {
		t.Fatalf("chunks.Iterator error: %v", err)
	}
	if len(metas) != 2 {
		t.Fatalf("got %d chunks, want 2 (the layout change should have split into a new chunk)", len(metas))
	}
	if metas[0].ts != base || metas[1].ts != base+15000 {
		t.Fatalf("chunk timestamps = [%d, %d], want [%d, %d]", metas[0].ts, metas[1].ts, base, base+15000)
	}
	histEqual(t, metas[0].h, h1)
	histEqual(t, metas[1].h, h2)
}

// TestChunkQuerierFloatHistogramSeriesRoundTrip is
// TestChunkQuerierHistogramSeriesRoundTrip's FloatHistogram counterpart - a
// stable-layout, no-counter-reset sequence must round-trip through ONE real
// chunkenc.FloatHistogramChunk, bit-exact via floatHistEqual.
func TestChunkQuerierFloatHistogramSeriesRoundTrip(t *testing.T) {
	h := NewHead(1, 1, 1)
	app := h.Appender(context.Background())
	l := labels.FromStrings(
		labels.MetricName, "request_latency",
		"cluster", "c", "namespace", "n", "pod", "p", "container", "co", "node", "no", "job", "j",
	)
	base := int64(1700000000000)
	samples := []*histogram.FloatHistogram{
		{Schema: 0, Count: 1, Sum: 1, PositiveSpans: []histogram.Span{{Offset: 0, Length: 1}}, PositiveBuckets: []float64{1}},
		{Schema: 0, Count: 3, Sum: 4, PositiveSpans: []histogram.Span{{Offset: 0, Length: 1}}, PositiveBuckets: []float64{3}},
		{Schema: 0, Count: 6, Sum: 9, PositiveSpans: []histogram.Span{{Offset: 0, Length: 1}}, PositiveBuckets: []float64{6}},
	}
	for i, hg := range samples {
		if _, err := app.AppendHistogram(0, l, base+int64(i)*15000, nil, hg); err != nil {
			t.Fatalf("AppendHistogram(FloatHistogram) %d: %v", i, err)
		}
	}

	cq, err := h.ChunkQuerier(math.MinInt64, math.MaxInt64)
	if err != nil {
		t.Fatalf("ChunkQuerier: %v", err)
	}
	defer cq.Close()
	css := cq.Select(context.Background(), false, nil, labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "request_latency"))
	if !css.Next() {
		t.Fatal("Select found no series")
	}
	cit := css.At().Iterator(nil)
	if !cit.Next() {
		t.Fatalf("chunks.Iterator returned no chunks: %v", cit.Err())
	}
	meta := cit.At()
	if cit.Next() {
		t.Fatal("expected exactly one chunk for a stable-layout, no-counter-reset sequence")
	}
	if err := cit.Err(); err != nil {
		t.Fatalf("chunks.Iterator error: %v", err)
	}
	wantMinTime, wantMaxTime := base, base+int64(len(samples)-1)*15000
	if meta.MinTime != wantMinTime || meta.MaxTime != wantMaxTime {
		t.Fatalf("meta.MinTime/MaxTime = %d/%d, want %d/%d", meta.MinTime, meta.MaxTime, wantMinTime, wantMaxTime)
	}

	it := meta.Chunk.Iterator(nil)
	for i, want := range samples {
		if it.Next() != chunkenc.ValFloatHistogram {
			t.Fatalf("sample %d: real chunk iterator exhausted early", i)
		}
		gotTS, got := it.AtFloatHistogram(nil)
		wantTS := base + int64(i)*15000
		if gotTS != wantTS {
			t.Fatalf("sample %d: ts = %d, want %d", i, gotTS, wantTS)
		}
		floatHistEqual(t, got, want)
	}
	if it.Next() != chunkenc.ValNone {
		t.Fatal("real chunk iterator has more samples than expected")
	}
	if it.Err() != nil {
		t.Fatalf("real chunk iterator error: %v", it.Err())
	}
}

// TestChunkQuerierFloatHistogramSeriesCounterResetSplitsChunk is
// TestChunkQuerierHistogramSeriesCounterResetSplitsChunk's FloatHistogram
// counterpart - confirms the real chunkenc.FloatHistogramAppender's own
// counter-reset detection splits into a new chunk here too.
func TestChunkQuerierFloatHistogramSeriesCounterResetSplitsChunk(t *testing.T) {
	h := NewHead(1, 1, 1)
	app := h.Appender(context.Background())
	l := labels.FromStrings(
		labels.MetricName, "request_latency",
		"cluster", "c", "namespace", "n", "pod", "p", "container", "co", "node", "no", "job", "j",
	)
	base := int64(1700000000000)
	big := &histogram.FloatHistogram{Schema: 0, Count: 100, Sum: 500, PositiveSpans: []histogram.Span{{Offset: 0, Length: 1}}, PositiveBuckets: []float64{100}}
	small := &histogram.FloatHistogram{Schema: 0, Count: 1, Sum: 2, PositiveSpans: []histogram.Span{{Offset: 0, Length: 1}}, PositiveBuckets: []float64{1}}

	if _, err := app.AppendHistogram(0, l, base, nil, big); err != nil {
		t.Fatalf("AppendHistogram(big): %v", err)
	}
	if _, err := app.AppendHistogram(0, l, base+15000, nil, small); err != nil {
		t.Fatalf("AppendHistogram(small): %v", err)
	}

	cq, err := h.ChunkQuerier(math.MinInt64, math.MaxInt64)
	if err != nil {
		t.Fatalf("ChunkQuerier: %v", err)
	}
	defer cq.Close()
	css := cq.Select(context.Background(), false, nil, labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "request_latency"))
	if !css.Next() {
		t.Fatal("Select found no series")
	}
	cit := css.At().Iterator(nil)

	var metas []struct {
		ts int64
		h  *histogram.FloatHistogram
	}
	for cit.Next() {
		meta := cit.At()
		it := meta.Chunk.Iterator(nil)
		if it.Next() != chunkenc.ValFloatHistogram {
			t.Fatalf("chunk has no samples: %v", it.Err())
		}
		ts, hg := it.AtFloatHistogram(nil)
		metas = append(metas, struct {
			ts int64
			h  *histogram.FloatHistogram
		}{ts, hg})
		if it.Next() != chunkenc.ValNone {
			t.Fatal("expected exactly one sample per chunk in this test's setup")
		}
	}
	if err := cit.Err(); err != nil {
		t.Fatalf("chunks.Iterator error: %v", err)
	}
	if len(metas) != 2 {
		t.Fatalf("got %d chunks, want 2 (the counter reset should have split into a new chunk)", len(metas))
	}
	if metas[0].ts != base || metas[1].ts != base+15000 {
		t.Fatalf("chunk timestamps = [%d, %d], want [%d, %d]", metas[0].ts, metas[1].ts, base, base+15000)
	}
	floatHistEqual(t, metas[0].h, big)
	floatHistEqual(t, metas[1].h, small)
}

// TestChunkQuerierFloatHistogramSeriesLayoutChangeSplitsChunk is
// TestChunkQuerierHistogramSeriesLayoutChangeSplitsChunk's FloatHistogram
// counterpart.
func TestChunkQuerierFloatHistogramSeriesLayoutChangeSplitsChunk(t *testing.T) {
	h := NewHead(1, 1, 1)
	app := h.Appender(context.Background())
	l := labels.FromStrings(
		labels.MetricName, "request_latency",
		"cluster", "c", "namespace", "n", "pod", "p", "container", "co", "node", "no", "job", "j",
	)
	base := int64(1700000000000)
	h1 := &histogram.FloatHistogram{Schema: 0, Count: 1, Sum: 1, PositiveSpans: []histogram.Span{{Offset: 0, Length: 1}}, PositiveBuckets: []float64{1}}
	h2 := &histogram.FloatHistogram{Schema: 1, Count: 5, Sum: 9, PositiveSpans: []histogram.Span{{Offset: 0, Length: 2}}, PositiveBuckets: []float64{3, 2}}

	if _, err := app.AppendHistogram(0, l, base, nil, h1); err != nil {
		t.Fatalf("AppendHistogram(h1): %v", err)
	}
	if _, err := app.AppendHistogram(0, l, base+15000, nil, h2); err != nil {
		t.Fatalf("AppendHistogram(h2): %v", err)
	}

	cq, err := h.ChunkQuerier(math.MinInt64, math.MaxInt64)
	if err != nil {
		t.Fatalf("ChunkQuerier: %v", err)
	}
	defer cq.Close()
	css := cq.Select(context.Background(), false, nil, labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "request_latency"))
	if !css.Next() {
		t.Fatal("Select found no series")
	}
	cit := css.At().Iterator(nil)

	var metas []struct {
		ts int64
		h  *histogram.FloatHistogram
	}
	for cit.Next() {
		meta := cit.At()
		it := meta.Chunk.Iterator(nil)
		if it.Next() != chunkenc.ValFloatHistogram {
			t.Fatalf("chunk has no samples: %v", it.Err())
		}
		ts, hg := it.AtFloatHistogram(nil)
		metas = append(metas, struct {
			ts int64
			h  *histogram.FloatHistogram
		}{ts, hg})
		if it.Next() != chunkenc.ValNone {
			t.Fatal("expected exactly one sample per chunk in this test's setup")
		}
	}
	if err := cit.Err(); err != nil {
		t.Fatalf("chunks.Iterator error: %v", err)
	}
	if len(metas) != 2 {
		t.Fatalf("got %d chunks, want 2 (the layout change should have split into a new chunk)", len(metas))
	}
	if metas[0].ts != base || metas[1].ts != base+15000 {
		t.Fatalf("chunk timestamps = [%d, %d], want [%d, %d]", metas[0].ts, metas[1].ts, base, base+15000)
	}
	floatHistEqual(t, metas[0].h, h1)
	floatHistEqual(t, metas[1].h, h2)
}

func TestChunkQuerierLabelValuesAndNames(t *testing.T) {
	h := buildQueryHead(t)
	cq, err := h.ChunkQuerier(math.MinInt64, math.MaxInt64)
	if err != nil {
		t.Fatalf("ChunkQuerier: %v", err)
	}
	defer cq.Close()

	names, _, err := cq.LabelValues(context.Background(), labels.MetricName, nil)
	if err != nil {
		t.Fatalf("LabelValues: %v", err)
	}
	want := []string{"request_duration_bucket", "up"}
	if !stringSliceEqual(names, want) {
		t.Fatalf("LabelValues(__name__) = %v, want %v", names, want)
	}
}
