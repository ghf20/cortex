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

func TestChunkQuerierHistogramSeriesReturnsNoChunks(t *testing.T) {
	h := NewHead(1, 1, 1)
	app := h.Appender(context.Background())
	l := labels.FromStrings(
		labels.MetricName, "request_latency",
		"cluster", "c", "namespace", "n", "pod", "p", "container", "co", "node", "no", "job", "j",
	)
	hist := &histogram.Histogram{Schema: 0, Count: 1, Sum: 1, PositiveSpans: []histogram.Span{{Offset: 0, Length: 1}}, PositiveBuckets: []int64{1}}
	if _, err := app.AppendHistogram(0, l, 1700000000000, hist, nil); err != nil {
		t.Fatalf("AppendHistogram: %v", err)
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
	if cit.Next() {
		t.Fatal("histogram series should return no chunks (stated gap - see Head.ChunkQuerier doc comment), not fabricate one")
	}
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
