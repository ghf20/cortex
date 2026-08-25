package columnarhead

import (
	"testing"

	"github.com/prometheus/prometheus/model/histogram"
)

func histEqual(t *testing.T, got, want *histogram.Histogram) {
	t.Helper()
	if got.Schema != want.Schema || got.ZeroThreshold != want.ZeroThreshold ||
		got.ZeroCount != want.ZeroCount || got.Count != want.Count || got.Sum != want.Sum {
		t.Fatalf("scalar fields: got %+v, want %+v", got, want)
	}
	if !spansEqual(got.PositiveSpans, want.PositiveSpans) {
		t.Fatalf("PositiveSpans: got %v, want %v", got.PositiveSpans, want.PositiveSpans)
	}
	if !spansEqual(got.NegativeSpans, want.NegativeSpans) {
		t.Fatalf("NegativeSpans: got %v, want %v", got.NegativeSpans, want.NegativeSpans)
	}
	if !int64SliceEqual(got.PositiveBuckets, want.PositiveBuckets) {
		t.Fatalf("PositiveBuckets: got %v, want %v", got.PositiveBuckets, want.PositiveBuckets)
	}
	if !int64SliceEqual(got.NegativeBuckets, want.NegativeBuckets) {
		t.Fatalf("NegativeBuckets: got %v, want %v", got.NegativeBuckets, want.NegativeBuckets)
	}
}

func int64SliceEqual(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestHistogramStoreRoundTrip_SingleSample(t *testing.T) {
	hst := NewHistogramStore()
	h := &histogram.Histogram{
		Schema:          0,
		ZeroThreshold:   0.001,
		ZeroCount:       5,
		Count:           20,
		Sum:             123.4,
		PositiveSpans:   []histogram.Span{{Offset: 0, Length: 3}},
		PositiveBuckets: []int64{2, 1, -1}, // absolute: 2, 3, 2
	}
	if err := hst.Append(0, 1700000000000, h); err != nil {
		t.Fatalf("Append: %v", err)
	}

	it := hst.Iterator(0)
	if !it.Next() {
		t.Fatal("Next() = false, want true")
	}
	gotTS, gotH := it.At()
	if gotTS != 1700000000000 {
		t.Fatalf("ts = %d, want 1700000000000", gotTS)
	}
	histEqual(t, gotH, h)
	if it.Next() {
		t.Fatal("Next() = true after the only sample, want false")
	}
}

func TestHistogramStoreRoundTrip_MultipleSamples(t *testing.T) {
	hst := NewHistogramStore()
	base := func(count, zeroCount uint64, sum float64, buckets []int64) *histogram.Histogram {
		return &histogram.Histogram{
			Schema:          1,
			ZeroThreshold:   0.0001,
			ZeroCount:       zeroCount,
			Count:           count,
			Sum:             sum,
			PositiveSpans:   []histogram.Span{{Offset: -2, Length: 4}},
			NegativeSpans:   []histogram.Span{{Offset: 0, Length: 2}},
			PositiveBuckets: buckets,
			NegativeBuckets: []int64{1, 0},
		}
	}
	samples := []*histogram.Histogram{
		base(10, 2, 5.5, []int64{1, 0, 0, 1}),   // absolute: 1,1,1,2
		base(15, 3, 8.25, []int64{1, 1, -1, 2}), // absolute: 1,2,1,3
		base(15, 3, 8.25, []int64{0, 0, 0, 0}),  // unchanged - exercises the zero-delta path
		base(40, 9, -12.75, []int64{5, -3, 2, -1}),
	}
	ts := int64(1700000000000)
	for _, h := range samples {
		if err := hst.Append(0, ts, h); err != nil {
			t.Fatalf("Append at ts=%d: %v", ts, err)
		}
		ts += 15000
	}

	it := hst.Iterator(0)
	i := 0
	wantTS := int64(1700000000000)
	for it.Next() {
		gotTS, gotH := it.At()
		if gotTS != wantTS {
			t.Fatalf("sample %d: ts = %d, want %d", i, gotTS, wantTS)
		}
		histEqual(t, gotH, samples[i])
		wantTS += 15000
		i++
	}
	if i != len(samples) {
		t.Fatalf("decoded %d samples, want %d", i, len(samples))
	}
}

func TestHistogramStoreRejectsCustomBuckets(t *testing.T) {
	hst := NewHistogramStore()
	h := &histogram.Histogram{Schema: histogram.CustomBucketsSchema, CustomValues: []float64{1, 2, 3}}
	if err := hst.Append(0, 1700000000000, h); err != ErrCustomBucketsUnsupported {
		t.Fatalf("Append with custom buckets = %v, want ErrCustomBucketsUnsupported", err)
	}
}

func TestHistogramStoreRejectsLayoutChange(t *testing.T) {
	hst := NewHistogramStore()
	h1 := &histogram.Histogram{
		Schema:          0,
		PositiveSpans:   []histogram.Span{{Offset: 0, Length: 2}},
		PositiveBuckets: []int64{1, 1},
	}
	if err := hst.Append(0, 1700000000000, h1); err != nil {
		t.Fatalf("Append: %v", err)
	}

	cases := map[string]*histogram.Histogram{
		"schema changed": {
			Schema: 1, PositiveSpans: h1.PositiveSpans, PositiveBuckets: []int64{1, 1},
		},
		"zero threshold changed": {
			Schema: 0, ZeroThreshold: 0.5, PositiveSpans: h1.PositiveSpans, PositiveBuckets: []int64{1, 1},
		},
		"span layout changed": {
			Schema:          0,
			PositiveSpans:   []histogram.Span{{Offset: 0, Length: 3}},
			PositiveBuckets: []int64{1, 1, 1},
		},
	}
	for name, h := range cases {
		t.Run(name, func(t *testing.T) {
			if err := hst.Append(0, 1700000015000, h); err != ErrHistogramLayoutChanged {
				t.Fatalf("Append with %s = %v, want ErrHistogramLayoutChanged", name, err)
			}
		})
	}
}

func TestHistogramStoreIsolatesSeries(t *testing.T) {
	hst := NewHistogramStore()
	hA := &histogram.Histogram{Schema: 0, PositiveSpans: []histogram.Span{{Offset: 0, Length: 1}}, PositiveBuckets: []int64{5}}
	hB := &histogram.Histogram{Schema: 2, PositiveSpans: []histogram.Span{{Offset: 1, Length: 2}}, PositiveBuckets: []int64{7, 1}}

	if err := hst.Append(10, 1700000000000, hA); err != nil {
		t.Fatalf("Append(10): %v", err)
	}
	if err := hst.Append(20, 1700000000000, hB); err != nil {
		t.Fatalf("Append(20): %v", err)
	}
	if hst.NumSeries() != 2 {
		t.Fatalf("NumSeries() = %d, want 2", hst.NumSeries())
	}

	itA := hst.Iterator(10)
	if !itA.Next() {
		t.Fatal("series 10: Next() = false")
	}
	_, gotA := itA.At()
	histEqual(t, gotA, hA)

	itB := hst.Iterator(20)
	if !itB.Next() {
		t.Fatal("series 20: Next() = false")
	}
	_, gotB := itB.At()
	histEqual(t, gotB, hB)
}

func TestHistogramStoreUnknownRefIteratorIsEmpty(t *testing.T) {
	hst := NewHistogramStore()
	it := hst.Iterator(999)
	if it.Next() {
		t.Fatal("Next() on an unknown ref = true, want false")
	}
}

// TestHistogramStoreTruncate covers the same decode/re-encode path as
// SeriesStore.Truncate, but for the map-backed histogram store: samples older than
// mint are dropped, retained ones round-trip exactly, and a neighboring series is
// untouched.
func TestHistogramStoreTruncate(t *testing.T) {
	hst := NewHistogramStore()
	mk := func(count uint64, buckets []int64) *histogram.Histogram {
		return &histogram.Histogram{
			Schema:          0,
			ZeroThreshold:   0.001,
			Count:           count,
			Sum:             float64(count),
			PositiveSpans:   []histogram.Span{{Offset: 0, Length: 3}},
			PositiveBuckets: buckets,
		}
	}
	samples := []*histogram.Histogram{
		mk(10, []int64{1, 0, 0}),
		mk(20, []int64{1, 1, 0}),
		mk(30, []int64{2, -1, 1}),
		mk(40, []int64{0, 1, 0}),
	}
	ts := int64(1700000000000)
	var timestamps []int64
	for _, h := range samples {
		if err := hst.Append(0, ts, h); err != nil {
			t.Fatalf("Append: %v", err)
		}
		timestamps = append(timestamps, ts)
		ts += 15000
	}
	otherH := mk(1, []int64{1, 0, 0})
	if err := hst.Append(1, 1700000000000, otherH); err != nil {
		t.Fatalf("Append(other): %v", err)
	}

	n := hst.Truncate(0, timestamps[2])
	if n != 2 {
		t.Fatalf("Truncate returned %d, want 2", n)
	}

	it := hst.Iterator(0)
	i := 2
	for it.Next() {
		gotTS, gotH := it.At()
		if gotTS != timestamps[i] {
			t.Fatalf("sample %d: ts = %d, want %d", i-2, gotTS, timestamps[i])
		}
		histEqual(t, gotH, samples[i])
		i++
	}
	if i != len(samples) {
		t.Fatalf("decoded up to index %d, want %d", i, len(samples))
	}

	// The untouched neighbor must still decode correctly.
	oit := hst.Iterator(1)
	if !oit.Next() {
		t.Fatal("other series: Next() = false, want true")
	}
	_, gotOther := oit.At()
	histEqual(t, gotOther, otherH)
}

// TestHistogramStoreTruncateToEmptyThenReappend covers mint past every existing
// sample: Has(ref) drops to false (see Truncate's doc comment on why that's fine),
// and a later real Append recreates the series exactly like any first-ever sample.
func TestHistogramStoreTruncateToEmptyThenReappend(t *testing.T) {
	hst := NewHistogramStore()
	h := &histogram.Histogram{
		Schema: 0, Count: 1, Sum: 1,
		PositiveSpans: []histogram.Span{{Offset: 0, Length: 1}}, PositiveBuckets: []int64{1},
	}
	if err := hst.Append(0, 1700000000000, h); err != nil {
		t.Fatalf("Append: %v", err)
	}

	if n := hst.Truncate(0, 1800000000000); n != 0 {
		t.Fatalf("Truncate returned %d, want 0", n)
	}
	if hst.Has(0) {
		t.Fatal("Has(0) = true after truncate-to-empty, want false")
	}

	h2 := &histogram.Histogram{
		Schema: 0, Count: 2, Sum: 2,
		PositiveSpans: []histogram.Span{{Offset: 0, Length: 1}}, PositiveBuckets: []int64{2},
	}
	if err := hst.Append(0, 1800000000000, h2); err != nil {
		t.Fatalf("Append after truncate-to-empty: %v", err)
	}
	it := hst.Iterator(0)
	if !it.Next() {
		t.Fatal("Next() = false after re-append, want true")
	}
	_, got := it.At()
	histEqual(t, got, h2)
}
