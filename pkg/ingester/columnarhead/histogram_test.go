package columnarhead

import (
	"math"
	"testing"

	"github.com/prometheus/prometheus/model/histogram"
)

// histEqual deliberately does NOT compare CounterResetHint: real Prometheus's
// own chunk-level readback recomputes it from chunk position
// (chunkenc/histogram_meta.go's counterResetHint(header, numRead)), not by
// echoing back whatever was appended - so a chunk-path test's "got" (from a
// real chunkenc.HistogramAppender-built chunk) and its own literal input
// fixture's "want" can legitimately differ here without either being wrong.
// Tests that care about CounterResetHint specifically assert on it directly
// - see TestHistogramStoreRoundTripGaugeHistogram and
// TestChunkQuerierGaugeHistogramNeverSplitsOnBucketDecrease.
func histEqual(t *testing.T, got, want *histogram.Histogram) {
	t.Helper()
	// Sum/ZeroThreshold compared by bit pattern, not !=: a staleness marker's
	// Sum is NaN, and NaN != NaN always, regardless of whether the payload bits
	// (what actually matters - value.IsStaleNaN checks the exact pattern)
	// match. Found via TestDifferentialHistogramStalenessRealVsColumnar, this
	// package's first histogram differential test to exercise a NaN Sum -
	// matches assertSamplesBitIdentical's own precedent for the plain-float
	// path.
	if got.Schema != want.Schema || math.Float64bits(got.ZeroThreshold) != math.Float64bits(want.ZeroThreshold) ||
		got.ZeroCount != want.ZeroCount || got.Count != want.Count || math.Float64bits(got.Sum) != math.Float64bits(want.Sum) {
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
	if !float64SliceEqual(got.CustomValues, want.CustomValues) {
		t.Fatalf("CustomValues: got %v, want %v", got.CustomValues, want.CustomValues)
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

// floatHistEqual: see histEqual's own doc comment for why CounterResetHint is
// deliberately excluded from this comparison.
func floatHistEqual(t *testing.T, got, want *histogram.FloatHistogram) {
	t.Helper()
	// Bit-pattern comparison for every float64 scalar field, not !=  - see
	// histEqual's identical comment (NaN != NaN always, regardless of payload).
	if got.Schema != want.Schema || math.Float64bits(got.ZeroThreshold) != math.Float64bits(want.ZeroThreshold) ||
		math.Float64bits(got.ZeroCount) != math.Float64bits(want.ZeroCount) ||
		math.Float64bits(got.Count) != math.Float64bits(want.Count) ||
		math.Float64bits(got.Sum) != math.Float64bits(want.Sum) {
		t.Fatalf("scalar fields: got %+v, want %+v", got, want)
	}
	if !spansEqual(got.PositiveSpans, want.PositiveSpans) {
		t.Fatalf("PositiveSpans: got %v, want %v", got.PositiveSpans, want.PositiveSpans)
	}
	if !spansEqual(got.NegativeSpans, want.NegativeSpans) {
		t.Fatalf("NegativeSpans: got %v, want %v", got.NegativeSpans, want.NegativeSpans)
	}
	if !float64SliceEqual(got.PositiveBuckets, want.PositiveBuckets) {
		t.Fatalf("PositiveBuckets: got %v, want %v", got.PositiveBuckets, want.PositiveBuckets)
	}
	if !float64SliceEqual(got.NegativeBuckets, want.NegativeBuckets) {
		t.Fatalf("NegativeBuckets: got %v, want %v", got.NegativeBuckets, want.NegativeBuckets)
	}
	if !float64SliceEqual(got.CustomValues, want.CustomValues) {
		t.Fatalf("CustomValues: got %v, want %v", got.CustomValues, want.CustomValues)
	}
}

func float64SliceEqual(a, b []float64) bool {
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

// TestHistogramStoreRoundTripFloat_MultipleSamples is
// TestHistogramStoreRoundTrip_MultipleSamples's FloatHistogram counterpart -
// same shape (stable layout, several samples including a genuinely unchanged
// one), but through AppendFloat/AtFloat's per-bucket gorilla XOR encoding
// instead of Append/At's per-bucket varbit delta encoding (see histoSeries'
// own doc comment on why these are two different schemes, not one shared
// path). FloatHistogram buckets are already absolute (unlike Histogram's
// delta-encoded ones), so samples below list absolute values directly.
func TestHistogramStoreRoundTripFloat_MultipleSamples(t *testing.T) {
	hst := NewHistogramStore()
	base := func(count, zeroCount, sum float64, posBuckets []float64) *histogram.FloatHistogram {
		return &histogram.FloatHistogram{
			Schema:          1,
			ZeroThreshold:   0.0001,
			ZeroCount:       zeroCount,
			Count:           count,
			Sum:             sum,
			PositiveSpans:   []histogram.Span{{Offset: -2, Length: 4}},
			NegativeSpans:   []histogram.Span{{Offset: 0, Length: 2}},
			PositiveBuckets: posBuckets,
			NegativeBuckets: []float64{1, 1},
		}
	}
	samples := []*histogram.FloatHistogram{
		base(10, 2, 5.5, []float64{1, 1, 1, 2}),
		base(15, 3, 8.25, []float64{2, 3, 2, 4}),
		base(15, 3, 8.25, []float64{2, 3, 2, 4}), // genuinely unchanged - exercises the zero-XOR-delta path
		base(40, 9, -12.75, []float64{7, 4, 6, 5}),
	}
	ts := int64(1700000000000)
	for _, h := range samples {
		if err := hst.AppendFloat(0, ts, h); err != nil {
			t.Fatalf("AppendFloat at ts=%d: %v", ts, err)
		}
		ts += 15000
	}

	if !hst.IsFloat(0) {
		t.Fatal("IsFloat(0) = false for a series that only ever received FloatHistogram samples")
	}

	it := hst.Iterator(0)
	i := 0
	wantTS := int64(1700000000000)
	for it.Next() {
		gotTS, gotH := it.AtFloat()
		if gotTS != wantTS {
			t.Fatalf("sample %d: ts = %d, want %d", i, gotTS, wantTS)
		}
		floatHistEqual(t, gotH, samples[i])
		wantTS += 15000
		i++
	}
	if i != len(samples) {
		t.Fatalf("decoded %d samples, want %d", i, len(samples))
	}
}

// TestHistogramStoreRoundTripCustomBuckets confirms schema -53 (NHCB) round-trips
// bit-exact, including CustomValues - previously rejected outright
// (ErrCustomBucketsUnsupported, now retired). PositiveSpans/PositiveBuckets are
// span+delta-encoded through the exact same path an exponential-schema histogram
// uses (histoSegment's own doc comment); CustomValues is the one genuinely new
// piece of state.
func TestHistogramStoreRoundTripCustomBuckets(t *testing.T) {
	hst := NewHistogramStore()
	samples := []*histogram.Histogram{
		{
			Schema: histogram.CustomBucketsSchema, Count: 6, Sum: 12.5,
			PositiveSpans:   []histogram.Span{{Offset: 0, Length: 3}},
			PositiveBuckets: []int64{1, 2, 0}, // absolute: 1, 3, 3
			CustomValues:    []float64{1, 2, 5},
		},
		{
			Schema: histogram.CustomBucketsSchema, Count: 7, Sum: 20,
			PositiveSpans:   []histogram.Span{{Offset: 0, Length: 3}},
			PositiveBuckets: []int64{2, 1, -1}, // absolute (cumulative): 2, 3, 2 -> sums to Count 7
			CustomValues:    []float64{1, 2, 5},
		},
	}

	base := int64(1700000000000)
	for i, h := range samples {
		if err := hst.Append(0, base+int64(i)*15000, h); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}

	it := hst.Iterator(0)
	for i, want := range samples {
		if !it.Next() {
			t.Fatalf("sample %d: Next() = false", i)
		}
		ts, got := it.At()
		if ts != base+int64(i)*15000 {
			t.Fatalf("sample %d: ts = %d, want %d", i, ts, base+int64(i)*15000)
		}
		histEqual(t, got, want)
		if got.Schema != histogram.CustomBucketsSchema {
			t.Fatalf("sample %d: Schema = %d, want %d (CustomBucketsSchema)", i, got.Schema, histogram.CustomBucketsSchema)
		}
	}
	if it.Next() {
		t.Fatal("more samples than expected")
	}
}

// TestHistogramStoreRoundTripCustomBucketsFloat is
// TestHistogramStoreRoundTripCustomBuckets's FloatHistogram counterpart.
func TestHistogramStoreRoundTripCustomBucketsFloat(t *testing.T) {
	hst := NewHistogramStore()
	samples := []*histogram.FloatHistogram{
		{
			Schema: histogram.CustomBucketsSchema, Count: 6, Sum: 12.5,
			PositiveSpans:   []histogram.Span{{Offset: 0, Length: 3}},
			PositiveBuckets: []float64{1, 3, 3},
			CustomValues:    []float64{1, 2, 5},
		},
		{
			Schema: histogram.CustomBucketsSchema, Count: 7, Sum: 20,
			PositiveSpans:   []histogram.Span{{Offset: 0, Length: 3}},
			PositiveBuckets: []float64{2, 3, 2},
			CustomValues:    []float64{1, 2, 5},
		},
	}
	base := int64(1700000000000)
	for i, h := range samples {
		if err := hst.AppendFloat(0, base+int64(i)*15000, h); err != nil {
			t.Fatalf("AppendFloat %d: %v", i, err)
		}
	}

	it := hst.Iterator(0)
	for i, want := range samples {
		if !it.Next() {
			t.Fatalf("sample %d: Next() = false", i)
		}
		ts, got := it.AtFloat()
		if ts != base+int64(i)*15000 {
			t.Fatalf("sample %d: ts = %d, want %d", i, ts, base+int64(i)*15000)
		}
		floatHistEqual(t, got, want)
	}
	if it.Next() {
		t.Fatal("more samples than expected")
	}
}

// TestHistogramStoreCustomBucketsBoundaryChangeStartsNewSegment confirms a
// genuine CustomValues change (same span shape, different boundaries) is
// detected as a real layout change and starts a new segment - the one thing
// sameLayout's existing schema/zeroThreshold/span checks alone can't catch for
// an NHCB series, since those all trivially match across a boundary-only change.
func TestHistogramStoreCustomBucketsBoundaryChangeStartsNewSegment(t *testing.T) {
	hst := NewHistogramStore()
	h1 := &histogram.Histogram{
		Schema: histogram.CustomBucketsSchema, Count: 1, Sum: 1,
		PositiveSpans: []histogram.Span{{Offset: 0, Length: 1}}, PositiveBuckets: []int64{1},
		CustomValues: []float64{10},
	}
	h2 := &histogram.Histogram{
		Schema: histogram.CustomBucketsSchema, Count: 2, Sum: 2,
		PositiveSpans: []histogram.Span{{Offset: 0, Length: 1}}, PositiveBuckets: []int64{2},
		CustomValues: []float64{20}, // same span shape, different boundary
	}
	if err := hst.Append(0, 1700000000000, h1); err != nil {
		t.Fatalf("Append h1: %v", err)
	}
	if err := hst.Append(0, 1700000015000, h2); err != nil {
		t.Fatalf("Append h2: %v", err)
	}

	it := hst.Iterator(0)
	if !it.Next() {
		t.Fatal("sample 0: Next() = false")
	}
	_, got0 := it.At()
	histEqual(t, got0, h1)
	if !it.Next() {
		t.Fatal("sample 1: Next() = false")
	}
	_, got1 := it.At()
	histEqual(t, got1, h2)
	if it.Next() {
		t.Fatal("more samples than expected")
	}
}

// TestHistogramStoreRoundTripGaugeHistogram confirms CounterResetHint ==
// GaugeType survives Append -> HistogramIterator.At round-trip - the one
// CounterResetHint distinction this format commits to preserving (see
// decodedCounterResetHint's own doc comment for why the other three values
// deliberately do NOT round-trip literally).
func TestHistogramStoreRoundTripGaugeHistogram(t *testing.T) {
	hst := NewHistogramStore()
	h := &histogram.Histogram{
		CounterResetHint: histogram.GaugeType,
		Schema:           0, Count: 5, Sum: 10,
		PositiveSpans: []histogram.Span{{Offset: 0, Length: 1}}, PositiveBuckets: []int64{5},
	}
	if err := hst.Append(0, 1700000000000, h); err != nil {
		t.Fatalf("Append: %v", err)
	}
	it := hst.Iterator(0)
	if !it.Next() {
		t.Fatal("Next() = false")
	}
	_, got := it.At()
	if got.CounterResetHint != histogram.GaugeType {
		t.Fatalf("CounterResetHint = %v, want GaugeType", got.CounterResetHint)
	}
}

// TestHistogramStoreCounterResetHintCollapsesToUnknown confirms the three
// non-Gauge CounterResetHint values (Unknown/CounterReset/NotCounterReset) all
// come back as UnknownCounterReset, not a literal echo of what was appended -
// deliberate, not a bug: see decodedCounterResetHint's own doc comment for why
// only GaugeType is safely preservable through a direct (non-real-chunk) read.
func TestHistogramStoreCounterResetHintCollapsesToUnknown(t *testing.T) {
	cases := map[string]histogram.CounterResetHint{
		"unknown":           histogram.UnknownCounterReset,
		"counter reset":     histogram.CounterReset,
		"not counter reset": histogram.NotCounterReset,
	}
	for name, hint := range cases {
		t.Run(name, func(t *testing.T) {
			hst := NewHistogramStore()
			h := &histogram.Histogram{
				CounterResetHint: hint,
				Schema:           0, Count: 1, Sum: 1,
				PositiveSpans: []histogram.Span{{Offset: 0, Length: 1}}, PositiveBuckets: []int64{1},
			}
			if err := hst.Append(0, 1700000000000, h); err != nil {
				t.Fatalf("Append: %v", err)
			}
			it := hst.Iterator(0)
			if !it.Next() {
				t.Fatal("Next() = false")
			}
			_, got := it.At()
			if got.CounterResetHint != histogram.UnknownCounterReset {
				t.Fatalf("CounterResetHint = %v, want UnknownCounterReset", got.CounterResetHint)
			}
		})
	}
}

// TestHistogramStoreRoundTripGaugeHistogramFloat is
// TestHistogramStoreRoundTripGaugeHistogram's FloatHistogram counterpart.
func TestHistogramStoreRoundTripGaugeHistogramFloat(t *testing.T) {
	hst := NewHistogramStore()
	h := &histogram.FloatHistogram{
		CounterResetHint: histogram.GaugeType,
		Schema:           0, Count: 5, Sum: 10,
		PositiveSpans: []histogram.Span{{Offset: 0, Length: 1}}, PositiveBuckets: []float64{5},
	}
	if err := hst.AppendFloat(0, 1700000000000, h); err != nil {
		t.Fatalf("AppendFloat: %v", err)
	}
	it := hst.Iterator(0)
	if !it.Next() {
		t.Fatal("Next() = false")
	}
	_, got := it.AtFloat()
	if got.CounterResetHint != histogram.GaugeType {
		t.Fatalf("CounterResetHint = %v, want GaugeType", got.CounterResetHint)
	}
}

// TestHistogramStoreLayoutChangeStartsNewSegment confirms Append no longer rejects
// a genuine schema/zero-threshold/span change (histoSegment's own doc comment on
// why) - it starts a fresh segment instead, and a full decode still returns every
// sample, each carrying ITS OWN layout, in the order appended. Replaces the old
// TestHistogramStoreRejectsLayoutChange, whose whole premise (any layout change
// errors) is no longer true.
func TestHistogramStoreLayoutChangeStartsNewSegment(t *testing.T) {
	h1 := &histogram.Histogram{
		Schema:          0,
		PositiveSpans:   []histogram.Span{{Offset: 0, Length: 2}},
		PositiveBuckets: []int64{1, 1},
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
	for name, h2 := range cases {
		t.Run(name, func(t *testing.T) {
			hst := NewHistogramStore()
			if err := hst.Append(0, 1700000000000, h1); err != nil {
				t.Fatalf("Append h1: %v", err)
			}
			if err := hst.Append(0, 1700000015000, h2); err != nil {
				t.Fatalf("Append with %s = %v, want no error (new segment)", name, err)
			}

			it := hst.Iterator(0)
			if !it.Next() {
				t.Fatal("sample 0: Next() = false")
			}
			ts0, got0 := it.At()
			if ts0 != 1700000000000 {
				t.Fatalf("sample 0 ts = %d, want 1700000000000", ts0)
			}
			histEqual(t, got0, h1)

			if !it.Next() {
				t.Fatal("sample 1: Next() = false")
			}
			ts1, got1 := it.At()
			if ts1 != 1700000015000 {
				t.Fatalf("sample 1 ts = %d, want 1700000015000", ts1)
			}
			histEqual(t, got1, h2)

			if it.Next() {
				t.Fatal("more samples than expected")
			}
		})
	}
}

// TestHistogramStoreLayoutChangeStartsNewSegmentFloat is
// TestHistogramStoreLayoutChangeStartsNewSegment's FloatHistogram counterpart.
func TestHistogramStoreLayoutChangeStartsNewSegmentFloat(t *testing.T) {
	h1 := &histogram.FloatHistogram{
		Schema:          0,
		PositiveSpans:   []histogram.Span{{Offset: 0, Length: 2}},
		PositiveBuckets: []float64{1, 1},
	}
	cases := map[string]*histogram.FloatHistogram{
		"schema changed": {
			Schema: 1, PositiveSpans: h1.PositiveSpans, PositiveBuckets: []float64{1, 1},
		},
		"zero threshold changed": {
			Schema: 0, ZeroThreshold: 0.5, PositiveSpans: h1.PositiveSpans, PositiveBuckets: []float64{1, 1},
		},
		"span layout changed": {
			Schema:          0,
			PositiveSpans:   []histogram.Span{{Offset: 0, Length: 3}},
			PositiveBuckets: []float64{1, 1, 1},
		},
	}
	for name, h2 := range cases {
		t.Run(name, func(t *testing.T) {
			hst := NewHistogramStore()
			if err := hst.AppendFloat(0, 1700000000000, h1); err != nil {
				t.Fatalf("AppendFloat h1: %v", err)
			}
			if err := hst.AppendFloat(0, 1700000015000, h2); err != nil {
				t.Fatalf("AppendFloat with %s = %v, want no error (new segment)", name, err)
			}

			it := hst.Iterator(0)
			if !it.Next() {
				t.Fatal("sample 0: Next() = false")
			}
			ts0, got0 := it.AtFloat()
			if ts0 != 1700000000000 {
				t.Fatalf("sample 0 ts = %d, want 1700000000000", ts0)
			}
			floatHistEqual(t, got0, h1)

			if !it.Next() {
				t.Fatal("sample 1: Next() = false")
			}
			ts1, got1 := it.AtFloat()
			if ts1 != 1700000015000 {
				t.Fatalf("sample 1 ts = %d, want 1700000015000", ts1)
			}
			floatHistEqual(t, got1, h2)

			if it.Next() {
				t.Fatal("more samples than expected")
			}
		})
	}
}

// TestHistogramStoreRejectsBucketCountMismatch confirms ErrHistogramLayoutChanged's
// new, narrower meaning still fires: a sample whose spans/schema/zero-threshold
// match the current segment (so Append reuses it, not a new one) but whose own
// bucket slice length doesn't match what those spans imply - an internally
// inconsistent input, not a legitimate layout change, still a guarded error.
func TestHistogramStoreRejectsBucketCountMismatch(t *testing.T) {
	hst := NewHistogramStore()
	h1 := &histogram.Histogram{
		Schema:          0,
		PositiveSpans:   []histogram.Span{{Offset: 0, Length: 2}},
		PositiveBuckets: []int64{1, 1},
	}
	if err := hst.Append(0, 1700000000000, h1); err != nil {
		t.Fatalf("Append h1: %v", err)
	}
	hBad := &histogram.Histogram{
		Schema:          0,
		PositiveSpans:   h1.PositiveSpans, // same spans -> Append reuses the current segment
		PositiveBuckets: []int64{1, 1, 1}, // but 3 buckets, not the 2 those spans imply
	}
	if err := hst.Append(0, 1700000015000, hBad); err != ErrHistogramLayoutChanged {
		t.Fatalf("Append with mismatched bucket count = %v, want ErrHistogramLayoutChanged", err)
	}
}

// TestHistogramStoreRejectsBucketCountMismatchFloat is
// TestHistogramStoreRejectsBucketCountMismatch's FloatHistogram counterpart.
func TestHistogramStoreRejectsBucketCountMismatchFloat(t *testing.T) {
	hst := NewHistogramStore()
	h1 := &histogram.FloatHistogram{
		Schema:          0,
		PositiveSpans:   []histogram.Span{{Offset: 0, Length: 2}},
		PositiveBuckets: []float64{1, 1},
	}
	if err := hst.AppendFloat(0, 1700000000000, h1); err != nil {
		t.Fatalf("AppendFloat h1: %v", err)
	}
	hBad := &histogram.FloatHistogram{
		Schema:          0,
		PositiveSpans:   h1.PositiveSpans,
		PositiveBuckets: []float64{1, 1, 1},
	}
	if err := hst.AppendFloat(0, 1700000015000, hBad); err != ErrHistogramLayoutChanged {
		t.Fatalf("AppendFloat with mismatched bucket count = %v, want ErrHistogramLayoutChanged", err)
	}
}

// TestHistogramStoreTruncateAcrossLayoutChange confirms Truncate's decode-then-
// re-Append round trip correctly re-segments a kept range that spans a genuine
// layout change, rather than assuming (as a single-segment implementation would
// have to) that every retained sample shares one layout.
func TestHistogramStoreTruncateAcrossLayoutChange(t *testing.T) {
	hst := NewHistogramStore()
	h1 := &histogram.Histogram{Schema: 0, PositiveSpans: []histogram.Span{{Offset: 0, Length: 1}}, PositiveBuckets: []int64{1}}
	h2 := &histogram.Histogram{Schema: 0, PositiveSpans: []histogram.Span{{Offset: 0, Length: 1}}, PositiveBuckets: []int64{2}} // aged out by Truncate
	h3 := &histogram.Histogram{Schema: 1, PositiveSpans: []histogram.Span{{Offset: 0, Length: 2}}, PositiveBuckets: []int64{3, 1}}
	base := int64(1700000000000)
	if err := hst.Append(0, base, h1); err != nil {
		t.Fatalf("Append h1: %v", err)
	}
	if err := hst.Append(0, base+15000, h2); err != nil {
		t.Fatalf("Append h2: %v", err)
	}
	if err := hst.Append(0, base+30000, h3); err != nil {
		t.Fatalf("Append h3: %v", err)
	}

	n := hst.Truncate(0, base+20000) // drops h1, h2 (both < mint), keeps h3
	if n != 1 {
		t.Fatalf("Truncate retained %d samples, want 1", n)
	}

	it := hst.Iterator(0)
	if !it.Next() {
		t.Fatal("Next() = false after Truncate")
	}
	ts, got := it.At()
	if ts != base+30000 {
		t.Fatalf("ts = %d, want %d", ts, base+30000)
	}
	histEqual(t, got, h3)
	if it.Next() {
		t.Fatal("more samples than expected after Truncate")
	}
}

// TestHistogramStoreIsolatesMixedTypeSeries confirms the isFloat discriminator
// (histoSeries' own doc comment) genuinely isolates an int-typed series from a
// float-typed one sharing the same store's map - not just that each round-trips
// correctly in isolation (TestHistogramStoreRoundTrip_MultipleSamples/
// RoundTripFloat_MultipleSamples already cover that), but that neither's
// decode path is affected by the other's presence.
func TestHistogramStoreIsolatesMixedTypeSeries(t *testing.T) {
	hst := NewHistogramStore()
	hInt := &histogram.Histogram{Schema: 0, PositiveSpans: []histogram.Span{{Offset: 0, Length: 1}}, PositiveBuckets: []int64{5}}
	hFloat := &histogram.FloatHistogram{Schema: 2, PositiveSpans: []histogram.Span{{Offset: 1, Length: 2}}, PositiveBuckets: []float64{7, 8}}

	if err := hst.Append(10, 1700000000000, hInt); err != nil {
		t.Fatalf("Append(10): %v", err)
	}
	if err := hst.AppendFloat(20, 1700000000000, hFloat); err != nil {
		t.Fatalf("AppendFloat(20): %v", err)
	}
	if hst.NumSeries() != 2 {
		t.Fatalf("NumSeries() = %d, want 2", hst.NumSeries())
	}
	if hst.IsFloat(10) {
		t.Fatal("IsFloat(10) = true for an int-typed series")
	}
	if !hst.IsFloat(20) {
		t.Fatal("IsFloat(20) = false for a float-typed series")
	}

	itInt := hst.Iterator(10)
	if !itInt.Next() {
		t.Fatal("series 10: Next() = false")
	}
	_, gotInt := itInt.At()
	histEqual(t, gotInt, hInt)

	itFloat := hst.Iterator(20)
	if !itFloat.Next() {
		t.Fatal("series 20: Next() = false")
	}
	_, gotFloat := itFloat.AtFloat()
	floatHistEqual(t, gotFloat, hFloat)

	// Mixing types on either series must be rejected.
	if err := hst.AppendFloat(10, 1700000015000, hFloat); err != ErrHistogramTypeChanged {
		t.Fatalf("AppendFloat on an int-typed series = %v, want ErrHistogramTypeChanged", err)
	}
	if err := hst.Append(20, 1700000015000, hInt); err != ErrHistogramTypeChanged {
		t.Fatalf("Append on a float-typed series = %v, want ErrHistogramTypeChanged", err)
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

// TestHistogramStoreTruncateFloat is TestHistogramStoreTruncate's FloatHistogram
// counterpart - Truncate's decode/re-encode-in-place approach goes through a
// completely different branch for a float-typed series (AtFloat/AppendFloat,
// not At/Append), so this isn't implied by the int test passing.
func TestHistogramStoreTruncateFloat(t *testing.T) {
	hst := NewHistogramStore()
	mk := func(count float64, buckets []float64) *histogram.FloatHistogram {
		return &histogram.FloatHistogram{
			Schema:          0,
			ZeroThreshold:   0.001,
			Count:           count,
			Sum:             count,
			PositiveSpans:   []histogram.Span{{Offset: 0, Length: 3}},
			PositiveBuckets: buckets,
		}
	}
	samples := []*histogram.FloatHistogram{
		mk(10, []float64{1, 1, 1}),
		mk(20, []float64{2, 2, 1}),
		mk(30, []float64{4, 1, 2}),
		mk(40, []float64{4, 2, 2}),
	}
	ts := int64(1700000000000)
	var timestamps []int64
	for _, h := range samples {
		if err := hst.AppendFloat(0, ts, h); err != nil {
			t.Fatalf("AppendFloat: %v", err)
		}
		timestamps = append(timestamps, ts)
		ts += 15000
	}
	otherH := mk(1, []float64{1, 0, 0})
	if err := hst.AppendFloat(1, 1700000000000, otherH); err != nil {
		t.Fatalf("AppendFloat(other): %v", err)
	}

	n := hst.Truncate(0, timestamps[2])
	if n != 2 {
		t.Fatalf("Truncate returned %d, want 2", n)
	}
	if !hst.IsFloat(0) {
		t.Fatal("IsFloat(0) = false after Truncate - the re-encoded series lost its float typing")
	}

	it := hst.Iterator(0)
	i := 2
	for it.Next() {
		gotTS, gotH := it.AtFloat()
		if gotTS != timestamps[i] {
			t.Fatalf("sample %d: ts = %d, want %d", i-2, gotTS, timestamps[i])
		}
		floatHistEqual(t, gotH, samples[i])
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
	_, gotOther := oit.AtFloat()
	floatHistEqual(t, gotOther, otherH)
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
