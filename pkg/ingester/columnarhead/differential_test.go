package columnarhead

import (
	"context"
	"fmt"
	"math"
	"os"
	"sort"
	"testing"

	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

// This file is the differential harness design doc §4 calls for ("build it before
// the head... it is what makes 'bit-identical' a defensible claim rather than a
// hope"). It feeds the SAME workload into a real tsdb.Head and this package's Head,
// queries both through their real storage.Querier/Select paths, and asserts the
// results are bit-identical - not just "close," since a canonicalized NaN payload
// would silently break rate()'s staleness detection without ever showing up as a
// visible numeric difference.

// diffSeries is one series' full workload: its labels and every (timestamp, value)
// sample to append, in order.
type diffSeries struct {
	labels  labels.Labels
	samples []sample
}

// diffWorkload is the shared golden dataset. Includes the design doc's own flagged
// highest-risk case (a staleness marker mid-stream - §4: "get this wrong and rate()
// silently changes behaviour at series end"), ordinary NaN/+Inf/-Inf, and the same
// timestamp delta-of-delta bucket boundaries TestVarbitBoundaries already tests in
// isolation - now exercised end-to-end through both real storage engines, not just
// this package's own encoder in a vacuum.
func diffWorkload() []diffSeries {
	tgt := func(pod string) labels.Labels {
		return labels.FromStrings(
			labels.MetricName, "cpu_seconds_total",
			"cluster", "eks-prod-1", "namespace", "ns-7", "pod", pod,
			"container", "app", "node", "ip-10-1-2-3", "job", "cadvisor",
		)
	}
	base := int64(1700000000000)

	return []diffSeries{
		{tgt("payments-api-1"), seqSamples(base, []int64{15000, 15000, 15000}, []float64{1, 1, 0, 1})},
		{tgt("payments-api-2"), seqSamples(base, []int64{15000, 15000, 15000, 15000}, // near-constant: exercises the 1-bit "unchanged" path
			[]float64{100, 100, 100, 100, 100})},
		{tgt("payments-api-3"), seqSamples(base, []int64{15000, 15000, 15000}, // monotonic counter
			[]float64{1000, 1003, 1007, 1012})},
		{tgt("payments-api-4"), seqSamples(base, []int64{15000, 15000}, // large jump, forces a new XOR window
			[]float64{1, 1 << 40, 3})},
		{tgt("payments-api-5"), seqSamples(base, []int64{15000, 15000, 15000}, // staleness marker mid-stream
			[]float64{5, 6, math.Float64frombits(value.StaleNaN), 7})},
		{tgt("payments-api-6"), seqSamples(base, []int64{15000, 15000, 15000}, // ordinary NaN and +/-Inf
			[]float64{math.NaN(), math.Inf(1), math.Inf(-1), 1.5})},
		{tgt("payments-api-7"), dodBoundarySamples(base)},
	}
}

// seqSamples builds a sample slice from a starting timestamp, per-step deltas
// (len(deltas) == len(vals)-1), and values.
func seqSamples(base int64, deltas []int64, vals []float64) []sample {
	if len(deltas) != len(vals)-1 {
		panic(fmt.Sprintf("seqSamples: %d deltas for %d values (want len(vals)-1)", len(deltas), len(vals)))
	}
	out := make([]sample, len(vals))
	ts := base
	out[0] = sample{ts, vals[0]}
	for i, d := range deltas {
		ts += d
		out[i+1] = sample{ts, vals[i+1]}
	}
	return out
}

// dodBoundarySamples hits the exact timestamp delta-of-delta bucket boundaries
// TestVarbitBoundaries tests in isolation (0, +/-64, +/-256, +/-2047), while keeping
// every actual per-sample delta positive - real tsdb.Head requires monotonically
// increasing timestamps (no OOO enabled in this comparison), so a fair differential
// test can't use negative deltas directly; the boundary values are hit via
// delta-of-delta against a ~15s baseline instead, which stays comfortably positive.
func dodBoundarySamples(base int64) []sample {
	firstDelta := int64(15000)
	dods := []int64{0, 64, -64, 256, -256, 2047, -2047}
	deltas := make([]int64, 0, len(dods)+1)
	deltas = append(deltas, firstDelta)
	cur := firstDelta
	for _, d := range dods {
		cur += d
		deltas = append(deltas, cur)
	}
	vals := make([]float64, len(deltas)+1)
	for i := range vals {
		vals[i] = float64(i + 1)
	}
	return seqSamples(base, deltas, vals)
}

func newRealHead(t *testing.T) *tsdb.Head {
	t.Helper()
	dir, err := os.MkdirTemp("", "diffhead")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	opts := tsdb.DefaultHeadOptions()
	opts.ChunkDirRoot = dir
	opts.ChunkRange = 2 * 60 * 60 * 1000
	opts.IsolationDisabled = true
	h, err := tsdb.NewHead(nil, nil, nil, nil, opts, nil)
	if err != nil {
		t.Fatalf("tsdb.NewHead: %v", err)
	}
	t.Cleanup(func() { h.Close() })
	return h
}

func appendToReal(t *testing.T, h *tsdb.Head, workload []diffSeries) {
	t.Helper()
	app := h.Appender(context.Background())
	for _, s := range workload {
		var ref storage.SeriesRef
		for _, sm := range s.samples {
			var err error
			ref, err = app.Append(ref, s.labels, sm.ts, sm.v)
			if err != nil {
				t.Fatalf("real head Append(%v, %d, %v): %v", s.labels, sm.ts, sm.v, err)
			}
		}
	}
	if err := app.Commit(); err != nil {
		t.Fatalf("real head Commit: %v", err)
	}
}

func appendToColumnar(t *testing.T, h *Head, workload []diffSeries) {
	t.Helper()
	app := h.Appender(context.Background())
	for _, s := range workload {
		var ref storage.SeriesRef
		for _, sm := range s.samples {
			var err error
			ref, err = app.Append(ref, s.labels, sm.ts, sm.v)
			if err != nil {
				t.Fatalf("columnar head Append(%v, %d, %v): %v", s.labels, sm.ts, sm.v, err)
			}
		}
	}
	if err := app.Commit(); err != nil {
		t.Fatalf("columnar head Commit: %v", err)
	}
}

// collectSeries drains ss into a labels-key -> samples map, using each series' fully
// sorted label string as the key so comparison is independent of iteration order.
func collectSeries(t *testing.T, ss storage.SeriesSet) map[string][]sample {
	t.Helper()
	out := make(map[string][]sample)
	for ss.Next() {
		series := ss.At()
		key := series.Labels().String()
		it := series.Iterator(nil)
		var samples []sample
		for it.Next() == chunkenc.ValFloat {
			ts, v := it.At()
			samples = append(samples, sample{ts, v})
		}
		if err := it.Err(); err != nil {
			t.Fatalf("iterator error for series %s: %v", key, err)
		}
		if _, dup := out[key]; dup {
			t.Fatalf("duplicate series key %q returned by Select", key)
		}
		out[key] = samples
	}
	if err := ss.Err(); err != nil {
		t.Fatalf("SeriesSet error: %v", err)
	}
	return out
}

// assertSamplesBitIdentical compares two series' samples with math.Float64bits, not
// ==, specifically because NaN != NaN under normal float comparison - this is exactly
// the check that would catch a staleness marker silently getting canonicalized to a
// different (still-NaN, but wrong-payload) bit pattern somewhere in the pipeline.
func assertSamplesBitIdentical(t *testing.T, seriesKey string, real, columnar []sample) {
	t.Helper()
	if len(real) != len(columnar) {
		t.Errorf("series %s: real head has %d samples, columnar has %d", seriesKey, len(real), len(columnar))
		return
	}
	for i := range real {
		if real[i].ts != columnar[i].ts {
			t.Errorf("series %s, sample %d: ts real=%d columnar=%d", seriesKey, i, real[i].ts, columnar[i].ts)
		}
		rb, cb := math.Float64bits(real[i].v), math.Float64bits(columnar[i].v)
		if rb != cb {
			t.Errorf("series %s, sample %d: value real=%v (bits %x) columnar=%v (bits %x) - not bit-identical",
				seriesKey, i, real[i].v, rb, columnar[i].v, cb)
		}
	}
}

func TestDifferentialRealVsColumnar(t *testing.T) {
	workload := diffWorkload()

	realHead := newRealHead(t)
	appendToReal(t, realHead, workload)

	colHead := NewHead(len(workload), 1, 16)
	appendToColumnar(t, colHead, workload)

	realQuerier, err := tsdb.NewBlockQuerier(realHead, math.MinInt64, math.MaxInt64)
	if err != nil {
		t.Fatalf("real head querier: %v", err)
	}
	defer realQuerier.Close()
	colQuerier, err := colHead.Querier(math.MinInt64, math.MaxInt64)
	if err != nil {
		t.Fatalf("columnar head querier: %v", err)
	}
	defer colQuerier.Close()

	matchAll := labels.MustNewMatcher(labels.MatchRegexp, labels.MetricName, ".+")
	realSeries := collectSeries(t, realQuerier.Select(context.Background(), true, nil, matchAll))
	colSeries := collectSeries(t, colQuerier.Select(context.Background(), true, nil, matchAll))

	if len(realSeries) != len(colSeries) {
		t.Errorf("real head returned %d series, columnar returned %d", len(realSeries), len(colSeries))
	}

	var keys []string
	for k := range realSeries {
		keys = append(keys, k)
	}
	for k := range colSeries {
		if _, ok := realSeries[k]; !ok {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)

	for _, key := range keys {
		real, ok := realSeries[key]
		if !ok {
			t.Errorf("series %s present in columnar, missing from real head", key)
			continue
		}
		col, ok := colSeries[key]
		if !ok {
			t.Errorf("series %s present in real head, missing from columnar", key)
			continue
		}
		assertSamplesBitIdentical(t, key, real, col)
	}
}

// TestDifferentialTimeRangeFiltering runs the same workload/comparison as
// TestDifferentialRealVsColumnar but with a BOUNDED query range instead of
// [MinInt64, MaxInt64] - confirms real tsdb.Head and columnarhead.Head agree on
// which samples fall inside a real, non-trivial window, not just that an unbounded
// scan matches (the case every other test here already covers).
func TestDifferentialTimeRangeFiltering(t *testing.T) {
	workload := diffWorkload()

	realHead := newRealHead(t)
	appendToReal(t, realHead, workload)
	colHead := NewHead(len(workload), 1, 16)
	appendToColumnar(t, colHead, workload)

	// Chosen to land strictly inside most series' timestamp spans (base+15000 to
	// base+90000ish), excluding each series' first and/or last sample - a window that
	// only matched everything or nothing wouldn't actually exercise filtering.
	base := int64(1700000000000)
	mint, maxt := base+15000, base+75000

	realQuerier, err := tsdb.NewBlockQuerier(realHead, mint, maxt)
	if err != nil {
		t.Fatalf("real head querier: %v", err)
	}
	defer realQuerier.Close()
	colQuerier, err := colHead.Querier(mint, maxt)
	if err != nil {
		t.Fatalf("columnar head querier: %v", err)
	}
	defer colQuerier.Close()

	matchAll := labels.MustNewMatcher(labels.MatchRegexp, labels.MetricName, ".+")
	realSeries := collectSeries(t, realQuerier.Select(context.Background(), true, nil, matchAll))
	colSeries := collectSeries(t, colQuerier.Select(context.Background(), true, nil, matchAll))

	totalReal, totalCol := 0, 0
	for _, s := range realSeries {
		totalReal += len(s)
	}
	for _, s := range colSeries {
		totalCol += len(s)
	}
	t.Logf("bounded range [%d, %d]: real=%d samples, columnar=%d samples", mint, maxt, totalReal, totalCol)
	if totalReal == 0 {
		t.Fatal("sanity check failed: bounded range matched zero real samples - window doesn't actually exercise filtering")
	}
	// Also confirm the bound genuinely excluded something, relative to the unbounded
	// case (33 total samples - see TestDifferentialSanityCheck) - otherwise this
	// window isn't actually testing the filtering boundary at all.
	if totalReal >= 33 {
		t.Fatalf("bounded range matched %d samples, expected fewer than the unbounded total (33) - window doesn't exclude anything", totalReal)
	}

	for key, real := range realSeries {
		col, ok := colSeries[key]
		if !ok {
			t.Errorf("series %s present in real head, missing from columnar", key)
			continue
		}
		assertSamplesBitIdentical(t, key, real, col)
		for _, sm := range real {
			if sm.ts < mint || sm.ts > maxt {
				t.Errorf("series %s: real head returned sample at ts=%d, outside requested [%d, %d]", key, sm.ts, mint, maxt)
			}
		}
	}
}

func TestDifferentialSanityCheck(t *testing.T) {
	workload := diffWorkload()
	realHead := newRealHead(t)
	appendToReal(t, realHead, workload)
	colHead := NewHead(len(workload), 1, 16)
	appendToColumnar(t, colHead, workload)

	realQuerier, _ := tsdb.NewBlockQuerier(realHead, math.MinInt64, math.MaxInt64)
	defer realQuerier.Close()
	colQuerier, _ := colHead.Querier(math.MinInt64, math.MaxInt64)
	defer colQuerier.Close()

	matchAll := labels.MustNewMatcher(labels.MatchRegexp, labels.MetricName, ".+")
	realSeries := collectSeries(t, realQuerier.Select(context.Background(), true, nil, matchAll))
	colSeries := collectSeries(t, colQuerier.Select(context.Background(), true, nil, matchAll))

	t.Logf("real series: %d, columnar series: %d", len(realSeries), len(colSeries))
	totalSamples := 0
	for k, v := range realSeries {
		t.Logf("  %s: %d samples", k, len(v))
		totalSamples += len(v)
	}
	t.Logf("total real samples compared: %d", totalSamples)
	if len(realSeries) == 0 || totalSamples == 0 {
		t.Fatal("sanity check failed: zero series or zero samples - the differential test would pass vacuously")
	}
}

// histDiffWorkload is the shared golden native-histogram dataset for
// TestDifferentialHistogramRealVsColumnar - deliberately kept within
// HistogramStore's own stated scope (see its doc comment): stable schema/
// zero-threshold/span layout for a series' whole lifetime, only bucket counts/
// sum/count/zero-count changing sample to sample. Counter-reset detection and
// mid-stream layout changes are a real, separate, not-yet-built gap (Phase 3) -
// this harness doesn't exercise them, matching what's actually implemented on
// both sides fairly (real tsdb.Head supports far more than this; comparing
// outside HistogramStore's scope would only prove real Prometheus's own
// correctness, not columnarhead's).
func histDiffWorkload() []*histogram.Histogram {
	// mk computes Count itself from zeroCount and the buckets' absolute values
	// (each bucket's absolute count is the running cumulative sum of its
	// spatial deltas, real histogram.Histogram's own documented encoding -
	// "guarantees that the count in the previous bucket is the sum of the
	// current and all the deltas before it") - real tsdb.Head validates that
	// Count equals zeroCount plus every bucket's absolute count on Append
	// (found the hard way: hand-picked Count values from histogram_test.go's
	// own lower-level HistogramStore unit test - which doesn't validate this,
	// since it only exercises the encoding format directly - are rejected
	// here). Computing it mechanically avoids hand-arithmetic errors.
	mk := func(zeroCount uint64, sum float64, posDeltas, negDeltas []int64) *histogram.Histogram {
		count := zeroCount
		var cur int64
		for _, d := range posDeltas {
			cur += d
			count += uint64(cur)
		}
		cur = 0
		for _, d := range negDeltas {
			cur += d
			count += uint64(cur)
		}
		return &histogram.Histogram{
			Schema:          1,
			ZeroThreshold:   0.0001,
			ZeroCount:       zeroCount,
			Count:           count,
			Sum:             sum,
			PositiveSpans:   []histogram.Span{{Offset: -2, Length: 4}},
			NegativeSpans:   []histogram.Span{{Offset: 0, Length: 2}},
			PositiveBuckets: posDeltas,
			NegativeBuckets: negDeltas,
		}
	}
	return []*histogram.Histogram{
		mk(2, 5.5, []int64{1, 0, 0, 1}, []int64{1, 0}),
		mk(3, 8.25, []int64{1, 1, -1, 2}, []int64{0, 1}),
		mk(3, 8.25, []int64{1, 1, -1, 2}, []int64{0, 1}), // identical spatial deltas to the previous sample - a real, legitimate "no new observations this interval" case
		mk(9, -12.75, []int64{5, -3, 2, -1}, []int64{2, -1}),
	}
}

func appendHistogramsToReal(t *testing.T, h *tsdb.Head, l labels.Labels, base int64, hists []*histogram.Histogram) {
	t.Helper()
	app := h.Appender(context.Background())
	var ref storage.SeriesRef
	ts := base
	for _, hg := range hists {
		var err error
		ref, err = app.AppendHistogram(ref, l, ts, hg, nil)
		if err != nil {
			t.Fatalf("real head AppendHistogram(ts=%d): %v", ts, err)
		}
		ts += 15000
	}
	if err := app.Commit(); err != nil {
		t.Fatalf("real head Commit: %v", err)
	}
}

func appendHistogramsToColumnar(t *testing.T, h *Head, l labels.Labels, base int64, hists []*histogram.Histogram) {
	t.Helper()
	app := h.Appender(context.Background())
	var ref storage.SeriesRef
	ts := base
	for _, hg := range hists {
		var err error
		ref, err = app.AppendHistogram(ref, l, ts, hg, nil)
		if err != nil {
			t.Fatalf("columnar head AppendHistogram(ts=%d): %v", ts, err)
		}
		ts += 15000
	}
	if err := app.Commit(); err != nil {
		t.Fatalf("columnar head Commit: %v", err)
	}
}

// collectHistogramSamples drains a single matching series' (ts, *histogram.Histogram)
// pairs from ss - like collectSeries, but for the histogram value type, and
// asserting exactly one matching series (this harness always queries by exact
// series label match, unlike the float differential tests' multi-series scan).
func collectHistogramSamples(t *testing.T, ss storage.SeriesSet) []struct {
	ts int64
	h  *histogram.Histogram
} {
	t.Helper()
	if !ss.Next() {
		t.Fatalf("SeriesSet: no series returned")
	}
	series := ss.At()
	it := series.Iterator(nil)
	var out []struct {
		ts int64
		h  *histogram.Histogram
	}
	for it.Next() == chunkenc.ValHistogram {
		ts, h := it.AtHistogram(nil)
		out = append(out, struct {
			ts int64
			h  *histogram.Histogram
		}{ts, h})
	}
	if err := it.Err(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	if ss.Next() {
		t.Fatalf("SeriesSet: more than one matching series")
	}
	if err := ss.Err(); err != nil {
		t.Fatalf("SeriesSet error: %v", err)
	}
	return out
}

// TestDifferentialHistogramRealVsColumnar is Phase 6's histogram extension to
// the float differential harness above (CHECKLIST.md: "what remains here is
// extending the same harness to histograms, OOO, exemplars, and metadata") -
// same real tsdb.Head vs. columnarhead.Head comparison, same bit-exact bar
// (histEqual, not just "close"), for native histograms within HistogramStore's
// documented stable-layout scope.
func TestDifferentialHistogramRealVsColumnar(t *testing.T) {
	l := labels.FromStrings(
		labels.MetricName, "request_duration_seconds",
		"cluster", "eks-prod-1", "namespace", "ns-7", "pod", "payments-api-1",
		"container", "app", "node", "ip-10-1-2-3", "job", "cadvisor",
	)
	base := int64(1700000000000)
	workload := histDiffWorkload()

	realHead := newRealHead(t)
	appendHistogramsToReal(t, realHead, l, base, workload)
	colHead := NewHead(1, 1, 16)
	appendHistogramsToColumnar(t, colHead, l, base, workload)

	realQuerier, err := tsdb.NewBlockQuerier(realHead, math.MinInt64, math.MaxInt64)
	if err != nil {
		t.Fatalf("real head querier: %v", err)
	}
	defer realQuerier.Close()
	colQuerier, err := colHead.Querier(math.MinInt64, math.MaxInt64)
	if err != nil {
		t.Fatalf("columnar head querier: %v", err)
	}
	defer colQuerier.Close()

	m := labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "request_duration_seconds")
	realSamples := collectHistogramSamples(t, realQuerier.Select(context.Background(), true, nil, m))
	colSamples := collectHistogramSamples(t, colQuerier.Select(context.Background(), true, nil, m))

	if len(realSamples) != len(colSamples) {
		t.Fatalf("real head returned %d histogram samples, columnar returned %d", len(realSamples), len(colSamples))
	}
	if len(realSamples) != len(workload) {
		t.Fatalf("sanity check failed: got %d samples back, want %d (the differential test would pass vacuously otherwise)", len(realSamples), len(workload))
	}
	for i := range realSamples {
		if realSamples[i].ts != colSamples[i].ts {
			t.Fatalf("sample %d: ts real=%d columnar=%d", i, realSamples[i].ts, colSamples[i].ts)
		}
		histEqual(t, colSamples[i].h, realSamples[i].h)
	}
}

// newRealDBWithOOO opens a real *tsdb.DB (not a bare *tsdb.Head, unlike
// newRealHead) with out-of-order ingestion enabled - OOO-aware querying needs
// db.Querier's internal NewHeadAndOOOQuerier/isolation-state wiring
// (vendor/.../tsdb/db.go, ooo_head_read.go), which isn't reachable from outside
// the tsdb package against a bare *Head the way the float/histogram
// differential tests above use one directly.
func newRealDBWithOOO(t *testing.T, window int64) *tsdb.DB {
	t.Helper()
	dir := t.TempDir()
	db, err := tsdb.Open(dir, nil, nil, &tsdb.Options{
		RetentionDuration:    int64(1000 * 60 * 60 * 24),
		MinBlockDuration:     2 * 60 * 60 * 1000,
		MaxBlockDuration:     2 * 60 * 60 * 1000,
		NoLockfile:           true,
		OutOfOrderTimeWindow: window,
		IsolationDisabled:    true,
	}, nil)
	if err != nil {
		t.Fatalf("tsdb.Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// TestDifferentialOOORealVsColumnar is Phase 6's OOO extension to the
// differential harness: append the same in-order-then-out-of-order sequence to
// a real *tsdb.DB (OOO enabled) and to columnarhead.Head (SetOOOTimeWindow),
// and confirm both produce the identical merged, strictly-timestamp-ordered
// stream - not just that each side accepts the OOO sample, but that the
// MERGE point (real tsdb's NewHeadAndOOOQuerier vs. this package's
// mergedIterator, ooo.go) agrees exactly.
func TestDifferentialOOORealVsColumnar(t *testing.T) {
	const window = 60_000
	l := labels.FromStrings(
		labels.MetricName, "cpu_seconds_total",
		"cluster", "eks-prod-1", "namespace", "ns-7", "pod", "payments-api-1",
		"container", "app", "node", "ip-10-1-2-3", "job", "cadvisor",
	)
	base := int64(1700000000000)
	// In-order: 0s, 30s. Then OOO: 15s (within the 60s window) - lands between
	// the two in-order samples once merged.
	inOrder := []sample{{base, 1}, {base + 30000, 3}}
	ooo := sample{base + 15000, 2}
	want := []sample{{base, 1}, {base + 15000, 2}, {base + 30000, 3}}

	realDB := newRealDBWithOOO(t, window)
	realApp := realDB.Appender(context.Background())
	var ref storage.SeriesRef
	for _, sm := range inOrder {
		var err error
		ref, err = realApp.Append(ref, l, sm.ts, sm.v)
		if err != nil {
			t.Fatalf("real db Append(in-order, ts=%d): %v", sm.ts, err)
		}
	}
	if _, err := realApp.Append(ref, l, ooo.ts, ooo.v); err != nil {
		t.Fatalf("real db Append(OOO, ts=%d): %v", ooo.ts, err)
	}
	if err := realApp.Commit(); err != nil {
		t.Fatalf("real db Commit: %v", err)
	}

	colHead := NewHead(1, 1, 16)
	colHead.SetOOOTimeWindow(window)
	colApp := colHead.Appender(context.Background())
	var colRef storage.SeriesRef
	for _, sm := range inOrder {
		var err error
		colRef, err = colApp.Append(colRef, l, sm.ts, sm.v)
		if err != nil {
			t.Fatalf("columnar head Append(in-order, ts=%d): %v", sm.ts, err)
		}
	}
	if _, err := colApp.Append(colRef, l, ooo.ts, ooo.v); err != nil {
		t.Fatalf("columnar head Append(OOO, ts=%d): %v", ooo.ts, err)
	}
	if err := colApp.Commit(); err != nil {
		t.Fatalf("columnar head Commit: %v", err)
	}

	realQuerier, err := realDB.Querier(math.MinInt64, math.MaxInt64)
	if err != nil {
		t.Fatalf("real db querier: %v", err)
	}
	defer realQuerier.Close()
	colQuerier, err := colHead.Querier(math.MinInt64, math.MaxInt64)
	if err != nil {
		t.Fatalf("columnar head querier: %v", err)
	}
	defer colQuerier.Close()

	m := labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "cpu_seconds_total")
	realSeries := collectSeries(t, realQuerier.Select(context.Background(), true, nil, m))
	colSeries := collectSeries(t, colQuerier.Select(context.Background(), true, nil, m))

	key := l.String()
	real, ok := realSeries[key]
	if !ok {
		t.Fatalf("series missing from real db result: %v", realSeries)
	}
	col, ok := colSeries[key]
	if !ok {
		t.Fatalf("series missing from columnar head result: %v", colSeries)
	}
	if len(real) != len(want) {
		t.Fatalf("sanity check failed: real db returned %d samples, want %d - the differential test would pass vacuously otherwise", len(real), len(want))
	}
	assertSamplesBitIdentical(t, "real vs want", real, want)
	assertSamplesBitIdentical(t, key, real, col)
}

// TestDifferentialExemplarRealVsColumnar is Phase 6's exemplar extension to the
// differential harness. Deliberately stays within a known, already-documented
// gap rather than exercising it: exemplarStorage.append (exemplar.go) never
// records the original exemplar.Exemplar's HasTs bit, so columnarhead's
// ExemplarQuerier always reports HasTs: true (see CHECKLIST.md's Phase 7 step 4
// note) - every exemplar here is constructed with HasTs: true already, so this
// comparison exercises real correctness (labels, value, timestamp, matcher
// semantics) without generating known-gap noise unrelated to what's being
// tested.
func TestDifferentialExemplarRealVsColumnar(t *testing.T) {
	l := labels.FromStrings(
		labels.MetricName, "cpu_seconds_total",
		"cluster", "eks-prod-1", "namespace", "ns-7", "pod", "payments-api-1",
		"container", "app", "node", "ip-10-1-2-3", "job", "cadvisor",
	)
	base := int64(1700000000000)
	ex1 := exemplar.Exemplar{Labels: labels.FromStrings("trace_id", "abc123"), Value: 1.5, Ts: base, HasTs: true}
	ex2 := exemplar.Exemplar{Labels: labels.FromStrings("trace_id", "def456"), Value: 2.5, Ts: base + 15000, HasTs: true}

	dir := t.TempDir()
	realDB, err := tsdb.Open(dir, nil, nil, &tsdb.Options{
		RetentionDuration:     int64(1000 * 60 * 60 * 24),
		MinBlockDuration:      2 * 60 * 60 * 1000,
		MaxBlockDuration:      2 * 60 * 60 * 1000,
		NoLockfile:            true,
		IsolationDisabled:     true,
		EnableExemplarStorage: true,
		MaxExemplars:          100,
	}, nil)
	if err != nil {
		t.Fatalf("tsdb.Open: %v", err)
	}
	t.Cleanup(func() { realDB.Close() })

	realApp := realDB.Appender(context.Background())
	ref, err := realApp.Append(0, l, base, 1)
	if err != nil {
		t.Fatalf("real db Append: %v", err)
	}
	for _, e := range []exemplar.Exemplar{ex1, ex2} {
		if _, err := realApp.AppendExemplar(ref, l, e); err != nil {
			t.Fatalf("real db AppendExemplar: %v", err)
		}
	}
	if err := realApp.Commit(); err != nil {
		t.Fatalf("real db Commit: %v", err)
	}

	colHead := NewHead(1, 1, 16)
	colApp := colHead.Appender(context.Background())
	colRef, err := colApp.Append(0, l, base, 1)
	if err != nil {
		t.Fatalf("columnar head Append: %v", err)
	}
	for _, e := range []exemplar.Exemplar{ex1, ex2} {
		if _, err := colApp.AppendExemplar(colRef, l, e); err != nil {
			t.Fatalf("columnar head AppendExemplar: %v", err)
		}
	}
	if err := colApp.Commit(); err != nil {
		t.Fatalf("columnar head Commit: %v", err)
	}

	realEQ, err := realDB.ExemplarQuerier(context.Background())
	if err != nil {
		t.Fatalf("real db ExemplarQuerier: %v", err)
	}
	colEQ, err := colHead.ExemplarQuerier(context.Background())
	if err != nil {
		t.Fatalf("columnar head ExemplarQuerier: %v", err)
	}

	m := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "cpu_seconds_total")}
	realResults, err := realEQ.Select(base-1, base+30000, m)
	if err != nil {
		t.Fatalf("real db Select: %v", err)
	}
	colResults, err := colEQ.Select(base-1, base+30000, m)
	if err != nil {
		t.Fatalf("columnar head Select: %v", err)
	}

	if len(realResults) != 1 || len(colResults) != 1 {
		t.Fatalf("sanity check failed: real=%d columnar=%d result sets, want exactly 1 each", len(realResults), len(colResults))
	}
	if !labels.Equal(realResults[0].SeriesLabels, l) {
		t.Fatalf("real db SeriesLabels = %v, want %v", realResults[0].SeriesLabels, l)
	}
	if !labels.Equal(colResults[0].SeriesLabels, l) {
		t.Fatalf("columnar head SeriesLabels = %v, want %v", colResults[0].SeriesLabels, l)
	}

	want := []exemplar.Exemplar{ex1, ex2}
	for name, got := range map[string][]exemplar.Exemplar{"real": realResults[0].Exemplars, "columnar": colResults[0].Exemplars} {
		if len(got) != len(want) {
			t.Fatalf("%s: got %d exemplars, want %d", name, len(got), len(want))
		}
		for i, w := range want {
			g := got[i]
			if g.Ts != w.Ts || g.Value != w.Value || !labels.Equal(g.Labels, w.Labels) || g.HasTs != w.HasTs {
				t.Fatalf("%s exemplar %d = %+v, want %+v", name, i, g, w)
			}
		}
	}
}
