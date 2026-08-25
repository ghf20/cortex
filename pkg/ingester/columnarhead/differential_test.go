package columnarhead

import (
	"context"
	"fmt"
	"math"
	"os"
	"sort"
	"testing"

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
