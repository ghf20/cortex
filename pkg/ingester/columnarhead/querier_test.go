package columnarhead

import (
	"context"
	"math"
	"path/filepath"
	"testing"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/index"
)

func buildQueryHead(t *testing.T) *Head {
	t.Helper()
	h := NewHead(4, 2, 8)
	tgtA := TargetLabels{Cluster: "c1", Namespace: "n1", Pod: "p1", Container: "co", Node: "no1", Job: "j"}
	tgtB := TargetLabels{Cluster: "c2", Namespace: "n2", Pod: "p2", Container: "co", Node: "no2", Job: "j"}

	must := func(ref uint32, err error) uint32 {
		t.Helper()
		if err != nil {
			t.Fatalf("setup: %v", err)
		}
		return ref
	}
	up1 := must(h.GetOrCreateSeries(tgtA, "up", "", ""))
	up2 := must(h.GetOrCreateSeries(tgtB, "up", "", ""))
	bucket := must(h.GetOrCreateSeries(tgtA, "request_duration_bucket", "le", "0.1"))

	if err := h.Append(up1, 1700000000000, 1); err != nil {
		t.Fatalf("Append up1: %v", err)
	}
	if err := h.Append(up1, 1700000015000, 0); err != nil {
		t.Fatalf("Append up1: %v", err)
	}
	if err := h.Append(up2, 1700000000000, 1); err != nil {
		t.Fatalf("Append up2: %v", err)
	}
	if err := h.Append(bucket, 1700000000000, 3); err != nil {
		t.Fatalf("Append bucket: %v", err)
	}
	return h
}

func TestQuerierSelectByName(t *testing.T) {
	h := buildQueryHead(t)
	q, err := h.Querier(math.MinInt64, math.MaxInt64)
	if err != nil {
		t.Fatalf("Querier: %v", err)
	}
	defer q.Close()

	m := labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "up")
	ss := q.Select(context.Background(), false, nil, m)

	var gotLabels []labels.Labels
	for ss.Next() {
		gotLabels = append(gotLabels, ss.At().Labels())
	}
	if err := ss.Err(); err != nil {
		t.Fatalf("SeriesSet.Err(): %v", err)
	}
	if len(gotLabels) != 2 {
		t.Fatalf("Select(__name__=\"up\") returned %d series, want 2", len(gotLabels))
	}
	for _, l := range gotLabels {
		if l.Get(labels.MetricName) != "up" {
			t.Fatalf("selected series has __name__=%q, want \"up\"", l.Get(labels.MetricName))
		}
	}
}

// TestQuerierSelectSortSeries builds series with __name__ in descending alphabetical
// order (so creation order and label-sorted order disagree), then checks
// sortSeries=false preserves creation order while sortSeries=true produces strictly
// increasing labels.Compare order.
func TestQuerierSelectSortSeries(t *testing.T) {
	h := NewHead(4, 2, 8)
	tgt := TargetLabels{Cluster: "c", Namespace: "n", Pod: "p", Container: "co", Node: "no", Job: "j"}
	for _, name := range []string{"charlie", "bravo", "alpha"} {
		if _, err := h.GetOrCreateSeries(tgt, name, "", ""); err != nil {
			t.Fatalf("GetOrCreateSeries(%q): %v", name, err)
		}
	}

	q, err := h.Querier(math.MinInt64, math.MaxInt64)
	if err != nil {
		t.Fatalf("Querier: %v", err)
	}
	defer q.Close()

	unsorted := collectLabels(t, q.Select(context.Background(), false, nil))
	if got := unsorted[0].Get(labels.MetricName); got != "charlie" {
		t.Fatalf("sortSeries=false __name__[0] = %q, want \"charlie\" (creation order)", got)
	}

	sorted := collectLabels(t, q.Select(context.Background(), true, nil))
	want := []string{"alpha", "bravo", "charlie"}
	for i, l := range sorted {
		if got := l.Get(labels.MetricName); got != want[i] {
			t.Fatalf("sortSeries=true __name__[%d] = %q, want %q", i, got, want[i])
		}
	}
	for i := 1; i < len(sorted); i++ {
		if labels.Compare(sorted[i-1], sorted[i]) >= 0 {
			t.Fatalf("sortSeries=true output not strictly increasing at %d: %v then %v", i, sorted[i-1], sorted[i])
		}
	}

	assertSortedSeriesWriteToRealIndex(t, sorted)
}

func collectLabels(t *testing.T, ss storage.SeriesSet) []labels.Labels {
	t.Helper()
	var out []labels.Labels
	for ss.Next() {
		out = append(out, ss.At().Labels())
	}
	if err := ss.Err(); err != nil {
		t.Fatalf("SeriesSet.Err(): %v", err)
	}
	return out
}

// assertSortedSeriesWriteToRealIndex is the decisive check: feed lset directly into
// Prometheus's own index.Writer.AddSeries, which errors on any pair not in strict
// labels.Compare order (see index.go's "out-of-order series added" check) - proving
// sortRefsByLabels produces an order real Prometheus code actually accepts, not just
// an order that happens to look right from the inside.
func assertSortedSeriesWriteToRealIndex(t *testing.T, series []labels.Labels) {
	t.Helper()
	seen := make(map[string]struct{})
	for _, l := range series {
		l.Range(func(lb labels.Label) {
			seen[lb.Name] = struct{}{}
			seen[lb.Value] = struct{}{}
		})
	}
	symbols := sortedKeys(seen)

	w, err := index.NewWriter(context.Background(), filepath.Join(t.TempDir(), "index"))
	if err != nil {
		t.Fatalf("index.NewWriter: %v", err)
	}
	defer w.Close()

	for _, sym := range symbols {
		if err := w.AddSymbol(sym); err != nil {
			t.Fatalf("AddSymbol(%q): %v", sym, err)
		}
	}
	for i, l := range series {
		if err := w.AddSeries(storage.SeriesRef(i), l); err != nil {
			t.Fatalf("AddSeries(%v): %v", l, err)
		}
	}
}

func TestQuerierSelectByExtraLabel(t *testing.T) {
	h := buildQueryHead(t)
	q, err := h.Querier(math.MinInt64, math.MaxInt64)
	if err != nil {
		t.Fatalf("Querier: %v", err)
	}
	defer q.Close()

	m := labels.MustNewMatcher(labels.MatchEqual, "le", "0.1")
	ss := q.Select(context.Background(), false, nil, m)
	count := 0
	for ss.Next() {
		l := ss.At().Labels()
		if l.Get("le") != "0.1" {
			t.Fatalf("selected series has le=%q, want \"0.1\"", l.Get("le"))
		}
		count++
	}
	if count != 1 {
		t.Fatalf("Select(le=\"0.1\") returned %d series, want 1", count)
	}
}

func TestQuerierSelectReadsRealSamples(t *testing.T) {
	h := buildQueryHead(t)
	q, err := h.Querier(math.MinInt64, math.MaxInt64)
	if err != nil {
		t.Fatalf("Querier: %v", err)
	}
	defer q.Close()

	m := labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "up")
	m2 := labels.MustNewMatcher(labels.MatchEqual, "cluster", "c1")
	ss := q.Select(context.Background(), false, nil, m, m2)
	if !ss.Next() {
		t.Fatal("Select(__name__=up, cluster=c1) returned no series")
	}
	it := ss.At().Iterator(nil)

	var got []sample
	for it.Next() == chunkenc.ValFloat {
		ts, v := it.At()
		got = append(got, sample{ts, v})
	}
	if err := it.Err(); err != nil {
		t.Fatalf("iterator Err(): %v", err)
	}
	assertSamplesEqual(t, got, []sample{{1700000000000, 1}, {1700000015000, 0}})
	if ss.Next() {
		t.Fatal("expected exactly one series matching __name__=up, cluster=c1")
	}
}

func TestQuerierTimeRangeFiltering(t *testing.T) {
	h := NewHead(1, 1, 1)
	app := h.Appender(context.Background())
	l := labels.FromStrings(
		labels.MetricName, "up",
		"cluster", "c", "namespace", "n", "pod", "p", "container", "co", "node", "no", "job", "j",
	)
	ts := []int64{1700000000000, 1700000015000, 1700000030000, 1700000045000, 1700000060000}
	var ref storage.SeriesRef
	for _, t0 := range ts {
		var err error
		ref, err = app.Append(ref, l, t0, 1)
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	cases := []struct {
		name           string
		mint, maxt     int64
		wantTimestamps []int64
	}{
		{"full range", math.MinInt64, math.MaxInt64, ts},
		{"exact single point", 1700000030000, 1700000030000, []int64{1700000030000}},
		{"middle window", 1700000015000, 1700000045000, ts[1:4]},
		{"before all samples", 1600000000000, 1600000001000, nil},
		{"after all samples", 1800000000000, 1800000001000, nil},
		{"inclusive lower bound", 1700000015000, math.MaxInt64, ts[1:]},
		{"inclusive upper bound", math.MinInt64, 1700000045000, ts[:4]},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			q, err := h.Querier(c.mint, c.maxt)
			if err != nil {
				t.Fatalf("Querier: %v", err)
			}
			defer q.Close()
			ss := q.Select(context.Background(), false, nil, labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "up"))
			var got []int64
			if ss.Next() {
				it := ss.At().Iterator(nil)
				for it.Next() == chunkenc.ValFloat {
					gotTS, _ := it.At()
					got = append(got, gotTS)
				}
			}
			if !int64SliceEqual(got, c.wantTimestamps) {
				t.Fatalf("mint=%d maxt=%d: got timestamps %v, want %v", c.mint, c.maxt, got, c.wantTimestamps)
			}
		})
	}
}

func TestQuerierLabelValuesAndNames(t *testing.T) {
	h := buildQueryHead(t)
	q, err := h.Querier(math.MinInt64, math.MaxInt64)
	if err != nil {
		t.Fatalf("Querier: %v", err)
	}
	defer q.Close()

	names, _, err := q.LabelValues(context.Background(), labels.MetricName, nil)
	if err != nil {
		t.Fatalf("LabelValues: %v", err)
	}
	wantNames := []string{"request_duration_bucket", "up"}
	if !stringSliceEqual(names, wantNames) {
		t.Fatalf("LabelValues(__name__) = %v, want %v", names, wantNames)
	}

	allNames, _, err := q.LabelNames(context.Background(), nil)
	if err != nil {
		t.Fatalf("LabelNames: %v", err)
	}
	wantAllNames := []string{"__name__", "cluster", "container", "job", "le", "namespace", "node", "pod"}
	if !stringSliceEqual(allNames, wantAllNames) {
		t.Fatalf("LabelNames() = %v, want %v", allNames, wantAllNames)
	}
}

func stringSliceEqual(a, b []string) bool {
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

func TestQuerierSelectHistogramSeries(t *testing.T) {
	h := NewHead(1, 1, 1)
	app := h.Appender(context.Background())
	l := labels.FromStrings(
		labels.MetricName, "request_latency",
		"cluster", "c", "namespace", "n", "pod", "p", "container", "co", "node", "no", "job", "j",
	)
	hist := &histogram.Histogram{
		Schema: 0, Count: 5, Sum: 12.5,
		PositiveSpans: []histogram.Span{{Offset: 0, Length: 2}}, PositiveBuckets: []int64{2, 1},
	}
	if _, err := app.AppendHistogram(0, l, 1700000000000, hist, nil); err != nil {
		t.Fatalf("AppendHistogram: %v", err)
	}

	q, err := h.Querier(math.MinInt64, math.MaxInt64)
	if err != nil {
		t.Fatalf("Querier: %v", err)
	}
	defer q.Close()

	ss := q.Select(context.Background(), false, nil, labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "request_latency"))
	if !ss.Next() {
		t.Fatal("Select found no histogram series")
	}
	it := ss.At().Iterator(nil)
	vt := it.Next()
	if vt != chunkenc.ValHistogram {
		t.Fatalf("Next() = %v, want ValHistogram", vt)
	}
	gotTS, gotH := it.AtHistogram(nil)
	if gotTS != 1700000000000 {
		t.Fatalf("ts = %d, want 1700000000000", gotTS)
	}
	histEqual(t, gotH, hist)

	// At() (the float accessor) must panic on a histogram iterator, matching real
	// Prometheus's own histogramIterator precedent.
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("At() on a histogram iterator should panic, matching real Prometheus precedent")
			}
		}()
		it.At()
	}()

	_, gotFH := it.AtFloatHistogram(nil)
	if gotFH.Count != float64(hist.Count) || gotFH.Sum != hist.Sum {
		t.Fatalf("AtFloatHistogram() = %+v, want Count=%v Sum=%v", gotFH, hist.Count, hist.Sum)
	}
}

func TestFloatIteratorPanicsOnAtHistogram(t *testing.T) {
	h := NewHead(1, 1, 1)
	app := h.Appender(context.Background())
	l := labels.FromStrings(
		labels.MetricName, "up",
		"cluster", "c", "namespace", "n", "pod", "p", "container", "co", "node", "no", "job", "j",
	)
	if _, err := app.Append(0, l, 1700000000000, 1); err != nil {
		t.Fatalf("Append: %v", err)
	}
	q, err := h.Querier(math.MinInt64, math.MaxInt64)
	if err != nil {
		t.Fatalf("Querier: %v", err)
	}
	defer q.Close()
	ss := q.Select(context.Background(), false, nil)
	if !ss.Next() {
		t.Fatal("Select found no series")
	}
	it := ss.At().Iterator(nil)
	if it.Next() != chunkenc.ValFloat {
		t.Fatal("Next() != ValFloat")
	}
	defer func() {
		if recover() == nil {
			t.Fatal("AtHistogram() on a float iterator should panic, matching real Prometheus precedent")
		}
	}()
	it.AtHistogram(nil)
}
