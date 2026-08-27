package columnarhead

import (
	"context"
	"fmt"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/storage"
)

func TestAppenderRoundTrip(t *testing.T) {
	h := NewHead(1, 1, 1)
	app := h.Appender(context.Background())

	l := labels.FromStrings(
		labels.MetricName, "up",
		"cluster", "eks-prod-1",
		"namespace", "ns-7",
		"pod", "payments-api-abc123",
		"container", "app",
		"node", "ip-10-1-2-3",
		"job", "cadvisor",
	)

	ref, err := app.Append(0, l, 1700000000000, 1)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if ref == 0 {
		t.Fatal("Append returned ref 0 for a real series")
	}
	ref2, err := app.Append(ref, l, 1700000015000, 0)
	if err != nil {
		t.Fatalf("Append (second sample): %v", err)
	}
	if ref2 != ref {
		t.Fatalf("second Append on the same series returned a different ref: %d vs %d", ref2, ref)
	}
	if err := app.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// ref is external (storage.SeriesRef, 1-based - 0 is reserved); Head.Iterator
	// takes an internal 0-based ref, so translate back.
	it := h.Iterator(uint32(ref) - 1)
	var got []sample
	for it.Next() {
		ts, v := it.At()
		got = append(got, sample{ts, v})
	}
	assertSamplesEqual(t, got, []sample{{1700000000000, 1}, {1700000015000, 0}})
}

// TestAppenderRefFastPath confirms a non-zero ref from a prior Append skips label
// resolution entirely (the actual point of accepting ref, not just conformance) - and
// that a stale/bogus ref falls back to full resolution instead of panicking on an
// out-of-range SeriesStore index.
func TestAppenderRefFastPath(t *testing.T) {
	h := NewHead(1, 1, 1)
	app := h.Appender(context.Background())

	l := labels.FromStrings(
		labels.MetricName, "up",
		"cluster", "c", "namespace", "n", "pod", "p", "container", "co", "node", "no", "job", "j",
	)
	ref, err := app.Append(0, l, 1700000000000, 1)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	// A ref-only append with garbage labels must still land on the right series - if
	// this fell through to label resolution, splitLabels would reject the garbage
	// labels and this would error instead of succeeding.
	if _, err := app.Append(ref, labels.EmptyLabels(), 1700000015000, 2); err != nil {
		t.Fatalf("ref-based Append with no labels: %v", err)
	}

	it := h.Iterator(uint32(ref) - 1)
	var got []sample
	for it.Next() {
		ts, v := it.At()
		got = append(got, sample{ts, v})
	}
	assertSamplesEqual(t, got, []sample{{1700000000000, 1}, {1700000015000, 2}})

	// A stale ref (e.g. from an emptied Head) must fall back to full resolution, not
	// panic on an out-of-range SeriesStore index.
	fresh := NewHead(1, 1, 1)
	freshApp := fresh.Appender(context.Background())
	staleRef, err := freshApp.Append(ref, l, 1700000000000, 9)
	if err != nil {
		t.Fatalf("Append with a ref from a different Head: %v", err)
	}
	if staleRef == 0 {
		t.Fatal("fallback resolution returned ref 0 for a real series")
	}
}

func TestAppenderDedupesAcrossCalls(t *testing.T) {
	h := NewHead(2, 2, 2)
	app := h.Appender(context.Background())

	l := labels.FromStrings(
		labels.MetricName, "cpu_seconds_total",
		"cluster", "c", "namespace", "n", "pod", "p", "container", "co", "node", "no", "job", "j",
	)
	ref1, err := app.Append(0, l, 1700000000000, 1)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	ref2, err := app.Append(0, l, 1700000015000, 2)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if ref1 != ref2 {
		t.Fatalf("identical labels got different refs across separate Append(0, ...) calls: %d, %d", ref1, ref2)
	}
	if h.NumSeries() != 1 {
		t.Fatalf("NumSeries() = %d, want 1", h.NumSeries())
	}
}

func TestAppenderWithLocalLabel(t *testing.T) {
	h := NewHead(1, 1, 1)
	app := h.Appender(context.Background())

	base := []string{
		labels.MetricName, "request_duration_bucket",
		"cluster", "c", "namespace", "n", "pod", "p", "container", "co", "node", "no", "job", "j",
	}
	l1 := labels.FromStrings(append(append([]string{}, base...), "le", "0.1")...)
	l2 := labels.FromStrings(append(append([]string{}, base...), "le", "0.5")...)

	ref1, err := app.Append(0, l1, 1700000000000, 3)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	ref2, err := app.Append(0, l2, 1700000000000, 7)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if ref1 == ref2 {
		t.Fatal("different le values got the same series ref")
	}
}

// TestAppenderRejectsUnsupportedShape covers what's STILL unsupported after
// variable-length local labels replaced the old "at most one extra label" cap
// (see CHECKLIST.md) - missing __name__, and more than 255 extra labels
// (SeriesStore.localCount's uint8 width). Two extra labels, rejected before this
// change, is now covered by TestAppenderAcceptsMultipleExtraLabels instead - a
// shape this test used to reject that now belongs in an "accepts" test, not
// lingering here as a stale case.
func TestAppenderRejectsUnsupportedShape(t *testing.T) {
	h := NewHead(1, 1, 1)
	app := h.Appender(context.Background())

	tooMany := []labels.Label{{Name: labels.MetricName, Value: "m"}}
	for i := 0; i < 256; i++ {
		tooMany = append(tooMany, labels.Label{Name: fmt.Sprintf("extra%d", i), Value: "v"})
	}

	cases := map[string]labels.Labels{
		"no __name__":             labels.FromStrings("cluster", "c"),
		"256 extra labels (>255)": labels.New(tooMany...),
	}
	for name, l := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := app.Append(0, l, 1700000000000, 1); err != ErrUnsupportedLabelShape {
				t.Fatalf("Append(%v) = %v, want ErrUnsupportedLabelShape", l, err)
			}
		})
	}
}

// TestAppenderAcceptsMultipleExtraLabels is the real-world shape variable-length
// local labels exist for (CHECKLIST.md): more than one non-target extra label -
// rejected outright before this change, ErrUnsupportedLabelShape regardless of
// how few labels beyond one.
func TestAppenderAcceptsMultipleExtraLabels(t *testing.T) {
	h := NewHead(1, 1, 1)
	app := h.Appender(context.Background())

	l := labels.FromStrings(
		labels.MetricName, "testhistogram_bucket",
		"le", "0.1", "start", "positive",
	)
	ref, err := app.Append(0, l, 1700000000000, 1)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	internalRef, ok := toInternalRef(ref, h.NumSeries())
	if !ok {
		t.Fatalf("toInternalRef(%d) failed", ref)
	}
	if got := h.SeriesLabels(internalRef); !labels.Equal(got, l) {
		t.Fatalf("SeriesLabels = %v, want %v", got, l)
	}
}

// TestAppenderAcceptsInstanceAsTargetLabel is the real-world shape instance's
// addition to the target set exists for (see CHECKLIST.md's label-shape
// scope-limit entry, and targetFields' doc comment in target.go): job + instance +
// one more label is the standard Prometheus scrape-target shape (instance is
// attached to every scraped series automatically, by every Prometheus scrape
// config) - before instance joined the six original target labels, this exact
// shape had two non-target extra labels (instance, group) and was rejected.
func TestAppenderAcceptsInstanceAsTargetLabel(t *testing.T) {
	h := NewHead(1, 1, 1)
	app := h.Appender(context.Background())

	l := labels.FromStrings(
		labels.MetricName, "http_requests",
		"job", "api-server", "instance", "0", "group", "production",
	)
	ref, err := app.Append(0, l, 1700000000000, 1)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	internalRef, ok := toInternalRef(ref, h.NumSeries())
	if !ok {
		t.Fatalf("toInternalRef(%d) failed", ref)
	}
	if got := h.SeriesLabels(internalRef); !labels.Equal(got, l) {
		t.Fatalf("SeriesLabels = %v, want %v", got, l)
	}
}

// TestAppenderRejectsDuplicateLabelName is CHECKLIST.md's port of real
// Prometheus's own TestAddDuplicateLabelName (tsdb/head_test.go) - found
// missing entirely, not just untested: before ErrDuplicateLabelName existed,
// this exact shape silently corrupted the stored series (both same-named
// labels landed as separate entries) instead of being rejected.
func TestAppenderRejectsDuplicateLabelName(t *testing.T) {
	h := NewHead(1, 1, 1)
	app := h.Appender(context.Background())

	cases := map[string]labels.Labels{
		"name repeated, different values": labels.FromStrings(
			labels.MetricName, "up", "a", "c", "a", "b",
		),
		"name repeated, same value": labels.FromStrings(
			labels.MetricName, "up", "a", "c", "a", "c",
		),
		"extra label repeated (le)": labels.FromStrings(
			labels.MetricName, "up", "job", "prometheus", "le", "500", "le", "400",
		),
	}
	for name, l := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := app.Append(0, l, 0, 0); err != ErrDuplicateLabelName {
				t.Fatalf("Append(%v) = %v, want ErrDuplicateLabelName", l, err)
			}
		})
	}
}

func TestAppenderGetRef(t *testing.T) {
	h := NewHead(1, 1, 1)
	app := h.Appender(context.Background())

	l := labels.FromStrings(
		labels.MetricName, "up",
		"cluster", "c", "namespace", "n", "pod", "p", "container", "co", "node", "no", "job", "j",
	)

	getRef, ok := app.(storage.GetRef)
	if !ok {
		t.Fatal("headAppender does not implement storage.GetRef")
	}

	// Before the series exists, GetRef must report unknown, not fabricate a ref.
	if ref, gotL := getRef.GetRef(l, 0); ref != 0 || gotL.Len() != 0 {
		t.Fatalf("GetRef before creation = (%d, %v), want (0, empty)", ref, gotL)
	}

	created, err := app.Append(0, l, 1700000000000, 1)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	if ref, gotL := getRef.GetRef(l, 0); ref != created || gotL.Len() == 0 {
		t.Fatalf("GetRef after creation = (%d, %v), want (%d, non-empty)", ref, gotL, created)
	}
}

func TestAppenderUnimplementedMethodsFailLoudly(t *testing.T) {
	h := NewHead(1, 1, 1)
	app := h.Appender(context.Background())
	l := labels.EmptyLabels()

	if _, err := app.AppendHistogramSTZeroSample(0, l, 0, 0, nil, nil); err != ErrNotImplemented {
		t.Errorf("AppendHistogramSTZeroSample = %v, want ErrNotImplemented", err)
	}
}

func TestAppenderHistogram(t *testing.T) {
	h := NewHead(1, 1, 1)
	app := h.Appender(context.Background())
	l := labels.FromStrings(
		labels.MetricName, "request_duration_seconds",
		"cluster", "c", "namespace", "n", "pod", "p", "container", "co", "node", "no", "job", "j",
	)

	hist := &histogram.Histogram{
		Schema:          0,
		ZeroThreshold:   0.001,
		ZeroCount:       2,
		Count:           10,
		Sum:             42.5,
		PositiveSpans:   []histogram.Span{{Offset: 0, Length: 2}},
		PositiveBuckets: []int64{3, 1},
	}
	ref, err := app.AppendHistogram(0, l, 1700000000000, hist, nil)
	if err != nil {
		t.Fatalf("AppendHistogram: %v", err)
	}
	if ref == 0 {
		t.Fatal("AppendHistogram returned ref 0 for a real series")
	}

	it := h.HistogramIterator(uint32(ref) - 1)
	if !it.Next() {
		t.Fatal("HistogramIterator.Next() = false")
	}
	gotTS, gotH := it.At()
	if gotTS != 1700000000000 {
		t.Fatalf("ts = %d, want 1700000000000", gotTS)
	}
	histEqual(t, gotH, hist)

	// A series can't switch between Histogram and FloatHistogram samples
	// mid-stream - failed loudly (ErrHistogramTypeChanged), not silently
	// mishandled. See TestAppenderFloatHistogram for genuine FloatHistogram
	// support on its own series.
	if _, err := app.AppendHistogram(0, l, 1700000015000, nil, &histogram.FloatHistogram{}); err != ErrHistogramTypeChanged {
		t.Fatalf("AppendHistogram(FloatHistogram) on an existing int-histogram series = %v, want ErrHistogramTypeChanged", err)
	}

	// The ref fast path must also work.
	hist2 := &histogram.Histogram{
		Schema: 0, ZeroThreshold: 0.001, ZeroCount: 2, Count: 12, Sum: 50,
		PositiveSpans: hist.PositiveSpans, PositiveBuckets: []int64{4, 0},
	}
	if _, err := app.AppendHistogram(ref, labels.EmptyLabels(), 1700000030000, hist2, nil); err != nil {
		t.Fatalf("AppendHistogram via ref fast path: %v", err)
	}
	// HistogramIterator is a fixed snapshot at creation time (same established
	// pattern as SeriesStore.Iterator) - a fresh call is needed to see the new sample.
	it2 := h.HistogramIterator(uint32(ref) - 1)
	if !it2.Next() {
		t.Fatal("fresh HistogramIterator should have a first sample")
	}
	if !it2.Next() {
		t.Fatal("fresh HistogramIterator should have a second sample")
	}
	_, gotH2 := it2.At()
	histEqual(t, gotH2, hist2)
}

func TestAppenderFloatHistogram(t *testing.T) {
	h := NewHead(1, 1, 1)
	app := h.Appender(context.Background())
	l := labels.FromStrings(
		labels.MetricName, "request_duration_seconds",
		"cluster", "c", "namespace", "n", "pod", "p", "container", "co", "node", "no", "job", "j",
	)

	fh := &histogram.FloatHistogram{
		Schema:          0,
		ZeroThreshold:   0.001,
		ZeroCount:       2.5,
		Count:           10.5,
		Sum:             42.5,
		PositiveSpans:   []histogram.Span{{Offset: 0, Length: 2}},
		PositiveBuckets: []float64{3, 4}, // absolute, unlike Histogram's delta-encoded buckets
	}
	ref, err := app.AppendHistogram(0, l, 1700000000000, nil, fh)
	if err != nil {
		t.Fatalf("AppendHistogram(FloatHistogram): %v", err)
	}
	if ref == 0 {
		t.Fatal("AppendHistogram returned ref 0 for a real series")
	}

	if !h.HasFloatHistogram(uint32(ref) - 1) {
		t.Fatal("HasFloatHistogram = false for a series that only ever received FloatHistogram samples")
	}

	it := h.HistogramIterator(uint32(ref) - 1)
	if !it.Next() {
		t.Fatal("HistogramIterator.Next() = false")
	}
	gotTS, gotFH := it.AtFloat()
	if gotTS != 1700000000000 {
		t.Fatalf("ts = %d, want 1700000000000", gotTS)
	}
	floatHistEqual(t, gotFH, fh)

	// A series that only ever received int Histogram samples must reject a
	// FloatHistogram, the mirror image of TestAppenderHistogram's check.
	l2 := labels.FromStrings(
		labels.MetricName, "other_duration_seconds",
		"cluster", "c", "namespace", "n", "pod", "p", "container", "co", "node", "no", "job", "j",
	)
	if _, err := app.AppendHistogram(0, l2, 1700000000000, &histogram.Histogram{PositiveSpans: []histogram.Span{{Offset: 0, Length: 1}}, PositiveBuckets: []int64{1}}, nil); err != nil {
		t.Fatalf("AppendHistogram(int) on a fresh series: %v", err)
	}
	if _, err := app.AppendHistogram(0, l2, 1700000015000, nil, fh); err != ErrHistogramTypeChanged {
		t.Fatalf("AppendHistogram(FloatHistogram) on an existing int-histogram series = %v, want ErrHistogramTypeChanged", err)
	}

	// The ref fast path must also work.
	fh2 := &histogram.FloatHistogram{
		Schema: 0, ZeroThreshold: 0.001, ZeroCount: 3, Count: 15, Sum: 50,
		PositiveSpans: fh.PositiveSpans, PositiveBuckets: []float64{4, 5},
	}
	if _, err := app.AppendHistogram(ref, labels.EmptyLabels(), 1700000030000, nil, fh2); err != nil {
		t.Fatalf("AppendHistogram(FloatHistogram) via ref fast path: %v", err)
	}
	it2 := h.HistogramIterator(uint32(ref) - 1)
	if !it2.Next() {
		t.Fatal("fresh HistogramIterator should have a first sample")
	}
	if !it2.Next() {
		t.Fatal("fresh HistogramIterator should have a second sample")
	}
	_, gotFH2 := it2.AtFloat()
	floatHistEqual(t, gotFH2, fh2)
}

func TestAppenderExemplar(t *testing.T) {
	h := NewHead(1, 1, 1)
	app := h.Appender(context.Background())
	l := labels.FromStrings(
		labels.MetricName, "requests_total",
		"cluster", "c", "namespace", "n", "pod", "p", "container", "co", "node", "no", "job", "j",
	)

	// Exemplars must NOT create a series - same posture as UpdateMetadata.
	if _, err := app.AppendExemplar(0, l, exemplar.Exemplar{Value: 1, Ts: 1700000000000}); err != ErrSeriesNotFound {
		t.Fatalf("AppendExemplar on an unknown series = %v, want ErrSeriesNotFound", err)
	}
	if h.NumSeries() != 0 {
		t.Fatalf("NumSeries() = %d after AppendExemplar on an unknown series, want 0", h.NumSeries())
	}

	ref, err := app.Append(0, l, 1700000000000, 1)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	traceLabels := labels.FromStrings("trace_id", "abc123")
	want := exemplar.Exemplar{Labels: traceLabels, Value: 42.5, Ts: 1700000000000, HasTs: true}
	gotRef, err := app.AppendExemplar(0, l, want)
	if err != nil {
		t.Fatalf("AppendExemplar: %v", err)
	}
	if gotRef != ref {
		t.Fatalf("AppendExemplar returned ref %d, want %d", gotRef, ref)
	}

	got := h.Exemplars(uint32(ref) - 1)
	if len(got) != 1 {
		t.Fatalf("Exemplars(ref) has %d entries, want 1", len(got))
	}
	if got[0].value != want.Value || got[0].ts != want.Ts || got[0].labels["trace_id"] != "abc123" {
		t.Fatalf("Exemplars(ref)[0] = %+v, want value=%v ts=%v trace_id=abc123", got[0], want.Value, want.Ts)
	}

	// The ref fast path must also work.
	second := exemplar.Exemplar{Value: 99, Ts: 1700000015000}
	if _, err := app.AppendExemplar(ref, labels.EmptyLabels(), second); err != nil {
		t.Fatalf("AppendExemplar via ref fast path: %v", err)
	}
	got = h.Exemplars(uint32(ref) - 1)
	if len(got) != 2 {
		t.Fatalf("Exemplars(ref) has %d entries after a second append, want 2", len(got))
	}
}

// TestAppenderExemplarBeforeSeriesStart ports real Prometheus's TestHeadExemplars:
// an exemplar timestamped well before the series' own sample timestamps is valid -
// histogram buckets that haven't updated in a while can still export exemplars from
// an hour ago - and must not be rejected.
func TestAppenderExemplarBeforeSeriesStart(t *testing.T) {
	h := NewHead(1, 1, 1)
	app := h.Appender(context.Background())
	l := labels.FromStrings(
		labels.MetricName, "requests_total",
		"cluster", "c", "namespace", "n", "pod", "p", "container", "co", "node", "no", "job", "j",
	)
	ref, err := app.Append(0, l, 100, 100)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	traceLabels := labels.FromStrings("trace_id", "123")
	old := exemplar.Exemplar{Labels: traceLabels, Value: 1, Ts: -1000, HasTs: true}
	if _, err := app.AppendExemplar(ref, l, old); err != nil {
		t.Fatalf("AppendExemplar with ts before series start: %v", err)
	}

	got := h.Exemplars(uint32(ref) - 1)
	if len(got) != 1 || got[0].ts != -1000 {
		t.Fatalf("Exemplars(ref) = %+v, want one entry with ts=-1000", got)
	}
}

// TestExemplarStorageRingWraps verifies the ring buffer actually overwrites the
// oldest entry when full, rather than growing unboundedly or silently dropping new
// writes - the core property that makes it a bounded, real implementation instead of
// just a slice with extra steps.
func TestExemplarStorageRingWraps(t *testing.T) {
	es := newExemplarStorage(3)
	for i := 0; i < 5; i++ {
		es.append(1, exemplar.Exemplar{Value: float64(i), Ts: int64(i)})
	}
	if es.Len() != 3 {
		t.Fatalf("Len() = %d, want 3 (capacity)", es.Len())
	}
	got := es.forSeries(1)
	if len(got) != 3 {
		t.Fatalf("forSeries(1) has %d entries, want 3", len(got))
	}
	// Entries 0 and 1 were overwritten; 2, 3, 4 should remain, oldest first.
	wantTS := []int64{2, 3, 4}
	for i, e := range got {
		if e.ts != wantTS[i] {
			t.Fatalf("forSeries(1)[%d].ts = %d, want %d", i, e.ts, wantTS[i])
		}
	}
}

func TestAppenderSTZeroSample(t *testing.T) {
	h := NewHead(1, 1, 1)
	app := h.Appender(context.Background())
	l := labels.FromStrings(
		labels.MetricName, "requests_total",
		"cluster", "c", "namespace", "n", "pod", "p", "container", "co", "node", "no", "job", "j",
	)

	// A collision with the incoming sample's own timestamp must be rejected -
	// storage.StartTimestampAppender's contract gives the real sample priority.
	if _, err := app.AppendSTZeroSample(0, l, 1700000015000, 1700000015000); err != ErrSTZeroSampleCollision {
		t.Fatalf("AppendSTZeroSample(st == t) = %v, want ErrSTZeroSampleCollision", err)
	}
	if _, err := app.AppendSTZeroSample(0, l, 1700000015000, 1700000020000); err != ErrSTZeroSampleCollision {
		t.Fatalf("AppendSTZeroSample(st > t) = %v, want ErrSTZeroSampleCollision", err)
	}

	// Normal case: ST strictly before the real sample, called first (as the contract
	// requires), creates the series.
	ref, err := app.AppendSTZeroSample(0, l, 1700000015000, 1700000000000)
	if err != nil {
		t.Fatalf("AppendSTZeroSample: %v", err)
	}
	if ref == 0 {
		t.Fatal("AppendSTZeroSample returned ref 0 for a real series")
	}
	if _, err := app.Append(ref, l, 1700000015000, 1); err != nil {
		t.Fatalf("Append (the real sample): %v", err)
	}

	it := h.Iterator(uint32(ref) - 1)
	var got []sample
	for it.Next() {
		ts, v := it.At()
		got = append(got, sample{ts, v})
	}
	assertSamplesEqual(t, got, []sample{{1700000000000, 0}, {1700000015000, 1}})

	// A stale/duplicate ST (not after the one already recorded) must be rejected.
	if _, err := app.AppendSTZeroSample(ref, l, 1700000030000, 1700000000000); err != ErrSTZeroSampleTooOld {
		t.Fatalf("AppendSTZeroSample with a repeated st = %v, want ErrSTZeroSampleTooOld", err)
	}
	if _, err := app.AppendSTZeroSample(ref, l, 1700000030000, 1699999999999); err != ErrSTZeroSampleTooOld {
		t.Fatalf("AppendSTZeroSample with an older st = %v, want ErrSTZeroSampleTooOld", err)
	}
}

func TestAppenderUpdateMetadata(t *testing.T) {
	h := NewHead(1, 1, 1)
	app := h.Appender(context.Background())
	l := labels.FromStrings(
		labels.MetricName, "up",
		"cluster", "c", "namespace", "n", "pod", "p", "container", "co", "node", "no", "job", "j",
	)

	// UpdateMetadata must NOT create a series - it's an error against an unknown one.
	if _, err := app.UpdateMetadata(0, l, metadata.Metadata{Type: model.MetricTypeGauge}); err != ErrSeriesNotFound {
		t.Fatalf("UpdateMetadata on an unknown series = %v, want ErrSeriesNotFound", err)
	}
	if h.NumSeries() != 0 {
		t.Fatalf("NumSeries() = %d after UpdateMetadata on an unknown series, want 0 (must not create)", h.NumSeries())
	}

	ref, err := app.Append(0, l, 1700000000000, 1)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	want := metadata.Metadata{Type: model.MetricTypeCounter, Unit: "seconds", Help: "cpu time"}
	gotRef, err := app.UpdateMetadata(0, l, want)
	if err != nil {
		t.Fatalf("UpdateMetadata: %v", err)
	}
	if gotRef != ref {
		t.Fatalf("UpdateMetadata returned ref %d, want %d", gotRef, ref)
	}
	got, ok := h.Metadata(uint32(ref) - 1)
	if !ok || got != want {
		t.Fatalf("Metadata(ref) = (%v, %v), want (%v, true)", got, ok, want)
	}

	// The ref fast path must also work, without re-resolving via labels.
	updated := metadata.Metadata{Type: model.MetricTypeCounter, Unit: "seconds", Help: "updated"}
	if _, err := app.UpdateMetadata(ref, labels.EmptyLabels(), updated); err != nil {
		t.Fatalf("UpdateMetadata via ref fast path: %v", err)
	}
	got, ok = h.Metadata(uint32(ref) - 1)
	if !ok || got != updated {
		t.Fatalf("Metadata(ref) after ref-based update = (%v, %v), want (%v, true)", got, ok, updated)
	}
}

func TestAppenderCommitRollbackAreNoOps(t *testing.T) {
	h := NewHead(1, 1, 1)
	app := h.Appender(context.Background())
	if err := app.Commit(); err != nil {
		t.Errorf("Commit: %v", err)
	}
	if err := app.Rollback(); err != nil {
		t.Errorf("Rollback: %v", err)
	}
	app.SetOptions(nil) // must not panic
}
