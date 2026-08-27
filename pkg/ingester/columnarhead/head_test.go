package columnarhead

import (
	"errors"
	"fmt"
	"runtime"
	"testing"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"
)

// mockLifecycleCallback records every PreCreation/PostCreation/PostDeletion call it
// sees, and can be configured to reject a specific metric name - just enough to test
// SetSeriesLifecycleCallback's contract without depending on Cortex's own
// userTSDB (a real, separate package).
type mockLifecycleCallback struct {
	reject      map[string]error
	preCalls    []labels.Labels
	postCalls   []labels.Labels
	deleteCalls []map[chunks.HeadSeriesRef]labels.Labels
}

func (m *mockLifecycleCallback) PreCreation(l labels.Labels) error {
	m.preCalls = append(m.preCalls, l)
	if err, ok := m.reject[l.Get(labels.MetricName)]; ok {
		return err
	}
	return nil
}

func (m *mockLifecycleCallback) PostCreation(l labels.Labels) {
	m.postCalls = append(m.postCalls, l)
}

func (m *mockLifecycleCallback) PostDeletion(deleted map[chunks.HeadSeriesRef]labels.Labels) {
	m.deleteCalls = append(m.deleteCalls, deleted)
}

// TestHeadSeriesLifecycleCallback is the decisive test for
// SetSeriesLifecycleCallback: PreCreation fires exactly once per genuinely new
// series (never on a dedup hit), a PreCreation error blocks creation entirely
// (no series, no ref consumed), and PostCreation fires exactly once per series
// actually created.
func TestHeadSeriesLifecycleCallback(t *testing.T) {
	h := NewHead(4, 1, 4)
	cb := &mockLifecycleCallback{reject: map[string]error{"blocked_metric": errors.New("rejected by limiter")}}
	h.SetSeriesLifecycleCallback(cb)

	tgt := TargetLabels{Cluster: "c", Namespace: "n", Pod: "p", Container: "co", Node: "no", Job: "j"}

	ref1, err := h.GetOrCreateSeries(tgt, "up")
	if err != nil {
		t.Fatalf("GetOrCreateSeries(up): %v", err)
	}
	if len(cb.preCalls) != 1 || len(cb.postCalls) != 1 {
		t.Fatalf("after first creation: preCalls=%d postCalls=%d, want 1/1", len(cb.preCalls), len(cb.postCalls))
	}

	// A dedup hit (same target+metric+no local label) must NOT fire either
	// callback again.
	ref1Again, err := h.GetOrCreateSeries(tgt, "up")
	if err != nil {
		t.Fatalf("GetOrCreateSeries(up) again: %v", err)
	}
	if ref1Again != ref1 {
		t.Fatalf("dedup broke: got ref %d, want %d", ref1Again, ref1)
	}
	if len(cb.preCalls) != 1 || len(cb.postCalls) != 1 {
		t.Fatalf("after dedup hit: preCalls=%d postCalls=%d, want unchanged 1/1", len(cb.preCalls), len(cb.postCalls))
	}

	// A rejected PreCreation must block creation entirely - no series created,
	// no PostCreation call, the error propagated verbatim.
	before := h.NumSeries()
	_, err = h.GetOrCreateSeries(tgt, "blocked_metric")
	if err == nil || err.Error() != "rejected by limiter" {
		t.Fatalf("GetOrCreateSeries(blocked_metric) = %v, want the PreCreation rejection error", err)
	}
	if h.NumSeries() != before {
		t.Fatalf("NumSeries() = %d after a rejected creation, want unchanged %d", h.NumSeries(), before)
	}
	if len(cb.postCalls) != 1 {
		t.Fatalf("PostCreation called %d times after a PreCreation rejection, want still 1 (unchanged)", len(cb.postCalls))
	}

	// A second, genuinely different series must fire both callbacks again,
	// with the correct reconstructed labels (see buildLabels).
	ref2, err := h.GetOrCreateSeries(tgt, "down")
	if err != nil {
		t.Fatalf("GetOrCreateSeries(down): %v", err)
	}
	if ref2 == ref1 {
		t.Fatal("down got the same ref as up - dedup incorrectly matched a different metric")
	}
	// preCalls is 3 here, not 2: PreCreation legitimately fired (and rejected)
	// for the blocked_metric attempt above too - that's the whole point of it
	// being able to reject. postCalls stays at 2 since that attempt never
	// completed creation.
	if len(cb.preCalls) != 3 || len(cb.postCalls) != 2 {
		t.Fatalf("after second creation: preCalls=%d postCalls=%d, want 3/2", len(cb.preCalls), len(cb.postCalls))
	}
	wantLbls := labels.FromStrings(labels.MetricName, "down", "cluster", "c", "namespace", "n", "pod", "p", "container", "co", "node", "no", "job", "j")
	if !labels.Equal(cb.preCalls[2], wantLbls) {
		t.Fatalf("PreCreation labels = %v, want %v", cb.preCalls[2], wantLbls)
	}
	if !labels.Equal(cb.postCalls[1], wantLbls) {
		t.Fatalf("PostCreation labels = %v, want %v", cb.postCalls[1], wantLbls)
	}
}

// TestHeadNoLifecycleCallbackIsSafeDefault confirms every existing caller
// (nothing sets a callback) is completely unaffected - the nil default.
func TestHeadNoLifecycleCallbackIsSafeDefault(t *testing.T) {
	h := NewHead(1, 1, 1)
	tgt := TargetLabels{Cluster: "c", Namespace: "n", Pod: "p", Container: "co", Node: "no", Job: "j"}
	if _, err := h.GetOrCreateSeries(tgt, "up"); err != nil {
		t.Fatalf("GetOrCreateSeries with no callback set: %v", err)
	}
}

// TestHeadTruncateRemovesEmptySeries is the decisive test for series removal
// (external review, "cardinality accounting only ever grows"): a series
// Truncate empties completely must become fully undiscoverable - gone from
// NumLiveSeries, gone from a name-based lookup, and PostDeletion must fire with
// its labels - not just have its arena bytes reclaimed while staying
// permanently "live" in every index.
func TestHeadTruncateRemovesEmptySeries(t *testing.T) {
	h := NewHead(1, 1, 1)
	cb := &mockLifecycleCallback{}
	h.SetSeriesLifecycleCallback(cb)
	tgt := TargetLabels{Cluster: "c", Namespace: "n", Pod: "p", Container: "co", Node: "no", Job: "j"}

	ref, err := h.GetOrCreateSeries(tgt, "up")
	if err != nil {
		t.Fatalf("GetOrCreateSeries: %v", err)
	}
	// PostCreation already fired once for this - reset so this test only
	// asserts on what happens around the deletion itself.
	cb.postCalls = nil

	if err := h.Append(ref, 1700000000000, 1); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if got := h.NumLiveSeries(); got != 1 {
		t.Fatalf("NumLiveSeries() before Truncate = %d, want 1", got)
	}

	h.Truncate(1700000000001) // ages out the only sample

	if got := h.NumLiveSeries(); got != 0 {
		t.Fatalf("NumLiveSeries() after Truncate = %d, want 0 (the series should be gone)", got)
	}
	if refs, ok := h.SeriesRefsForName("up"); ok && len(refs) != 0 {
		t.Fatalf("SeriesRefsForName(\"up\") after Truncate = %v, want empty or not-found", refs)
	}
	if len(cb.deleteCalls) != 1 {
		t.Fatalf("PostDeletion called %d times, want 1", len(cb.deleteCalls))
	}
	deleted := cb.deleteCalls[0]
	if len(deleted) != 1 {
		t.Fatalf("PostDeletion's map has %d entries, want 1", len(deleted))
	}
	wantLbls := labels.FromStrings(labels.MetricName, "up", "cluster", "c", "namespace", "n", "pod", "p", "container", "co", "node", "no", "job", "j")
	for _, got := range deleted {
		if !labels.Equal(got, wantLbls) {
			t.Fatalf("PostDeletion labels = %v, want %v", got, wantLbls)
		}
	}

	// The same target/metric reappearing must get a genuinely NEW ref, not
	// resurrect the deleted one - GetOrCreateSeries' dedup key is gone.
	newRef, err := h.GetOrCreateSeries(tgt, "up")
	if err != nil {
		t.Fatalf("GetOrCreateSeries after deletion: %v", err)
	}
	if newRef == ref {
		t.Fatalf("GetOrCreateSeries after deletion reused the old ref %d instead of allocating a new one", ref)
	}
	if got := h.NumLiveSeries(); got != 1 {
		t.Fatalf("NumLiveSeries() after re-creation = %d, want 1", got)
	}
}

// TestHeadTruncateKeepsPartiallyRetainedSeries confirms a series with SOME
// samples still inside the retained range is NOT removed - only a series with
// NOTHING left (float, histogram, and OOO all empty) qualifies.
func TestHeadTruncateKeepsPartiallyRetainedSeries(t *testing.T) {
	h := NewHead(1, 1, 1)
	cb := &mockLifecycleCallback{}
	h.SetSeriesLifecycleCallback(cb)
	tgt := TargetLabels{Cluster: "c", Namespace: "n", Pod: "p", Container: "co", Node: "no", Job: "j"}

	ref, err := h.GetOrCreateSeries(tgt, "up")
	if err != nil {
		t.Fatalf("GetOrCreateSeries: %v", err)
	}
	base := int64(1700000000000)
	if err := h.Append(ref, base, 1); err != nil {
		t.Fatalf("Append 0: %v", err)
	}
	if err := h.Append(ref, base+30000, 2); err != nil {
		t.Fatalf("Append 1: %v", err)
	}

	h.Truncate(base + 15000) // drops the first sample, keeps the second

	if got := h.NumLiveSeries(); got != 1 {
		t.Fatalf("NumLiveSeries() after partial Truncate = %d, want 1 (series has a retained sample)", got)
	}
	if len(cb.deleteCalls) != 0 {
		t.Fatalf("PostDeletion called %d times, want 0", len(cb.deleteCalls))
	}
	refs, ok := h.SeriesRefsForName("up")
	if !ok || len(refs) != 1 || refs[0] != ref {
		t.Fatalf("SeriesRefsForName(\"up\") = %v, %v, want [%d], true", refs, ok, ref)
	}
}

// TestHeadTruncateNoLifecycleCallbackIsSafeDefault confirms removal still
// happens with no callback set (the nil default) - PostDeletion is optional,
// the removal itself is not conditioned on having one.
func TestHeadTruncateNoLifecycleCallbackIsSafeDefault(t *testing.T) {
	h := NewHead(1, 1, 1)
	tgt := TargetLabels{Cluster: "c", Namespace: "n", Pod: "p", Container: "co", Node: "no", Job: "j"}
	ref, err := h.GetOrCreateSeries(tgt, "up")
	if err != nil {
		t.Fatalf("GetOrCreateSeries: %v", err)
	}
	if err := h.Append(ref, 1700000000000, 1); err != nil {
		t.Fatalf("Append: %v", err)
	}
	h.Truncate(1700000000001)
	if got := h.NumLiveSeries(); got != 0 {
		t.Fatalf("NumLiveSeries() = %d, want 0", got)
	}
}

// TestHeadNumSeriesStaysAllocatedCountAfterDeletion confirms NumSeries (the
// array-bounds count querier.go/appender.go rely on) is UNCHANGED by
// deletion - only NumLiveSeries shrinks. Getting this wrong would silently
// break full-scan queries/LabelValues/LabelNames for any ref numerically past
// a shrunk live count (see NumSeries' own doc comment).
func TestHeadNumSeriesStaysAllocatedCountAfterDeletion(t *testing.T) {
	h := NewHead(1, 1, 1)
	tgt := TargetLabels{Cluster: "c", Namespace: "n", Pod: "p", Container: "co", Node: "no", Job: "j"}
	ref, err := h.GetOrCreateSeries(tgt, "up")
	if err != nil {
		t.Fatalf("GetOrCreateSeries: %v", err)
	}
	if err := h.Append(ref, 1700000000000, 1); err != nil {
		t.Fatalf("Append: %v", err)
	}
	before := h.NumSeries()
	h.Truncate(1700000000001)
	if got := h.NumSeries(); got != before {
		t.Fatalf("NumSeries() after deletion = %d, want unchanged %d", got, before)
	}
	if got := h.NumLiveSeries(); got != 0 {
		t.Fatalf("NumLiveSeries() after deletion = %d, want 0", got)
	}
}

func TestHeadDedupesTargetsAndSeries(t *testing.T) {
	h := NewHead(10, 10, 10)
	tgt := TargetLabels{
		Cluster: "eks-prod-1", Namespace: "ns-7", Pod: "payments-api-abc123",
		Container: "app", Node: "ip-10-1-2-3", Job: "cadvisor",
	}

	ref1, err := h.GetOrCreateSeries(tgt, "cpu_seconds_total")
	if err != nil {
		t.Fatalf("GetOrCreateSeries: %v", err)
	}
	ref2, err := h.GetOrCreateSeries(tgt, "cpu_seconds_total")
	if err != nil {
		t.Fatalf("GetOrCreateSeries: %v", err)
	}
	if ref1 != ref2 {
		t.Fatalf("identical (target, metric, local) got different refs: %d, %d - not deduplicated", ref1, ref2)
	}
	if h.NumSeries() != 1 {
		t.Fatalf("NumSeries() = %d, want 1", h.NumSeries())
	}
	if h.NumTargets() != 1 {
		t.Fatalf("NumTargets() = %d, want 1", h.NumTargets())
	}

	// A different metric on the SAME target must share the target but get a new series.
	ref3, err := h.GetOrCreateSeries(tgt, "memory_bytes")
	if err != nil {
		t.Fatalf("GetOrCreateSeries: %v", err)
	}
	if ref3 == ref1 {
		t.Fatal("different metric names on the same target got the same series ref")
	}
	if h.NumTargets() != 1 {
		t.Fatalf("NumTargets() = %d after a second metric on the same target, want 1 (target unchanged)", h.NumTargets())
	}
	if h.NumSeries() != 2 {
		t.Fatalf("NumSeries() = %d, want 2", h.NumSeries())
	}

	// A histogram bucket (different localLabel) is a distinct series from the same
	// metric name with no local label.
	ref4, err := h.GetOrCreateSeries(tgt, "request_duration_bucket", labels.Label{Name: "le", Value: "0.1"})
	if err != nil {
		t.Fatalf("GetOrCreateSeries: %v", err)
	}
	ref5, err := h.GetOrCreateSeries(tgt, "request_duration_bucket", labels.Label{Name: "le", Value: "0.5"})
	if err != nil {
		t.Fatalf("GetOrCreateSeries: %v", err)
	}
	if ref4 == ref5 {
		t.Fatal("different le values got the same series ref")
	}

	// A different target (different pod) must get its own target record, even with
	// otherwise-identical metric/local-label.
	tgt2 := tgt
	tgt2.Pod = "payments-api-def456"
	ref6, err := h.GetOrCreateSeries(tgt2, "cpu_seconds_total")
	if err != nil {
		t.Fatalf("GetOrCreateSeries: %v", err)
	}
	if ref6 == ref1 {
		t.Fatal("different pods got the same series ref")
	}
	if h.NumTargets() != 2 {
		t.Fatalf("NumTargets() = %d after a second pod, want 2", h.NumTargets())
	}
}

// TestHeadSeriesLabels proves Head.SeriesLabels correctly reconstructs a series' full
// label set, including the exact bug this file's GetOrCreateSeries signature change
// fixed: two series with the same local-label VALUE but different NAMES (e.g. a
// histogram's le="0.1" vs a summary's quantile="0.1") must be distinct series with
// correctly distinguishable reconstructed labels - not merged via a shared value-only
// key, and not read back with the wrong label name.
func TestHeadSeriesLabels(t *testing.T) {
	h := NewHead(3, 1, 1)
	tgt := TargetLabels{
		Cluster: "c", Namespace: "n", Pod: "p", Container: "co", Node: "no", Job: "j",
	}

	noLocalRef, err := h.GetOrCreateSeries(tgt, "up")
	if err != nil {
		t.Fatalf("GetOrCreateSeries: %v", err)
	}
	leRef, err := h.GetOrCreateSeries(tgt, "request_duration_bucket", labels.Label{Name: "le", Value: "0.1"})
	if err != nil {
		t.Fatalf("GetOrCreateSeries: %v", err)
	}
	quantileRef, err := h.GetOrCreateSeries(tgt, "request_duration", labels.Label{Name: "quantile", Value: "0.1"})
	if err != nil {
		t.Fatalf("GetOrCreateSeries: %v", err)
	}
	if leRef == quantileRef {
		t.Fatal("le=\"0.1\" and quantile=\"0.1\" got the same series ref - the exact bug this test guards against")
	}

	want := labels.FromStrings(
		labels.MetricName, "up",
		"cluster", "c", "namespace", "n", "pod", "p", "container", "co", "node", "no", "job", "j",
	)
	if got := h.SeriesLabels(noLocalRef); !labels.Equal(got, want) {
		t.Fatalf("SeriesLabels(noLocalRef) = %v, want %v", got, want)
	}

	wantLE := labels.FromStrings(
		labels.MetricName, "request_duration_bucket", "le", "0.1",
		"cluster", "c", "namespace", "n", "pod", "p", "container", "co", "node", "no", "job", "j",
	)
	if got := h.SeriesLabels(leRef); !labels.Equal(got, wantLE) {
		t.Fatalf("SeriesLabels(leRef) = %v, want %v", got, wantLE)
	}

	wantQuantile := labels.FromStrings(
		labels.MetricName, "request_duration", "quantile", "0.1",
		"cluster", "c", "namespace", "n", "pod", "p", "container", "co", "node", "no", "job", "j",
	)
	if got := h.SeriesLabels(quantileRef); !labels.Equal(got, wantQuantile) {
		t.Fatalf("SeriesLabels(quantileRef) = %v, want %v", got, wantQuantile)
	}
}

// TestHeadSeriesLabelsOmitsEmptyTargetLabels is the decisive test for a real,
// previously latent bug (found via a genuinely minimal end-to-end push through
// the real ingester, CHECKLIST.md's Phase 7 step 5 notes): a target label never
// set on the original series (GetOrCreateSeries interns "" for it, same as any
// other string - there's no separate "absent" sentinel) must be OMITTED from
// SeriesLabels' reconstruction, not included as a real, present, empty-string
// label pair - real Prometheus label-set semantics treat the two as identical
// (Labels.Get returns "" either way, no matcher distinguishes them), and
// splitLabels (appender.go) already accepts a series with some or all target
// labels absent, so the read side must round-trip that faithfully.
func TestHeadSeriesLabelsOmitsEmptyTargetLabels(t *testing.T) {
	h := NewHead(2, 2, 2)

	noTargetRef, err := h.GetOrCreateSeries(TargetLabels{}, "up")
	if err != nil {
		t.Fatalf("GetOrCreateSeries (no target labels at all): %v", err)
	}
	want := labels.FromStrings(labels.MetricName, "up")
	if got := h.SeriesLabels(noTargetRef); !labels.Equal(got, want) {
		t.Fatalf("SeriesLabels(noTargetRef) = %v, want %v (no target labels present)", got, want)
	}
	// SeriesLabelValue must agree with SeriesLabels: "" for an absent target
	// label either way, not just at the full-label-set level.
	if v := h.SeriesLabelValue(noTargetRef, "cluster"); v != "" {
		t.Fatalf("SeriesLabelValue(noTargetRef, \"cluster\") = %q, want \"\"", v)
	}

	partialTarget := TargetLabels{Cluster: "c", Job: "j"} // namespace/pod/container/node all absent
	partialRef, err := h.GetOrCreateSeries(partialTarget, "down")
	if err != nil {
		t.Fatalf("GetOrCreateSeries (partial target labels): %v", err)
	}
	wantPartial := labels.FromStrings(labels.MetricName, "down", "cluster", "c", "job", "j")
	if got := h.SeriesLabels(partialRef); !labels.Equal(got, wantPartial) {
		t.Fatalf("SeriesLabels(partialRef) = %v, want %v (only cluster/job present)", got, wantPartial)
	}
}

func TestHeadAppendAndIterate(t *testing.T) {
	h := NewHead(1, 1, 1)
	tgt := TargetLabels{Cluster: "c", Namespace: "n", Pod: "p", Container: "co", Node: "no", Job: "j"}
	ref, err := h.GetOrCreateSeries(tgt, "up")
	if err != nil {
		t.Fatalf("GetOrCreateSeries: %v", err)
	}

	want := []sample{{1700000000000, 1}, {1700000015000, 1}, {1700000030000, 0}}
	for _, sm := range want {
		if err := h.Append(ref, sm.ts, sm.v); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	it := h.Iterator(ref)
	var got []sample
	for it.Next() {
		ts, v := it.At()
		got = append(got, sample{ts, v})
	}
	assertSamplesEqual(t, got, want)
}

// TestHeadRejectsHistogramBackwardsTimestamp is CHECKLIST.md's port of real
// Prometheus's TestHeadAppender_AppendFloatWithSameTimestampAsPreviousHistogram
// (tsdb/head_test.go), extended: found while porting that HistogramStore.Append
// had NO ordering check at all, not just the specific cross-type gap that test
// covers - confirmed via a direct repro (not assumed) that a backward timestamp
// was silently accepted, corrupting the stored, supposedly-monotonic delta-of-
// delta-encoded sequence.
func TestHeadRejectsHistogramBackwardsTimestamp(t *testing.T) {
	h := NewHead(1, 1, 1)
	tgt := TargetLabels{Cluster: "c", Namespace: "n", Pod: "p", Container: "co", Node: "no", Job: "j"}
	ref, err := h.GetOrCreateSeries(tgt, "request_latency")
	if err != nil {
		t.Fatalf("GetOrCreateSeries: %v", err)
	}

	hg1 := &histogram.Histogram{Schema: 0, Sum: 1, Count: 1}
	if err := h.AppendHistogram(ref, 2000, hg1); err != nil {
		t.Fatalf("AppendHistogram @2000: %v", err)
	}
	hg2 := &histogram.Histogram{Schema: 0, Sum: 2, Count: 2}
	if err := h.AppendHistogram(ref, 1000, hg2); err != storage.ErrOutOfOrderSample {
		t.Fatalf("AppendHistogram @1000 (backwards) = %v, want ErrOutOfOrderSample", err)
	}
	if err := h.AppendHistogram(ref, 2000, hg2); err != storage.ErrDuplicateSampleForTimestamp {
		t.Fatalf("AppendHistogram @2000 (duplicate ts) = %v, want ErrDuplicateSampleForTimestamp", err)
	}

	it := h.HistogramIterator(ref)
	n := 0
	for it.Next() {
		ts, hg := it.At()
		if ts != 2000 || hg.Sum != 1 || hg.Count != 1 {
			t.Fatalf("sample %d: ts=%d sum=%v count=%v, want the ORIGINAL @2000 sample unchanged - a rejected append must not corrupt what's already stored", n, ts, hg.Sum, hg.Count)
		}
		n++
	}
	if n != 1 {
		t.Fatalf("HistogramIterator returned %d samples, want exactly 1 (the rejected appends above must not have landed)", n)
	}
}

// TestHeadAcceptsExactDuplicateHistogramAsNoOp ports real Prometheus's own
// allowance in memSeries.appendableHistogram/appendableFloatHistogram: an
// exact-value duplicate at the same timestamp as the last sample is a silent
// no-op, not an error (federation/retries produce exact duplicates in valid,
// non-noteworthy cases) - only a DIFFERENT value at the same timestamp is
// storage.ErrDuplicateSampleForTimestamp (TestHeadRejectsHistogramBackwardsTimestamp
// above). Found missing while porting TestHeadAppendHistogramAndCommitConcurrency
// (concurrency_test.go): HistogramStore had no way to compare an incoming sample
// against the last stored one, so every same-timestamp append was rejected
// unconditionally regardless of value - fixed via HistogramStore.LastEquals/
// LastEqualsFloat, reusing the owning segment's own already-tracked last-sample
// state (histogram.go).
func TestHeadAcceptsExactDuplicateHistogramAsNoOp(t *testing.T) {
	tgt := TargetLabels{Cluster: "c", Namespace: "n", Pod: "p", Container: "co", Node: "no", Job: "j"}
	t.Run("integer histogram", func(t *testing.T) {
		h := NewHead(1, 1, 1)
		ref, err := h.GetOrCreateSeries(tgt, "request_latency")
		if err != nil {
			t.Fatalf("GetOrCreateSeries: %v", err)
		}
		hg := &histogram.Histogram{Schema: 0, Sum: 1, Count: 1}
		if err := h.AppendHistogram(ref, 2000, hg); err != nil {
			t.Fatalf("AppendHistogram @2000: %v", err)
		}
		// A fresh pointer with the identical value - equality must be by value, not
		// by pointer identity.
		dup := &histogram.Histogram{Schema: 0, Sum: 1, Count: 1}
		if err := h.AppendHistogram(ref, 2000, dup); err != nil {
			t.Fatalf("AppendHistogram @2000 (exact duplicate) = %v, want nil (silent no-op)", err)
		}
		it := h.HistogramIterator(ref)
		n := 0
		for it.Next() {
			n++
		}
		if n != 1 {
			t.Fatalf("HistogramIterator returned %d samples, want exactly 1 - the duplicate must not have been stored as a second sample", n)
		}
	})
	t.Run("float histogram", func(t *testing.T) {
		h := NewHead(1, 1, 1)
		ref, err := h.GetOrCreateSeries(tgt, "request_latency")
		if err != nil {
			t.Fatalf("GetOrCreateSeries: %v", err)
		}
		fh := &histogram.FloatHistogram{Schema: 0, Sum: 1, Count: 1}
		if err := h.AppendFloatHistogram(ref, 2000, fh); err != nil {
			t.Fatalf("AppendFloatHistogram @2000: %v", err)
		}
		dup := &histogram.FloatHistogram{Schema: 0, Sum: 1, Count: 1}
		if err := h.AppendFloatHistogram(ref, 2000, dup); err != nil {
			t.Fatalf("AppendFloatHistogram @2000 (exact duplicate) = %v, want nil (silent no-op)", err)
		}
		it := h.HistogramIterator(ref)
		n := 0
		for it.Next() {
			n++
		}
		if n != 1 {
			t.Fatalf("HistogramIterator returned %d samples, want exactly 1 - the duplicate must not have been stored as a second sample", n)
		}
	})
}

// TestHeadRejectsFloatHistogramDifferentValueAtSameTimestamp is
// TestHeadRejectsHistogramBackwardsTimestamp's float-histogram counterpart: a
// DIFFERENT value at the same timestamp must still be rejected - the exact-duplicate
// allowance (TestHeadAcceptsExactDuplicateHistogramAsNoOp) is by value, not a blanket
// "any same-timestamp append is fine."
func TestHeadRejectsFloatHistogramDifferentValueAtSameTimestamp(t *testing.T) {
	h := NewHead(1, 1, 1)
	tgt := TargetLabels{Cluster: "c", Namespace: "n", Pod: "p", Container: "co", Node: "no", Job: "j"}
	ref, err := h.GetOrCreateSeries(tgt, "request_latency")
	if err != nil {
		t.Fatalf("GetOrCreateSeries: %v", err)
	}
	fh1 := &histogram.FloatHistogram{Schema: 0, Sum: 1, Count: 1}
	if err := h.AppendFloatHistogram(ref, 2000, fh1); err != nil {
		t.Fatalf("AppendFloatHistogram @2000: %v", err)
	}
	fh2 := &histogram.FloatHistogram{Schema: 0, Sum: 2, Count: 2}
	if err := h.AppendFloatHistogram(ref, 2000, fh2); err != storage.ErrDuplicateSampleForTimestamp {
		t.Fatalf("AppendFloatHistogram @2000 (different value, same ts) = %v, want ErrDuplicateSampleForTimestamp", err)
	}
	it := h.HistogramIterator(ref)
	n := 0
	for it.Next() {
		_, fh := it.AtFloat()
		if fh.Sum != 1 || fh.Count != 1 {
			t.Fatalf("sample %d: sum=%v count=%v, want the ORIGINAL @2000 sample unchanged", n, fh.Sum, fh.Count)
		}
		n++
	}
	if n != 1 {
		t.Fatalf("HistogramIterator returned %d samples, want exactly 1", n)
	}
}

// TestHeadRejectsHistogramSchemaChangeAtSameTimestamp ports real Prometheus's
// TestAmendHistogramDatapointCausesError (tsdb/db_test.go)'s histogram case: a
// second histogram at the same timestamp with a DIFFERENT schema is still a
// conflicting value, not a "start a new segment" case - segment layout changes
// (histoSegment's own doc comment) only apply going FORWARD in time, never to
// resolve a same-timestamp collision. Already correctly handled by
// HistogramStore.LastEquals - sameLayout fails on the schema mismatch, so
// LastEquals returns false and the sample is rejected - this just adds direct
// coverage for that specific case rather than relying on it being incidentally
// covered by LastEquals' other tests.
func TestHeadRejectsHistogramSchemaChangeAtSameTimestamp(t *testing.T) {
	tgt := TargetLabels{Cluster: "c", Namespace: "n", Pod: "p", Container: "co", Node: "no", Job: "j"}
	h := NewHead(1, 1, 1)
	ref, err := h.GetOrCreateSeries(tgt, "m")
	if err != nil {
		t.Fatalf("GetOrCreateSeries: %v", err)
	}
	hg := &histogram.Histogram{
		Schema: 3, Count: 52, Sum: 2.7, ZeroThreshold: 0.1, ZeroCount: 42,
		PositiveSpans:   []histogram.Span{{Offset: 0, Length: 4}, {Offset: 10, Length: 3}},
		PositiveBuckets: []int64{1, 2, -2, 1, -1, 0, 0},
	}
	if err := h.AppendHistogram(ref, 0, hg.Copy()); err != nil {
		t.Fatalf("first: %v", err)
	}
	hg.Schema = 2
	if err := h.AppendHistogram(ref, 0, hg.Copy()); err != storage.ErrDuplicateSampleForTimestamp {
		t.Fatalf("schema change at same ts = %v, want ErrDuplicateSampleForTimestamp", err)
	}
}

// TestHeadRejectsCrossTypeSameTimestamp is CHECKLIST.md's port of real
// Prometheus's TestHeadAppender_AppendFloatWithSameTimestampAsPreviousHistogram
// (tsdb/head_test.go): a single (series, timestamp) slot is exactly one type -
// found missing here entirely (a float landing at the same ts as an existing
// histogram sample, and vice versa, were both silently accepted before this,
// landing an ambiguous sample mixed_iterator.go's own tie-break comment assumed
// could never happen).
func TestHeadRejectsCrossTypeSameTimestamp(t *testing.T) {
	tgt := TargetLabels{Cluster: "c", Namespace: "n", Pod: "p", Container: "co", Node: "no", Job: "j"}
	t.Run("float after histogram", func(t *testing.T) {
		h := NewHead(1, 1, 1)
		ref, err := h.GetOrCreateSeries(tgt, "request_latency")
		if err != nil {
			t.Fatalf("GetOrCreateSeries: %v", err)
		}
		hg := &histogram.Histogram{Schema: 0, Sum: 1, Count: 1}
		if err := h.AppendHistogram(ref, 2000, hg); err != nil {
			t.Fatalf("AppendHistogram @2000: %v", err)
		}
		wantErr := storage.NewDuplicateHistogramToFloatErr(2000, 10.0)
		if err := h.Append(ref, 2000, 10.0); err == nil || err.Error() != wantErr.Error() {
			t.Fatalf("Append(float @2000, same ts as histogram) = %v, want %v", err, wantErr)
		}
	})
	t.Run("histogram after float", func(t *testing.T) {
		h := NewHead(1, 1, 1)
		ref, err := h.GetOrCreateSeries(tgt, "request_latency")
		if err != nil {
			t.Fatalf("GetOrCreateSeries: %v", err)
		}
		if err := h.Append(ref, 2000, 10.0); err != nil {
			t.Fatalf("Append @2000: %v", err)
		}
		hg := &histogram.Histogram{Schema: 0, Sum: 1, Count: 1}
		if err := h.AppendHistogram(ref, 2000, hg); err != storage.ErrDuplicateSampleForTimestamp {
			t.Fatalf("AppendHistogram(@2000, same ts as float) = %v, want ErrDuplicateSampleForTimestamp", err)
		}
	})
}

// TestHeadTruncate covers Head.Truncate's role as orchestrator across both stores:
// a float series and a histogram series each get some samples dropped, one series
// (the counter) gets truncated down to zero remaining samples entirely, and the head
// itself keeps reporting the same series count throughout - no series is ever removed
// by Truncate (see its doc comment).
func TestHeadTruncate(t *testing.T) {
	h := NewHead(2, 2, 2)
	tgt := TargetLabels{Cluster: "c", Namespace: "n", Pod: "p", Container: "co", Node: "no", Job: "j"}

	gaugeRef, err := h.GetOrCreateSeries(tgt, "temperature")
	if err != nil {
		t.Fatalf("GetOrCreateSeries(gauge): %v", err)
	}
	gauge := []sample{
		{1700000000000, 10}, {1700000015000, 20}, {1700000030000, 30}, {1700000045000, 40},
	}
	for _, sm := range gauge {
		if err := h.Append(gaugeRef, sm.ts, sm.v); err != nil {
			t.Fatalf("Append(gauge): %v", err)
		}
	}

	histRef, err := h.GetOrCreateSeries(tgt, "request_latency")
	if err != nil {
		t.Fatalf("GetOrCreateSeries(hist): %v", err)
	}
	hists := []*histogram.Histogram{
		{Schema: 0, Count: 1, Sum: 1, PositiveSpans: []histogram.Span{{Offset: 0, Length: 1}}, PositiveBuckets: []int64{1}},
		{Schema: 0, Count: 2, Sum: 2, PositiveSpans: []histogram.Span{{Offset: 0, Length: 1}}, PositiveBuckets: []int64{1}},
	}
	histTS := []int64{1700000000000, 1700000015000}
	for i, hg := range hists {
		if err := h.AppendHistogram(histRef, histTS[i], hg); err != nil {
			t.Fatalf("AppendHistogram: %v", err)
		}
	}

	// Truncate everything before the gauge's 3rd sample - drops the histogram
	// series' entire range too (its last sample is older than the new mint).
	h.Truncate(1700000030000)

	var gotGauge []sample
	git := h.Iterator(gaugeRef)
	for git.Next() {
		ts, v := git.At()
		gotGauge = append(gotGauge, sample{ts, v})
	}
	assertSamplesEqual(t, gotGauge, gauge[2:])

	hit := h.HistogramIterator(histRef)
	if hit.Next() {
		t.Fatal("histogram series: Next() = true after truncating its whole range, want false")
	}

	if h.NumSeries() != 2 {
		t.Fatalf("NumSeries() = %d after Truncate, want 2 (no series ever removed)", h.NumSeries())
	}

	// MinTime() must advance to the truncation boundary - matching real
	// tsdb.Head.truncateMemory's own convention (h.minTime.Store(mint)). Without
	// this, a caller relying on MinTime() shrinking after Truncate to know
	// what's left to do (e.g. a periodic auto-compaction loop) would recompute
	// the same already-empty range forever - a real bug found exactly that way
	// while wiring a columnarhead-backed tsdbStore (CHECKLIST.md's Phase 7).
	if h.MinTime() != 1700000030000 {
		t.Fatalf("MinTime() = %d after Truncate(1700000030000), want 1700000030000", h.MinTime())
	}

	// A Truncate with an OLDER mint than the current MinTime must be a no-op -
	// MinTime never moves backward.
	h.Truncate(1700000000000)
	if h.MinTime() != 1700000030000 {
		t.Fatalf("MinTime() = %d after a no-op Truncate with an older mint, want unchanged 1700000030000", h.MinTime())
	}
}

// TestHeadAtScale measures the real, honest end-to-end memory cost of the actual
// ingest path - Head, not just SeriesStore's own footprint - on the same k8s-shaped
// workload used throughout CHECKLIST.md (25,000 pods x 200 series/pod = 5M series
// worth of target sharing shape, scaled down to 500k series to match the other
// per-package tests). This is the first real measurement of what a live head actually
// costs INCLUDING the Go map-based dedup indexes (targetIndex, seriesIndex,
// liveInterner's index) that a live system needs but the static MPHF/SymbolTable
// don't - see Head's and liveInterner's doc comments for why those aren't used here.
func TestHeadAtScale(t *testing.T) {
	if testing.Short() {
		t.Skip("500k-series head build; skipped in -short")
	}
	const (
		n            = 500_000
		seriesPerTgt = 200
		numTargets   = n / seriesPerTgt
		numMetrics   = 400
	)

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	h := NewHead(n, numTargets, numMetrics+numTargets*1+16)
	les := []string{"0.005", "0.01", "0.025", "0.05", "0.1", "0.25", "0.5", "1", "2.5", "5", "10", "+Inf"}
	refs := make([]uint32, n)
	ts := int64(1700000000000)
	for i := 0; i < n; i++ {
		tgt := TargetLabels{
			Cluster:   "eks-prod-1",
			Namespace: "ns-7",
			Pod:       fmt.Sprintf("payments-api-7d9f8b6c4-%06x", i/seriesPerTgt),
			Container: "app",
			Node:      "ip-10-1-2-3.ec2.internal",
			Job:       "cadvisor",
		}
		metric := fmt.Sprintf("container_metric_name_number_%03d_total", i%numMetrics)
		var extra []labels.Label
		if i%20 < 6 { // roughly matches the histogram-bucket share used elsewhere
			extra = []labels.Label{{Name: "le", Value: les[i%len(les)]}}
		}
		ref, err := h.GetOrCreateSeries(tgt, metric, extra...)
		if err != nil {
			t.Fatalf("series %d: %v", i, err)
		}
		refs[i] = ref
		if err := h.Append(ref, ts, float64(i%2)); err != nil {
			t.Fatalf("series %d: Append: %v", i, err)
		}
	}
	ts += 15000
	for round := 1; round < 8; round++ {
		for i, ref := range refs {
			if err := h.Append(ref, ts, float64((i+round)%2)); err != nil {
				t.Fatalf("round %d, series %d: %v", round, i, err)
			}
		}
		ts += 15000
	}

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(h)
	runtime.KeepAlive(refs)

	heapBytes := after.HeapAlloc - before.HeapAlloc
	t.Logf("Head at scale: %d series, %d targets, %d symbols", h.NumSeries(), h.NumTargets(), h.NumSymbols())
	t.Logf("REAL total heap (series+targets+symbols+all live dedup maps): %.1f MB (%.1f B/series)",
		float64(heapBytes)/1e6, float64(heapBytes)/n)
	t.Logf("target sharing ratio achieved: %.0f series/target (design doc measured 200:1 on a "+
		"comparable workload)", float64(h.NumSeries())/float64(h.NumTargets()))
	t.Logf("component sizes (excluding live map overhead, which isn't separable from heap "+
		"totals without an unsafe per-object accounting trick): symbols blob %d B, targets %d B",
		h.symbols.BlobBytes(), h.targets.SizeBytes())

	// Correctness spot-check at scale, not just the small hand-written cases above.
	for i := 0; i < n; i += 50_000 {
		it := h.Iterator(refs[i])
		count := 0
		for it.Next() {
			count++
		}
		if count != 8 {
			t.Fatalf("series %d: decoded %d samples, want 8", i, count)
		}
	}
}
