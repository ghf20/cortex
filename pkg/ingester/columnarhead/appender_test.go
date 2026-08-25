package columnarhead

import (
	"context"
	"testing"

	"github.com/prometheus/prometheus/model/exemplar"
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

func TestAppenderRejectsUnsupportedShape(t *testing.T) {
	h := NewHead(1, 1, 1)
	app := h.Appender(context.Background())

	cases := map[string]labels.Labels{
		"no __name__": labels.FromStrings("cluster", "c"),
		"two extra labels beyond __name__": labels.FromStrings(
			labels.MetricName, "m", "extra1", "a", "extra2", "b",
		),
	}
	for name, l := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := app.Append(0, l, 1700000000000, 1); err != ErrUnsupportedLabelShape {
				t.Fatalf("Append(%v) = %v, want ErrUnsupportedLabelShape", l, err)
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

	if _, err := app.AppendExemplar(0, l, exemplar.Exemplar{}); err != ErrNotImplemented {
		t.Errorf("AppendExemplar = %v, want ErrNotImplemented", err)
	}
	if _, err := app.AppendHistogram(0, l, 0, nil, nil); err != ErrNotImplemented {
		t.Errorf("AppendHistogram = %v, want ErrNotImplemented", err)
	}
	if _, err := app.AppendHistogramSTZeroSample(0, l, 0, 0, nil, nil); err != ErrNotImplemented {
		t.Errorf("AppendHistogramSTZeroSample = %v, want ErrNotImplemented", err)
	}
	if _, err := app.UpdateMetadata(0, l, metadata.Metadata{}); err != ErrNotImplemented {
		t.Errorf("UpdateMetadata = %v, want ErrNotImplemented", err)
	}
	if _, err := app.AppendSTZeroSample(0, l, 0, 0); err != ErrNotImplemented {
		t.Errorf("AppendSTZeroSample = %v, want ErrNotImplemented", err)
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
