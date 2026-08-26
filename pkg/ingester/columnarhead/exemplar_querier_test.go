package columnarhead

import (
	"context"
	"testing"

	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
)

// TestExemplarQuerierSelect is the decisive test for ExemplarQuerier: matcher-set
// AND/OR semantics (storage.ExemplarQuerier's documented contract), time-range
// filtering, and result ordering by series labels - checked against real appended
// exemplars via the real Appender, not by poking exemplarStorage's internals.
func TestExemplarQuerierSelect(t *testing.T) {
	h := NewHead(4, 1, 8)
	app := h.Appender(context.Background())

	lA := labels.FromStrings(labels.MetricName, "requests_total", "cluster", "c", "namespace", "n", "pod", "pa", "container", "co", "node", "no", "job", "j")
	lB := labels.FromStrings(labels.MetricName, "requests_total", "cluster", "c", "namespace", "n", "pod", "pb", "container", "co", "node", "no", "job", "j")
	lC := labels.FromStrings(labels.MetricName, "errors_total", "cluster", "c", "namespace", "n", "pod", "pc", "container", "co", "node", "no", "job", "j")

	if _, err := app.Append(0, lA, 1000, 1); err != nil {
		t.Fatalf("Append A: %v", err)
	}
	if _, err := app.Append(0, lB, 1000, 1); err != nil {
		t.Fatalf("Append B: %v", err)
	}
	if _, err := app.Append(0, lC, 1000, 1); err != nil {
		t.Fatalf("Append C: %v", err)
	}

	traceA := labels.FromStrings("trace_id", "aaa")
	traceB := labels.FromStrings("trace_id", "bbb")
	traceC := labels.FromStrings("trace_id", "ccc")
	if _, err := app.AppendExemplar(0, lA, exemplar.Exemplar{Labels: traceA, Value: 1, Ts: 1000}); err != nil {
		t.Fatalf("AppendExemplar A: %v", err)
	}
	if _, err := app.AppendExemplar(0, lA, exemplar.Exemplar{Labels: traceA, Value: 2, Ts: 2000}); err != nil {
		t.Fatalf("AppendExemplar A2: %v", err)
	}
	if _, err := app.AppendExemplar(0, lB, exemplar.Exemplar{Labels: traceB, Value: 1, Ts: 1500}); err != nil {
		t.Fatalf("AppendExemplar B: %v", err)
	}
	if _, err := app.AppendExemplar(0, lC, exemplar.Exemplar{Labels: traceC, Value: 1, Ts: 1000}); err != nil {
		t.Fatalf("AppendExemplar C: %v", err)
	}

	eq, err := h.ExemplarQuerier(context.Background())
	if err != nil {
		t.Fatalf("ExemplarQuerier: %v", err)
	}

	t.Run("matches within a set is AND", func(t *testing.T) {
		// __name__=requests_total AND pod=pa - only series A.
		got, err := eq.Select(0, 3000, []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "requests_total"),
			labels.MustNewMatcher(labels.MatchEqual, "pod", "pa"),
		})
		if err != nil {
			t.Fatalf("Select: %v", err)
		}
		if len(got) != 1 {
			t.Fatalf("got %d series, want 1", len(got))
		}
		if got[0].SeriesLabels.Get("pod") != "pa" {
			t.Fatalf("got series pod=%s, want pa", got[0].SeriesLabels.Get("pod"))
		}
		if len(got[0].Exemplars) != 2 {
			t.Fatalf("got %d exemplars for series A, want 2", len(got[0].Exemplars))
		}
	})

	t.Run("matches between sets is OR, sorted by series labels", func(t *testing.T) {
		// pod=pa OR pod=pc - series A and C, no B.
		got, err := eq.Select(0, 3000,
			[]*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "pod", "pa")},
			[]*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "pod", "pc")},
		)
		if err != nil {
			t.Fatalf("Select: %v", err)
		}
		if len(got) != 2 {
			t.Fatalf("got %d series, want 2", len(got))
		}
		if labels.Compare(got[0].SeriesLabels, got[1].SeriesLabels) >= 0 {
			t.Fatalf("results not sorted by series labels: %v then %v", got[0].SeriesLabels, got[1].SeriesLabels)
		}
		for _, r := range got {
			pod := r.SeriesLabels.Get("pod")
			if pod != "pa" && pod != "pc" {
				t.Fatalf("unexpected series in OR result: pod=%s", pod)
			}
		}
	})

	t.Run("time range filtering", func(t *testing.T) {
		// Only ts in [1600, 3000] - excludes A's two exemplars (1000, 2000... wait 2000 is in range)
		got, err := eq.Select(1600, 3000, []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "requests_total"),
		})
		if err != nil {
			t.Fatalf("Select: %v", err)
		}
		// Only series A's ts=2000 exemplar survives the [1600,3000] window; B's ts=1500 is excluded.
		if len(got) != 1 {
			t.Fatalf("got %d series, want 1 (only A's later exemplar in range)", len(got))
		}
		if got[0].SeriesLabels.Get("pod") != "pa" {
			t.Fatalf("got series pod=%s, want pa", got[0].SeriesLabels.Get("pod"))
		}
		if len(got[0].Exemplars) != 1 || got[0].Exemplars[0].Ts != 2000 {
			t.Fatalf("got exemplars %+v, want exactly [ts=2000]", got[0].Exemplars)
		}
	})

	t.Run("no match returns empty, not an error", func(t *testing.T) {
		got, err := eq.Select(0, 3000, []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "does_not_exist"),
		})
		if err != nil {
			t.Fatalf("Select: %v", err)
		}
		if len(got) != 0 {
			t.Fatalf("got %d series, want 0", len(got))
		}
	})
}
