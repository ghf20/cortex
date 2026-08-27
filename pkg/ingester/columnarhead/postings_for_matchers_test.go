package columnarhead

import (
	"context"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
)

// TestPostingsForMatchers is the decisive test for PostingsForMatchers: it must
// return exactly the same series a Querier.Select with the identical matchers
// would, just as index.Postings instead of a storage.SeriesSet - checked against
// real appended series, not synthetic refs.
func TestPostingsForMatchers(t *testing.T) {
	h := NewHead(8, 1, 8)
	tgt := TargetLabels{Cluster: "c", Namespace: "n", Pod: "p", Container: "co", Node: "no", Job: "j"}

	refUp, err := h.GetOrCreateSeries(tgt, "up", labels.Label{Name: "le", Value: "0.1"})
	if err != nil {
		t.Fatalf("GetOrCreateSeries(up): %v", err)
	}
	refDown, err := h.GetOrCreateSeries(tgt, "down", labels.Label{Name: "le", Value: "0.1"})
	if err != nil {
		t.Fatalf("GetOrCreateSeries(down): %v", err)
	}
	refUp2, err := h.GetOrCreateSeries(tgt, "up", labels.Label{Name: "le", Value: "0.5"})
	if err != nil {
		t.Fatalf("GetOrCreateSeries(up 0.5): %v", err)
	}

	t.Run("exact name matcher", func(t *testing.T) {
		p, err := h.PostingsForMatchers(context.Background(), labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "up"))
		if err != nil {
			t.Fatalf("PostingsForMatchers: %v", err)
		}
		got := collectRefs(t, p)
		want := map[storage.SeriesRef]bool{storage.SeriesRef(refUp): true, storage.SeriesRef(refUp2): true}
		if len(got) != len(want) {
			t.Fatalf("got %d refs, want %d: %v", len(got), len(want), got)
		}
		for _, r := range got {
			if !want[r] {
				t.Fatalf("unexpected ref %d in result", r)
			}
		}
	})

	t.Run("name plus local label matcher", func(t *testing.T) {
		p, err := h.PostingsForMatchers(context.Background(),
			labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "up"),
			labels.MustNewMatcher(labels.MatchEqual, "le", "0.5"),
		)
		if err != nil {
			t.Fatalf("PostingsForMatchers: %v", err)
		}
		got := collectRefs(t, p)
		if len(got) != 1 || got[0] != storage.SeriesRef(refUp2) {
			t.Fatalf("got %v, want exactly [%d]", got, refUp2)
		}
	})

	t.Run("no match", func(t *testing.T) {
		p, err := h.PostingsForMatchers(context.Background(), labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "does_not_exist"))
		if err != nil {
			t.Fatalf("PostingsForMatchers: %v", err)
		}
		got := collectRefs(t, p)
		if len(got) != 0 {
			t.Fatalf("got %v, want none", got)
		}
	})

	t.Run("no matchers returns every series", func(t *testing.T) {
		p, err := h.PostingsForMatchers(context.Background())
		if err != nil {
			t.Fatalf("PostingsForMatchers: %v", err)
		}
		got := collectRefs(t, p)
		if len(got) != 3 {
			t.Fatalf("got %d refs, want 3 (every series): %v", len(got), got)
		}
	})

	_ = refDown
}

func collectRefs(t *testing.T, p interface {
	Next() bool
	At() storage.SeriesRef
	Err() error
}) []storage.SeriesRef {
	t.Helper()
	var out []storage.SeriesRef
	for p.Next() {
		out = append(out, p.At())
	}
	if err := p.Err(); err != nil {
		t.Fatalf("postings iteration error: %v", err)
	}
	return out
}
