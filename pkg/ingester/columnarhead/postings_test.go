package columnarhead

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
)

// TestPostingsShortcutMatchesFullScan builds a small multi-metric head and confirms
// Select with an exact __name__ matcher returns exactly the same series as a
// (manually forced) full scan would - the optimization must not change results, only
// how fast they're found.
func TestPostingsShortcutMatchesFullScan(t *testing.T) {
	h := NewHead(10, 3, 10)
	app := h.Appender(context.Background())
	mk := func(name, pod string) labels.Labels {
		return labels.FromStrings(
			labels.MetricName, name,
			"cluster", "c", "namespace", "n", "pod", pod, "container", "co", "node", "no", "job", "j",
		)
	}
	// Interleave creation across two metric names, so postings for one name are not
	// contiguous in ref-space - a real test of whether the postings list itself (not
	// just "the first few refs") is being used correctly.
	want := map[string]bool{}
	for i := 0; i < 6; i++ {
		name := "metric_a"
		if i%2 == 0 {
			name = "metric_b"
		}
		l := mk(name, fmt.Sprintf("pod-%d", i))
		if _, err := app.Append(0, l, 1700000000000, float64(i)); err != nil {
			t.Fatalf("Append: %v", err)
		}
		if name == "metric_a" {
			want[l.String()] = true
		}
	}

	q, err := h.Querier(math.MinInt64, math.MaxInt64)
	if err != nil {
		t.Fatalf("Querier: %v", err)
	}
	defer q.Close()

	ss := q.Select(context.Background(), false, nil, labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "metric_a"))
	got := map[string]bool{}
	for ss.Next() {
		got[ss.At().Labels().String()] = true
	}
	if len(got) != len(want) {
		t.Fatalf("Select(__name__=\"metric_a\") returned %d series, want %d", len(got), len(want))
	}
	for k := range want {
		if !got[k] {
			t.Errorf("missing expected series %s", k)
		}
	}
}

// TestPostingsShortcutWithAdditionalMatchers confirms the remaining (non-__name__)
// matchers are still correctly applied to the postings candidate set - the shortcut
// narrows the scan, it doesn't replace matching entirely.
func TestPostingsShortcutWithAdditionalMatchers(t *testing.T) {
	h := NewHead(10, 3, 10)
	app := h.Appender(context.Background())
	base := []string{labels.MetricName, "up", "container", "co", "node", "no", "job", "j"}
	l1 := labels.FromStrings(append(append([]string{}, base...), "cluster", "c1", "namespace", "n", "pod", "p1")...)
	l2 := labels.FromStrings(append(append([]string{}, base...), "cluster", "c2", "namespace", "n", "pod", "p2")...)
	if _, err := app.Append(0, l1, 1700000000000, 1); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if _, err := app.Append(0, l2, 1700000000000, 1); err != nil {
		t.Fatalf("Append: %v", err)
	}

	q, err := h.Querier(math.MinInt64, math.MaxInt64)
	if err != nil {
		t.Fatalf("Querier: %v", err)
	}
	defer q.Close()

	ss := q.Select(context.Background(), false, nil,
		labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "up"),
		labels.MustNewMatcher(labels.MatchEqual, "cluster", "c1"),
	)
	var got []labels.Labels
	for ss.Next() {
		got = append(got, ss.At().Labels())
	}
	if len(got) != 1 {
		t.Fatalf("Select(__name__=up, cluster=c1) returned %d series, want 1", len(got))
	}
	if got[0].Get("cluster") != "c1" {
		t.Fatalf("returned series has cluster=%q, want c1", got[0].Get("cluster"))
	}
}

// TestPostingsShortcutUnknownName confirms an exact __name__ matcher for a name that
// was never created returns zero series (not a fall-through to a full scan that would
// incorrectly match nothing, or panic on a nil postings list).
func TestPostingsShortcutUnknownName(t *testing.T) {
	h := NewHead(10, 3, 10)
	app := h.Appender(context.Background())
	l := labels.FromStrings(
		labels.MetricName, "up", "cluster", "c", "namespace", "n", "pod", "p", "container", "co", "node", "no", "job", "j",
	)
	if _, err := app.Append(0, l, 1700000000000, 1); err != nil {
		t.Fatalf("Append: %v", err)
	}

	q, err := h.Querier(math.MinInt64, math.MaxInt64)
	if err != nil {
		t.Fatalf("Querier: %v", err)
	}
	defer q.Close()
	ss := q.Select(context.Background(), false, nil, labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "never_created"))
	if ss.Next() {
		t.Fatal("Select(__name__=\"never_created\") returned a series, want none")
	}
}

// TestPostingsShortcutFallsBackForRegex confirms a regex __name__ matcher (no exact
// match to shortcut on) still correctly falls back to a full scan rather than
// matching nothing or panicking.
func TestPostingsShortcutFallsBackForRegex(t *testing.T) {
	h := NewHead(10, 3, 10)
	app := h.Appender(context.Background())
	base := []string{"cluster", "c", "namespace", "n", "pod", "p", "container", "co", "node", "no", "job", "j"}
	l1 := labels.FromStrings(append([]string{labels.MetricName, "up"}, base...)...)
	l2 := labels.FromStrings(append([]string{labels.MetricName, "upstream"}, base...)...)
	if _, err := app.Append(0, l1, 1700000000000, 1); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if _, err := app.Append(0, l2, 1700000000000, 1); err != nil {
		t.Fatalf("Append: %v", err)
	}

	q, err := h.Querier(math.MinInt64, math.MaxInt64)
	if err != nil {
		t.Fatalf("Querier: %v", err)
	}
	defer q.Close()
	ss := q.Select(context.Background(), false, nil, labels.MustNewMatcher(labels.MatchRegexp, labels.MetricName, "up.*"))
	count := 0
	for ss.Next() {
		count++
	}
	if count != 2 {
		t.Fatalf("Select(__name__=~\"up.*\") returned %d series, want 2 (both up and upstream)", count)
	}
}
