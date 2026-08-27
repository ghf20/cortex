package columnarhead

import (
	"context"
	"testing"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

// TestHeadSeriesIteratorMergesFloatThenHistogram is the shape promqltest's
// functions.test actually exercises (path="/bar"'s mixed load block): a series
// appended float samples, then later histogram samples. Before mixedTypeIterator
// existed, headSeries.Iterator picked histogram-only storage once ANY histogram
// sample landed, silently dropping the float prefix - mutation-tested below.
func TestHeadSeriesIteratorMergesFloatThenHistogram(t *testing.T) {
	h := NewHead(64, 8, 64)
	app := h.Appender(context.Background())
	l := labels.FromStrings(labels.MetricName, "http_requests_histogram", "path", "bar")

	for i := int64(0); i < 8; i++ {
		if _, err := app.Append(0, l, i*300000, 0); err != nil {
			t.Fatalf("append float %d: %v", i, err)
		}
	}
	for i := int64(8); i < 10; i++ {
		hg := &histogram.Histogram{Schema: 0, Sum: 1, Count: 1}
		if _, err := app.AppendHistogram(0, l, i*300000, hg, nil); err != nil {
			t.Fatalf("append hist %d: %v", i, err)
		}
	}
	if err := app.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	q, err := h.Querier(0, 3000000)
	if err != nil {
		t.Fatalf("querier: %v", err)
	}
	defer q.Close()
	ss := q.Select(context.Background(), false, nil, labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "http_requests_histogram"))
	total := 0
	for ss.Next() {
		it := ss.At().Iterator(nil)
		var lastTS int64 = -1
		floatCount, histCount := 0, 0
		for vt := it.Next(); vt != chunkenc.ValNone; vt = it.Next() {
			ts := it.AtT()
			if ts <= lastTS {
				t.Fatalf("out of order or duplicate: prevTS=%d curTS=%d", lastTS, ts)
			}
			lastTS = ts
			switch vt {
			case chunkenc.ValFloat:
				floatCount++
				if _, v := it.At(); v != 0 {
					t.Errorf("unexpected float value %v at ts=%d", v, ts)
				}
			case chunkenc.ValHistogram:
				histCount++
				if hts, hg := it.AtHistogram(nil); hts != ts || hg.Sum != 1 || hg.Count != 1 {
					t.Errorf("unexpected histogram at ts=%d: %+v", ts, hg)
				}
			default:
				t.Fatalf("unexpected value type %v at ts=%d", vt, ts)
			}
			total++
		}
		if floatCount != 8 || histCount != 2 {
			t.Fatalf("floatCount=%d histCount=%d, want 8/2", floatCount, histCount)
		}
	}
	if total != 10 {
		t.Fatalf("total samples = %d, want 10", total)
	}
}

// TestHeadSeriesIteratorMergesHistogramThenFloat is the reverse order - histogram
// samples first, then float - checking the merge is symmetric, not fixed for only
// the one order functions.test happens to exercise.
func TestHeadSeriesIteratorMergesHistogramThenFloat(t *testing.T) {
	h := NewHead(64, 8, 64)
	app := h.Appender(context.Background())
	l := labels.FromStrings(labels.MetricName, "mixed_reverse")

	for i := int64(0); i < 3; i++ {
		hg := &histogram.Histogram{Schema: 0, Sum: 1, Count: 1}
		if _, err := app.AppendHistogram(0, l, i*300000, hg, nil); err != nil {
			t.Fatalf("append hist %d: %v", i, err)
		}
	}
	for i := int64(3); i < 9; i++ {
		if _, err := app.Append(0, l, i*300000, float64(i)); err != nil {
			t.Fatalf("append float %d: %v", i, err)
		}
	}
	if err := app.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	q, err := h.Querier(0, 3000000)
	if err != nil {
		t.Fatalf("querier: %v", err)
	}
	defer q.Close()
	ss := q.Select(context.Background(), false, nil, labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "mixed_reverse"))
	total := 0
	for ss.Next() {
		it := ss.At().Iterator(nil)
		var lastTS int64 = -1
		floatCount, histCount := 0, 0
		for vt := it.Next(); vt != chunkenc.ValNone; vt = it.Next() {
			ts := it.AtT()
			if ts <= lastTS {
				t.Fatalf("out of order: prevTS=%d curTS=%d", lastTS, ts)
			}
			lastTS = ts
			switch vt {
			case chunkenc.ValHistogram:
				histCount++
			case chunkenc.ValFloat:
				floatCount++
			}
			total++
		}
		if floatCount != 6 || histCount != 3 {
			t.Fatalf("floatCount=%d histCount=%d, want 6/3", floatCount, histCount)
		}
	}
	if total != 9 {
		t.Fatalf("total = %d, want 9", total)
	}
}
