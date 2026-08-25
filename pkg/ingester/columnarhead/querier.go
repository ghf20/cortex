package columnarhead

import (
	"context"
	"sort"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/annotations"
)

// Querier returns a storage.Querier over h's current state - a live, unlocked
// snapshot of whatever series exist at call time (Head has no locking anywhere yet;
// a real concurrent-safe implementation is separate, later work). mint/maxt are
// accepted for interface conformance but NOT used to filter samples yet - every
// matching series returns its full sample history regardless of the requested time
// range. This is a real, stated limitation, not silently dropped: a correct
// implementation needs Select's iterators to respect [mint, maxt], which isn't built
// yet - see CHECKLIST.md.
func (h *Head) Querier(_, _ int64) (storage.Querier, error) {
	return &headQuerier{h: h}, nil
}

type headQuerier struct {
	h *Head
}

var _ storage.Querier = (*headQuerier)(nil)

func (q *headQuerier) Close() error { return nil }

// Select scans every series in h and checks every matcher against each one's
// reconstructed labels. Design doc §3.4's target architecture (postings for
// __name__ only, then linear-scan the rest) is NOT implemented here - this is the
// honest, correct, unoptimized full scan that architecture is meant to build on top
// of, not the target design itself; see CHECKLIST.md. sortSeries and hints.Start/End
// are accepted but not applied - results come out in ascending ref order, a stable
// but arbitrary order, not a label-sorted guarantee.
func (q *headQuerier) Select(_ context.Context, _ bool, _ *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	var refs []uint32
	n := uint32(q.h.NumSeries())
	for ref := uint32(0); ref < n; ref++ {
		if matchesAll(q.h.SeriesLabels(ref), matchers) {
			refs = append(refs, ref)
		}
	}
	return &headSeriesSet{h: q.h, refs: refs}
}

func matchesAll(lbls labels.Labels, matchers []*labels.Matcher) bool {
	for _, m := range matchers {
		if !m.Matches(lbls.Get(m.Name)) {
			return false
		}
	}
	return true
}

func (q *headQuerier) LabelValues(_ context.Context, name string, _ *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	seen := make(map[string]struct{})
	n := uint32(q.h.NumSeries())
	for ref := uint32(0); ref < n; ref++ {
		lbls := q.h.SeriesLabels(ref)
		if !matchesAll(lbls, matchers) {
			continue
		}
		if v := lbls.Get(name); v != "" {
			seen[v] = struct{}{}
		}
	}
	return sortedKeys(seen), nil, nil
}

func (q *headQuerier) LabelNames(_ context.Context, _ *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	seen := make(map[string]struct{})
	n := uint32(q.h.NumSeries())
	for ref := uint32(0); ref < n; ref++ {
		lbls := q.h.SeriesLabels(ref)
		if !matchesAll(lbls, matchers) {
			continue
		}
		lbls.Range(func(l labels.Label) { seen[l.Name] = struct{}{} })
	}
	return sortedKeys(seen), nil, nil
}

func sortedKeys(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// headSeriesSet iterates a fixed slice of series refs selected by Select - a snapshot
// at Select time, not a live view.
type headSeriesSet struct {
	h    *Head
	refs []uint32
	i    int
	cur  uint32
}

var _ storage.SeriesSet = (*headSeriesSet)(nil)

func (s *headSeriesSet) Next() bool {
	if s.i >= len(s.refs) {
		return false
	}
	s.cur = s.refs[s.i]
	s.i++
	return true
}

func (s *headSeriesSet) At() storage.Series                { return &headSeries{h: s.h, ref: s.cur} }
func (s *headSeriesSet) Err() error                        { return nil }
func (s *headSeriesSet) Warnings() annotations.Annotations { return nil }

type headSeries struct {
	h   *Head
	ref uint32
}

var _ storage.Series = (*headSeries)(nil)

func (s *headSeries) Labels() labels.Labels {
	return s.h.SeriesLabels(s.ref)
}

// Iterator returns a chunkenc.Iterator over s's samples: a histogram-backed one if
// this series ever received a histogram sample, a float-backed one otherwise. A
// series is one or the other, never both (matches real Prometheus semantics that a
// series' sample type doesn't change mid-stream - see HistogramStore's doc comment).
// The passed-in iterator (for reuse) is ignored; this always allocates fresh, unlike
// real chunk iterators that support in-place reuse - a real optimization opportunity,
// not attempted here.
func (s *headSeries) Iterator(_ chunkenc.Iterator) chunkenc.Iterator {
	if s.h.histograms.Has(s.ref) {
		return &histogramSampleIterator{it: s.h.HistogramIterator(s.ref)}
	}
	return &floatSampleIterator{it: s.h.Iterator(s.ref)}
}

// floatSampleIterator adapts *Iterator (this package's float sample iterator) to
// chunkenc.Iterator.
type floatSampleIterator struct {
	it      *Iterator
	started bool
	done    bool
	curTS   int64
	curVal  float64
}

var _ chunkenc.Iterator = (*floatSampleIterator)(nil)

func (fi *floatSampleIterator) Next() chunkenc.ValueType {
	if fi.done || !fi.it.Next() {
		fi.done = true
		return chunkenc.ValNone
	}
	fi.started = true
	fi.curTS, fi.curVal = fi.it.At()
	return chunkenc.ValFloat
}

func (fi *floatSampleIterator) Seek(t int64) chunkenc.ValueType {
	if fi.done {
		return chunkenc.ValNone
	}
	if fi.started && fi.curTS >= t {
		return chunkenc.ValFloat
	}
	// No random access in the underlying Iterator - a real chunk iterator could seek
	// more efficiently; this is a correct but unoptimized linear scan forward from
	// wherever we are, stated rather than silently assumed O(1).
	for {
		if vt := fi.Next(); vt == chunkenc.ValNone {
			return chunkenc.ValNone
		}
		if fi.curTS >= t {
			return chunkenc.ValFloat
		}
	}
}

func (fi *floatSampleIterator) At() (int64, float64) { return fi.curTS, fi.curVal }
func (fi *floatSampleIterator) AtT() int64           { return fi.curTS }

// AtHistogram/AtFloatHistogram panic on a float iterator, matching real Prometheus's
// own xorIterator precedent exactly (vendor/.../tsdb/chunkenc/xor.go) - a caller that
// saw ValFloat from Next()/Seek() should never call these.
func (fi *floatSampleIterator) AtHistogram(*histogram.Histogram) (int64, *histogram.Histogram) {
	panic("columnarhead: cannot call floatSampleIterator.AtHistogram")
}

func (fi *floatSampleIterator) AtFloatHistogram(*histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	panic("columnarhead: cannot call floatSampleIterator.AtFloatHistogram")
}

func (fi *floatSampleIterator) Err() error { return nil }

// histogramSampleIterator adapts *HistogramIterator to chunkenc.Iterator.
type histogramSampleIterator struct {
	it      *HistogramIterator
	started bool
	done    bool
	curTS   int64
	curH    *histogram.Histogram
}

var _ chunkenc.Iterator = (*histogramSampleIterator)(nil)

func (hi *histogramSampleIterator) Next() chunkenc.ValueType {
	if hi.done || !hi.it.Next() {
		hi.done = true
		return chunkenc.ValNone
	}
	hi.started = true
	hi.curTS, hi.curH = hi.it.At()
	return chunkenc.ValHistogram
}

func (hi *histogramSampleIterator) Seek(t int64) chunkenc.ValueType {
	if hi.done {
		return chunkenc.ValNone
	}
	if hi.started && hi.curTS >= t {
		return chunkenc.ValHistogram
	}
	for {
		if vt := hi.Next(); vt == chunkenc.ValNone {
			return chunkenc.ValNone
		}
		if hi.curTS >= t {
			return chunkenc.ValHistogram
		}
	}
}

// At panics, matching real Prometheus's own histogramIterator precedent exactly
// (vendor/.../tsdb/chunkenc/histogram.go) - a caller that saw ValHistogram from
// Next()/Seek() should never call this.
func (hi *histogramSampleIterator) At() (int64, float64) {
	panic("columnarhead: cannot call histogramSampleIterator.At")
}

func (hi *histogramSampleIterator) AtHistogram(dst *histogram.Histogram) (int64, *histogram.Histogram) {
	if dst != nil {
		*dst = *hi.curH
		return hi.curTS, dst
	}
	return hi.curTS, hi.curH
}

// AtFloatHistogram converts the current integer histogram to a FloatHistogram,
// matching real Prometheus's histogramIterator.AtFloatHistogram precedent (it
// documents supporting exactly this: "It also works if the value is a histogram with
// integer counts"). Cheap here since HistogramIterator already tracks absolute
// per-bucket counts internally (it.it.posAbs/negAbs) - converting to float deltas is
// the same shape as deltaEncode, just producing []float64 instead of []int64.
func (hi *histogramSampleIterator) AtFloatHistogram(fh *histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	if fh == nil {
		fh = &histogram.FloatHistogram{}
	}
	h := hi.curH
	fh.Schema = h.Schema
	fh.ZeroThreshold = h.ZeroThreshold
	fh.ZeroCount = float64(h.ZeroCount)
	fh.Count = float64(h.Count)
	fh.Sum = h.Sum
	fh.PositiveSpans = h.PositiveSpans
	fh.NegativeSpans = h.NegativeSpans
	fh.PositiveBuckets = deltaEncodeFloat(hi.it.posAbs)
	fh.NegativeBuckets = deltaEncodeFloat(hi.it.negAbs)
	return hi.curTS, fh
}

// deltaEncodeFloat is deltaEncode's float64 counterpart, for AtFloatHistogram - same
// first-absolute-then-relative-deltas shape FloatHistogram's own bucket fields use.
func deltaEncodeFloat(abs []int64) []float64 {
	if len(abs) == 0 {
		return nil
	}
	out := make([]float64, len(abs))
	var prev int64
	for i, v := range abs {
		out[i] = float64(v - prev)
		prev = v
	}
	return out
}

func (hi *histogramSampleIterator) AtT() int64 { return hi.curTS }
func (hi *histogramSampleIterator) Err() error { return nil }
