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

// Querier returns a storage.Querier over h's current state. mint/maxt bound every
// returned series' iterator to that inclusive range - a series with matcher hits but
// zero samples inside [mint, maxt] is still returned (matching real Prometheus's own
// lazy behavior: the caller's iterator just yields nothing), only the samples
// themselves are filtered, not series membership.
//
// Takes h's indexMu read lock PLUS every shard's read lock, for the ENTIRE query
// lifetime, released by Close() - not just for Select() itself. This is necessary,
// not overcautious: Select's returned SeriesSet/Series/Iterator all lazily read h's
// arena/maps well after Select returns, and SeriesStore.growSlot can reallocate that
// arena's backing array out from under a concurrent reader if a write were allowed to
// proceed mid-query. Callers MUST call Close() (storage.Querier's own documented
// contract) - forgetting to is a real, listed risk here specifically: an un-closed
// querier is not just a resource leak, it wedges every future write to EVERY shard
// against this Head forever.
//
// All shard locks are acquired in ascending shard-index order, matching the fixed
// order Truncate/Flush/Compact use (see Head's doc comment on the locking design in
// CHECKLIST.md) - the same fixed order everywhere avoids a lock-ordering deadlock
// between a Querier and a concurrent Truncate/Flush/Compact call.
func (h *Head) Querier(mint, maxt int64) (storage.Querier, error) {
	h.indexMu.RLock()
	for _, shard := range h.shards {
		shard.mu.RLock()
	}
	return &headQuerier{h: h, mint: mint, maxt: maxt}, nil
}

type headQuerier struct {
	h          *Head
	mint, maxt int64
	closed     bool
}

var _ storage.Querier = (*headQuerier)(nil)

// Close releases every lock taken by Head.Querier. Idempotent, matching
// storage.Querier's documented contract ("safe to be called multiple times") - a
// second RUnlock on an already-released sync.RWMutex would panic, so this guards
// against exactly that.
func (q *headQuerier) Close() error {
	if q.closed {
		return nil
	}
	q.closed = true
	for _, shard := range q.h.shards {
		shard.mu.RUnlock()
	}
	q.h.indexMu.RUnlock()
	return nil
}

// Select implements design doc §3.4's architecture: if matchers include an exact
// (MatchEqual) selector on __name__, look up its postings (Head.SeriesRefsForName)
// and linear-scan the REMAINING matchers only within that candidate set, instead of
// every series in h - "the leading selector in essentially every real query," per
// the design doc, and now measured, not assumed (see CHECKLIST.md for real numbers:
// TestPostingsSpeedup). Falls back to a full scan when there's no exact __name__
// matcher (regex/negation on __name__, or none at all) - matching the design doc's
// own stated scope: this accelerates the common case, not every possible query
// shape. hints.Func/Grouping/etc. are accepted but not applied. hints.Start/End are
// ignored in favor of the querier's own mint/maxt (from Head.Querier). sortSeries, when
// true, sorts the result by labels.Compare - required by callers like
// tsdb.CreateBlock/index.Writer.AddSeries that need strictly increasing label order;
// when false, results come out in whatever order the candidate set is in (postings
// creation order, or ascending ref order for the full-scan fallback).
func (q *headQuerier) Select(_ context.Context, sortSeries bool, _ *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	candidates, rest := q.candidateRefs(matchers)
	var refs []uint32
	for _, ref := range candidates {
		if refMatchesAll(q.h, ref, rest) {
			refs = append(refs, ref)
		}
	}
	if sortSeries {
		sortRefsByLabels(q.h, refs)
	}
	return &headSeriesSet{h: q.h, refs: refs, mint: q.mint, maxt: q.maxt}
}

// sortRefsByLabels sorts refs in place by their reconstructed labels, using the same
// labels.Compare ordering index.Writer.AddSeries requires from its caller (strictly
// increasing, series-by-series).
func sortRefsByLabels(h *Head, refs []uint32) {
	sort.Slice(refs, func(i, j int) bool {
		return labels.Compare(h.SeriesLabels(refs[i]), h.SeriesLabels(refs[j])) < 0
	})
}

// refMatchesAll checks matchers against ref's labels one at a time via
// Head.SeriesLabelValue, not by reconstructing the full label set first
// (SeriesLabels' ScratchBuilder+Sort) - measured to dominate a full scan's cost (see
// CHECKLIST.md), and wasted work for every candidate that fails an early matcher.
// LabelValues/LabelNames still use SeriesLabels directly (they need the whole set
// regardless), only Select's per-candidate filtering goes through this cheaper path.
func refMatchesAll(h *Head, ref uint32, matchers []*labels.Matcher) bool {
	for _, m := range matchers {
		if !m.Matches(h.SeriesLabelValue(ref, m.Name)) {
			return false
		}
	}
	return true
}

// candidateRefs returns the series refs to scan and the matchers still left to check
// against each one. If matchers includes an exact __name__ matcher, the candidate set
// is that name's postings list and __name__ is dropped from the remaining matchers
// (already satisfied by construction - every ref in the list has that name). Otherwise
// the candidate set is every series in h and all matchers still need checking.
func (q *headQuerier) candidateRefs(matchers []*labels.Matcher) (candidates []uint32, rest []*labels.Matcher) {
	for i, m := range matchers {
		if m.Type != labels.MatchEqual || m.Name != labels.MetricName {
			continue
		}
		refs, ok := q.h.SeriesRefsForName(m.Value)
		if !ok {
			return nil, nil // metric name doesn't exist at all - no series can match
		}
		rest = make([]*labels.Matcher, 0, len(matchers)-1)
		rest = append(rest, matchers[:i]...)
		rest = append(rest, matchers[i+1:]...)
		return refs, rest
	}
	// No exact __name__ matcher - full scan fallback.
	all := make([]uint32, q.h.NumSeries())
	for i := range all {
		all[i] = uint32(i)
	}
	return all, matchers
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
	h          *Head
	refs       []uint32
	mint, maxt int64
	i          int
	cur        uint32
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

func (s *headSeriesSet) At() storage.Series {
	return &headSeries{h: s.h, ref: s.cur, mint: s.mint, maxt: s.maxt}
}
func (s *headSeriesSet) Err() error                        { return nil }
func (s *headSeriesSet) Warnings() annotations.Annotations { return nil }

type headSeries struct {
	h          *Head
	ref        uint32
	mint, maxt int64
}

var _ storage.Series = (*headSeries)(nil)

func (s *headSeries) Labels() labels.Labels {
	return s.h.SeriesLabels(s.ref)
}

// Iterator returns a chunkenc.Iterator over s's samples, bounded to [s.mint, s.maxt]:
// an integer-histogram-backed one if this series ever received a Histogram sample, a
// float-histogram-backed one if it ever received a FloatHistogram sample, a
// float-backed one otherwise. A series is exactly one of the three, never more than
// one (matches real Prometheus semantics that a series' sample type doesn't change
// mid-stream - see HistogramStore's doc comment, and ErrHistogramTypeChanged for the
// int/float histogram case specifically). The passed-in iterator (for reuse) is
// ignored; this always allocates fresh, unlike real chunk iterators that support
// in-place reuse - a real optimization opportunity, not attempted here.
func (s *headSeries) Iterator(_ chunkenc.Iterator) chunkenc.Iterator {
	if s.h.HasHistogram(s.ref) {
		if s.h.HasFloatHistogram(s.ref) {
			return &floatHistogramSampleIterator{it: s.h.HistogramIterator(s.ref), mint: s.mint, maxt: s.maxt}
		}
		return &histogramSampleIterator{it: s.h.HistogramIterator(s.ref), mint: s.mint, maxt: s.maxt}
	}
	var src floatSource = s.h.Iterator(s.ref)
	if ooo := s.h.OOOSamples(s.ref); len(ooo) > 0 {
		src = newMergedIterator(src, ooo)
	}
	return &floatSampleIterator{it: src, mint: s.mint, maxt: s.maxt}
}

// floatSampleIterator adapts a floatSource (this package's raw in-order
// *Iterator, or a *mergedIterator when the series has OOO samples too - see
// ooo.go) to chunkenc.Iterator, bounded to [mint, maxt] inclusive.
type floatSampleIterator struct {
	it         floatSource
	mint, maxt int64
	started    bool
	done       bool
	curTS      int64
	curVal     float64
}

var _ chunkenc.Iterator = (*floatSampleIterator)(nil)

func (fi *floatSampleIterator) Next() chunkenc.ValueType {
	for {
		if fi.done || !fi.it.Next() {
			fi.done = true
			return chunkenc.ValNone
		}
		ts, v := fi.it.At()
		if ts < fi.mint {
			continue // before the requested range - keep scanning, don't surface it
		}
		if ts > fi.maxt {
			fi.done = true          // past the requested range - the underlying stream is
			return chunkenc.ValNone // time-ordered, so nothing later can be in range either
		}
		fi.started = true
		fi.curTS, fi.curVal = ts, v
		return chunkenc.ValFloat
	}
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

// histogramSampleIterator adapts *HistogramIterator to chunkenc.Iterator, bounded to
// [mint, maxt] inclusive.
type histogramSampleIterator struct {
	it         *HistogramIterator
	mint, maxt int64
	started    bool
	done       bool
	curTS      int64
	curH       *histogram.Histogram
}

var _ chunkenc.Iterator = (*histogramSampleIterator)(nil)

func (hi *histogramSampleIterator) Next() chunkenc.ValueType {
	for {
		if hi.done || !hi.it.Next() {
			hi.done = true
			return chunkenc.ValNone
		}
		ts, h := hi.it.At()
		if ts < hi.mint {
			continue
		}
		if ts > hi.maxt {
			hi.done = true
			return chunkenc.ValNone
		}
		hi.started = true
		hi.curTS, hi.curH = ts, h
		return chunkenc.ValHistogram
	}
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
// integer counts") - and real Histogram.ToFloat's exact bucket representation
// (vendor/.../model/histogram/histogram.go): FloatHistogram.PositiveBuckets/
// NegativeBuckets are each bucket's ABSOLUTE count directly, NOT spatially
// delta-encoded the way Histogram's own integer buckets are ("Each represents an
// absolute count", float_histogram.go's own doc comment). Cheap here since
// HistogramIterator already tracks absolute per-bucket counts internally
// (it.it.posAbs/negAbs) - just a []int64 -> []float64 conversion, no re-encoding.
//
// This used to incorrectly delta-encode the result (deltaEncodeFloat, since
// removed) - a real, latent bug: the only existing test exercising this path
// checked Count/Sum but never the buckets themselves, so it went uncaught until
// this was re-derived carefully against real Histogram.ToFloat while building
// native FloatHistogram storage. Fixed here; see
// TestHistogramSampleIteratorAtFloatHistogramBucketsAreAbsolute.
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
	fh.PositiveBuckets = int64ToFloat64Slice(hi.it.posAbs)
	fh.NegativeBuckets = int64ToFloat64Slice(hi.it.negAbs)
	return hi.curTS, fh
}

// int64ToFloat64Slice converts abs's absolute per-bucket counts to float64 - no
// delta re-encoding, matching FloatHistogram.PositiveBuckets/NegativeBuckets'
// already-absolute representation (see AtFloatHistogram's own doc comment).
func int64ToFloat64Slice(abs []int64) []float64 {
	if len(abs) == 0 {
		return nil
	}
	out := make([]float64, len(abs))
	for i, v := range abs {
		out[i] = float64(v)
	}
	return out
}

func (hi *histogramSampleIterator) AtT() int64 { return hi.curTS }
func (hi *histogramSampleIterator) Err() error { return nil }

// floatHistogramSampleIterator adapts *HistogramIterator to chunkenc.Iterator for a
// genuinely float-typed series (HistogramStore.IsFloat true) - the mirror image of
// histogramSampleIterator's integer path. Next()/Seek() report chunkenc.
// ValFloatHistogram (not ValHistogram), AtFloatHistogram returns the REAL stored
// float data directly (no int->float conversion - contrast with
// histogramSampleIterator.AtFloatHistogram, which converts), and AtHistogram panics -
// there is no lossless float->int conversion, matching real Prometheus's own
// floatHistogramIterator precedent (vendor/.../tsdb/chunkenc/floathistogram.go).
type floatHistogramSampleIterator struct {
	it         *HistogramIterator
	mint, maxt int64
	started    bool
	done       bool
	curTS      int64
	curFH      *histogram.FloatHistogram
}

var _ chunkenc.Iterator = (*floatHistogramSampleIterator)(nil)

func (hi *floatHistogramSampleIterator) Next() chunkenc.ValueType {
	for {
		if hi.done || !hi.it.Next() {
			hi.done = true
			return chunkenc.ValNone
		}
		ts, fh := hi.it.AtFloat()
		if ts < hi.mint {
			continue
		}
		if ts > hi.maxt {
			hi.done = true
			return chunkenc.ValNone
		}
		hi.started = true
		hi.curTS, hi.curFH = ts, fh
		return chunkenc.ValFloatHistogram
	}
}

func (hi *floatHistogramSampleIterator) Seek(t int64) chunkenc.ValueType {
	if hi.done {
		return chunkenc.ValNone
	}
	if hi.started && hi.curTS >= t {
		return chunkenc.ValFloatHistogram
	}
	for {
		if vt := hi.Next(); vt == chunkenc.ValNone {
			return chunkenc.ValNone
		}
		if hi.curTS >= t {
			return chunkenc.ValFloatHistogram
		}
	}
}

// At panics, matching real Prometheus's own floatHistogramIterator precedent
// exactly - a caller that saw ValFloatHistogram from Next()/Seek() should never
// call this.
func (hi *floatHistogramSampleIterator) At() (int64, float64) {
	panic("columnarhead: cannot call floatHistogramSampleIterator.At")
}

// AtHistogram panics: unlike the integer->float direction
// (histogramSampleIterator.AtFloatHistogram), converting float counts back to
// integer counts would be lossy, and real Prometheus's own chunk iterators don't
// attempt it either.
func (hi *floatHistogramSampleIterator) AtHistogram(*histogram.Histogram) (int64, *histogram.Histogram) {
	panic("columnarhead: cannot call floatHistogramSampleIterator.AtHistogram - float->int would be lossy")
}

func (hi *floatHistogramSampleIterator) AtFloatHistogram(dst *histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	if dst != nil {
		*dst = *hi.curFH
		return hi.curTS, dst
	}
	return hi.curTS, hi.curFH
}

func (hi *floatHistogramSampleIterator) AtT() int64 { return hi.curTS }
func (hi *floatHistogramSampleIterator) Err() error { return nil }
