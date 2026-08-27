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
	// No exact __name__ matcher - full scan fallback. Reads h.nextRef directly,
	// NOT the self-locking Head.NumSeries() - q already holds indexMu.RLock()
	// for its whole lifetime (Head.Querier's own doc comment), and Go's
	// sync.RWMutex explicitly warns recursive RLock() can deadlock once a
	// Lock() call is pending elsewhere: confirmed the hard way (a real,
	// previously-latent hazard TestHeadConcurrentAppendQueryTruncateCompact
	// caught the moment Truncate started taking indexMu.Lock() too - nothing
	// contended for the write lock before that, so the recursive read was
	// harmless until then). Matches every other Head read method here
	// (SeriesLabels, SeriesRefsForName, etc.) already assuming the caller
	// holds the lock, rather than re-acquiring it.
	all := make([]uint32, q.h.nextRef)
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
	// h.nextRef directly, not Head.NumSeries() - see candidateRefs' identical
	// note on why the self-locking accessor deadlocks here.
	n := q.h.nextRef
	for ref := uint32(0); ref < n; ref++ {
		lbls := q.h.SeriesLabels(ref)
		if !matchesAll(lbls, matchers) {
			continue
		}
		if !q.seriesHasSampleInRange(ref) {
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
	// h.nextRef directly, not Head.NumSeries() - see candidateRefs' identical
	// note on why the self-locking accessor deadlocks here.
	n := q.h.nextRef
	for ref := uint32(0); ref < n; ref++ {
		lbls := q.h.SeriesLabels(ref)
		if !matchesAll(lbls, matchers) {
			continue
		}
		if !q.seriesHasSampleInRange(ref) {
			continue
		}
		lbls.Range(func(l labels.Label) { seen[l.Name] = struct{}{} })
	}
	return sortedKeys(seen), nil, nil
}

// seriesHasSampleInRange reports whether ref has at least one sample inside
// [q.mint, q.maxt], matching real tsdb.Head's LabelNames/LabelValues (backed by
// head.indexRange(mint,maxt) there - see TestHeadLabelNamesValuesWithMinMaxRange in
// upstream tsdb/head_test.go): a series whose samples fall entirely outside the
// queried window must not contribute its labels, even though Select's own series
// membership is intentionally looser (headSeries.Iterator's doc comment). Reuses
// headSeries.Iterator - the same bounded, mixed-type-aware construction Select
// already relies on - rather than duplicating its float/histogram/OOO merge logic.
func (q *headQuerier) seriesHasSampleInRange(ref uint32) bool {
	it := (&headSeries{h: q.h, ref: ref, mint: q.mint, maxt: q.maxt}).Iterator(nil)
	return it.Next() != chunkenc.ValNone
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

// Iterator returns a chunkenc.Iterator over s's samples, bounded to [s.mint, s.maxt].
// This package stores float samples (SeriesStore) and histogram samples
// (HistogramStore) in entirely separate stores keyed by the same ref - real
// Prometheus keeps no such split (a real series can genuinely mix float and
// histogram samples over its lifetime, which is exactly why PromQL's
// resets()/changes()/delta()/idelta()/irate() all treat a type change as an implicit
// reset or emit a specific warning for it). A series that received samples of only
// one kind gets that store's iterator directly; one that received both (Append and
// AppendHistogram both accept either kind on any series unconditionally, no error
// either way) gets a mixedTypeIterator merging both stores' streams by timestamp -
// see mixed_iterator.go. Found via promqltest's `functions.test` (a real upstream
// test file exercising exactly this shape, `path="/bar"`'s mixed load block) - the
// float prefix used to be silently invisible to every reader (queries,
// resets()/changes()' reset detection, everything); see CHECKLIST.md for the
// characterization before this was fixed. The passed-in iterator (for reuse) is
// ignored; this always allocates fresh, unlike real chunk iterators that support
// in-place reuse - a real optimization opportunity, not attempted here.
func (s *headSeries) Iterator(_ chunkenc.Iterator) chunkenc.Iterator {
	hasHist := s.h.HasHistogram(s.ref)
	hasFloat := s.h.HasFloat(s.ref)

	var histIt chunkenc.Iterator
	if hasHist {
		if s.h.HasFloatHistogram(s.ref) {
			histIt = &floatHistogramSampleIterator{it: s.h.HistogramIterator(s.ref), mint: s.mint, maxt: s.maxt}
		} else {
			histIt = &histogramSampleIterator{it: s.h.HistogramIterator(s.ref), mint: s.mint, maxt: s.maxt}
		}
	}
	newFloatIt := func() chunkenc.Iterator {
		var src floatSource = s.h.Iterator(s.ref)
		if ooo := s.h.OOOSamples(s.ref); len(ooo) > 0 {
			src = newMergedIterator(src, ooo)
		}
		return &floatSampleIterator{it: src, mint: s.mint, maxt: s.maxt}
	}

	switch {
	case hasHist && hasFloat:
		return newMixedTypeIterator(newFloatIt(), histIt)
	case hasHist:
		return histIt
	default:
		// Also the "ref exists but has zero samples of either kind yet" case (a
		// real race window: a series is created, then queried, before its first
		// Append lands) - s.h.Iterator/floatSampleIterator are always valid, even
		// empty, so this still returns a usable iterator rather than nil. Losing
		// this unconditional construction (gating it behind hasFloat like histIt
		// is gated behind hasHist) is exactly what
		// TestHeadConcurrentAppendQueryTruncateCompact caught: a nil
		// chunkenc.Iterator interface value, panicking on the reader's very next
		// Next() call - found and fixed while building this method, not by
		// inspection.
		return newFloatIt()
	}
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
// [mint, maxt] inclusive. resetState recomputes each sample's real CounterResetHint
// in Next() - see histogram_reset_hint.go's own doc comment.
type histogramSampleIterator struct {
	it         *HistogramIterator
	mint, maxt int64
	started    bool
	done       bool
	curTS      int64
	curH       *histogram.Histogram
	resetState histogramResetState
	err        error
}

var _ chunkenc.Iterator = (*histogramSampleIterator)(nil)

func (hi *histogramSampleIterator) Next() chunkenc.ValueType {
	for {
		if hi.done || hi.err != nil || !hi.it.Next() {
			hi.done = true
			return chunkenc.ValNone
		}
		ts, h := hi.it.At()
		if ts > hi.maxt {
			hi.done = true
			return chunkenc.ValNone
		}
		// resetState must see EVERY sample from the series' true start, not just
		// ones inside [mint, maxt] - real chunk boundaries are a write-time
		// property of the whole series, computed once, independent of any later
		// query's mint (a real block's chunks don't move because a query
		// happens to start mid-chunk). Feeding it only the in-window samples
		// would make every window's first sample look like a chunk boundary
		// regardless of whether the real series actually had one there -
		// exactly the bug a query starting mid-series (rather than at the
		// series' true start) surfaced (histogram_reset_hint.go's own doc
		// comment). mint only filters what's YIELDED to the caller, below.
		if err := hi.resetState.apply(h, ts); err != nil {
			hi.err = err
			hi.done = true
			return chunkenc.ValNone
		}
		if ts < hi.mint {
			continue
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
//
// CustomValues and CounterResetHint were ALSO silently dropped here until
// found while scoping the counter-reset-hint work - real Histogram.ToFloat
// copies both (vendor/.../model/histogram/histogram.go:
// "fh.CounterResetHint = h.CounterResetHint", "fh.CustomValues =
// h.CustomValues" for the custom-buckets case) - another real, latent gap the
// only existing test never exercised (built before NHCB support existed at
// all).
func (hi *histogramSampleIterator) AtFloatHistogram(fh *histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	if fh == nil {
		fh = &histogram.FloatHistogram{}
	}
	h := hi.curH
	fh.CounterResetHint = h.CounterResetHint
	fh.Schema = h.Schema
	fh.ZeroThreshold = h.ZeroThreshold
	fh.ZeroCount = float64(h.ZeroCount)
	fh.Count = float64(h.Count)
	fh.Sum = h.Sum
	fh.PositiveSpans = h.PositiveSpans
	fh.NegativeSpans = h.NegativeSpans
	fh.PositiveBuckets = int64ToFloat64Slice(hi.it.posAbs)
	fh.NegativeBuckets = int64ToFloat64Slice(hi.it.negAbs)
	fh.CustomValues = h.CustomValues
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
func (hi *histogramSampleIterator) Err() error { return hi.err }

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
	resetState floatHistogramResetState
	err        error
}

var _ chunkenc.Iterator = (*floatHistogramSampleIterator)(nil)

func (hi *floatHistogramSampleIterator) Next() chunkenc.ValueType {
	for {
		if hi.done || hi.err != nil || !hi.it.Next() {
			hi.done = true
			return chunkenc.ValNone
		}
		ts, fh := hi.it.AtFloat()
		if ts > hi.maxt {
			hi.done = true
			return chunkenc.ValNone
		}
		// See histogramSampleIterator.Next's identical comment: resetState must
		// see every sample from the series' true start, not just [mint, maxt].
		if err := hi.resetState.apply(fh, ts); err != nil {
			hi.err = err
			hi.done = true
			return chunkenc.ValNone
		}
		if ts < hi.mint {
			continue
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
func (hi *floatHistogramSampleIterator) Err() error { return hi.err }
