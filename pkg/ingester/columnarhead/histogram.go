package columnarhead

import (
	"errors"

	"github.com/prometheus/prometheus/model/histogram"
)

// ErrHistogramLayoutChanged is returned when a histogram sample's schema, zero
// threshold, or span layout doesn't match the series' first sample. Real Prometheus
// histograms support schema/span changes over time (e.g. an exponential bucket schema
// upgrade); this prototype does not - see CHECKLIST.md for why that's a deliberate,
// stated scope limit, not an oversight. Design doc §8 already calls native histograms
// "the largest single implementation item" of the whole project; this covers the
// stable-layout common case, not full generality.
var ErrHistogramLayoutChanged = errors.New("columnarhead: histogram schema/zero-threshold/span layout changed - unsupported by this prototype")

// ErrCustomBucketsUnsupported is returned for schema -53 (custom bucket bounds via
// CustomValues). Not implemented - see CHECKLIST.md.
var ErrCustomBucketsUnsupported = errors.New("columnarhead: custom bucket boundaries (schema -53) are not supported by this prototype")

// ErrFloatHistogramUnsupported is returned by the storage.HistogramAppender path when
// only a *histogram.FloatHistogram is given (h == nil, fh != nil). Only integer-count
// native histograms (*histogram.Histogram) are implemented - see CHECKLIST.md.
var ErrFloatHistogramUnsupported = errors.New("columnarhead: FloatHistogram is not supported by this prototype, only Histogram")

// histoInitialArenaBytes is a histogram series' starting arena allocation, doubled via
// simple append-growth on demand (see growHisto) - not the shared-arena/free-list
// machinery SeriesStore's float path uses (series.go). That's a real, separate,
// already-proven technique; unifying histogram storage with it is future work, stated
// explicitly rather than silently assumed away.
const histoInitialArenaBytes = 64

// histoSeries is one series' histogram value stream: a delta/XOR-encoded bit arena
// using the same primitives as the float path (writeBits/writeVarbit/writeValue/
// writeTimestamp from bits.go/tsenc.go/valenc.go), reused rather than reinvented.
// Schema, zero threshold, and span layout are fixed at the first sample and validated
// unchanged on every subsequent one (sameLayout) - this is what makes cross-sample
// per-bucket delta encoding well-defined: matching span layout guarantees a matching,
// index-aligned bucket count every time.
type histoSeries struct {
	schema        int32
	zeroThreshold float64
	posSpans      []histogram.Span
	negSpans      []histogram.Span

	arena  []byte
	bitOff uint32

	ts  tsState
	sum valueState

	lastZeroCount  uint64
	lastCount      uint64
	lastPosBuckets []int64 // absolute per-bucket counts from the previous sample
	lastNegBuckets []int64

	nSamples uint32
}

// HistogramStore holds histogram-typed series, keyed by the same series refs Head's
// SeriesStore assigns for float series - a given series is either float-typed (in
// SeriesStore) or histogram-typed (here), matching real Prometheus semantics that a
// series' sample type doesn't change mid-stream. A plain Go map, not a columnar slab:
// per-series state here (spans, scratch bucket slices) doesn't fit a fixed-width
// record the way float series do, and histogram series are the minority case this
// prototype is scoped for.
type HistogramStore struct {
	series map[uint32]*histoSeries
}

// NewHistogramStore returns an empty store.
func NewHistogramStore() *HistogramStore {
	return &HistogramStore{series: make(map[uint32]*histoSeries)}
}

// NumSeries returns the number of histogram series created so far.
func (hst *HistogramStore) NumSeries() int {
	return len(hst.series)
}

// Has reports whether ref has any histogram samples - the read path's way of telling
// a histogram-typed series from a float-typed one, since both share the same ref
// space (see the type's doc comment).
func (hst *HistogramStore) Has(ref uint32) bool {
	_, ok := hst.series[ref]
	return ok
}

// Append encodes one histogram sample for the series at ref, creating its histogram
// stream on first use.
func (hst *HistogramStore) Append(ref uint32, ts int64, h *histogram.Histogram) error {
	if histogram.IsCustomBucketsSchema(h.Schema) {
		return ErrCustomBucketsUnsupported
	}

	s, ok := hst.series[ref]
	if !ok {
		s = &histoSeries{
			schema:        h.Schema,
			zeroThreshold: h.ZeroThreshold,
			posSpans:      append([]histogram.Span(nil), h.PositiveSpans...),
			negSpans:      append([]histogram.Span(nil), h.NegativeSpans...),
			arena:         make([]byte, histoInitialArenaBytes),
			sum:           newValueState(),
		}
		hst.series[ref] = s
	} else if !sameLayout(s, h) {
		return ErrHistogramLayoutChanged
	}

	posAbs := absoluteBuckets(h.PositiveBuckets)
	negAbs := absoluteBuckets(h.NegativeBuckets)
	if s.nSamples > 0 && (len(posAbs) != len(s.lastPosBuckets) || len(negAbs) != len(s.lastNegBuckets)) {
		// Spans matched (sameLayout passed) but bucket count differs. Shouldn't
		// happen if spans truly determine bucket count consistently - guarded rather
		// than trusted, so a violated assumption fails loudly instead of silently
		// misaligning the delta stream.
		return ErrHistogramLayoutChanged
	}

	// Conservative but real upper bound on bits this sample needs, so the growth loop
	// below is provably sufficient rather than tuned to a workload: ts (68 worst
	// case) + sum (77, matching series.go's own value worst-case) + zeroCount+count
	// (136: two varbit fields, 68 worst case each, covers both the first-sample-raw-64
	// and subsequent-delta-varbit cases since 68 > 64) + one varbit (68 worst case)
	// per bucket.
	needBits := uint32(68+77+136) + uint32(len(posAbs)+len(negAbs))*68
	growHisto(s, needBits)

	n := s.nSamples
	s.bitOff = writeTimestamp(s.arena, 0, s.bitOff, ts, &s.ts, n)
	s.bitOff = writeValue(s.arena, 0, s.bitOff, h.Sum, &s.sum, n == 0)

	if n == 0 {
		s.bitOff = writeBits(s.arena, 0, s.bitOff, h.ZeroCount, 64)
		s.bitOff = writeBits(s.arena, 0, s.bitOff, h.Count, 64)
		for _, v := range posAbs {
			s.bitOff = writeVarbit(s.arena, 0, s.bitOff, v)
		}
		for _, v := range negAbs {
			s.bitOff = writeVarbit(s.arena, 0, s.bitOff, v)
		}
	} else {
		s.bitOff = writeVarbit(s.arena, 0, s.bitOff, int64(h.ZeroCount)-int64(s.lastZeroCount))
		s.bitOff = writeVarbit(s.arena, 0, s.bitOff, int64(h.Count)-int64(s.lastCount))
		for i, v := range posAbs {
			s.bitOff = writeVarbit(s.arena, 0, s.bitOff, v-s.lastPosBuckets[i])
		}
		for i, v := range negAbs {
			s.bitOff = writeVarbit(s.arena, 0, s.bitOff, v-s.lastNegBuckets[i])
		}
	}

	s.lastZeroCount = h.ZeroCount
	s.lastCount = h.Count
	s.lastPosBuckets = posAbs
	s.lastNegBuckets = negAbs
	s.nSamples = n + 1
	return nil
}

// growHisto doubles s.arena until it has room for needBits more, starting from
// histoInitialArenaBytes - simple append-growth, not the free-list/reuse machinery
// series.go's growSlot has (see the HistogramStore doc comment on why).
func growHisto(s *histoSeries, needBits uint32) {
	for s.bitOff+needBits > uint32(len(s.arena))*8 {
		s.arena = append(s.arena, make([]byte, len(s.arena))...)
	}
}

func sameLayout(s *histoSeries, h *histogram.Histogram) bool {
	if s.schema != h.Schema || s.zeroThreshold != h.ZeroThreshold {
		return false
	}
	return spansEqual(s.posSpans, h.PositiveSpans) && spansEqual(s.negSpans, h.NegativeSpans)
}

func spansEqual(a, b []histogram.Span) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// absoluteBuckets converts Prometheus's own in-memory bucket representation (first
// element absolute, rest are deltas relative to the previous element) into plain
// absolute counts - the form this store's cross-sample delta encoding needs.
func absoluteBuckets(deltas []int64) []int64 {
	if len(deltas) == 0 {
		return nil
	}
	out := make([]int64, len(deltas))
	var cur int64
	for i, d := range deltas {
		cur += d
		out[i] = cur
	}
	return out
}

// deltaEncode is absoluteBuckets's inverse: converts plain absolute counts back into
// Prometheus's expected first-absolute-then-relative-deltas representation, for
// handing decoded samples back out through HistogramIterator.
func deltaEncode(abs []int64) []int64 {
	if len(abs) == 0 {
		return nil
	}
	out := make([]int64, len(abs))
	var prev int64
	for i, v := range abs {
		out[i] = v - prev
		prev = v
	}
	return out
}

// HistogramIterator replays a histogram series' encoded samples in order.
type HistogramIterator struct {
	s     *histoSeries
	off   uint32
	i     uint32
	total uint32

	ts  tsState
	sum valueState

	zeroCount uint64
	count     uint64
	posAbs    []int64
	negAbs    []int64

	curTS int64
	curH  *histogram.Histogram
}

// Iterator returns a fresh HistogramIterator over ref's currently encoded samples. An
// unknown ref yields an iterator whose Next() always returns false, rather than a nil
// pointer panic - matching SeriesStore.Iterator's forgiving-on-empty behavior isn't
// quite right here since ref might just never have had a histogram appended (a
// legitimate, common case for a mixed-type Head), not a caller bug.
func (hst *HistogramStore) Iterator(ref uint32) *HistogramIterator {
	s := hst.series[ref]
	if s == nil {
		return &HistogramIterator{}
	}
	return &HistogramIterator{
		s:      s,
		total:  s.nSamples,
		sum:    newValueState(),
		posAbs: make([]int64, len(s.lastPosBuckets)),
		negAbs: make([]int64, len(s.lastNegBuckets)),
	}
}

// Next advances to the next sample, returning false when exhausted.
func (it *HistogramIterator) Next() bool {
	if it.s == nil || it.i >= it.total {
		return false
	}
	ts, off := readTimestamp(it.s.arena, 0, it.off, &it.ts, it.i)
	sum, off2 := readValue(it.s.arena, 0, off, &it.sum, it.i == 0)
	off = off2

	if it.i == 0 {
		zc, o := readBits(it.s.arena, 0, off, 64)
		c, o2 := readBits(it.s.arena, 0, o, 64)
		off = o2
		it.zeroCount, it.count = zc, c
		for j := range it.posAbs {
			v, o3 := readVarbit(it.s.arena, 0, off)
			it.posAbs[j], off = v, o3
		}
		for j := range it.negAbs {
			v, o3 := readVarbit(it.s.arena, 0, off)
			it.negAbs[j], off = v, o3
		}
	} else {
		dzc, o := readVarbit(it.s.arena, 0, off)
		dc, o2 := readVarbit(it.s.arena, 0, o)
		off = o2
		it.zeroCount = uint64(int64(it.zeroCount) + dzc)
		it.count = uint64(int64(it.count) + dc)
		for j := range it.posAbs {
			d, o3 := readVarbit(it.s.arena, 0, off)
			it.posAbs[j] += d
			off = o3
		}
		for j := range it.negAbs {
			d, o3 := readVarbit(it.s.arena, 0, off)
			it.negAbs[j] += d
			off = o3
		}
	}

	it.curTS = ts
	it.curH = &histogram.Histogram{
		Schema:          it.s.schema,
		ZeroThreshold:   it.s.zeroThreshold,
		ZeroCount:       it.zeroCount,
		Count:           it.count,
		Sum:             sum,
		PositiveSpans:   it.s.posSpans,
		NegativeSpans:   it.s.negSpans,
		PositiveBuckets: deltaEncode(it.posAbs),
		NegativeBuckets: deltaEncode(it.negAbs),
	}
	it.off = off
	it.i++
	return true
}

// At returns the sample most recently produced by Next.
func (it *HistogramIterator) At() (int64, *histogram.Histogram) {
	return it.curTS, it.curH
}

// Truncate drops every sample with ts < mint from ref's histogram stream, re-encoding
// the retained range as a fresh stream in place - the same decode/re-encode approach
// as SeriesStore.Truncate, for the same reason (no seek/cut point in a cross-sample
// delta-encoded stream). Returns the number of samples retained. A no-op (returns 0)
// if ref has no histogram stream at all (Has(ref) false) - not a caller bug, since a
// mixed-type Head can have plenty of refs with no histogram samples.
//
// If every sample ages out, ref's entry is dropped from the map entirely rather than
// kept as an empty placeholder: Has(ref) then reports false, and the read path falls
// back to treating ref as float-typed - which is harmless here, since SeriesStore's
// own record for a pure-histogram ref was never appended to and already reports zero
// samples on that path too. A later real histogram sample on the same ref recreates
// the entry exactly as Append already does for any first-ever sample.
func (hst *HistogramStore) Truncate(ref uint32, mint int64) int {
	if !hst.Has(ref) {
		return 0
	}
	it := hst.Iterator(ref)
	var tss []int64
	var kept []*histogram.Histogram
	for it.Next() {
		ts, h := it.At()
		if ts < mint {
			continue
		}
		tss = append(tss, ts)
		kept = append(kept, h)
	}

	delete(hst.series, ref)
	for i, ts := range tss {
		_ = hst.Append(ref, ts, kept[i])
	}
	return len(kept)
}
