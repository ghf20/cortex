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

// ErrHistogramTypeChanged is returned when a series' first histogram sample was one
// of *histogram.Histogram/*histogram.FloatHistogram and a later sample on the same
// ref is the other - real Prometheus semantics don't allow a series' sample type to
// change mid-stream any more than switching between float and histogram samples
// does (see HistogramStore's own doc comment on that). Failing loudly here matches
// ErrHistogramLayoutChanged's own posture: a violated assumption should error, not
// silently misinterpret one representation's bits as the other's.
var ErrHistogramTypeChanged = errors.New("columnarhead: series switched between Histogram and FloatHistogram samples - unsupported")

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
// unchanged on every subsequent one (sameLayout/sameLayoutFloat) - this is what makes
// cross-sample per-bucket delta encoding well-defined: matching span layout guarantees
// a matching, index-aligned bucket count every time.
//
// isFloat picks which of the two disjoint field groups below is populated and which
// encoding scheme Append/AppendFloat use - a series is either integer-count or
// float-count for its whole life (ErrHistogramTypeChanged rejects switching), mirroring
// how HistogramStore itself is a separate store from SeriesStore's plain floats: real
// Prometheus doesn't let a series' sample kind change mid-stream, and this format's
// cross-sample delta encoding depends on that being true.
//
// The integer path (isFloat == false) delta-encodes each bucket's ABSOLUTE count as a
// varbit integer relative to the previous sample's absolute count for that same bucket
// position - cheap for the common case (typical bucket deltas are small integers).
// Real Histogram.PositiveBuckets/NegativeBuckets are themselves spatially delta-encoded
// (each element relative to the PREVIOUS BUCKET, not the previous sample) -
// absoluteBuckets/deltaEncode convert between that and this store's own per-position
// absolute values.
//
// The float path (isFloat == true) cannot reuse that scheme: real
// FloatHistogram.PositiveBuckets/NegativeBuckets are already absolute per-bucket
// float64 counts (no spatial delta at all - "Each represents an absolute count", see
// vendor/.../model/histogram/float_histogram.go), and a float value has no useful
// "small integer delta" structure to exploit the way an int one does. Reused instead:
// the SAME gorilla XOR float encoding sum/regular float samples already use
// (writeValue/valenc.go), just applied PER BUCKET POSITION - one independent
// valueState per bucket, since XOR encoding needs its own leading/trailing/lastBits
// window per value-stream, not a single shared one.
type histoSeries struct {
	schema        int32
	zeroThreshold float64
	posSpans      []histogram.Span
	negSpans      []histogram.Span

	isFloat bool

	arena  []byte
	bitOff uint32

	ts  tsState
	sum valueState

	// Integer path only (isFloat == false).
	lastZeroCount  uint64
	lastCount      uint64
	lastPosBuckets []int64 // absolute per-bucket counts from the previous sample
	lastNegBuckets []int64

	// Float path only (isFloat == true) - one XOR value-stream per bucket
	// position, sized once at series creation (bucket count is fixed for the
	// series' life, same as the integer path).
	zeroCountVal valueState
	countVal     valueState
	posVal       []valueState
	negVal       []valueState

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

// Has reports whether ref has any histogram samples (integer or float) - the read
// path's way of telling a histogram-typed series from a float-typed one, since both
// share the same ref space (see the type's doc comment).
func (hst *HistogramStore) Has(ref uint32) bool {
	_, ok := hst.series[ref]
	return ok
}

// IsFloat reports whether ref's histogram samples are FloatHistogram-typed (true) or
// Histogram-typed (false) - meaningless (and returns false) if Has(ref) is false. The
// read path's way of choosing between HistogramIterator's At/AtFloat accessors, and
// between Head.ChunkQuerier's int/float chunk encoding paths.
func (hst *HistogramStore) IsFloat(ref uint32) bool {
	s := hst.series[ref]
	return s != nil && s.isFloat
}

// Append encodes one integer-count histogram sample for the series at ref, creating
// its histogram stream on first use.
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
	} else if s.isFloat {
		return ErrHistogramTypeChanged
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

// AppendFloat encodes one float-count histogram sample for the series at ref,
// creating its histogram stream on first use - the FloatHistogram counterpart to
// Append, see histoSeries' own doc comment for why the encoding scheme genuinely
// differs (per-bucket gorilla XOR, not per-bucket varbit delta) rather than being a
// thin parallel path.
func (hst *HistogramStore) AppendFloat(ref uint32, ts int64, h *histogram.FloatHistogram) error {
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
			isFloat:       true,
			zeroCountVal:  newValueState(),
			countVal:      newValueState(),
			posVal:        newValueStates(len(h.PositiveBuckets)),
			negVal:        newValueStates(len(h.NegativeBuckets)),
		}
		hst.series[ref] = s
	} else if !s.isFloat {
		return ErrHistogramTypeChanged
	} else if !sameLayoutFloat(s, h) {
		return ErrHistogramLayoutChanged
	}

	if s.nSamples > 0 && (len(h.PositiveBuckets) != len(s.posVal) || len(h.NegativeBuckets) != len(s.negVal)) {
		// Same guard Append has, for the same reason - spans matched but bucket
		// count differs shouldn't happen, fail loudly rather than misalign.
		return ErrHistogramLayoutChanged
	}

	// Worst case per XOR-encoded value is 77 bits (1+1+5+6+64 - a "new window"
	// write; see valenc.go/writeValue and series.go's own identical comment for
	// the sum field), applied here to sum, zeroCount, count, and every bucket -
	// all genuinely independent value-streams under this scheme.
	needBits := uint32(68+77*3) + uint32(len(h.PositiveBuckets)+len(h.NegativeBuckets))*77
	growHisto(s, needBits)

	n := s.nSamples
	first := n == 0
	s.bitOff = writeTimestamp(s.arena, 0, s.bitOff, ts, &s.ts, n)
	s.bitOff = writeValue(s.arena, 0, s.bitOff, h.Sum, &s.sum, first)
	s.bitOff = writeValue(s.arena, 0, s.bitOff, h.ZeroCount, &s.zeroCountVal, first)
	s.bitOff = writeValue(s.arena, 0, s.bitOff, h.Count, &s.countVal, first)
	for i, v := range h.PositiveBuckets {
		s.bitOff = writeValue(s.arena, 0, s.bitOff, v, &s.posVal[i], first)
	}
	for i, v := range h.NegativeBuckets {
		s.bitOff = writeValue(s.arena, 0, s.bitOff, v, &s.negVal[i], first)
	}

	s.nSamples = n + 1
	return nil
}

// newValueStates returns n freshly-initialized valueStates - each bucket position's
// own independent XOR window needs newValueState()'s noWindow sentinel, not the
// slice's zero value.
func newValueStates(n int) []valueState {
	if n == 0 {
		return nil
	}
	out := make([]valueState, n)
	for i := range out {
		out[i] = newValueState()
	}
	return out
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

func sameLayoutFloat(s *histoSeries, h *histogram.FloatHistogram) bool {
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

// HistogramIterator replays a histogram series' encoded samples in order - either
// integer- or float-typed (see HistogramStore.IsFloat), never both for the same
// series. Matching the "wrong accessor panics" convention this package's other
// dual-type iterators already use (querier.go's floatSampleIterator/
// histogramSampleIterator), At panics on a float-typed series and AtFloat panics on
// an integer-typed one - a caller is expected to check IsFloat (or the
// chunkenc.ValueType Next-equivalent one layer up) before choosing which to call,
// not call both defensively.
type HistogramIterator struct {
	s     *histoSeries
	off   uint32
	i     uint32
	total uint32

	ts  tsState
	sum valueState

	// Integer path scratch state.
	zeroCount uint64
	count     uint64
	posAbs    []int64
	negAbs    []int64

	// Float path scratch state.
	zeroCountVal valueState
	countVal     valueState
	posVal       []valueState
	negVal       []valueState
	zeroCountF   float64
	countF       float64
	posF         []float64
	negF         []float64

	curTS int64
	curH  *histogram.Histogram
	curFH *histogram.FloatHistogram
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
	it := &HistogramIterator{
		s:     s,
		total: s.nSamples,
		sum:   newValueState(),
	}
	if s.isFloat {
		it.zeroCountVal = newValueState()
		it.countVal = newValueState()
		it.posVal = newValueStates(len(s.posVal))
		it.negVal = newValueStates(len(s.negVal))
		it.posF = make([]float64, len(s.posVal))
		it.negF = make([]float64, len(s.negVal))
	} else {
		it.posAbs = make([]int64, len(s.lastPosBuckets))
		it.negAbs = make([]int64, len(s.lastNegBuckets))
	}
	return it
}

// Next advances to the next sample, returning false when exhausted.
func (it *HistogramIterator) Next() bool {
	if it.s == nil || it.i >= it.total {
		return false
	}
	if it.s.isFloat {
		return it.nextFloat()
	}
	return it.nextInt()
}

func (it *HistogramIterator) nextInt() bool {
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

func (it *HistogramIterator) nextFloat() bool {
	first := it.i == 0
	ts, off := readTimestamp(it.s.arena, 0, it.off, &it.ts, it.i)
	sum, off := readValue(it.s.arena, 0, off, &it.sum, first)
	zc, off := readValue(it.s.arena, 0, off, &it.zeroCountVal, first)
	c, off := readValue(it.s.arena, 0, off, &it.countVal, first)
	it.zeroCountF, it.countF = zc, c
	for j := range it.posF {
		v, o := readValue(it.s.arena, 0, off, &it.posVal[j], first)
		it.posF[j], off = v, o
	}
	for j := range it.negF {
		v, o := readValue(it.s.arena, 0, off, &it.negVal[j], first)
		it.negF[j], off = v, o
	}

	it.curTS = ts
	it.curFH = &histogram.FloatHistogram{
		Schema:          it.s.schema,
		ZeroThreshold:   it.s.zeroThreshold,
		ZeroCount:       it.zeroCountF,
		Count:           it.countF,
		Sum:             sum,
		PositiveSpans:   it.s.posSpans,
		NegativeSpans:   it.s.negSpans,
		PositiveBuckets: append([]float64(nil), it.posF...),
		NegativeBuckets: append([]float64(nil), it.negF...),
	}
	it.off = off
	it.i++
	return true
}

// At returns the sample most recently produced by Next - panics if this is a
// float-typed series (see the type's doc comment).
func (it *HistogramIterator) At() (int64, *histogram.Histogram) {
	if it.s != nil && it.s.isFloat {
		panic("columnarhead: cannot call HistogramIterator.At on a float-typed series - use AtFloat")
	}
	return it.curTS, it.curH
}

// AtFloat returns the sample most recently produced by Next - panics if this is an
// integer-typed series (see the type's doc comment).
func (it *HistogramIterator) AtFloat() (int64, *histogram.FloatHistogram) {
	if it.s == nil || !it.s.isFloat {
		panic("columnarhead: cannot call HistogramIterator.AtFloat on an integer-typed series - use At")
	}
	return it.curTS, it.curFH
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
// the entry exactly as Append/AppendFloat already does for any first-ever sample.
func (hst *HistogramStore) Truncate(ref uint32, mint int64) int {
	if !hst.Has(ref) {
		return 0
	}
	isFloat := hst.series[ref].isFloat
	it := hst.Iterator(ref)

	if isFloat {
		var tss []int64
		var kept []*histogram.FloatHistogram
		for it.Next() {
			ts, h := it.AtFloat()
			if ts < mint {
				continue
			}
			tss = append(tss, ts)
			kept = append(kept, h)
		}
		delete(hst.series, ref)
		for i, ts := range tss {
			_ = hst.AppendFloat(ref, ts, kept[i])
		}
		return len(kept)
	}

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
