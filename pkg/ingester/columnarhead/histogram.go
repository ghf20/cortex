package columnarhead

import (
	"errors"

	"github.com/prometheus/prometheus/model/histogram"
)

// ErrHistogramLayoutChanged is returned when a layout-changed sample's own bucket
// count doesn't match its declared spans - a violated assumption (spans are supposed
// to determine bucket count consistently), not an expected mid-stream event any more.
// A genuine schema/zero-threshold/span change no longer errors at all: see
// histoSegment's own doc comment for why it now starts a new segment instead.
var ErrHistogramLayoutChanged = errors.New("columnarhead: histogram bucket count doesn't match its declared spans")

// ErrHistogramTypeChanged is returned when a series' first histogram sample was one
// of *histogram.Histogram/*histogram.FloatHistogram and a later sample on the same
// ref is the other - real Prometheus semantics don't allow a series' sample type to
// change mid-stream any more than switching between float and histogram samples
// does (see HistogramStore's own doc comment on that). Unlike a schema/zero-
// threshold/span change (histoSegment), this still fails loudly rather than starting
// a new segment - out of scope for the mid-stream-layout-change work (CHECKLIST.md's
// Phase 3 explicitly scopes that to schema/zero-threshold/span, not sample type).
var ErrHistogramTypeChanged = errors.New("columnarhead: series switched between Histogram and FloatHistogram samples - unsupported")

// histoInitialArenaBytes is a histogram segment's starting arena allocation, doubled
// via simple append-growth on demand (see growHistoSeg) - not the shared-arena/
// free-list machinery SeriesStore's float path uses (series.go). That's a real,
// separate, already-proven technique; unifying histogram storage with it is future
// work, stated explicitly rather than silently assumed away.
const histoInitialArenaBytes = 64

// histoSegment is one layout-stable run of a series' histogram samples: a delta/XOR-
// encoded bit arena using the same primitives as the float path (writeBits/
// writeVarbit/writeValue/writeTimestamp from bits.go/tsenc.go/valenc.go), reused
// rather than reinvented. Schema, zero threshold, and span layout are fixed for a
// segment's whole life (sameLayout/sameLayoutFloat) - this is what makes cross-sample
// per-bucket delta encoding well-defined within it: matching span layout guarantees a
// matching, index-aligned bucket count every time.
//
// A series (histoSeries) is a SEQUENCE of these, not a single one: real Prometheus
// histogram chunks handle a schema/zero-threshold/span change by starting a new
// chunk with the new layout (chunkenc.HistogramAppender.AppendHistogram's own
// recode-or-new-chunk contract - the same shape chunk_querier.go's counter-reset
// handling already uses), not by rejecting the series outright. A previous version
// of this type WAS the whole series (one fixed layout, ErrHistogramLayoutChanged on
// any change) - segmenting it is what lets Append/AppendFloat now start a new
// segment instead of erroring, matching that real behavior. Nothing downstream
// needs to know: HistogramIterator walks segments transparently, and
// HistogramStore.Truncate's decode-then-re-Append round trip re-segments on replay
// for free, no special-casing.
//
// The integer path (isFloat == false on the owning histoSeries) delta-encodes each
// bucket's ABSOLUTE count as a varbit integer relative to the previous sample's
// absolute count for that same bucket position - cheap for the common case (typical
// bucket deltas are small integers). Real Histogram.PositiveBuckets/NegativeBuckets
// are themselves spatially delta-encoded (each element relative to the PREVIOUS
// BUCKET, not the previous sample) - absoluteBuckets/deltaEncode convert between
// that and this store's own per-position absolute values.
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
type histoSegment struct {
	schema        int32
	zeroThreshold float64
	posSpans      []histogram.Span
	negSpans      []histogram.Span
	// customValues holds the custom (usually upper) bucket bounds for a schema
	// -53 (histogram.CustomBucketsSchema, "NHCB") segment, nil otherwise. Real
	// Prometheus semantics: only used when schema is -53, in which case
	// zeroThreshold/negSpans (and the int/float path's zeroCount/negBuckets
	// equivalents) are themselves unused - a real NHCB sample always carries
	// them as their zero value (see vendor's Histogram.Copy(), which explicitly
	// zeroes them for a custom-bucket histogram rather than copying whatever
	// was there), so nothing here needs to special-case skipping their own
	// encode/decode: they just naturally round-trip as zero/nil. posSpans/
	// posBuckets are reused UNCHANGED from the exponential-schema path - real
	// NHCB buckets are span+delta-encoded exactly the same way, just with
	// custom-values-derived boundaries instead of schema-derived ones.
	customValues []float64

	arena  []byte
	bitOff uint32

	ts  tsState
	sum valueState

	// Integer path only (owning histoSeries.isFloat == false).
	lastZeroCount  uint64
	lastCount      uint64
	lastPosBuckets []int64 // absolute per-bucket counts from the previous sample
	lastNegBuckets []int64

	// Float path only (owning histoSeries.isFloat == true) - one XOR value-stream
	// per bucket position, sized once at segment creation (bucket count is fixed
	// for the segment's life, same as the integer path).
	zeroCountVal valueState
	countVal     valueState
	posVal       []valueState
	negVal       []valueState

	nSamples uint32
}

// newHistoSegment starts a fresh integer-path segment with the given layout.
// customValues is nil for an exponential-schema segment (see histoSegment's own
// doc comment for the schema -53/NHCB case).
func newHistoSegment(schema int32, zeroThreshold float64, posSpans, negSpans []histogram.Span, customValues []float64) *histoSegment {
	return &histoSegment{
		schema:        schema,
		zeroThreshold: zeroThreshold,
		posSpans:      append([]histogram.Span(nil), posSpans...),
		negSpans:      append([]histogram.Span(nil), negSpans...),
		customValues:  append([]float64(nil), customValues...),
		arena:         make([]byte, histoInitialArenaBytes),
		sum:           newValueState(),
	}
}

// newHistoSegmentFloat starts a fresh float-path segment with the given layout and
// bucket counts (nPos/nNeg) - the per-bucket XOR windows must be sized up front,
// unlike the integer path's varbit encoding which needs no per-position state.
func newHistoSegmentFloat(schema int32, zeroThreshold float64, posSpans, negSpans []histogram.Span, customValues []float64, nPos, nNeg int) *histoSegment {
	seg := newHistoSegment(schema, zeroThreshold, posSpans, negSpans, customValues)
	seg.zeroCountVal = newValueState()
	seg.countVal = newValueState()
	seg.posVal = newValueStates(nPos)
	seg.negVal = newValueStates(nNeg)
	return seg
}

// histoSeries is one series' full histogram value stream, as a sequence of
// layout-stable segments (see histoSegment's own doc comment for why). isFloat picks
// which of histoSegment's two disjoint field groups every segment uses and which
// encoding scheme Append/AppendFloat use - a series is either integer-count or
// float-count for its whole life (ErrHistogramTypeChanged rejects switching),
// mirroring how HistogramStore itself is a separate store from SeriesStore's plain
// floats: real Prometheus doesn't let a series' sample kind change mid-stream, and
// this format's cross-sample delta/XOR encoding depends on that being true within a
// segment.
type histoSeries struct {
	isFloat  bool
	segments []*histoSegment
}

// lastSegment returns the series' current (most recently appended-to) segment, or
// nil if the series has none yet - only possible transiently inside Append/
// AppendFloat before they create the first one, never in a stored *histoSeries
// (HistogramStore never keeps an entry with zero segments - see Truncate's doc
// comment on why an all-aged-out series is deleted from the map entirely instead).
func (s *histoSeries) lastSegment() *histoSegment {
	if len(s.segments) == 0 {
		return nil
	}
	return s.segments[len(s.segments)-1]
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
// its histogram stream on first use. A schema/zero-threshold/span change from the
// current segment's layout starts a fresh segment (histoSegment's own doc comment)
// rather than erroring - only a genuine bucket-count/span mismatch WITHIN a
// (supposedly) stable layout, or a Histogram/FloatHistogram type switch, still does.
// Schema -53 (custom bucket boundaries, "NHCB") is accepted like any other schema -
// its PositiveSpans/PositiveBuckets are span+delta-encoded exactly the same way, the
// only difference is CustomValues instead of schema-derived bucket boundaries (see
// histoSegment's own doc comment).
func (hst *HistogramStore) Append(ref uint32, ts int64, h *histogram.Histogram) error {
	s, ok := hst.series[ref]
	if !ok {
		s = &histoSeries{}
		hst.series[ref] = s
	} else if s.isFloat {
		return ErrHistogramTypeChanged
	}

	seg := s.lastSegment()
	if seg == nil || !sameLayout(seg, h) {
		seg = newHistoSegment(h.Schema, h.ZeroThreshold, h.PositiveSpans, h.NegativeSpans, h.CustomValues)
		s.segments = append(s.segments, seg)
	}

	posAbs := absoluteBuckets(h.PositiveBuckets)
	negAbs := absoluteBuckets(h.NegativeBuckets)
	if seg.nSamples > 0 && (len(posAbs) != len(seg.lastPosBuckets) || len(negAbs) != len(seg.lastNegBuckets)) {
		// Spans matched (sameLayout passed) but bucket count differs. Shouldn't
		// happen if spans truly determine bucket count consistently - guarded rather
		// than trusted, so a violated assumption fails loudly instead of silently
		// misaligning the delta stream.
		return ErrHistogramLayoutChanged
	}

	// Conservative but real upper bound on bits this sample needs, so the growth loop
	// below is provably sufficient rather than tuned to a workload: ts (68 worst
	// case) + sum (77, matching series.go's own value worst-case) + counterResetHint
	// (2, fixed-width raw) + zeroCount+count (136: two varbit fields, 68 worst case
	// each, covers both the first-sample-raw-64 and subsequent-delta-varbit cases
	// since 68 > 64) + one varbit (68 worst case) per bucket.
	needBits := uint32(68+77+2+136) + uint32(len(posAbs)+len(negAbs))*68
	growHistoSeg(seg, needBits)

	n := seg.nSamples
	seg.bitOff = writeTimestamp(seg.arena, 0, seg.bitOff, ts, &seg.ts, n)
	seg.bitOff = writeValue(seg.arena, 0, seg.bitOff, h.Sum, &seg.sum, n == 0)
	// CounterResetHint is raw per-sample state, not delta/XOR-encoded like
	// everything else here - it's a real, independent signal per sample (real
	// Prometheus semantics: GaugeType/CounterReset must be explicitly honored
	// by AppendHistogram regardless of what the bucket-comparison heuristic
	// alone would conclude - vendor/.../tsdb/chunkenc/histogram.go's own
	// AppendHistogram), not a value with any useful cross-sample structure to
	// exploit. 2 bits covers all 4 real values (Unknown/CounterReset/
	// NotCounterReset/Gauge - histogram.CounterResetHint is a byte with only
	// those defined).
	seg.bitOff = writeBits(seg.arena, 0, seg.bitOff, uint64(h.CounterResetHint), 2)

	if n == 0 {
		seg.bitOff = writeBits(seg.arena, 0, seg.bitOff, h.ZeroCount, 64)
		seg.bitOff = writeBits(seg.arena, 0, seg.bitOff, h.Count, 64)
		for _, v := range posAbs {
			seg.bitOff = writeVarbit(seg.arena, 0, seg.bitOff, v)
		}
		for _, v := range negAbs {
			seg.bitOff = writeVarbit(seg.arena, 0, seg.bitOff, v)
		}
	} else {
		seg.bitOff = writeVarbit(seg.arena, 0, seg.bitOff, int64(h.ZeroCount)-int64(seg.lastZeroCount))
		seg.bitOff = writeVarbit(seg.arena, 0, seg.bitOff, int64(h.Count)-int64(seg.lastCount))
		for i, v := range posAbs {
			seg.bitOff = writeVarbit(seg.arena, 0, seg.bitOff, v-seg.lastPosBuckets[i])
		}
		for i, v := range negAbs {
			seg.bitOff = writeVarbit(seg.arena, 0, seg.bitOff, v-seg.lastNegBuckets[i])
		}
	}

	seg.lastZeroCount = h.ZeroCount
	seg.lastCount = h.Count
	seg.lastPosBuckets = posAbs
	seg.lastNegBuckets = negAbs
	seg.nSamples = n + 1
	return nil
}

// AppendFloat encodes one float-count histogram sample for the series at ref,
// creating its histogram stream on first use - the FloatHistogram counterpart to
// Append, see histoSegment's own doc comment for why the encoding scheme genuinely
// differs (per-bucket gorilla XOR, not per-bucket varbit delta) rather than being a
// thin parallel path, and for the same start-a-new-segment-on-layout-change
// behavior Append has. Schema -53 (NHCB) accepted the same way Append's own doc
// comment describes.
func (hst *HistogramStore) AppendFloat(ref uint32, ts int64, h *histogram.FloatHistogram) error {
	s, ok := hst.series[ref]
	if !ok {
		s = &histoSeries{isFloat: true}
		hst.series[ref] = s
	} else if !s.isFloat {
		return ErrHistogramTypeChanged
	}

	seg := s.lastSegment()
	if seg == nil || !sameLayoutFloat(seg, h) {
		seg = newHistoSegmentFloat(h.Schema, h.ZeroThreshold, h.PositiveSpans, h.NegativeSpans, h.CustomValues, len(h.PositiveBuckets), len(h.NegativeBuckets))
		s.segments = append(s.segments, seg)
	}

	if seg.nSamples > 0 && (len(h.PositiveBuckets) != len(seg.posVal) || len(h.NegativeBuckets) != len(seg.negVal)) {
		// Same guard Append has, for the same reason - spans matched but bucket
		// count differs shouldn't happen, fail loudly rather than misalign.
		return ErrHistogramLayoutChanged
	}

	// Worst case per XOR-encoded value is 77 bits (1+1+5+6+64 - a "new window"
	// write; see valenc.go/writeValue and series.go's own identical comment for
	// the sum field), applied here to sum, zeroCount, count, and every bucket -
	// all genuinely independent value-streams under this scheme. +2 for
	// CounterResetHint (raw, not XOR-encoded - see Append's identical field for
	// why).
	needBits := uint32(68+77*3+2) + uint32(len(h.PositiveBuckets)+len(h.NegativeBuckets))*77
	growHistoSeg(seg, needBits)

	n := seg.nSamples
	first := n == 0
	seg.bitOff = writeTimestamp(seg.arena, 0, seg.bitOff, ts, &seg.ts, n)
	seg.bitOff = writeValue(seg.arena, 0, seg.bitOff, h.Sum, &seg.sum, first)
	seg.bitOff = writeBits(seg.arena, 0, seg.bitOff, uint64(h.CounterResetHint), 2)
	seg.bitOff = writeValue(seg.arena, 0, seg.bitOff, h.ZeroCount, &seg.zeroCountVal, first)
	seg.bitOff = writeValue(seg.arena, 0, seg.bitOff, h.Count, &seg.countVal, first)
	for i, v := range h.PositiveBuckets {
		seg.bitOff = writeValue(seg.arena, 0, seg.bitOff, v, &seg.posVal[i], first)
	}
	for i, v := range h.NegativeBuckets {
		seg.bitOff = writeValue(seg.arena, 0, seg.bitOff, v, &seg.negVal[i], first)
	}

	seg.nSamples = n + 1
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

// growHistoSeg doubles seg.arena until it has room for needBits more, starting from
// histoInitialArenaBytes - simple append-growth, not the free-list/reuse machinery
// series.go's growSlot has (see the HistogramStore doc comment on why).
func growHistoSeg(seg *histoSegment, needBits uint32) {
	for seg.bitOff+needBits > uint32(len(seg.arena))*8 {
		seg.arena = append(seg.arena, make([]byte, len(seg.arena))...)
	}
}

// sameLayout also compares customValues via histogram.CustomBucketBoundsMatch - a
// harmless no-op for a non-NHCB segment (both sides nil, so it's always true),
// the real check that catches a genuine custom-bucket-boundary change for an
// NHCB one (schema -53's own zeroThreshold/negSpans are always their zero value
// on a well-formed sample - see histoSegment's own doc comment - so the existing
// checks below already trivially pass for those; CustomValues is the one field
// that actually distinguishes one NHCB layout from another).
func sameLayout(seg *histoSegment, h *histogram.Histogram) bool {
	if seg.schema != h.Schema || seg.zeroThreshold != h.ZeroThreshold {
		return false
	}
	if !spansEqual(seg.posSpans, h.PositiveSpans) || !spansEqual(seg.negSpans, h.NegativeSpans) {
		return false
	}
	return histogram.CustomBucketBoundsMatch(seg.customValues, h.CustomValues)
}

// sameLayoutFloat is sameLayout's FloatHistogram counterpart - see its own doc
// comment.
func sameLayoutFloat(seg *histoSegment, h *histogram.FloatHistogram) bool {
	if seg.schema != h.Schema || seg.zeroThreshold != h.ZeroThreshold {
		return false
	}
	if !spansEqual(seg.posSpans, h.PositiveSpans) || !spansEqual(seg.negSpans, h.NegativeSpans) {
		return false
	}
	return histogram.CustomBucketBoundsMatch(seg.customValues, h.CustomValues)
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

// decodedCounterResetHint converts a decoded 2-bit raw CounterResetHint value
// into what a reader should actually see: GaugeType passes through unchanged,
// everything else (Unknown/CounterReset/NotCounterReset, all raw-stored as
// written - see Append/AppendFloat) collapses to UnknownCounterReset.
//
// This is deliberately NOT a literal echo of whatever was appended, even
// though the bits ARE stored verbatim. Real Prometheus's own chunk-level
// readback does not echo the appended hint either: it recomputes one from
// chunk position (chunkenc/histogram_meta.go's counterResetHint(header,
// numRead) - GaugeType if the chunk is gauge-typed, NotCounterReset for any
// sample after the chunk's first, UnknownCounterReset otherwise, even when the
// chunk's own header says CounterReset - "we have to return unknown... even
// if we know", its own comment says, since a reader can't trust two chunks
// are truly consecutive). columnarhead's histoSegment boundaries don't track
// counter-reset boundaries at all (HistogramStore's own doc comment: it never
// models or rejects a counter reset - only chunk_querier.go's real
// chunkenc.HistogramAppender does that, from bucket comparison, independent
// of whatever hint it's handed - already verified correct without this field
// existing at all, see TestChunkQuerierHistogramSeriesCounterResetSplitsChunk),
// so there is no position-based signal here to reproduce faithfully for the
// Counter-type cases. GaugeType is different: it's a genuinely stable,
// series-level property real AppendHistogram branches on immediately (its
// very first check), not something bucket comparison can infer after the
// fact - dropping it would make a real gauge histogram get chunk-encoded as
// if it were a counter, causing spurious reset-driven chunk splits on
// ordinary gauge fluctuation. That's the one distinction this format commits
// to actually preserving.
func decodedCounterResetHint(raw uint64) histogram.CounterResetHint {
	if histogram.CounterResetHint(raw) == histogram.GaugeType {
		return histogram.GaugeType
	}
	return histogram.UnknownCounterReset
}

// HistogramIterator replays a histogram series' encoded samples in order, across
// every layout segment in turn (see histoSegment's own doc comment) - either
// integer- or float-typed for the whole series (see HistogramStore.IsFloat), never
// both. Matching the "wrong accessor panics" convention this package's other
// dual-type iterators already use (querier.go's floatSampleIterator/
// histogramSampleIterator), At panics on a float-typed series and AtFloat panics on
// an integer-typed one - a caller is expected to check IsFloat (or the
// chunkenc.ValueType Next-equivalent one layer up) before choosing which to call,
// not call both defensively.
type HistogramIterator struct {
	s      *histoSeries
	segIdx int
	seg    *histoSegment
	off    uint32
	i      uint32
	total  uint32

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
	return &HistogramIterator{s: s, segIdx: -1}
}

// Next advances to the next sample, returning false when exhausted - moving to the
// next segment transparently once the current one runs out (the it.i >= it.total
// loop below), instead of stopping there.
func (it *HistogramIterator) Next() bool {
	if it.s == nil {
		return false
	}
	for it.i >= it.total {
		it.segIdx++
		if it.segIdx >= len(it.s.segments) {
			return false
		}
		it.startSegment(it.s.segments[it.segIdx])
	}
	if it.s.isFloat {
		return it.nextFloat()
	}
	return it.nextInt()
}

// startSegment resets every per-segment decode scratch field for seg - each segment
// restarts its own ts/sum/bucket encoding windows from scratch (a segment's first
// sample is always written "raw", never delta/XOR-relative to the PREVIOUS
// segment's last sample - see Append/AppendFloat), and bucket-count-sized scratch
// slices must be resized since bucket count can genuinely differ across a layout
// change.
func (it *HistogramIterator) startSegment(seg *histoSegment) {
	it.seg = seg
	it.off = 0
	it.i = 0
	it.total = seg.nSamples
	it.ts = tsState{}
	it.sum = newValueState()
	if it.s.isFloat {
		it.zeroCountVal = newValueState()
		it.countVal = newValueState()
		it.posVal = newValueStates(len(seg.posVal))
		it.negVal = newValueStates(len(seg.negVal))
		it.posF = make([]float64, len(seg.posVal))
		it.negF = make([]float64, len(seg.negVal))
	} else {
		it.posAbs = make([]int64, len(seg.lastPosBuckets))
		it.negAbs = make([]int64, len(seg.lastNegBuckets))
	}
}

func (it *HistogramIterator) nextInt() bool {
	ts, off := readTimestamp(it.seg.arena, 0, it.off, &it.ts, it.i)
	sum, off2 := readValue(it.seg.arena, 0, off, &it.sum, it.i == 0)
	off = off2
	hintBits, off3 := readBits(it.seg.arena, 0, off, 2)
	off = off3

	if it.i == 0 {
		zc, o := readBits(it.seg.arena, 0, off, 64)
		c, o2 := readBits(it.seg.arena, 0, o, 64)
		off = o2
		it.zeroCount, it.count = zc, c
		for j := range it.posAbs {
			v, o3 := readVarbit(it.seg.arena, 0, off)
			it.posAbs[j], off = v, o3
		}
		for j := range it.negAbs {
			v, o3 := readVarbit(it.seg.arena, 0, off)
			it.negAbs[j], off = v, o3
		}
	} else {
		dzc, o := readVarbit(it.seg.arena, 0, off)
		dc, o2 := readVarbit(it.seg.arena, 0, o)
		off = o2
		it.zeroCount = uint64(int64(it.zeroCount) + dzc)
		it.count = uint64(int64(it.count) + dc)
		for j := range it.posAbs {
			d, o3 := readVarbit(it.seg.arena, 0, off)
			it.posAbs[j] += d
			off = o3
		}
		for j := range it.negAbs {
			d, o3 := readVarbit(it.seg.arena, 0, off)
			it.negAbs[j] += d
			off = o3
		}
	}

	it.curTS = ts
	it.curH = &histogram.Histogram{
		CounterResetHint: decodedCounterResetHint(hintBits),
		Schema:           it.seg.schema,
		ZeroThreshold:    it.seg.zeroThreshold,
		ZeroCount:        it.zeroCount,
		Count:            it.count,
		Sum:              sum,
		PositiveSpans:    it.seg.posSpans,
		NegativeSpans:    it.seg.negSpans,
		PositiveBuckets:  deltaEncode(it.posAbs),
		NegativeBuckets:  deltaEncode(it.negAbs),
		CustomValues:     it.seg.customValues,
	}
	it.off = off
	it.i++
	return true
}

func (it *HistogramIterator) nextFloat() bool {
	first := it.i == 0
	ts, off := readTimestamp(it.seg.arena, 0, it.off, &it.ts, it.i)
	sum, off := readValue(it.seg.arena, 0, off, &it.sum, first)
	hintBits, off := readBits(it.seg.arena, 0, off, 2)
	zc, off := readValue(it.seg.arena, 0, off, &it.zeroCountVal, first)
	c, off := readValue(it.seg.arena, 0, off, &it.countVal, first)
	it.zeroCountF, it.countF = zc, c
	for j := range it.posF {
		v, o := readValue(it.seg.arena, 0, off, &it.posVal[j], first)
		it.posF[j], off = v, o
	}
	for j := range it.negF {
		v, o := readValue(it.seg.arena, 0, off, &it.negVal[j], first)
		it.negF[j], off = v, o
	}

	it.curTS = ts
	it.curFH = &histogram.FloatHistogram{
		CounterResetHint: decodedCounterResetHint(hintBits),
		Schema:           it.seg.schema,
		ZeroThreshold:    it.seg.zeroThreshold,
		ZeroCount:        it.zeroCountF,
		Count:            it.countF,
		Sum:              sum,
		PositiveSpans:    it.seg.posSpans,
		NegativeSpans:    it.seg.negSpans,
		PositiveBuckets:  append([]float64(nil), it.posF...),
		NegativeBuckets:  append([]float64(nil), it.negF...),
		CustomValues:     it.seg.customValues,
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
// Re-Appending the kept samples naturally re-segments them exactly as a fresh
// ingest would (Append/AppendFloat start a new segment on any layout change) - no
// special-casing needed here for a retained range that happens to span a layout
// change.
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
