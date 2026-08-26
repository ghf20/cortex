package columnarhead

import (
	"errors"
	"math"
)

// ErrTooManySamples is returned by Append if a series' in-flight sample count would
// overflow uint16 (65,535) - far beyond any realistic pre-flush sample count, so this
// exists only to fail loudly instead of silently wrapping.
var ErrTooManySamples = errors.New("columnarhead: series sample count overflow")

// initialSlotBytes is a series' starting arena allocation: enough for exactly one raw
// (timestamp, value) sample (64+64 bits = 16 bytes). Slots grow geometrically from here
// as needed (see growSlot) rather than reserving a large fixed slot up front - most
// series in a realistic workload (near-constant gauges, in particular) never need more.
const initialSlotBytes = 16

// firstSampleBits is the exact (not worst-case) cost of a series' first sample: a raw
// 64-bit timestamp plus a raw 64-bit value, both always written in full. Using the exact
// figure here (rather than maxSampleBits below) matters: every series pays this cost
// once, so an unnecessarily conservative bound here would force a needless grow event
// for every single series right out of the gate.
const firstSampleBits = 128

// maxSampleBits is a conservative worst-case bound on bits written by one Append call
// past the first sample: timestamp worst case (4-bit prefix + 64-bit raw fallback = 68)
// plus value worst case (2-bit prefix + 5 + 6 + 64-bit fallback = 77). Used only to
// decide whether a slot needs to grow before writing, so overshooting is safe - it just
// grows slightly earlier than the exact bit count would strictly require.
const maxSampleBits = 145

// SeriesStore holds columnar, pointerless per-series state: a tight parallel-slice
// record per series, plus a shared bit arena carrying delta-of-delta timestamps and
// Gorilla XOR-encoded values, interleaved per sample. See columnar-head-design.md §3.1
// and §3.2.
//
// Each series' slot grows geometrically (like append) rather than using one large fixed
// size. Growing moves the series' bits to a fresh, larger region and frees the old one
// into a size-classed free list (freeList), so a later Create or grow of a matching size
// reuses it instead of the arena growing unboundedly. Measured: without this, geometric
// growth alone is no better than one large fixed slot (see CHECKLIST.md) - reuse is what
// actually recovers the space. Reclaim is still partial: an abandoned region only helps
// if something else later requests exactly that size class: no splitting, no merging
// across classes. Full compaction (bench/05's approach) would do better; not built here.
type SeriesStore struct {
	// targetID is uint32, not uint16 like the other id fields: nameID/localRef come
	// from bounded vocabularies (metric names, "le"-style local label values) that
	// don't grow with fleet churn, but every new pod/target IS a new targetID, and a
	// live head accumulates those indefinitely over uptime - 65,536 target creations
	// is a realistic thing to hit in a high-churn cluster within weeks, not a
	// theoretical edge case. See CHECKLIST.md for the measured cost of this choice.
	targetID []uint32
	nameID   []uint16
	// localName/localRef are the name and value symbol refs of a series' one
	// supported non-target, non-__name__ label (see appender.go's
	// ErrUnsupportedLabelShape). Both are needed to reconstruct a series' full label
	// set on the read path - storing only the value (localRef alone, the original
	// shape of this struct) was enough for write-side dedup but made it impossible to
	// tell "le" from "quantile" from anything else when querying. Found and fixed
	// while building the Querier, not before - this store had no read path to expose
	// the gap until then.
	localName []uint16
	localRef  []uint16
	// hasLocal records, independent of localName/localRef's values, whether a series
	// actually has the extra label at all. 0 is a legitimate real id from liveInterner
	// (whichever string happens to be interned first) - localName/localRef == 0 does
	// NOT mean "absent," so absence needs its own explicit signal, not an inferred one.
	hasLocal []bool
	bitOff   []uint16
	nSamples []uint16

	// generation counts how many times Truncate has re-encoded this series' bytes
	// from scratch AT THE SAME slotOff - durability.go's Flush needs this: ordinary
	// Append only ever grows a series' used-byte count (or moves it to a new
	// slotOff via growSlot, itself detectable), but Truncate can shrink it, or
	// even coincidentally leave it unchanged, while completely rewriting the
	// bytes underneath - a byte-count-only high-water-mark comparison cannot
	// distinguish "nothing changed" from "truncated to the same size," so this
	// exists as an unambiguous signal. Bumped only by Truncate, never by Append/
	// growSlot (their own slotOff change already signals "reflush from scratch").
	generation []uint32

	slotOff []uint32 // byte offset of this series' current slot within arena
	slotCap []uint32 // current slot capacity, in bytes

	val []valueState
	ts  []tsState

	arena    []byte
	freeList map[uint32][]uint32 // size class (bytes) -> free byte offsets of that size

	// Diagnostics: how often alloc reused a freed region, and the free list's net
	// effect on arena growth for this run. AllocBytesRequested sums size across every
	// alloc() call (hit or miss); len(arena) only grows on misses - so
	// AllocBytesRequested-len(arena) is exactly how many bytes of fresh arena growth
	// the free list avoided, with no need for a separate control run.
	AllocHits, AllocMisses uint64
	AllocBytesRequested    uint64
}

// NewSeriesStore returns an empty store with capacity preallocated for expectedSeries.
func NewSeriesStore(expectedSeries int) *SeriesStore {
	return &SeriesStore{
		targetID:   make([]uint32, 0, expectedSeries),
		nameID:     make([]uint16, 0, expectedSeries),
		localName:  make([]uint16, 0, expectedSeries),
		localRef:   make([]uint16, 0, expectedSeries),
		hasLocal:   make([]bool, 0, expectedSeries),
		bitOff:     make([]uint16, 0, expectedSeries),
		nSamples:   make([]uint16, 0, expectedSeries),
		generation: make([]uint32, 0, expectedSeries),
		slotOff:    make([]uint32, 0, expectedSeries),
		slotCap:    make([]uint32, 0, expectedSeries),
		val:        make([]valueState, 0, expectedSeries),
		ts:         make([]tsState, 0, expectedSeries),
		arena:      make([]byte, 0, expectedSeries*initialSlotBytes),
		freeList:   make(map[uint32][]uint32),
	}
}

// alloc returns the byte offset of a zeroed region of exactly size bytes, reusing a
// freed region of the same size class if one exists. Zeroing is not just cleanliness:
// writeBits ORs new bits into arena, so a reused region with stale bits from its
// previous occupant would silently corrupt the new series' encoding.
//
// Free-list reuse is safe for DurableHead's durability too (see durability.go's
// package doc comment and generation/flushedSlotOff's comments) - per-series flush
// tracking (keyed by slotOff and a generation counter, not a single arena-wide
// high-water mark) correctly detects when a series' current bytes at its current
// slotOff need reflushing from scratch, regardless of whether that slotOff was
// freshly appended or reused from a different series. An earlier version of this
// package disabled reuse for durable stores based on a mistaken belief that reuse
// itself was the problem; per-series tracking already handles it, proven by
// TestDurableHeadSurvivesChainedReuse (mutation-tested: the check it exercises,
// removed, reproduces real data corruption after reload).
func (s *SeriesStore) alloc(size uint32) uint32 {
	s.AllocBytesRequested += uint64(size)
	if free := s.freeList[size]; len(free) > 0 {
		off := free[len(free)-1]
		s.freeList[size] = free[:len(free)-1]
		clear(s.arena[off : off+size])
		s.AllocHits++
		return off
	}
	s.AllocMisses++
	off := uint32(len(s.arena))
	s.arena = append(s.arena, make([]byte, size)...)
	return off
}

// free returns a size-byte region at off to the free list for reuse.
func (s *SeriesStore) free(off, size uint32) {
	s.freeList[size] = append(s.freeList[size], off)
}

// Create allocates a new series and returns its ref. hasLocal is false for a series
// with no extra label - localName/localRef are meaningless (and typically both 0) in
// that case, matching how appender.go treats an empty localLabel.
func (s *SeriesStore) Create(targetID uint32, nameID, localName, localRef uint16, hasLocal bool) uint32 {
	ref := uint32(len(s.targetID))
	s.targetID = append(s.targetID, targetID)
	s.nameID = append(s.nameID, nameID)
	s.localName = append(s.localName, localName)
	s.localRef = append(s.localRef, localRef)
	s.hasLocal = append(s.hasLocal, hasLocal)
	s.bitOff = append(s.bitOff, 0)
	s.nSamples = append(s.nSamples, 0)
	s.generation = append(s.generation, 0)
	s.val = append(s.val, newValueState())
	s.ts = append(s.ts, tsState{})

	off := s.alloc(initialSlotBytes)
	s.slotOff = append(s.slotOff, off)
	s.slotCap = append(s.slotCap, initialSlotBytes)
	return ref
}

// NumSeries returns the number of series created so far.
func (s *SeriesStore) NumSeries() int {
	return len(s.targetID)
}

// TargetID, NameID, LocalName, LocalRef, HasLocal expose a series' record fields for
// the read path (reconstructing its full label set) - see Head.SeriesLabels.
func (s *SeriesStore) TargetID(ref uint32) uint32  { return s.targetID[ref] }
func (s *SeriesStore) NameID(ref uint32) uint16    { return s.nameID[ref] }
func (s *SeriesStore) LocalName(ref uint32) uint16 { return s.localName[ref] }
func (s *SeriesStore) LocalRef(ref uint32) uint16  { return s.localRef[ref] }
func (s *SeriesStore) HasLocal(ref uint32) bool    { return s.hasLocal[ref] }

// Append encodes one (timestamp, value) sample for the series at ref. Timestamps are
// not required to be monotonic here - that validation belongs to the Appender layer
// (not yet built), which is also where out-of-order handling belongs.
func (s *SeriesStore) Append(ref uint32, ts int64, v float64) error {
	n := s.nSamples[ref]
	if n == math.MaxUint16 {
		return ErrTooManySamples
	}

	need := uint32(maxSampleBits)
	if n == 0 {
		need = firstSampleBits
	}
	off := uint32(s.bitOff[ref])
	cap := s.slotCap[ref]
	for off+need > cap*8 {
		cap *= 2
	}
	if cap != s.slotCap[ref] {
		s.growSlot(ref, cap)
	}
	base := s.slotOff[ref]

	off = writeTimestamp(s.arena, base, off, ts, &s.ts[ref], uint32(n))
	off = writeValue(s.arena, base, off, v, &s.val[ref], n == 0)

	s.bitOff[ref] = uint16(off)
	s.nSamples[ref] = n + 1
	return nil
}

// growSlot moves ref's encoded bits into a fresh region of arena with capacity newCap,
// updating slotOff/slotCap, and frees the old region for reuse by a future alloc of the
// same size.
func (s *SeriesStore) growSlot(ref uint32, newCap uint32) {
	oldOff, oldCap := s.slotOff[ref], s.slotCap[ref]
	usedBytes := (uint32(s.bitOff[ref]) + 7) / 8

	newOff := s.alloc(newCap)
	copy(s.arena[newOff:newOff+usedBytes], s.arena[oldOff:oldOff+usedBytes])
	s.free(oldOff, oldCap)

	s.slotOff[ref] = newOff
	s.slotCap[ref] = newCap
}

// Iterator replays a series' encoded samples in order from the start of its slot.
type Iterator struct {
	arena []byte
	base  uint32
	off   uint32
	total uint16
	i     uint16

	val valueState
	ts  tsState

	curTS  int64
	curVal float64
}

// Iterator returns a fresh Iterator over the series at ref's currently encoded samples.
func (s *SeriesStore) Iterator(ref uint32) *Iterator {
	return &Iterator{
		arena: s.arena,
		base:  s.slotOff[ref],
		total: s.nSamples[ref],
		val:   newValueState(),
	}
}

// Next advances to the next sample, returning false when exhausted.
func (it *Iterator) Next() bool {
	if it.i >= it.total {
		return false
	}
	ts, off := readTimestamp(it.arena, it.base, it.off, &it.ts, uint32(it.i))
	v, off2 := readValue(it.arena, it.base, off, &it.val, it.i == 0)
	it.curTS, it.curVal, it.off = ts, v, off2
	it.i++
	return true
}

// At returns the sample most recently produced by Next.
func (it *Iterator) At() (int64, float64) {
	return it.curTS, it.curVal
}

// Truncate drops every sample with ts < mint from ref's stream, re-encoding the
// retained range as a fresh stream in place. Returns the number of samples retained -
// 0 means every existing sample was older than mint, and ref stays allocated with an
// empty stream rather than being removed (see Head.Truncate's doc comment on why).
//
// There is no seek/cut point in this format to make this an offset move: every
// sample's encoding depends on all prior encoder state (delta-of-delta timestamps,
// Gorilla XOR values - see tsState/valueState), so truncating means fully decoding the
// retained range and re-encoding it from scratch, exactly as if every retained sample
// were being appended for the first time. The old bits aren't reused (bitOff/nSamples/
// ts/val reset to zero state before re-appending) but the slot itself is - re-Append
// never needs to grow past ref's current slotCap, since the retained range is a subset
// of what already fit there.
//
// Bumps generation: unlike ordinary Append (which only ever grows bitOff, or moves to
// a new slotOff via growSlot), this rewrites ref's bytes from scratch AT THE SAME
// slotOff, and the resulting byte count can shrink or even coincidentally match what
// was there before - durability.go's Flush needs an unambiguous "this was rewritten"
// signal that doesn't depend on comparing sizes (see generation's own doc comment).
func (s *SeriesStore) Truncate(ref uint32, mint int64) int {
	it := s.Iterator(ref)
	var tss []int64
	var vs []float64
	for it.Next() {
		ts, v := it.At()
		if ts < mint {
			continue
		}
		tss = append(tss, ts)
		vs = append(vs, v)
	}

	// writeBits ORs new bits into arena (see alloc's doc comment) - re-encoding into
	// the old, un-cleared region would silently corrupt the result with leftover
	// bits from the previous, longer encoding. Must clear before resetting bitOff.
	base, cap := s.slotOff[ref], s.slotCap[ref]
	clear(s.arena[base : base+cap])
	s.generation[ref]++

	s.bitOff[ref] = 0
	s.nSamples[ref] = 0
	s.ts[ref] = tsState{}
	s.val[ref] = newValueState()

	for i, ts := range tss {
		// Append only fails past 65,535 samples (ErrTooManySamples) - impossible here
		// since tss is a subset of what already fit under that same limit.
		_ = s.Append(ref, ts, vs[i])
	}
	return len(tss)
}
