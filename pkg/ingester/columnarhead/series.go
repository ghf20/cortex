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

// maxSampleBits is a conservative worst-case bound on bits written by one Append call:
// timestamp worst case (4-bit prefix + 64-bit raw fallback = 68) plus value worst case
// (2-bit prefix + 5 + 6 + 64-bit fallback = 77). Used only to decide whether a slot needs
// to grow before writing, so overshooting is safe - it just grows slightly earlier than
// the exact bit count would strictly require.
const maxSampleBits = 145

// SeriesStore holds columnar, pointerless per-series state: a tight parallel-slice
// record per series, plus a shared bit arena carrying delta-of-delta timestamps and
// Gorilla XOR-encoded values, interleaved per sample. See columnar-head-design.md §3.1
// and §3.2.
//
// Each series' slot grows geometrically (like append) rather than using one large fixed
// size. Growing copies the series' bits to a fresh, larger region at the end of arena;
// the old region is abandoned, not reclaimed - full compaction/reclaim (bench/05's
// approach, or tying reclaim to chunk-cut/flush boundaries the way Prometheus's real
// head does) is a follow-up, not solved here.
type SeriesStore struct {
	targetID []uint16
	nameID   []uint16
	localRef []uint16
	bitOff   []uint16
	nSamples []uint16

	slotOff []uint32 // byte offset of this series' current slot within arena
	slotCap []uint32 // current slot capacity, in bytes

	val []valueState
	ts  []tsState

	arena []byte
}

// NewSeriesStore returns an empty store with capacity preallocated for expectedSeries.
func NewSeriesStore(expectedSeries int) *SeriesStore {
	return &SeriesStore{
		targetID: make([]uint16, 0, expectedSeries),
		nameID:   make([]uint16, 0, expectedSeries),
		localRef: make([]uint16, 0, expectedSeries),
		bitOff:   make([]uint16, 0, expectedSeries),
		nSamples: make([]uint16, 0, expectedSeries),
		slotOff:  make([]uint32, 0, expectedSeries),
		slotCap:  make([]uint32, 0, expectedSeries),
		val:      make([]valueState, 0, expectedSeries),
		ts:       make([]tsState, 0, expectedSeries),
		arena:    make([]byte, 0, expectedSeries*initialSlotBytes),
	}
}

// Create allocates a new series and returns its ref.
func (s *SeriesStore) Create(targetID, nameID, localRef uint16) uint32 {
	ref := uint32(len(s.targetID))
	s.targetID = append(s.targetID, targetID)
	s.nameID = append(s.nameID, nameID)
	s.localRef = append(s.localRef, localRef)
	s.bitOff = append(s.bitOff, 0)
	s.nSamples = append(s.nSamples, 0)
	s.val = append(s.val, newValueState())
	s.ts = append(s.ts, tsState{})

	off := uint32(len(s.arena))
	s.arena = append(s.arena, make([]byte, initialSlotBytes)...)
	s.slotOff = append(s.slotOff, off)
	s.slotCap = append(s.slotCap, initialSlotBytes)
	return ref
}

// NumSeries returns the number of series created so far.
func (s *SeriesStore) NumSeries() int {
	return len(s.targetID)
}

// Append encodes one (timestamp, value) sample for the series at ref. Timestamps are
// not required to be monotonic here - that validation belongs to the Appender layer
// (not yet built), which is also where out-of-order handling belongs.
func (s *SeriesStore) Append(ref uint32, ts int64, v float64) error {
	n := s.nSamples[ref]
	if n == math.MaxUint16 {
		return ErrTooManySamples
	}

	off := uint32(s.bitOff[ref])
	cap := s.slotCap[ref]
	for off+maxSampleBits > cap*8 {
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
// updating slotOff/slotCap. The old region is left behind, unreclaimed.
func (s *SeriesStore) growSlot(ref uint32, newCap uint32) {
	oldOff := s.slotOff[ref]
	usedBytes := (uint32(s.bitOff[ref]) + 7) / 8

	newOff := uint32(len(s.arena))
	s.arena = append(s.arena, make([]byte, newCap)...)
	copy(s.arena[newOff:newOff+usedBytes], s.arena[oldOff:oldOff+usedBytes])

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
