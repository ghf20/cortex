package columnarhead

import "errors"

// ErrSlotFull is returned by Append when a series' fixed arena slot has no room left
// for another sample. Growable/compacting slots are a follow-up (see CHECKLIST.md).
var ErrSlotFull = errors.New("columnarhead: series arena slot full")

// defaultSlotBytes is a generous fixed per-series allocation. Real slot sizing (packed
// to actual usage, growable) is tracked as follow-up work, not solved here.
const defaultSlotBytes = 128

// maxSampleBits is a conservative worst-case bound on bits written by one Append call:
// timestamp worst case (4-bit prefix + 64-bit raw fallback = 68) plus value worst case
// (2-bit prefix + 5 + 6 + 64-bit fallback = 77). Used only for the pre-write bounds
// check, so overshooting is safe - it just rejects slightly before the slot is
// literally full rather than exactly at capacity.
const maxSampleBits = 145

// SeriesStore holds columnar, pointerless per-series state: a tight parallel-slice
// record per series, plus a shared bit arena carrying delta-of-delta timestamps and
// Gorilla XOR-encoded values, interleaved per sample. See columnar-head-design.md §3.1
// and §3.2.
type SeriesStore struct {
	targetID []uint16
	nameID   []uint16
	localRef []uint16
	bitOff   []uint16
	nSamples []uint32

	val []valueState
	ts  []tsState

	arena     []byte
	slotBytes uint32
}

// NewSeriesStore returns an empty store with capacity preallocated for expectedSeries.
func NewSeriesStore(expectedSeries int) *SeriesStore {
	return &SeriesStore{
		targetID:  make([]uint16, 0, expectedSeries),
		nameID:    make([]uint16, 0, expectedSeries),
		localRef:  make([]uint16, 0, expectedSeries),
		bitOff:    make([]uint16, 0, expectedSeries),
		nSamples:  make([]uint32, 0, expectedSeries),
		val:       make([]valueState, 0, expectedSeries),
		ts:        make([]tsState, 0, expectedSeries),
		arena:     make([]byte, 0, expectedSeries*defaultSlotBytes),
		slotBytes: defaultSlotBytes,
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
	s.arena = append(s.arena, make([]byte, s.slotBytes)...)
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
	base := ref * s.slotBytes
	off := uint32(s.bitOff[ref])
	n := s.nSamples[ref]

	if off+maxSampleBits > s.slotBytes*8 {
		return ErrSlotFull
	}

	off = writeTimestamp(s.arena, base, off, ts, &s.ts[ref], n)
	off = writeValue(s.arena, base, off, v, &s.val[ref], n == 0)

	s.bitOff[ref] = uint16(off)
	s.nSamples[ref] = n + 1
	return nil
}

// Iterator replays a series' encoded samples in order from the start of its slot.
type Iterator struct {
	arena []byte
	base  uint32
	off   uint32
	total uint32
	i     uint32

	val valueState
	ts  tsState

	curTS  int64
	curVal float64
}

// Iterator returns a fresh Iterator over the series at ref's currently encoded samples.
func (s *SeriesStore) Iterator(ref uint32) *Iterator {
	return &Iterator{
		arena: s.arena,
		base:  ref * s.slotBytes,
		total: s.nSamples[ref],
		val:   newValueState(),
	}
}

// Next advances to the next sample, returning false when exhausted.
func (it *Iterator) Next() bool {
	if it.i >= it.total {
		return false
	}
	ts, off := readTimestamp(it.arena, it.base, it.off, &it.ts, it.i)
	v, off2 := readValue(it.arena, it.base, off, &it.val, it.i == 0)
	it.curTS, it.curVal, it.off = ts, v, off2
	it.i++
	return true
}

// At returns the sample most recently produced by Next.
func (it *Iterator) At() (int64, float64) {
	return it.curTS, it.curVal
}
