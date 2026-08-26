package columnarhead

import "sort"

// This file is the OOO (out-of-order) sample support scoped in CHECKLIST.md's
// Phase 4 entry before being built - float samples only, matching Phase 2's own
// "floats only first" precedent (histogram OOO is a stated, deferred extension,
// not silently dropped).
//
// oooSample is one out-of-order (timestamp, value) pair, held separately from
// SeriesStore's arena-encoded in-order stream: that encoding has no seek/insert
// point (every sample's bits depend on all prior encoder state - see series.go's
// Truncate doc comment for the same reasoning applied there), so an OOO sample
// structurally cannot be spliced into it. Real Prometheus's own OOOChunk makes
// the identical choice for the identical reason: samples are stored
// UNCOMPRESSED, by its own doc comment ("allow easy sorting... perhaps we can be
// more efficient later") - copying that choice here is validated by their own
// scoping, not a shortcut being taken against it.
type oooSample struct {
	ts int64
	v  float64
}

// oooSeriesBuffer is one series' bounded, sorted-by-timestamp OOO float sample
// buffer.
type oooSeriesBuffer struct {
	samples []oooSample
}

// insert adds (ts, v) in sorted position, rejecting an exact-timestamp
// duplicate within the buffer itself (mirrors real Prometheus's
// OOOChunk.Insert). Returns false if ts is already present.
func (b *oooSeriesBuffer) insert(ts int64, v float64) bool {
	n := len(b.samples)
	if n == 0 || ts > b.samples[n-1].ts {
		b.samples = append(b.samples, oooSample{ts, v})
		return true
	}
	i := sort.Search(n, func(i int) bool { return b.samples[i].ts >= ts })
	if i < n && b.samples[i].ts == ts {
		return false
	}
	b.samples = append(b.samples, oooSample{})
	copy(b.samples[i+1:], b.samples[i:])
	b.samples[i] = oooSample{ts, v}
	return true
}

// trim drops every sample older than minTS - the OOO time window's boundary,
// which moves forward as the head's own max timestamp advances. This is the
// ONLY reclaim mechanism for aged-out OOO data right now: compaction-time
// promotion of surviving OOO samples into SeriesStore's own encoded stream is a
// real, stated scope cut (see CHECKLIST.md's OOO entry), not silently skipped.
func (b *oooSeriesBuffer) trim(minTS int64) {
	i := sort.Search(len(b.samples), func(i int) bool { return b.samples[i].ts >= minTS })
	if i > 0 {
		b.samples = append(b.samples[:0], b.samples[i:]...)
	}
}

// oooStore holds every series' OOO buffer, keyed by the same series refs
// SeriesStore uses. A plain Go map, like HistogramStore - OOO samples are
// expected to be the minority case in a realistic workload, not the common path
// this package's columnar design is optimized for.
type oooStore struct {
	series map[uint32]*oooSeriesBuffer
}

func newOOOStore() *oooStore {
	return &oooStore{series: make(map[uint32]*oooSeriesBuffer)}
}

// insert adds (ts, v) to ref's OOO buffer, creating it on first use. Returns
// false if ts duplicates an existing OOO entry for ref.
func (o *oooStore) insert(ref uint32, ts int64, v float64) bool {
	b := o.series[ref]
	if b == nil {
		b = &oooSeriesBuffer{}
		o.series[ref] = b
	}
	return b.insert(ts, v)
}

// trim drops ref's OOO samples older than minTS - a no-op if ref has no OOO
// buffer at all.
func (o *oooStore) trim(ref uint32, minTS int64) {
	if b := o.series[ref]; b != nil {
		b.trim(minTS)
	}
}

// samples returns ref's current OOO buffer contents, oldest first - nil if ref
// has none. The caller must not mutate the returned slice.
func (o *oooStore) samples(ref uint32) []oooSample {
	if b := o.series[ref]; b != nil {
		return b.samples
	}
	return nil
}

// floatSource is the minimal shape mergedIterator needs from an in-order
// stream - satisfied by *Iterator (series.go) without that type needing to know
// anything about OOO.
type floatSource interface {
	Next() bool
	At() (int64, float64)
}

// mergedIterator interleaves an in-order floatSource with a sorted OOO sample
// slice by timestamp, producing a single strictly-ordered stream. Real
// Prometheus does not need a bespoke merge for this - it reuses the SAME
// generic overlapping-chunks machinery it already needs for merging across
// multiple on-disk blocks (see CHECKLIST.md's OOO scoping pass for the exact
// citation). That machinery assumes multi-chunk-per-series storage, which this
// package's Head.ChunkQuerier explicitly does not have (one chunk per series,
// stated simplification) - so this is the one genuinely new piece OOO needs
// here that real Prometheus gets for free elsewhere.
type mergedIterator struct {
	inOrder floatSource
	ooo     []oooSample
	oooIdx  int

	haveInOrder bool
	inOrderTS   int64
	inOrderVal  float64
	inOrderDone bool

	curTS  int64
	curVal float64
}

func newMergedIterator(inOrder floatSource, ooo []oooSample) *mergedIterator {
	return &mergedIterator{inOrder: inOrder, ooo: ooo}
}

func (m *mergedIterator) fillInOrder() {
	if m.haveInOrder || m.inOrderDone {
		return
	}
	if m.inOrder.Next() {
		m.inOrderTS, m.inOrderVal = m.inOrder.At()
		m.haveInOrder = true
	} else {
		m.inOrderDone = true
	}
}

// Next advances to the next sample in timestamp order, preferring the in-order
// side on an exact tie (arbitrary but deterministic - see this package's
// scoping notes on why an OOO sample landing on an existing in-order timestamp
// isn't specially detected, matching real Prometheus's own scope here).
func (m *mergedIterator) Next() bool {
	m.fillInOrder()
	haveOOO := m.oooIdx < len(m.ooo)
	if !m.haveInOrder && !haveOOO {
		return false
	}
	if m.haveInOrder && (!haveOOO || m.inOrderTS <= m.ooo[m.oooIdx].ts) {
		m.curTS, m.curVal = m.inOrderTS, m.inOrderVal
		m.haveInOrder = false
		return true
	}
	m.curTS, m.curVal = m.ooo[m.oooIdx].ts, m.ooo[m.oooIdx].v
	m.oooIdx++
	return true
}

func (m *mergedIterator) At() (int64, float64) { return m.curTS, m.curVal }
