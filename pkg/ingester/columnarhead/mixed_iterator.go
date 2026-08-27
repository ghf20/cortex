package columnarhead

import (
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

// This file merges a series' float-typed stream (SeriesStore, wrapped by
// floatSampleIterator) and its histogram-typed stream (HistogramStore, wrapped by
// histogramSampleIterator or floatHistogramSampleIterator) into one strictly-ordered
// chunkenc.Iterator, for the case headSeries.Iterator's own doc comment describes: a
// series that received samples of both kinds, which this package used to handle by
// picking one store and silently discarding the other (found via promqltest - see
// CHECKLIST.md).
//
// Both sub-iterators are already independently time-ordered - SeriesStore and
// HistogramStore each only ever append in call order - so a plain two-way merge by
// timestamp is correct, no re-sorting needed. This mirrors real Prometheus's own
// precedent for a type-mixed series: a series' CHUNK LIST spans mixed types (each
// chunk internally homogeneous), and populateWithDelSeriesIterator
// (vendor/.../tsdb/querier.go) just walks that list in existing time order rather
// than re-sorting samples itself - the two stores here play the same role its two
// (or more) differently-typed chunks would.

// mixedTypeIterator merges floatIt and histIt (each already bounded to the query's
// [mint, maxt] by the caller) into one time-ordered chunkenc.Iterator.
type mixedTypeIterator struct {
	floatIt, histIt chunkenc.Iterator

	floatPeeked, floatDone bool
	histPeeked, histDone   bool
	floatVT, histVT        chunkenc.ValueType

	cur     chunkenc.Iterator
	curVT   chunkenc.ValueType
	started bool
}

func newMixedTypeIterator(floatIt, histIt chunkenc.Iterator) *mixedTypeIterator {
	return &mixedTypeIterator{floatIt: floatIt, histIt: histIt}
}

var _ chunkenc.Iterator = (*mixedTypeIterator)(nil)

func (m *mixedTypeIterator) fillFloat() {
	if m.floatPeeked || m.floatDone {
		return
	}
	if vt := m.floatIt.Next(); vt != chunkenc.ValNone {
		m.floatVT, m.floatPeeked = vt, true
	} else {
		m.floatDone = true
	}
}

func (m *mixedTypeIterator) fillHist() {
	if m.histPeeked || m.histDone {
		return
	}
	if vt := m.histIt.Next(); vt != chunkenc.ValNone {
		m.histVT, m.histPeeked = vt, true
	} else {
		m.histDone = true
	}
}

// Next advances to the next sample in timestamp order across both sides, preferring
// the float side on an exact timestamp tie - an arbitrary but deterministic
// tie-break (a real series should never actually have both a float AND a histogram
// sample at the identical timestamp), matching ooo.go's mergedIterator precedent for
// the same kind of tie.
func (m *mixedTypeIterator) Next() chunkenc.ValueType {
	m.fillFloat()
	m.fillHist()
	switch {
	case !m.floatPeeked && !m.histPeeked:
		return chunkenc.ValNone
	case m.floatPeeked && (!m.histPeeked || m.floatIt.AtT() <= m.histIt.AtT()):
		m.cur, m.curVT = m.floatIt, m.floatVT
		m.floatPeeked = false
	default:
		m.cur, m.curVT = m.histIt, m.histVT
		m.histPeeked = false
	}
	m.started = true
	return m.curVT
}

func (m *mixedTypeIterator) Seek(t int64) chunkenc.ValueType {
	if m.started && m.cur.AtT() >= t {
		return m.curVT
	}
	for {
		vt := m.Next()
		if vt == chunkenc.ValNone || m.cur.AtT() >= t {
			return vt
		}
	}
}

func (m *mixedTypeIterator) At() (int64, float64) { return m.cur.At() }

func (m *mixedTypeIterator) AtHistogram(dst *histogram.Histogram) (int64, *histogram.Histogram) {
	return m.cur.AtHistogram(dst)
}

func (m *mixedTypeIterator) AtFloatHistogram(dst *histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	return m.cur.AtFloatHistogram(dst)
}

func (m *mixedTypeIterator) AtT() int64 { return m.cur.AtT() }
func (m *mixedTypeIterator) Err() error { return nil }
