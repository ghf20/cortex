package columnarhead

import (
	"sort"

	"github.com/prometheus/prometheus/tsdb/chunks"
)

// This file merges a series' float-typed chunk(s) (singleChunkIterator) and its
// histogram-typed chunk(s) (histogramChunksIterator/floatHistogramChunksIterator)
// into one time-ordered chunks.Iterator - the ChunkQuerier-path version of the
// mixed-type-series gap headSeries.Iterator (querier.go, mixed_iterator.go) had
// until fixed there. See headChunkSeries.Iterator's own doc comment for how this
// was found (promqltest's functions.test) and why it needed its own fix here
// rather than reusing that one directly - this path returns chunks.Meta (real
// encoded chunk bytes), not decoded samples.
//
// Both sides' chunks.Meta lists are already individually time-ordered, and since a
// given timestamp is either a float sample or a histogram sample, never both,
// their time ranges never overlap each other - so a plain sort by MinTime over the
// combined list produces the correct chronological chunk sequence, no interleaved
// merge-as-you-go needed. Both singleChunkIterator and the histogram iterators
// already build their whole []chunks.Meta eagerly (not lazily/incrementally, see
// their own constructors), so gathering everything up front here matches their
// existing style instead of introducing a new lazy pattern for no reason.

// chunkMetaIterator is the minimal shape newMixedChunksIterator needs from either
// side's own iterator - singleChunkIterator, histogramChunksIterator, and
// floatHistogramChunksIterator (all defined in this file's package) already
// satisfy it, being chunks.Iterator implementations themselves.
type chunkMetaIterator interface {
	Next() bool
	At() chunks.Meta
	Err() error
}

type mixedChunksIterator struct {
	metas []chunks.Meta
	i     int
	err   error
}

var _ chunks.Iterator = (*mixedChunksIterator)(nil)

func newMixedChunksIterator(floatIt, histIt chunkMetaIterator) *mixedChunksIterator {
	var metas []chunks.Meta
	for floatIt.Next() {
		metas = append(metas, floatIt.At())
	}
	if err := floatIt.Err(); err != nil {
		return &mixedChunksIterator{err: err}
	}
	for histIt.Next() {
		metas = append(metas, histIt.At())
	}
	if err := histIt.Err(); err != nil {
		return &mixedChunksIterator{err: err}
	}
	sort.Slice(metas, func(i, j int) bool { return metas[i].MinTime < metas[j].MinTime })
	return &mixedChunksIterator{metas: metas}
}

func (it *mixedChunksIterator) At() chunks.Meta { return it.metas[it.i-1] }

func (it *mixedChunksIterator) Next() bool {
	if it.err != nil || it.i >= len(it.metas) {
		return false
	}
	it.i++
	return true
}

func (it *mixedChunksIterator) Err() error { return it.err }
