package columnarhead

import (
	"context"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/util/annotations"
)

// ChunkQuerier returns a storage.ChunkQuerier over h's current state, matching Phase
// 1's tsdbStore.ChunkQuerier(mint, maxt int64) (storage.ChunkQuerier, error)
// signature. Shares headQuerier's selection logic (postings shortcut, matcher
// filtering) entirely - only the returned series' Iterator differs (chunks.Iterator
// over chunks.Meta, not chunkenc.Iterator over decoded samples).
//
// Float series only: each selected series' entire [mint, maxt] sample range is
// decoded (reusing the existing Iterator) and re-encoded into ONE real
// chunkenc.XORChunk - genuinely valid, bit-for-bit decodable by any real Prometheus
// code, not a custom format. This does NOT match real TSDB's chunking (which splits
// a block's range into many chunks, e.g. one per ~120 samples); returning everything
// as a single chunk per series is a stated simplification, not silently divergent
// behavior - nothing here depends on a caller assuming multiple chunks per series.
// Histogram series are a stated gap: Next() returns false immediately, matching
// AppendHistogramSTZeroSample's precedent of declining rather than approximating
// (real Prometheus histogram chunks have counter-reset/recoding semantics this
// package's own HistogramStore doesn't implement - see its doc comment).
func (h *Head) ChunkQuerier(mint, maxt int64) (storage.ChunkQuerier, error) {
	return &headChunkQuerier{h: h, mint: mint, maxt: maxt}, nil
}

type headChunkQuerier struct {
	h          *Head
	mint, maxt int64
}

var _ storage.ChunkQuerier = (*headChunkQuerier)(nil)

func (q *headChunkQuerier) Close() error { return nil }

func (q *headChunkQuerier) LabelValues(ctx context.Context, name string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return (&headQuerier{h: q.h, mint: q.mint, maxt: q.maxt}).LabelValues(ctx, name, hints, matchers...)
}

func (q *headChunkQuerier) LabelNames(ctx context.Context, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return (&headQuerier{h: q.h, mint: q.mint, maxt: q.maxt}).LabelNames(ctx, hints, matchers...)
}

// Select shares headQuerier's exact candidate-selection logic (the postings
// shortcut, per-value matcher filtering, and sortSeries ordering) - only the series
// set returned wraps chunks instead of decoded samples.
func (q *headChunkQuerier) Select(_ context.Context, sortSeries bool, _ *storage.SelectHints, matchers ...*labels.Matcher) storage.ChunkSeriesSet {
	sq := &headQuerier{h: q.h, mint: q.mint, maxt: q.maxt}
	candidates, rest := sq.candidateRefs(matchers)
	var refs []uint32
	for _, ref := range candidates {
		if refMatchesAll(q.h, ref, rest) {
			refs = append(refs, ref)
		}
	}
	if sortSeries {
		sortRefsByLabels(q.h, refs)
	}
	return &headChunkSeriesSet{h: q.h, refs: refs, mint: q.mint, maxt: q.maxt}
}

type headChunkSeriesSet struct {
	h          *Head
	refs       []uint32
	mint, maxt int64
	i          int
	cur        uint32
}

var _ storage.ChunkSeriesSet = (*headChunkSeriesSet)(nil)

func (s *headChunkSeriesSet) Next() bool {
	if s.i >= len(s.refs) {
		return false
	}
	s.cur = s.refs[s.i]
	s.i++
	return true
}

func (s *headChunkSeriesSet) At() storage.ChunkSeries {
	return &headChunkSeries{h: s.h, ref: s.cur, mint: s.mint, maxt: s.maxt}
}
func (s *headChunkSeriesSet) Err() error                        { return nil }
func (s *headChunkSeriesSet) Warnings() annotations.Annotations { return nil }

type headChunkSeries struct {
	h          *Head
	ref        uint32
	mint, maxt int64
}

var _ storage.ChunkSeries = (*headChunkSeries)(nil)

func (s *headChunkSeries) Labels() labels.Labels {
	return s.h.SeriesLabels(s.ref)
}

func (s *headChunkSeries) Iterator(_ chunks.Iterator) chunks.Iterator {
	if s.h.histograms.Has(s.ref) {
		return &emptyChunksIterator{} // histogram chunks: stated gap, see type doc comment
	}
	return newSingleChunkIterator(s.h, s.ref, s.mint, s.maxt)
}

// emptyChunksIterator is chunks.Iterator's equivalent of an already-exhausted
// iterator - Next() always false. Used for histogram series (see Head.ChunkQuerier's
// doc comment on why) rather than panicking or fabricating a nonsensical chunk.
type emptyChunksIterator struct{}

func (emptyChunksIterator) At() chunks.Meta { return chunks.Meta{} }
func (emptyChunksIterator) Next() bool      { return false }
func (emptyChunksIterator) Err() error      { return nil }

// singleChunkIterator yields exactly one chunks.Meta (or none, if the series has no
// samples in [mint, maxt]) containing every decoded sample re-encoded into a real
// chunkenc.XORChunk - see Head.ChunkQuerier's doc comment for why this is one chunk,
// not real TSDB's multi-chunk-per-range shape.
type singleChunkIterator struct {
	meta chunks.Meta
	has  bool
	done bool
}

func newSingleChunkIterator(h *Head, ref uint32, mint, maxt int64) *singleChunkIterator {
	it := &floatSampleIterator{it: h.Iterator(ref), mint: mint, maxt: maxt}
	xc := chunkenc.NewXORChunk()
	app, _ := xc.Appender()

	var first, last int64
	n := 0
	for it.Next() == chunkenc.ValFloat {
		ts, v := it.At()
		app.Append(ts, v)
		if n == 0 {
			first = ts
		}
		last = ts
		n++
	}
	if n == 0 {
		return &singleChunkIterator{}
	}
	return &singleChunkIterator{
		has: true,
		meta: chunks.Meta{
			Chunk:   xc,
			MinTime: first,
			MaxTime: last,
		},
	}
}

func (s *singleChunkIterator) At() chunks.Meta { return s.meta }

func (s *singleChunkIterator) Next() bool {
	if s.done || !s.has {
		return false
	}
	s.done = true
	return true
}

func (s *singleChunkIterator) Err() error { return nil }
