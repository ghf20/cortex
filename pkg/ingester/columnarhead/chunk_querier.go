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
// Float series: the entire [mint, maxt] sample range is decoded (reusing the
// existing Iterator) and re-encoded into ONE real chunkenc.XORChunk - genuinely
// valid, bit-for-bit decodable by any real Prometheus code, not a custom format.
// This does NOT match real TSDB's chunking (which splits a block's range into many
// chunks, e.g. one per ~120 samples); returning everything as a single chunk per
// series is a stated simplification, not silently divergent behavior - nothing here
// depends on a caller assuming multiple chunks per series.
//
// Histogram series: re-encoded into one or more real chunkenc.HistogramChunks (see
// newHistogramChunksIterator) - POSSIBLY more than one, unlike the float path,
// because chunkenc.HistogramAppender.AppendHistogram can legitimately require
// starting a new chunk mid-stream on a counter reset it detects independently of
// anything HistogramStore itself tracks (HistogramStore has no counter-reset
// modeling of its own - see its doc comment). This used to be a stated gap (Next()
// returning false immediately for any histogram series, matching
// AppendHistogramSTZeroSample's precedent of declining rather than approximating) -
// found and closed after TestIngester_UseColumnarHead_QueryStream traced what it
// meant operationally: a real querier reading a columnarhead-backed ingester over
// the real gRPC chunks path got silently nothing back for histogram series.
func (h *Head) ChunkQuerier(mint, maxt int64) (storage.ChunkQuerier, error) {
	h.indexMu.RLock()
	for _, shard := range h.shards {
		shard.mu.RLock()
	}
	return &headChunkQuerier{h: h, mint: mint, maxt: maxt}, nil
}

type headChunkQuerier struct {
	h          *Head
	mint, maxt int64
	closed     bool
}

var _ storage.ChunkQuerier = (*headChunkQuerier)(nil)

// Close releases every lock taken by Head.ChunkQuerier - see headQuerier.Close's doc
// comment; the same reasoning (whole-query-lifetime locks, idempotence requirement,
// fixed ascending/descending shard order) applies here identically.
func (q *headChunkQuerier) Close() error {
	if q.closed {
		return nil
	}
	q.closed = true
	for _, shard := range q.h.shards {
		shard.mu.RUnlock()
	}
	q.h.indexMu.RUnlock()
	return nil
}

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
	if s.h.HasHistogram(s.ref) {
		return newHistogramChunksIterator(s.h, s.ref, s.mint, s.maxt)
	}
	return newSingleChunkIterator(s.h, s.ref, s.mint, s.maxt)
}

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
	var src floatSource = h.Iterator(ref)
	if ooo := h.OOOSamples(ref); len(ooo) > 0 {
		src = newMergedIterator(src, ooo)
	}
	it := &floatSampleIterator{it: src, mint: mint, maxt: maxt}
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

// histogramChunksIterator yields one or more chunks.Meta for a histogram series -
// see Head.ChunkQuerier's doc comment for why this is potentially-multi-chunk,
// unlike the float path's singleChunkIterator.
type histogramChunksIterator struct {
	metas []chunks.Meta
	i     int
	cur   chunks.Meta
	err   error
}

// newHistogramChunksIterator decodes ref's entire [mint, maxt] histogram sample
// range (reusing histogramSampleIterator, the same bounded source Select's regular,
// non-chunk path already uses) and re-encodes it into real chunkenc.HistogramChunks
// via the real chunkenc.HistogramAppender, appendOnly=false so a legitimate
// mid-stream counter reset starts a genuinely new chunk instead of erroring -
// exactly what a real *tsdb.Head's own chunk-writing path does, and the correct
// choice here since HistogramStore itself never models or rejects a counter reset
// (see its doc comment), so one can appear in the decoded stream at any point.
//
// prevApp (the appender active just before a split) is threaded into the next
// chunk's first AppendHistogram call, matching AppendHistogram's own documented
// contract ("prev is used to determine if there is a counter reset between the
// previous Appender and the current Appender... only taken into account when the
// first sample is being appended") - the same cross-chunk counter-reset-header
// wiring real Prometheus's own per-series chunk writing does.
func newHistogramChunksIterator(h *Head, ref uint32, mint, maxt int64) *histogramChunksIterator {
	src := &histogramSampleIterator{it: h.HistogramIterator(ref), mint: mint, maxt: maxt}

	var metas []chunks.Meta
	var chunk chunkenc.Chunk
	var app chunkenc.Appender
	var prevApp *chunkenc.HistogramAppender
	var first, last int64
	n := 0

	for src.Next() == chunkenc.ValHistogram {
		ts, hg := src.AtHistogram(nil)
		if chunk == nil {
			chunk = chunkenc.NewHistogramChunk()
			var err error
			if app, err = chunk.Appender(); err != nil {
				return &histogramChunksIterator{err: err}
			}
			first = ts
			n = 0
		}

		newChunk, _, newApp, err := app.AppendHistogram(prevApp, ts, hg, false)
		if err != nil {
			return &histogramChunksIterator{err: err}
		}
		if newChunk != nil {
			// The current chunk is done (a counter reset or layout change the
			// encoder detected forced a split) - flush it and start tracking
			// the new one, remembering the outgoing appender so the new
			// chunk's counter-reset header can be computed correctly.
			metas = append(metas, chunks.Meta{Chunk: chunk, MinTime: first, MaxTime: last})
			if ha, ok := app.(*chunkenc.HistogramAppender); ok {
				prevApp = ha
			}
			chunk = newChunk
			app = newApp
			first = ts
			n = 0
		} else {
			app = newApp
		}
		last = ts
		n++
	}
	if err := src.Err(); err != nil {
		return &histogramChunksIterator{err: err}
	}
	if n > 0 {
		metas = append(metas, chunks.Meta{Chunk: chunk, MinTime: first, MaxTime: last})
	}
	return &histogramChunksIterator{metas: metas}
}

func (it *histogramChunksIterator) At() chunks.Meta { return it.cur }

func (it *histogramChunksIterator) Next() bool {
	if it.err != nil || it.i >= len(it.metas) {
		return false
	}
	it.cur = it.metas[it.i]
	it.i++
	return true
}

func (it *histogramChunksIterator) Err() error { return it.err }
