package columnarhead

import (
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

// This file computes each histogram sample's real per-query CounterResetHint for
// the sample-level query path (headSeries.Iterator, via histogramSampleIterator/
// floatHistogramSampleIterator) - histogram.go's own decodedCounterResetHint
// collapses everything except GaugeType to UnknownCounterReset on decode, since
// histoSegment boundaries don't track counter-reset boundaries at all (only
// layout changes). That's correct for the FIRST sample after any real reset, but
// wrong for every sample AFTER that: real Prometheus's counterResetHint(header,
// numRead) (vendor/.../tsdb/chunkenc/histogram_meta.go) reports NotCounterReset
// for any non-first sample in a chunk - a distinction PromQL functions like
// changes()/resets()/histogram_count() over an aggregation depend on to detect
// conflicting resets (found via promqltest's native_histograms.test, which
// couldn't reach this deep into the file until the label-shape work unblocked
// it - see CHECKLIST.md).
//
// Rather than reimplementing real Prometheus's reset-detection logic (chunkenc's
// own HistogramAppender.appendable, a genuinely intricate span/bucket-layout
// comparison), this reuses the exact same REAL chunkenc.HistogramAppender/
// FloatHistogramAppender chunk_querier.go's newHistogramChunksIterator/
// newFloatHistogramChunksIterator already call for the ChunkQuerier path -
// proven correct there by TestChunkQuerierHistogramSeriesCounterResetSplitsChunk.
// The resulting chunk bytes are discarded; only the "did this sample start a
// fresh (possibly virtual - never persisted) chunk" signal matters, which decides
// this hint by the identical rule counterResetHint(header, numRead) uses:
// GaugeType passes through unchanged (already correct on decode), Unknown for a
// virtual chunk's first sample, NotCounterReset for any sample after that within
// the same one.
//
// Like chunk_querier.go's own equivalent, this starts fresh (prevApp nil) at
// whatever sample the query's mint happens to land on - it has no visibility into
// samples before mint, matching that already-accepted limitation exactly, not a
// new one.

// histogramResetState carries the real chunkenc.HistogramAppender state a
// histogramSampleIterator needs across calls to compute each sample's hint
// in order. Zero value is ready to use.
type histogramResetState struct {
	chunk   chunkenc.Chunk
	app     chunkenc.Appender
	prevApp *chunkenc.HistogramAppender
}

// apply feeds h (already decoded, in append order) through the real appender and
// overwrites h.CounterResetHint in place with the correctly computed value -
// see this file's own doc comment for the exact rule and why it's correct.
func (s *histogramResetState) apply(h *histogram.Histogram, t int64) error {
	startedNew := s.chunk == nil
	if startedNew {
		s.chunk = chunkenc.NewHistogramChunk()
		var err error
		if s.app, err = s.chunk.Appender(); err != nil {
			return err
		}
	}
	newChunk, _, newApp, err := s.app.AppendHistogram(s.prevApp, t, h, false)
	if err != nil {
		return err
	}
	if newChunk != nil {
		if ha, ok := s.app.(*chunkenc.HistogramAppender); ok {
			s.prevApp = ha
		}
		s.chunk = newChunk
		startedNew = true
	}
	s.app = newApp

	if h.CounterResetHint != histogram.GaugeType {
		if startedNew {
			h.CounterResetHint = histogram.UnknownCounterReset
		} else {
			h.CounterResetHint = histogram.NotCounterReset
		}
	}
	return nil
}

// floatHistogramResetState is histogramResetState's FloatHistogram counterpart -
// see its doc comment.
type floatHistogramResetState struct {
	chunk   chunkenc.Chunk
	app     chunkenc.Appender
	prevApp *chunkenc.FloatHistogramAppender
}

func (s *floatHistogramResetState) apply(h *histogram.FloatHistogram, t int64) error {
	startedNew := s.chunk == nil
	if startedNew {
		s.chunk = chunkenc.NewFloatHistogramChunk()
		var err error
		if s.app, err = s.chunk.Appender(); err != nil {
			return err
		}
	}
	newChunk, _, newApp, err := s.app.AppendFloatHistogram(s.prevApp, t, h, false)
	if err != nil {
		return err
	}
	if newChunk != nil {
		if ha, ok := s.app.(*chunkenc.FloatHistogramAppender); ok {
			s.prevApp = ha
		}
		s.chunk = newChunk
		startedNew = true
	}
	s.app = newApp

	if h.CounterResetHint != histogram.GaugeType {
		if startedNew {
			h.CounterResetHint = histogram.UnknownCounterReset
		} else {
			h.CounterResetHint = histogram.NotCounterReset
		}
	}
	return nil
}
