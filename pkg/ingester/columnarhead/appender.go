package columnarhead

import (
	"context"
	"errors"
	"math"

	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/storage"
)

// ErrUnsupportedLabelShape is returned by Append when the label set doesn't fit the
// shape this prototype can represent: __name__, the seven fixed target labels
// (cluster, namespace, pod, container, node, job, instance), and any number of
// additional labels up to 255 (SeriesStore.localCount's width - see series.go).
// Originally capped extra labels at exactly one (SeriesStore's single-localRef-field
// model, bench/04's original simplification) - lifted after bench/06_variable_labels
// measured a variable-length local-label scheme as CHEAPER, not more expensive, on a
// real workload-shaped label-count distribution (CHECKLIST.md). What remains
// unsupported: no __name__ at all, or (astronomically unlikely in practice) more
// than 255 extra labels on one series.
var ErrUnsupportedLabelShape = errors.New("columnarhead: label set doesn't fit this prototype's supported shape")

// ErrDuplicateLabelName is returned by Append when l has the same label name
// twice - matches real tsdb.Head's own rejection (headAppenderBase.getOrCreate,
// vendor/.../tsdb/head_append.go: `label name %q is not unique: invalid sample`),
// found missing here entirely while porting that package's own
// TestAddDuplicateLabelName (CHECKLIST.md) - without this check, splitLabels'
// l.Range would silently accept both same-named labels as separate entries in
// extra, corrupting the stored series with an invalid, ambiguous label set
// instead of rejecting the write.
var ErrDuplicateLabelName = errors.New("columnarhead: label set has a duplicate label name")

// ErrNotImplemented now guards exactly one remaining gap: AppendHistogramSTZeroSample
// (see its own doc comment for why). Exemplars, native histograms (including custom
// bucket boundaries, schema -53/NHCB - see histoSegment's own doc comment), metadata,
// and float start-timestamp zero samples - all originally listed here as missing, per
// design doc §9 gap #1 - are now implemented; see metadata.go, exemplar.go,
// histogram.go, and Head.SetSTZeroSample.
var ErrNotImplemented = errors.New("columnarhead: not implemented in this prototype")

const (
	labelCluster   = "cluster"
	labelNamespace = "namespace"
	labelPod       = "pod"
	labelContainer = "container"
	labelNode      = "node"
	labelJob       = "job"
	labelInstance  = "instance"
)

// Appender returns a storage.Appender backed by h, matching the exact signature
// Cortex's tsdbStore.Appender(ctx context.Context) storage.Appender (pkg/ingester,
// Phase 1, ingester.go:380) requires, and wired into that interface at the ingester
// level via columnarheadTSDBStore (gated by -blocks-storage.tsdb.use-columnar-head).
// ctx is accepted for signature conformance and currently unused: there is no
// cancellable work inside Append yet (no WAL, no network I/O) for it to govern.
//
// Transactions are not implemented: every Append takes effect immediately, and Commit/
// Rollback are no-ops (Rollback cannot actually undo anything already written).
// OOO checks and exemplars ARE implemented (ooo.go/exemplar.go) and per-tenant
// limits are enforced at the ingester level (Head.SetSeriesLifecycleCallback,
// wired to userDB in ingester.go's createTSDB) - what design doc §9 gap #1's
// original list still leaves genuinely missing here is WAL buffering (this
// package's own durability.go is a separate mechanism, not the appender doing
// buffered writes), real commit/rollback, tombstones, and duplicate-detection
// nuances beyond OOO's own dedup.
//
// Concurrency: headAppender itself holds no lock - every method below calls straight
// through to a self-locking Head method (GetOrCreateSeries, Append, LookupSeriesRef,
// etc; see Head's doc comment on the shardFor/indexMu locking split), so it's safe to
// call concurrently with other Appenders and with Querier/ChunkQuerier. There is no
// cross-call transaction to protect (Commit is a no-op, every Append is independently
// atomic already), so per-call locking inside Head is sufficient - nothing here needs
// to hold a lock across multiple calls.
func (h *Head) Appender(_ context.Context) storage.Appender {
	return &headAppender{h: h}
}

type headAppender struct {
	h *Head
}

var _ storage.Appender = (*headAppender)(nil)

// splitLabels extracts the target block, metric name, and every extra (non-target,
// non-__name__) label from l. Returns ErrUnsupportedLabelShape if l has no __name__
// or more than 255 extra labels - see ErrUnsupportedLabelShape's doc comment.
// extra's labels come out in l's own order, which - like every labels.Labels this
// package receives - is already sorted by name (Prometheus's own invariant): callers
// that build a canonical dedup key from extra (Head.GetOrCreateSeries/lookupSeries)
// rely on that order directly, without a separate sort.
func splitLabels(l labels.Labels) (target TargetLabels, metricName string, extra []labels.Label, err error) {
	// See ErrDuplicateLabelName's own doc comment. HasDuplicateLabelNames is
	// real Prometheus's own method (checks adjacent pairs, correct because
	// labels.Labels is always sorted by contract) - reused directly rather
	// than reimplemented.
	if _, dup := l.HasDuplicateLabelNames(); dup {
		return TargetLabels{}, "", nil, ErrDuplicateLabelName
	}
	metricName = l.Get(labels.MetricName)
	if metricName == "" {
		return TargetLabels{}, "", nil, ErrUnsupportedLabelShape
	}
	target = TargetLabels{
		Cluster:   l.Get(labelCluster),
		Namespace: l.Get(labelNamespace),
		Pod:       l.Get(labelPod),
		Container: l.Get(labelContainer),
		Node:      l.Get(labelNode),
		Job:       l.Get(labelJob),
		Instance:  l.Get(labelInstance),
	}

	l.Range(func(lb labels.Label) {
		switch lb.Name {
		case labels.MetricName, labelCluster, labelNamespace, labelPod, labelContainer, labelNode, labelJob, labelInstance:
			return
		}
		extra = append(extra, lb)
	})
	if len(extra) > math.MaxUint8 {
		return TargetLabels{}, "", nil, ErrUnsupportedLabelShape
	}
	return target, metricName, extra, nil
}

// Append resolves l to a series and records (t, v). If ref is non-zero and refers to
// a series this Head actually has (bounds-checked, not blindly trusted - a caller may
// legitimately pass a stale ref from a prior Head/appender per storage.Appender's
// documented contract that ref numbers "may be rejected... at any point"), it skips
// label resolution entirely and appends directly - the actual point of accepting ref
// at all, not just interface-shape conformance.
//
// storage.SeriesRef reserves 0 to mean "no reference" (never to be cached) - but
// SeriesStore.Create numbers series from 0, so the very first series ever created
// would otherwise collide with that sentinel. Every ref crossing this boundary is
// offset by 1 (toExternalRef/toInternalRef) specifically to keep 0 reserved; Head and
// SeriesStore's own internal uint32 refs stay 0-based and untouched everywhere else.
//
// A staleness marker (v is StaleNaN) targeting a series already known to be
// histogram-typed is converted to the equivalent histogram-typed stale sample
// instead of a plain float append - see appendStaleToHistogramSeries' own doc
// comment for why (matches real tsdb.Head's own behavior, found missing via
// TestDifferentialHistogramStalenessRealVsColumnar).
func (a *headAppender) Append(ref storage.SeriesRef, l labels.Labels, t int64, v float64) (storage.SeriesRef, error) {
	if ref != 0 {
		if internalRef, ok := toInternalRef(ref, a.h.NumSeries()); ok {
			if value.IsStaleNaN(v) && a.h.HasHistogram(internalRef) {
				return a.appendStaleToHistogramSeries(internalRef, t, v)
			}
			if err := a.h.Append(internalRef, t, v); err != nil {
				return 0, err
			}
			return ref, nil
		}
	}
	target, metricName, extra, err := splitLabels(l)
	if err != nil {
		return 0, err
	}
	seriesRef, err := a.h.GetOrCreateSeries(target, metricName, extra...)
	if err != nil {
		return 0, err
	}
	if value.IsStaleNaN(v) && a.h.HasHistogram(seriesRef) {
		return a.appendStaleToHistogramSeries(seriesRef, t, v)
	}
	if err := a.h.Append(seriesRef, t, v); err != nil {
		return 0, err
	}
	return toExternalRef(seriesRef), nil
}

// appendStaleToHistogramSeries converts a plain-float staleness append targeting a
// series already known to be histogram-typed into the equivalent histogram-typed
// stale sample - matching real tsdb.Head's own headAppender.Append
// (vendor/.../tsdb/head_append.go's a.typesInBatch check) in effect, though via a
// simpler, always-correct GLOBAL signal (Head.HasHistogram) rather than real
// Prometheus's own per-batch, per-appender heuristic (real Prometheus's own
// comment there: "not perfect but just an optimization for the more likely case" -
// a global check is strictly at least as reliable, not a compromise).
//
// Found via TestDifferentialHistogramStalenessRealVsColumnar: without this, a
// stale marker on a histogram series lands as a genuinely mixed-type float
// sample instead of matching real Prometheus's own converted-to-histogram
// representation - not wrong in the sense of losing data (mixed_iterator.go
// already handles either shape correctly), but a real, observable divergence
// from real behavior: the value TYPE PromQL sees at that timestamp, and the
// histogram's own zeroed Schema/Count/buckets real Prometheus's conversion
// produces (&histogram.Histogram{Sum: v}, every other field left at its zero
// value - matches real Prometheus's own construction exactly).
func (a *headAppender) appendStaleToHistogramSeries(ref uint32, t int64, v float64) (storage.SeriesRef, error) {
	if a.h.HasFloatHistogram(ref) {
		if err := a.h.AppendFloatHistogram(ref, t, &histogram.FloatHistogram{Sum: v}); err != nil {
			return 0, err
		}
	} else if err := a.h.AppendHistogram(ref, t, &histogram.Histogram{Sum: v}); err != nil {
		return 0, err
	}
	return toExternalRef(ref), nil
}

// GetRef returns lset's series ref if it's already known, without creating anything -
// 0 if unknown, matching storage.GetRef's documented contract. hash is accepted for
// interface conformance but unused: Head's dedup keys off the resolved
// (targetID, nameID, localRef) tuple, not a label hash.
func (a *headAppender) GetRef(lset labels.Labels, _ uint64) (storage.SeriesRef, labels.Labels) {
	target, metricName, extra, err := splitLabels(lset)
	if err != nil {
		return 0, labels.EmptyLabels()
	}
	ref, ok := a.h.LookupSeriesRef(target, metricName, extra...)
	if !ok {
		return 0, labels.EmptyLabels()
	}
	return toExternalRef(ref), lset
}

func toExternalRef(internalRef uint32) storage.SeriesRef {
	return storage.SeriesRef(internalRef) + 1
}

// toInternalRef translates an external ref back to internal, bounds-checking against
// numSeries so a stale or bogus caller-supplied ref falls back to full resolution
// instead of panicking on an out-of-range SeriesStore index.
func toInternalRef(ref storage.SeriesRef, numSeries int) (uint32, bool) {
	if ref == 0 {
		return 0, false
	}
	internalRef := uint32(ref) - 1
	return internalRef, internalRef < uint32(numSeries)
}

func (a *headAppender) SetOptions(*storage.AppendOptions) {
	// No-op: out-of-order handling isn't implemented (design doc §9 gap #4), so there
	// is nothing to configure yet. Not silently pretending to honor DiscardOutOfOrder
	// would mean rejecting every AppendOptions call, which is worse than documenting
	// the gap here - OOO append behavior is simply undefined in this prototype either way.
}

// Commit and Rollback are no-ops: every Append already took effect immediately, there
// is no buffered transaction to submit or discard. Rollback in particular CANNOT undo
// prior Append calls - this is a real, documented divergence from
// storage.AppenderTransaction's contract, not a full implementation.
func (a *headAppender) Commit() error   { return nil }
func (a *headAppender) Rollback() error { return nil }

// AppendExemplar resolves l to a series and stores e for it. Does NOT create a series
// that doesn't already exist - the real storage.ExemplarAppender's doc comment treats
// AppendExemplar generating a new series reference as "possible erroneous behaviour"
// (exemplars conceptually attach to an existing series' samples), so this returns
// ErrSeriesNotFound rather than silently creating a phantom series with no target/
// metric labels behind it - same posture as UpdateMetadata.
func (a *headAppender) AppendExemplar(ref storage.SeriesRef, l labels.Labels, e exemplar.Exemplar) (storage.SeriesRef, error) {
	if ref != 0 {
		if internalRef, ok := toInternalRef(ref, a.h.NumSeries()); ok {
			a.h.AppendExemplar(internalRef, e)
			return ref, nil
		}
	}
	target, metricName, extra, err := splitLabels(l)
	if err != nil {
		return 0, err
	}
	internalRef, ok := a.h.LookupSeriesRef(target, metricName, extra...)
	if !ok {
		return 0, ErrSeriesNotFound
	}
	a.h.AppendExemplar(internalRef, e)
	return toExternalRef(internalRef), nil
}

// AppendHistogram resolves l to a series (creating it if needed, same as Append) and
// records h or fh - exactly one is expected to be non-nil, matching
// storage.Appender's own documented contract. See Head.AppendHistogram/
// AppendFloatHistogram and histoSegment's doc comment for what's supported (a
// schema/zero-threshold/span change mid-series starts a new segment rather than
// erroring; custom bucket boundaries still aren't supported at all - both paths
// share that one remaining limit) and ErrHistogramTypeChanged for what happens if
// a series switches between the two kinds mid-stream.
func (a *headAppender) AppendHistogram(ref storage.SeriesRef, l labels.Labels, t int64, h *histogram.Histogram, fh *histogram.FloatHistogram) (storage.SeriesRef, error) {
	if h != nil {
		if ref != 0 {
			if internalRef, ok := toInternalRef(ref, a.h.NumSeries()); ok {
				if err := a.h.AppendHistogram(internalRef, t, h); err != nil {
					return 0, err
				}
				return ref, nil
			}
		}
		target, metricName, extra, err := splitLabels(l)
		if err != nil {
			return 0, err
		}
		seriesRef, err := a.h.GetOrCreateSeries(target, metricName, extra...)
		if err != nil {
			return 0, err
		}
		if err := a.h.AppendHistogram(seriesRef, t, h); err != nil {
			return 0, err
		}
		return toExternalRef(seriesRef), nil
	}

	if ref != 0 {
		if internalRef, ok := toInternalRef(ref, a.h.NumSeries()); ok {
			if err := a.h.AppendFloatHistogram(internalRef, t, fh); err != nil {
				return 0, err
			}
			return ref, nil
		}
	}
	target, metricName, extra, err := splitLabels(l)
	if err != nil {
		return 0, err
	}
	seriesRef, err := a.h.GetOrCreateSeries(target, metricName, extra...)
	if err != nil {
		return 0, err
	}
	if err := a.h.AppendFloatHistogram(seriesRef, t, fh); err != nil {
		return 0, err
	}
	return toExternalRef(seriesRef), nil
}

// AppendHistogramSTZeroSample is a deliberate, stated gap, not an oversight: it must
// be called BEFORE the paired AppendHistogram (per the interface's documented
// contract), meaning the series' schema/span layout isn't known yet - but
// HistogramStore's whole encoding model requires the FIRST sample to establish that
// layout (see its doc comment). Synthesizing a correct "zero histogram" with an
// as-yet-unknown layout is real complexity the float path's AppendSTZeroSample
// doesn't have (a float zero-sample is trivially 0.0 regardless of what follows).
func (a *headAppender) AppendHistogramSTZeroSample(storage.SeriesRef, labels.Labels, int64, int64, *histogram.Histogram, *histogram.FloatHistogram) (storage.SeriesRef, error) {
	return 0, ErrNotImplemented
}

// UpdateMetadata records m for the series ref/l resolves to. Unlike Append, this does
// NOT create a series that doesn't already exist - matches storage.MetadataUpdater's
// documented contract ("If the series does not exist, UpdateMetadata returns an
// error"). Uses the same ref fast path as Append (bounds-checked, falls back to label
// resolution on a stale/zero ref) before falling back to the read-only lookup path
// GetRef also uses.
func (a *headAppender) UpdateMetadata(ref storage.SeriesRef, l labels.Labels, m metadata.Metadata) (storage.SeriesRef, error) {
	if ref != 0 {
		if internalRef, ok := toInternalRef(ref, a.h.NumSeries()); ok {
			if err := a.h.SetMetadata(internalRef, m); err != nil {
				return 0, err
			}
			return ref, nil
		}
	}
	target, metricName, extra, err := splitLabels(l)
	if err != nil {
		return 0, err
	}
	internalRef, ok := a.h.LookupSeriesRef(target, metricName, extra...)
	if !ok {
		return 0, ErrSeriesNotFound
	}
	if err := a.h.SetMetadata(internalRef, m); err != nil {
		return 0, err
	}
	return toExternalRef(internalRef), nil
}

// AppendSTZeroSample resolves l to a series (creating it if needed, same as Append -
// this is typically the first call for a new series, establishing its start time
// before the corresponding real sample) and records a synthetic zero sample at st.
// Uses the same ref fast path as Append.
func (a *headAppender) AppendSTZeroSample(ref storage.SeriesRef, l labels.Labels, t, st int64) (storage.SeriesRef, error) {
	if ref != 0 {
		if internalRef, ok := toInternalRef(ref, a.h.NumSeries()); ok {
			if err := a.h.SetSTZeroSample(internalRef, t, st); err != nil {
				return 0, err
			}
			return ref, nil
		}
	}
	target, metricName, extra, err := splitLabels(l)
	if err != nil {
		return 0, err
	}
	seriesRef, err := a.h.GetOrCreateSeries(target, metricName, extra...)
	if err != nil {
		return 0, err
	}
	if err := a.h.SetSTZeroSample(seriesRef, t, st); err != nil {
		return 0, err
	}
	return toExternalRef(seriesRef), nil
}
