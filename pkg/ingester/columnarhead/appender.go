package columnarhead

import (
	"context"
	"errors"

	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/storage"
)

// ErrUnsupportedLabelShape is returned by Append when the label set doesn't fit the
// shape this prototype can represent: the six fixed target labels (cluster, namespace,
// pod, container, node, job) + __name__ + at most one additional label. This mirrors
// SeriesStore/Head's existing single-localRef-field model (bench/04's original
// simplification, unchanged through every commit so far) - it was never extended to
// hold an arbitrary label list, and Append is the first place that limitation meets
// real, arbitrary-shaped label sets instead of a workload deliberately built to fit it.
// Rejecting loudly here is a real, current scope limit of this prototype, not a bug to
// paper over silently - see design doc §9 gap #1 ("the appender is incomplete") and
// CHECKLIST.md.
var ErrUnsupportedLabelShape = errors.New("columnarhead: label set has more than one non-target, non-__name__ label - unsupported by this prototype")

// ErrNotImplemented is returned by every Appender method this prototype doesn't
// support yet: exemplars, native histograms, metadata, and start-timestamp zero
// samples. Design doc §9 gap #1 already calls these out as missing; returning a clear
// error here (rather than silently succeeding and dropping the data) makes that gap
// visible to any caller instead of a silent correctness hole.
var ErrNotImplemented = errors.New("columnarhead: not implemented in this prototype")

const (
	labelCluster   = "cluster"
	labelNamespace = "namespace"
	labelPod       = "pod"
	labelContainer = "container"
	labelNode      = "node"
	labelJob       = "job"
)

// Appender returns a storage.Appender backed by h, matching the exact signature
// Cortex's tsdbStore.Appender(ctx context.Context) storage.Appender (pkg/ingester,
// Phase 1, ingester.go:380) requires - though this is not yet wired into that
// interface at the ingester level; that integration is separate work. ctx is accepted
// for signature conformance and currently unused: there is no cancellable work inside
// Append yet (no WAL, no network I/O) for it to govern.
//
// Transactions are not implemented: every Append takes effect immediately, and Commit/
// Rollback are no-ops (Rollback cannot actually undo anything already written). This
// matches design doc §9 gap #1's already-documented list of what the appender omits
// (WAL buffering, commit/rollback, OOO checks, tombstones, duplicate detection,
// exemplars, per-tenant limits) - stated plainly here rather than left implicit.
func (h *Head) Appender(_ context.Context) storage.Appender {
	return &headAppender{h: h}
}

type headAppender struct {
	h *Head
}

var _ storage.Appender = (*headAppender)(nil)

// splitLabels extracts the target block, metric name, and at most one extra label from
// l. Returns ErrUnsupportedLabelShape if l doesn't fit that shape - see the type's
// doc comment on ErrUnsupportedLabelShape for why this limit exists.
func splitLabels(l labels.Labels) (TargetLabels, string, string, error) {
	metricName := l.Get(labels.MetricName)
	if metricName == "" {
		return TargetLabels{}, "", "", ErrUnsupportedLabelShape
	}
	target := TargetLabels{
		Cluster:   l.Get(labelCluster),
		Namespace: l.Get(labelNamespace),
		Pod:       l.Get(labelPod),
		Container: l.Get(labelContainer),
		Node:      l.Get(labelNode),
		Job:       l.Get(labelJob),
	}

	knownCount := 1 // __name__
	if target.Cluster != "" {
		knownCount++
	}
	if target.Namespace != "" {
		knownCount++
	}
	if target.Pod != "" {
		knownCount++
	}
	if target.Container != "" {
		knownCount++
	}
	if target.Node != "" {
		knownCount++
	}
	if target.Job != "" {
		knownCount++
	}

	var localLabel string
	var extra int
	l.Range(func(lb labels.Label) {
		switch lb.Name {
		case labels.MetricName, labelCluster, labelNamespace, labelPod, labelContainer, labelNode, labelJob:
			return
		}
		extra++
		localLabel = lb.Value
	})
	if extra > 1 || l.Len() != knownCount+extra {
		return TargetLabels{}, "", "", ErrUnsupportedLabelShape
	}
	return target, metricName, localLabel, nil
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
func (a *headAppender) Append(ref storage.SeriesRef, l labels.Labels, t int64, v float64) (storage.SeriesRef, error) {
	if ref != 0 {
		if internalRef, ok := toInternalRef(ref, a.h.NumSeries()); ok {
			if err := a.h.Append(internalRef, t, v); err != nil {
				return 0, err
			}
			return ref, nil
		}
	}
	target, metricName, localLabel, err := splitLabels(l)
	if err != nil {
		return 0, err
	}
	seriesRef, err := a.h.GetOrCreateSeries(target, metricName, localLabel)
	if err != nil {
		return 0, err
	}
	if err := a.h.Append(seriesRef, t, v); err != nil {
		return 0, err
	}
	return toExternalRef(seriesRef), nil
}

// GetRef returns lset's series ref if it's already known, without creating anything -
// 0 if unknown, matching storage.GetRef's documented contract. hash is accepted for
// interface conformance but unused: Head's dedup keys off the resolved
// (targetID, nameID, localRef) tuple, not a label hash.
func (a *headAppender) GetRef(lset labels.Labels, _ uint64) (storage.SeriesRef, labels.Labels) {
	target, metricName, localLabel, err := splitLabels(lset)
	if err != nil {
		return 0, labels.EmptyLabels()
	}
	tRefs, ok := a.h.lookupTarget(target)
	if !ok {
		return 0, labels.EmptyLabels()
	}
	ref, ok := a.h.lookupSeries(tRefs, metricName, localLabel)
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

func (a *headAppender) AppendExemplar(storage.SeriesRef, labels.Labels, exemplar.Exemplar) (storage.SeriesRef, error) {
	return 0, ErrNotImplemented
}

func (a *headAppender) AppendHistogram(storage.SeriesRef, labels.Labels, int64, *histogram.Histogram, *histogram.FloatHistogram) (storage.SeriesRef, error) {
	return 0, ErrNotImplemented
}

func (a *headAppender) AppendHistogramSTZeroSample(storage.SeriesRef, labels.Labels, int64, int64, *histogram.Histogram, *histogram.FloatHistogram) (storage.SeriesRef, error) {
	return 0, ErrNotImplemented
}

func (a *headAppender) UpdateMetadata(storage.SeriesRef, labels.Labels, metadata.Metadata) (storage.SeriesRef, error) {
	return 0, ErrNotImplemented
}

func (a *headAppender) AppendSTZeroSample(storage.SeriesRef, labels.Labels, int64, int64) (storage.SeriesRef, error) {
	return 0, ErrNotImplemented
}
