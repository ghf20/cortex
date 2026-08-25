package columnarhead

import (
	"errors"
	"math"
)

// ErrTooManySymbols is returned by GetOrCreateSeries if a metric name or local label
// value would be the 65,537th distinct one interned - SeriesStore's nameID/localRef
// fields are uint16 (see series.go's comment on why targetID is uint32 but these
// aren't: metric names and "le"-style local label values come from a bounded
// vocabulary that doesn't grow with fleet churn, unlike targets). Failing loudly here
// beats silently truncating an id and corrupting an unrelated series' lookup.
var ErrTooManySymbols = errors.New("columnarhead: nameID/localRef would overflow uint16")

// Head ties a live symbol interner, a target slab, and a series store into an actual
// ingest path: given raw label strings for a sample, resolve or create its target and
// series, then append. This is the first thing in this package that looks like
// something storage.Appender.Append could call - it is NOT wired into Cortex's
// tsdbStore interface (pkg/ingester's Phase 1 work) yet; that integration is separate,
// later work.
//
// Live lookup goes through plain Go maps (targetIndex, seriesIndex, and liveInterner's
// internal index), not the static MPHF/SymbolTable built earlier in this package - see
// liveInterner's doc comment for why. The MPHF/SymbolTable are for a rebuild-at-
// compaction path that doesn't exist yet; until it does, Head's real memory cost
// includes live Go map overhead on top of the tight series/target records, not the
// compact MPHF projection quoted elsewhere in CHECKLIST.md for the post-compaction
// case. See TestHeadAtScale for the honest, measured total.
type Head struct {
	symbols *liveInterner
	targets *TargetStore
	series  *SeriesStore

	targetIndex map[[targetFields]uint32]uint32
	seriesIndex map[seriesKey]uint32
}

// TargetLabels is the fixed 6-label shared block every series belongs to (§3.1).
type TargetLabels struct {
	Cluster, Namespace, Pod, Container, Node, Job string
}

type seriesKey struct {
	targetID uint32
	nameID   uint16
	localRef uint16
}

// NewHead returns an empty Head with capacity preallocated for the expected scale.
func NewHead(expectedSeries, expectedTargets, expectedSymbols int) *Head {
	return &Head{
		symbols:     newLiveInterner(expectedSymbols),
		targets:     NewTargetStore(expectedTargets),
		series:      NewSeriesStore(expectedSeries),
		targetIndex: make(map[[targetFields]uint32]uint32, expectedTargets),
		seriesIndex: make(map[seriesKey]uint32, expectedSeries),
	}
}

// GetOrCreateSeries resolves target+metricName+localLabel to a series ref, creating
// the target, symbols, and series record as needed - repeated calls with identical
// arguments return the same ref rather than creating duplicates. localLabel is the
// series-specific label besides __name__ (e.g. a histogram's "le" bucket value); pass
// "" if the series has none - matches SeriesStore's existing single-localRef-field
// model (bench/04's original simplification, carried through unchanged here).
func (h *Head) GetOrCreateSeries(target TargetLabels, metricName, localLabel string) (uint32, error) {
	tRefs := [targetFields]uint32{
		h.symbols.Intern(target.Cluster),
		h.symbols.Intern(target.Namespace),
		h.symbols.Intern(target.Pod),
		h.symbols.Intern(target.Container),
		h.symbols.Intern(target.Node),
		h.symbols.Intern(target.Job),
	}
	targetID, ok := h.targetIndex[tRefs]
	if !ok {
		targetID = h.targets.Create(tRefs)
		h.targetIndex[tRefs] = targetID
	}

	nameID32 := h.symbols.Intern(metricName)
	if nameID32 > math.MaxUint16 {
		return 0, ErrTooManySymbols
	}
	nameID := uint16(nameID32)

	var localRef uint16
	if localLabel != "" {
		localRef32 := h.symbols.Intern(localLabel)
		if localRef32 > math.MaxUint16 {
			return 0, ErrTooManySymbols
		}
		localRef = uint16(localRef32)
	}

	key := seriesKey{targetID, nameID, localRef}
	if ref, ok := h.seriesIndex[key]; ok {
		return ref, nil
	}
	ref := h.series.Create(targetID, nameID, localRef)
	h.seriesIndex[key] = ref
	return ref, nil
}

// Append encodes one sample for the series at ref.
func (h *Head) Append(ref uint32, ts int64, v float64) error {
	return h.series.Append(ref, ts, v)
}

// Iterator returns an iterator over ref's encoded samples.
func (h *Head) Iterator(ref uint32) *Iterator {
	return h.series.Iterator(ref)
}

// NumSeries, NumTargets, NumSymbols report the head's current cardinality.
func (h *Head) NumSeries() int  { return h.series.NumSeries() }
func (h *Head) NumTargets() int { return h.targets.NumTargets() }
func (h *Head) NumSymbols() int { return h.symbols.NumSymbols() }
