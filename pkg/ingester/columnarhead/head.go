package columnarhead

import (
	"errors"
	"math"

	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/metadata"
)

// defaultExemplarCapacity is a placeholder default for exemplarStorage's ring size,
// not a tuned value - real Cortex configures this per-tenant (maxExemplarsForUser in
// pkg/ingester/ingester.go). NewHead doesn't expose it as a parameter yet; that's the
// natural next step if/when this gets wired into real per-tenant config.
const defaultExemplarCapacity = 10_000

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

	metadata   *seriesMetadata
	lastST     map[uint32]int64 // series ref -> most recent start-timestamp recorded via SetSTZeroSample
	exemplars  *exemplarStorage
	histograms *HistogramStore
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
		metadata:    newSeriesMetadata(),
		lastST:      make(map[uint32]int64),
		exemplars:   newExemplarStorage(defaultExemplarCapacity),
		histograms:  NewHistogramStore(),
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

// lookupTarget returns target's symbol-ref tuple and whether it's already known,
// without creating anything - the read-only counterpart to GetOrCreateSeries's target
// resolution, for storage.GetRef.
func (h *Head) lookupTarget(target TargetLabels) ([targetFields]uint32, bool) {
	var tRefs [targetFields]uint32
	for i, s := range [targetFields]string{
		target.Cluster, target.Namespace, target.Pod, target.Container, target.Node, target.Job,
	} {
		id, ok := h.symbols.Lookup(s)
		if !ok {
			return tRefs, false
		}
		tRefs[i] = id
	}
	if _, ok := h.targetIndex[tRefs]; !ok {
		return tRefs, false
	}
	return tRefs, true
}

// lookupSeries returns the series ref for (tRefs, metricName, localLabel) and whether
// it's already known, without creating anything.
func (h *Head) lookupSeries(tRefs [targetFields]uint32, metricName, localLabel string) (uint32, bool) {
	targetID, ok := h.targetIndex[tRefs]
	if !ok {
		return 0, false
	}
	nameID32, ok := h.symbols.Lookup(metricName)
	if !ok || nameID32 > math.MaxUint16 {
		return 0, false
	}
	var localRef uint16
	if localLabel != "" {
		localRef32, ok := h.symbols.Lookup(localLabel)
		if !ok || localRef32 > math.MaxUint16 {
			return 0, false
		}
		localRef = uint16(localRef32)
	}
	ref, ok := h.seriesIndex[seriesKey{targetID, uint16(nameID32), localRef}]
	return ref, ok
}

// Append encodes one sample for the series at ref.
func (h *Head) Append(ref uint32, ts int64, v float64) error {
	return h.series.Append(ref, ts, v)
}

// ErrSeriesNotFound is returned by SetMetadata when ref doesn't correspond to a series
// this Head has created - unlike GetOrCreateSeries, metadata updates don't implicitly
// create series (matches storage.MetadataUpdater's documented contract: "If the series
// does not exist, UpdateMetadata returns an error").
var ErrSeriesNotFound = errors.New("columnarhead: series not found")

// SetMetadata records m for the series at ref. Returns ErrSeriesNotFound if ref is out
// of range for this Head.
func (h *Head) SetMetadata(ref uint32, m metadata.Metadata) error {
	if ref >= uint32(h.series.NumSeries()) {
		return ErrSeriesNotFound
	}
	h.metadata.set(ref, m)
	return nil
}

// Metadata returns the metadata recorded for ref, if any.
func (h *Head) Metadata(ref uint32) (metadata.Metadata, bool) {
	return h.metadata.get(ref)
}

// ErrSTZeroSampleCollision is returned by SetSTZeroSample when st is not strictly
// before the incoming sample's timestamp t - storage.StartTimestampAppender's
// documented contract says the real sample has priority in that case.
var ErrSTZeroSampleCollision = errors.New("columnarhead: start-timestamp collides with the incoming sample")

// ErrSTZeroSampleTooOld is returned by SetSTZeroSample when st is not after the most
// recently recorded start-timestamp for this series - a stale or duplicate zero
// sample, per storage.StartTimestampAppender's "st is too old" rejection case.
var ErrSTZeroSampleTooOld = errors.New("columnarhead: start-timestamp is not newer than the last one recorded")

// SetSTZeroSample records a synthetic zero-value sample at st for the series at ref,
// via the same value/timestamp encoding path a real Append uses. Must be called
// before the corresponding real sample's Append at timestamp t - see
// ErrSTZeroSampleCollision/ErrSTZeroSampleTooOld for the two rejection cases
// storage.StartTimestampAppender's contract documents.
func (h *Head) SetSTZeroSample(ref uint32, t, st int64) error {
	if st >= t {
		return ErrSTZeroSampleCollision
	}
	if last, ok := h.lastST[ref]; ok && st <= last {
		return ErrSTZeroSampleTooOld
	}
	if err := h.series.Append(ref, st, 0); err != nil {
		return err
	}
	h.lastST[ref] = st
	return nil
}

// AppendExemplar stores e for the series at ref. Does not validate ref against
// NumSeries() the way SetMetadata/Append do: an out-of-range ref just means the
// stored exemplar can never be retrieved by a real series, not a corruption risk
// (exemplarStorage indexes by ref value only, never dereferences it into SeriesStore).
func (h *Head) AppendExemplar(ref uint32, e exemplar.Exemplar) {
	h.exemplars.append(ref, e)
}

// Exemplars returns every currently retained exemplar for ref, oldest first.
func (h *Head) Exemplars(ref uint32) []exemplarEntry {
	return h.exemplars.forSeries(ref)
}

// AppendHistogram encodes one histogram sample for the series at ref. See
// HistogramStore's doc comment for what this does and does not support (stable
// schema/zero-threshold/span layout only, no custom buckets).
func (h *Head) AppendHistogram(ref uint32, ts int64, hg *histogram.Histogram) error {
	return h.histograms.Append(ref, ts, hg)
}

// HistogramIterator returns an iterator over ref's encoded histogram samples.
func (h *Head) HistogramIterator(ref uint32) *HistogramIterator {
	return h.histograms.Iterator(ref)
}

// Iterator returns an iterator over ref's encoded samples.
func (h *Head) Iterator(ref uint32) *Iterator {
	return h.series.Iterator(ref)
}

// NumSeries, NumTargets, NumSymbols report the head's current cardinality.
func (h *Head) NumSeries() int  { return h.series.NumSeries() }
func (h *Head) NumTargets() int { return h.targets.NumTargets() }
func (h *Head) NumSymbols() int { return h.symbols.NumSymbols() }
