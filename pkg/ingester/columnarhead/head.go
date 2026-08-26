package columnarhead

import (
	"errors"
	"math"
	"sync"

	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/storage"
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
//
// Concurrency: mu guards every field below. It is deliberately ONE lock for the whole
// Head, not real Prometheus's per-series striped locks - this package's arena/maps are
// shared, process-wide structures (SeriesStore.growSlot can reallocate the ENTIRE
// arena backing every series, not just one), so anything finer would need a real
// redesign, not just a bigger lock table. The tradeoff is real and stated plainly, not
// hidden: a single long-running query (which holds a read lock for its whole
// lifetime - see Querier/ChunkQuerier below) blocks every append for its duration,
// unlike real Prometheus's much more concurrent model.
//
// Most of Head's own methods (Append, AppendHistogram, GetOrCreateSeries,
// SeriesLabels, Iterator, NumSeries, etc.) do NOT lock internally - they assume the
// caller already holds mu, appropriately for a read or a write. The actual safe entry
// points for concurrent use are Appender() (each storage.Appender call takes the write
// lock for its own duration), Querier()/ChunkQuerier() (take the read lock at
// construction, released by Close() - the entire query's lifetime, not just Select()),
// and Truncate() (takes the write lock for its own call). Calling Head's other methods
// directly, concurrently with any of these, is not safe - fine for single-threaded
// test code, not for real concurrent ingest/query traffic.
type Head struct {
	mu sync.RWMutex

	symbols *liveInterner
	targets *TargetStore
	series  *SeriesStore

	targetIndex map[[targetFields]uint32]uint32
	seriesIndex map[seriesKey]uint32

	// namePostings is design doc §3.4's "postings for __name__ only": nameID -> every
	// series ref with that __name__, in creation order. Maintained incrementally in
	// GetOrCreateSeries (append-only, one entry added per new series, never on the
	// dedup-hit path) rather than built as a separate indexing pass - this is the
	// live-head counterpart to the static, MPHF-adjacent postings the design doc
	// describes for the post-compaction case; still a plain Go map like the rest of
	// this package's live indexes (targetIndex, seriesIndex), not yet the compact
	// structure a rebuild-at-compaction path would produce.
	namePostings map[uint16][]uint32

	metadata   *seriesMetadata
	lastST     map[uint32]int64 // series ref -> most recent start-timestamp recorded via SetSTZeroSample
	exemplars  *exemplarStorage
	histograms *HistogramStore

	// ooo, oooTimeWindow: out-of-order float sample support (see ooo.go). A
	// sample older than the in-order stream's own last timestamp is rejected
	// with storage.ErrOutOfOrderSample/ErrDuplicateSampleForTimestamp unless it
	// falls within oooTimeWindow of maxTime, in which case it lands in ooo
	// instead - matching real Prometheus's own default-disabled (oooTimeWindow
	// == 0) semantics until SetOOOTimeWindow is called with something positive.
	ooo           *oooStore
	oooTimeWindow int64

	// minTime, maxTime track the earliest and latest timestamp ever accepted
	// (in-order or OOO) across the whole head - maintained incrementally in
	// Append/AppendHistogram, not computed by a scan, matching real
	// tsdb.Head's own MinTime/MaxTime. Sentinel values (math.MaxInt64/
	// math.MinInt64) before any sample exists, mirroring real Prometheus's own
	// convention for an empty head.
	minTime, maxTime int64
}

// TargetLabels is the fixed 6-label shared block every series belongs to (§3.1).
type TargetLabels struct {
	Cluster, Namespace, Pod, Container, Node, Job string
}

type seriesKey struct {
	targetID  uint32
	nameID    uint16
	localName uint16
	localRef  uint16
	// hasLocal disambiguates "no local label" from "local label whose name and value
	// both happen to intern to id 0" - both would otherwise present as
	// localName=localRef=0 and collide in seriesIndex despite being different series.
	hasLocal bool
}

// NewHead returns an empty Head with capacity preallocated for the expected scale.
func NewHead(expectedSeries, expectedTargets, expectedSymbols int) *Head {
	return &Head{
		symbols:      newLiveInterner(expectedSymbols),
		targets:      NewTargetStore(expectedTargets),
		series:       NewSeriesStore(expectedSeries),
		targetIndex:  make(map[[targetFields]uint32]uint32, expectedTargets),
		seriesIndex:  make(map[seriesKey]uint32, expectedSeries),
		namePostings: make(map[uint16][]uint32),
		metadata:     newSeriesMetadata(),
		lastST:       make(map[uint32]int64),
		exemplars:    newExemplarStorage(defaultExemplarCapacity),
		histograms:   NewHistogramStore(),
		ooo:          newOOOStore(),
		minTime:      math.MaxInt64,
		maxTime:      math.MinInt64,
	}
}

// SetOOOTimeWindow configures how far behind the head's current max timestamp
// an out-of-order float sample may land and still be accepted (into the OOO
// buffer, see ooo.go) rather than rejected outright. 0 (NewHead's default)
// matches real Prometheus's own default-disabled behavior: any sample older
// than the in-order stream's last timestamp is rejected with
// storage.ErrOutOfOrderSample, not buffered. This is the mechanism
// tsdbStore.ApplyConfig would call into once real ingester wiring exists (see
// CHECKLIST.md) - exposed as its own method now so it's independently testable
// without that wiring.
func (h *Head) SetOOOTimeWindow(w int64) {
	h.oooTimeWindow = w
}

// MinTime, MaxTime return the earliest/latest timestamp accepted by the head so
// far (in-order or OOO), or (math.MaxInt64, math.MinInt64) if no sample has
// ever been accepted - matching real tsdb.Head's own empty-head convention.
func (h *Head) MinTime() int64 { return h.minTime }
func (h *Head) MaxTime() int64 { return h.maxTime }

// GetOrCreateSeries resolves target+metricName+(localName,localLabel) to a series
// ref, creating the target, symbols, and series record as needed - repeated calls
// with identical arguments return the same ref rather than creating duplicates.
// localName/localLabel are the name and value of the series-specific label besides
// __name__ (e.g. "le" and "0.1" for a histogram bucket); pass "" for both if the
// series has none - matches SeriesStore's existing single-extra-label model (bench/04's
// original simplification, carried through unchanged here). Both must store the NAME,
// not just the value: two series with the same value under different label names
// (e.g. le="0.1" vs quantile="0.1") are different series, and the read path needs the
// name to reconstruct a faithful label set (see Head.SeriesLabels) - a real gap found
// and fixed while building the Querier, since nothing needed to reconstruct full
// labels before then.
func (h *Head) GetOrCreateSeries(target TargetLabels, metricName, localName, localLabel string) (uint32, error) {
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

	hasLocal := localLabel != ""
	var localNameID, localRef uint16
	if hasLocal {
		localNameID32 := h.symbols.Intern(localName)
		localRef32 := h.symbols.Intern(localLabel)
		if localNameID32 > math.MaxUint16 || localRef32 > math.MaxUint16 {
			return 0, ErrTooManySymbols
		}
		localNameID = uint16(localNameID32)
		localRef = uint16(localRef32)
	}

	key := seriesKey{targetID, nameID, localNameID, localRef, hasLocal}
	if ref, ok := h.seriesIndex[key]; ok {
		return ref, nil
	}
	ref := h.series.Create(targetID, nameID, localNameID, localRef, hasLocal)
	h.seriesIndex[key] = ref
	h.namePostings[nameID] = append(h.namePostings[nameID], ref)
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

// lookupSeries returns the series ref for (tRefs, metricName, localName, localLabel)
// and whether it's already known, without creating anything.
func (h *Head) lookupSeries(tRefs [targetFields]uint32, metricName, localName, localLabel string) (uint32, bool) {
	targetID, ok := h.targetIndex[tRefs]
	if !ok {
		return 0, false
	}
	nameID32, ok := h.symbols.Lookup(metricName)
	if !ok || nameID32 > math.MaxUint16 {
		return 0, false
	}
	hasLocal := localLabel != ""
	var localNameID, localRef uint16
	if hasLocal {
		localNameID32, nameOK := h.symbols.Lookup(localName)
		localRef32, refOK := h.symbols.Lookup(localLabel)
		if !nameOK || !refOK || localNameID32 > math.MaxUint16 || localRef32 > math.MaxUint16 {
			return 0, false
		}
		localNameID = uint16(localNameID32)
		localRef = uint16(localRef32)
	}
	ref, ok := h.seriesIndex[seriesKey{targetID, uint16(nameID32), localNameID, localRef, hasLocal}]
	return ref, ok
}

// Append encodes one sample for the series at ref, after resolving whether it's
// in-order, out-of-order-but-acceptable, an allowed exact duplicate, or must be
// rejected - see appendable's doc comment for the exact rules, which mirror
// real Prometheus's own (see CHECKLIST.md's OOO scoping pass for the citations).
func (h *Head) Append(ref uint32, ts int64, v float64) error {
	action, err := h.appendable(ref, ts, v)
	switch action {
	case appendReject:
		return err
	case appendSkip:
		return nil
	case appendOOO:
		h.ooo.insert(ref, ts, v)
	default: // appendInOrder
		if err := h.series.Append(ref, ts, v); err != nil {
			return err
		}
	}
	h.updateMinMaxTime(ts)
	if h.oooTimeWindow > 0 {
		h.ooo.trim(ref, h.maxTime-h.oooTimeWindow)
	}
	return nil
}

// updateMinMaxTime folds ts into the head-wide MinTime/MaxTime, called by every
// path that accepts a sample (float in-order/OOO, histogram, ST-zero) so those
// two accessors reflect the whole head regardless of sample type.
func (h *Head) updateMinMaxTime(ts int64) {
	if ts < h.minTime {
		h.minTime = ts
	}
	if ts > h.maxTime {
		h.maxTime = ts
	}
}

type appendAction int

const (
	appendInOrder appendAction = iota
	appendOOO
	appendSkip   // exact (ts, value) duplicate of the last in-order sample - a real, valid case (federation, retries), not an error
	appendReject // err is set to the real storage sentinel the caller should see
)

// appendable decides how (ts, v) relates to ref's existing in-order float
// stream, matching real Prometheus's own memSeries.appendable semantics
// (vendor/.../tsdb/head_append.go) rather than inventing new ones:
//   - no samples yet: always in-order.
//   - ts strictly after the last in-order timestamp: in-order.
//   - ts equal to the last in-order timestamp: identical value is a silent
//     no-op (allowed - federation and retries produce exact duplicates in
//     valid, non-noteworthy cases); a different value is rejected with
//     storage.ErrDuplicateSampleForTimestamp, matching real Prometheus refusing
//     to let a later write silently override an earlier one at the same time.
//   - ts before the last in-order timestamp: accepted into the OOO buffer if
//     within oooTimeWindow of the head's current max timestamp, otherwise
//     rejected with storage.ErrOutOfOrderSample (oooTimeWindow == 0, real
//     Prometheus's own default) or storage.ErrTooOldSample (outside the window
//     but OOO is enabled).
//
// Does NOT check ts against every historical in-order timestamp, only the
// latest - matching real Prometheus's own scope exactly (their msMaxt check is
// the same single comparison), not exceeding it. An OOO sample landing on the
// same timestamp as some non-latest in-order sample isn't specially detected
// here, mirroring that real Prometheus doesn't detect it either.
func (h *Head) appendable(ref uint32, ts int64, v float64) (appendAction, error) {
	lastTS, lastBits, ok := h.series.LastSample(ref)
	if !ok {
		return appendInOrder, nil
	}
	if ts > lastTS {
		return appendInOrder, nil
	}
	if ts == lastTS {
		if lastBits == math.Float64bits(v) {
			return appendSkip, nil
		}
		return appendReject, storage.NewDuplicateFloatErr(ts, math.Float64frombits(lastBits), v)
	}
	if h.oooTimeWindow > 0 && ts >= h.maxTime-h.oooTimeWindow {
		return appendOOO, nil
	}
	if h.oooTimeWindow > 0 {
		return appendReject, storage.ErrTooOldSample
	}
	return appendReject, storage.ErrOutOfOrderSample
}

// SeriesLabels reconstructs ref's full label set: the six target labels, __name__,
// and the one optional extra label, in the shape splitLabels originally accepted -
// the read-side inverse of GetOrCreateSeries's write-side resolution.
func (h *Head) SeriesLabels(ref uint32) labels.Labels {
	tRefs := h.targets.Get(h.series.TargetID(ref))
	b := labels.NewScratchBuilder(8)
	b.Add(labels.MetricName, h.symbols.String(uint32(h.series.NameID(ref))))
	b.Add(labelCluster, h.symbols.String(tRefs[0]))
	b.Add(labelNamespace, h.symbols.String(tRefs[1]))
	b.Add(labelPod, h.symbols.String(tRefs[2]))
	b.Add(labelContainer, h.symbols.String(tRefs[3]))
	b.Add(labelNode, h.symbols.String(tRefs[4]))
	b.Add(labelJob, h.symbols.String(tRefs[5]))
	if h.series.HasLocal(ref) {
		b.Add(h.symbols.String(uint32(h.series.LocalName(ref))), h.symbols.String(uint32(h.series.LocalRef(ref))))
	}
	b.Sort()
	return b.Labels()
}

// SeriesLabelValue returns just ref's value for label name, without reconstructing
// the full label set SeriesLabels does (no ScratchBuilder, no Sort) - "" if ref has
// no such label. Exists specifically so Select's matcher-filtering step (querier.go's
// matchesAll) can check a candidate series against a handful of matchers without
// paying full-reconstruction cost for every candidate, most of which won't even pass
// - profiling showed SeriesLabels' builder+sort dominating a full scan's cost
// (measured: >50% of CPU time - see CHECKLIST.md), which this directly targets.
func (h *Head) SeriesLabelValue(ref uint32, name string) string {
	switch name {
	case labels.MetricName:
		return h.symbols.String(uint32(h.series.NameID(ref)))
	case labelCluster, labelNamespace, labelPod, labelContainer, labelNode, labelJob:
		tRefs := h.targets.Get(h.series.TargetID(ref))
		return h.symbols.String(tRefs[targetLabelIndex(name)])
	default:
		if h.series.HasLocal(ref) && h.symbols.String(uint32(h.series.LocalName(ref))) == name {
			return h.symbols.String(uint32(h.series.LocalRef(ref)))
		}
		return ""
	}
}

// targetLabelIndex maps one of the six fixed target label names to its position in
// TargetStore's [targetFields]uint32 tuple (§3.1's fixed cluster/namespace/pod/
// container/node/job order). Panics on an unrecognized name - callers (only
// SeriesLabelValue) already gate on the same name set in their switch, so reaching
// here with anything else is a programming error, not a real-world input to guard.
func targetLabelIndex(name string) int {
	switch name {
	case labelCluster:
		return 0
	case labelNamespace:
		return 1
	case labelPod:
		return 2
	case labelContainer:
		return 3
	case labelNode:
		return 4
	case labelJob:
		return 5
	}
	panic("columnarhead: targetLabelIndex called with an unrecognized label name: " + name)
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
	h.updateMinMaxTime(st)
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
	if err := h.histograms.Append(ref, ts, hg); err != nil {
		return err
	}
	h.updateMinMaxTime(ts)
	return nil
}

// HistogramIterator returns an iterator over ref's encoded histogram samples.
func (h *Head) HistogramIterator(ref uint32) *HistogramIterator {
	return h.histograms.Iterator(ref)
}

// Iterator returns an iterator over ref's encoded samples.
func (h *Head) Iterator(ref uint32) *Iterator {
	return h.series.Iterator(ref)
}

// SeriesRefsForName returns every series ref with __name__ == metricName, and
// whether metricName is known at all - the read path for design doc §3.4's
// __name__-postings shortcut (see Querier.Select). A read-only lookup: unlike
// GetOrCreateSeries, an unknown metricName is never interned as a side effect of
// asking whether it exists (same reasoning as lookupTarget/lookupSeries).
func (h *Head) SeriesRefsForName(metricName string) ([]uint32, bool) {
	nameID32, ok := h.symbols.Lookup(metricName)
	if !ok || nameID32 > math.MaxUint16 {
		return nil, false
	}
	refs, ok := h.namePostings[uint16(nameID32)]
	return refs, ok
}

// Truncate drops every sample with ts < mint across every series, both float
// (SeriesStore) and histogram (HistogramStore) - this package's counterpart to real
// Prometheus's tsdb.Head.Truncate, needed to keep a running head's memory bounded once
// compaction has made a range durable elsewhere (see CHECKLIST.md's Phase 5a: without
// this, the columnar arena only ever grows). Unlike real Prometheus, which drops
// per-series memChunk object references directly, this format has no seek/cut point -
// each series is one continuous cross-sample-encoded stream, so truncating means
// fully decoding and re-encoding the retained range (see SeriesStore.Truncate/
// HistogramStore.Truncate's doc comments).
//
// No series is ever removed from the head's indexes here: refs are permanent for the
// process's life (targetIndex/seriesIndex/namePostings all key on them directly), so a
// series truncated down to zero remaining samples just stays allocated and empty -
// exactly the already-supported "matcher hits, zero samples in range" case Querier's
// doc comment describes, not a new kind of state. That means this reclaims arena
// bytes for aged-out sample data (real, bounded memory reclaim) but NOT the
// O(1)-per-series index/postings/full-scan cost of series nobody will ever query
// again - full removal of wholly-empty series from those structures is a further,
// not-yet-built step.
//
// Takes the write lock for its whole call - a real entry point for concurrent use
// (see Head's doc comment), since a real compaction goroutine calling this runs
// concurrently with live append/query traffic.
func (h *Head) Truncate(mint int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := uint32(h.series.NumSeries())
	for ref := uint32(0); ref < n; ref++ {
		h.series.Truncate(ref, mint)
		h.histograms.Truncate(ref, mint)
	}
}

// NumSeries, NumTargets, NumSymbols report the head's current cardinality.
func (h *Head) NumSeries() int  { return h.series.NumSeries() }
func (h *Head) NumTargets() int { return h.targets.NumTargets() }
func (h *Head) NumSymbols() int { return h.symbols.NumSymbols() }
