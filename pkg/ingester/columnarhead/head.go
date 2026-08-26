package columnarhead

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"

	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/index"
)

// defaultExemplarCapacity is a placeholder default for exemplarStorage's ring size,
// not a tuned value - real Cortex configures this per-tenant (maxExemplarsForUser in
// pkg/ingester/ingester.go). NewHead doesn't expose it as a parameter yet; that's the
// natural next step if/when this gets wired into real per-tenant config.
const defaultExemplarCapacity = 10_000

// defaultNumShards is NewHead's shard count for SeriesStore/HistogramStore/oooStore
// (see seriesShard's doc comment). Deliberately NOT copying real Prometheus's
// stripeSeries sizing (DefaultStripeSize = 1<<14) - that's a bucket-array size for a
// scheme where each "stripe" is just a mutex guarding a map; each of OUR shards is a
// full SeriesStore with real (if small) fixed cost - a map, a mutex, parallel slices.
// Started modest per CHECKLIST.md's locked-down locking design, to be tuned against
// real contention measurements rather than copied from a different problem's number.
const defaultNumShards = 32

// ErrTooManySymbols is returned by GetOrCreateSeries if a metric name or local label
// value would be the 65,537th distinct one interned - SeriesStore's nameID/localRef
// fields are uint16 (see series.go's comment on why targetID is uint32 but these
// aren't: metric names and "le"-style local label values come from a bounded
// vocabulary that doesn't grow with fleet churn, unlike targets). Failing loudly here
// beats silently truncating an id and corrupting an unrelated series' lookup.
var ErrTooManySymbols = errors.New("columnarhead: nameID/localRef would overflow uint16")

// seriesShard is one partition of the head's per-series storage - see
// CHECKLIST.md's Phase 4 locking design for the full reasoning behind this specific
// split, summarized here: SeriesStore's shared arena is the one structure with a
// free-list-reuse + shared-backing-array hazard (growSlot's append() can reallocate
// the whole backing array, memmoving every existing byte - a race with ANY concurrent
// reader, and a freed region can be reused by a DIFFERENT series while a stale reader
// of the original owner is still mid-decode). Physically separating SeriesStore into
// N independent shards (own arena, own free list, not a global pool split N ways)
// confines reuse to within a shard purely as a byte-addressing fact - a freed region
// in shard K's arena can only ever be reused by a series that also lives in shard K.
// That collapses the stale-reader-vs-reuse hazard into a single-shard problem, which
// shard K's own lock already solves: a reader holding shard K's lock excludes any
// writer (growSlot/alloc/free) in shard K for the read's duration - the same safety
// proof as the old single coarse lock, just narrowed to shard scope. No epoch
// counters, reference counting, or reclamation scheme needed for this to be correct.
//
// HistogramStore and oooStore are bundled into the same shard (same lock, same local
// index) purely for simplicity - HistogramStore already has no cross-series reuse
// hazard of its own (independent per-series arenas, growHisto just appends; found
// while building histogram persistence), so it didn't need partitioning to be safe,
// but sharing SeriesStore's shard boundary keeps one lock covering a series'
// complete state rather than inventing a second, different partitioning scheme.
type seriesShard struct {
	mu         sync.RWMutex
	series     *SeriesStore
	histograms *HistogramStore
	ooo        *oooStore
}

// Head ties a live symbol interner, a target slab, and sharded per-series storage
// into an actual ingest path: given raw label strings for a sample, resolve or
// create its target and series, then append. This is the first thing in this
// package that looks like something storage.Appender.Append could call - it is NOT
// wired into Cortex's tsdbStore interface (pkg/ingester's Phase 1 work) yet; that
// integration is separate, later work.
//
// Live lookup goes through plain Go maps (targetIndex, seriesIndex, and liveInterner's
// internal index), not the static MPHF/SymbolTable built earlier in this package - see
// liveInterner's doc comment for why. The MPHF/SymbolTable are for a rebuild-at-
// compaction path that doesn't exist yet; until it does, Head's real memory cost
// includes live Go map overhead on top of the tight series/target records, not the
// compact MPHF projection quoted elsewhere in CHECKLIST.md for the post-compaction
// case. See TestHeadAtScale for the honest, measured total.
//
// Concurrency, per CHECKLIST.md's locked-down locking design (Phase A - writer/writer
// parallelism across shards, NOT yet reader/writer concurrency):
//   - indexMu guards symbols/targets/targetIndex/seriesIndex/namePostings/nextRef/
//     metadata/lastST/exemplars - structures that are either inherently global
//     (symbol interning needs centrally-coordinated id uniqueness to avoid two
//     goroutines minting different ids for the same string) or small/infrequent
//     enough that partitioning them wouldn't help. ALWAYS acquired before any
//     shard's lock, never after, throughout this package - the one global lock
//     ordering rule that makes multi-lock acquisition (Querier, Truncate) safe
//     without a more general deadlock-avoidance scheme.
//   - shards[ref%len(shards)] (local index ref/len(shards)) guards one partition's
//     SeriesStore/HistogramStore/oooStore - see seriesShard's doc comment.
//   - minTime/maxTime/oooTimeWindow are lock-free (atomic.Int64) - every append
//     touches minTime/maxTime regardless of shard, so a mutex here would
//     reintroduce a global bottleneck defeating the point of sharding.
//
// Most of Head's own per-series READ methods (SeriesLabels, SeriesLabelValue,
// Iterator, HistogramIterator, SeriesRefsForName) do NOT lock internally - they
// assume the caller already holds indexMu and every shard's lock, appropriately.
// The actual safe entry points for concurrent use are Appender() (each
// storage.Appender call resolves and locks exactly what it touches, no more),
// Querier()/ChunkQuerier() (take indexMu's read lock plus every shard's read lock,
// in that fixed order, at construction - released by Close(), the entire query's
// lifetime, not just Select() - this is why Phase A doesn't yet unlock reader/writer
// concurrency: a query still blocks every shard's writers for its duration, same as
// today, just narrowed from "every write" to "every write across every shard" which
// is identical since a query can touch any shard), and Truncate() (takes every
// shard's write lock in the same fixed order). Calling Head's read methods directly,
// concurrently with any of these, is not safe - fine for single-threaded test code,
// not for real concurrent ingest/query traffic.
type Head struct {
	indexMu sync.RWMutex

	symbols *liveInterner
	targets *TargetStore

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

	// nextRef is the next global series ref to assign - series refs are dense,
	// sequential, and permanent (0, 1, 2, ...), matching the pre-sharding design
	// exactly; only WHICH shard stores a given ref's data changed. NumSeries()
	// returns this directly rather than summing every shard's own count.
	nextRef uint32

	metadata  *seriesMetadata
	lastST    map[uint32]int64 // series ref -> most recent start-timestamp recorded via SetSTZeroSample
	exemplars *exemplarStorage

	shards []*seriesShard

	// lifecycleCallback, if set (SetSeriesLifecycleCallback), is invoked exactly
	// around genuine series creation in GetOrCreateSeries - see
	// SeriesLifecycleCallback's own doc comment. nil by default: every existing
	// NewHead/NewHeadWithShards caller and test is unaffected, no signature
	// churn needed to add this.
	lifecycleCallback SeriesLifecycleCallback

	// oooTimeWindow: out-of-order float sample support (see ooo.go). A sample older
	// than the in-order stream's own last timestamp is rejected with
	// storage.ErrOutOfOrderSample/ErrDuplicateSampleForTimestamp unless it falls
	// within oooTimeWindow of maxTime, in which case it lands in the relevant
	// shard's oooStore instead - matching real Prometheus's own default-disabled
	// (oooTimeWindow == 0) semantics until SetOOOTimeWindow is called with
	// something positive. atomic, not indexMu-guarded: read on every single
	// append regardless of shard, so a mutex here would be a new bottleneck.
	oooTimeWindow atomic.Int64

	// minTime, maxTime track the earliest and latest timestamp ever accepted
	// (in-order or OOO) across the whole head - maintained incrementally in
	// Append/AppendHistogram/SetSTZeroSample via lock-free CAS, not computed by a
	// scan, matching real tsdb.Head's own MinTime/MaxTime. Sentinel values
	// (math.MaxInt64/math.MinInt64) before any sample exists, mirroring real
	// Prometheus's own convention for an empty head.
	minTime, maxTime atomic.Int64
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

// NewHead returns an empty Head with capacity preallocated for the expected scale,
// using defaultNumShards shards. See NewHeadWithShards to control the shard count
// directly (e.g. for tests that want to force cross-shard scenarios deterministically).
func NewHead(expectedSeries, expectedTargets, expectedSymbols int) *Head {
	return NewHeadWithShards(expectedSeries, expectedTargets, expectedSymbols, defaultNumShards)
}

// NewHeadWithShards is NewHead with an explicit shard count - see defaultNumShards'
// doc comment for why this isn't sized like real Prometheus's stripeSeries.
func NewHeadWithShards(expectedSeries, expectedTargets, expectedSymbols, numShards int) *Head {
	if numShards < 1 {
		numShards = 1
	}
	perShard := (expectedSeries + numShards - 1) / numShards
	shards := make([]*seriesShard, numShards)
	for i := range shards {
		shards[i] = &seriesShard{
			series:     NewSeriesStore(perShard),
			histograms: NewHistogramStore(),
			ooo:        newOOOStore(),
		}
	}
	h := &Head{
		symbols:      newLiveInterner(expectedSymbols),
		targets:      NewTargetStore(expectedTargets),
		targetIndex:  make(map[[targetFields]uint32]uint32, expectedTargets),
		seriesIndex:  make(map[seriesKey]uint32, expectedSeries),
		namePostings: make(map[uint16][]uint32),
		metadata:     newSeriesMetadata(),
		lastST:       make(map[uint32]int64),
		exemplars:    newExemplarStorage(defaultExemplarCapacity),
		shards:       shards,
	}
	h.minTime.Store(math.MaxInt64)
	h.maxTime.Store(math.MinInt64)
	return h
}

// shardFor returns the shard owning ref, and ref's local index within it - a series'
// global ref maps to shards[ref % len(shards)], at local index ref / len(shards).
// Lock-free: shards itself is fixed-size for the Head's whole life, only its
// CONTENTS need the returned shard's own lock, which every caller of shardFor is
// responsible for acquiring appropriately (read or write) before touching them.
func (h *Head) shardFor(ref uint32) (*seriesShard, uint32) {
	n := uint32(len(h.shards))
	return h.shards[ref%n], ref / n
}

// SetOOOTimeWindow configures how far behind the head's current max timestamp
// an out-of-order float sample may land and still be accepted (into the relevant
// shard's OOO buffer, see ooo.go) rather than rejected outright. 0 (NewHead's
// default) matches real Prometheus's own default-disabled behavior: any sample
// older than the in-order stream's last timestamp is rejected with
// storage.ErrOutOfOrderSample, not buffered. This is the mechanism
// tsdbStore.ApplyConfig would call into once real ingester wiring exists (see
// CHECKLIST.md) - exposed as its own method now so it's independently testable
// without that wiring.
func (h *Head) SetOOOTimeWindow(w int64) {
	h.oooTimeWindow.Store(w)
}

// SeriesLifecycleCallback mirrors real tsdb.SeriesLifecycleCallback's PreCreation/
// PostCreation shape (vendor/.../tsdb/head.go) - a real, previously-missing hook
// found by an external review of Phase 7's ingester wiring: without it, a
// columnarhead-backed tenant's per-metric-name limit, per-label-set limits, active-
// series tracker, and MaxInMemorySeries/memory_series_created_total accounting all
// silently no-op, since nothing ever called into them (Cortex's userTSDB implements
// the real 3-method interface and is passed as tsdb.Options.SeriesLifecycleCallback
// for the real backend, but the columnar path had no equivalent hook at all).
//
// PostDeletion is deliberately NOT part of this interface: columnarhead never
// removes a series from its indexes at all, even after Truncate empties every
// sample it has (Head.Truncate's own doc comment states this explicitly) - there is
// no "series deleted" event to ever fire PostDeletion for. A real, stated
// consequence, not silently glossed over: counters PostDeletion would normally
// decrement (seriesInMetric, labelSetCounter, trackerCounter, instanceSeriesCount)
// only ever GROW for a columnar-backed tenant while its TSDB stays open, unlike the
// real backend where per-series GC keeps them accurate over time - they only reset
// when the whole tenant's TSDB is closed. A caller (e.g. Cortex's userTSDB) that
// already implements the full 3-method tsdb.SeriesLifecycleCallback interface
// satisfies this narrower one for free, no adapter needed.
type SeriesLifecycleCallback interface {
	// PreCreation is called before a genuinely new series is created (never on a
	// dedup hit against an existing one) - returning an error rejects the
	// creation, propagated as GetOrCreateSeries' own error.
	PreCreation(labels.Labels) error
	// PostCreation is called after a genuinely new series has been created.
	PostCreation(labels.Labels)
}

// SetSeriesLifecycleCallback installs cb, invoked around every genuinely new
// series GetOrCreateSeries creates from now on - see SeriesLifecycleCallback's own
// doc comment for what this does and does not cover. nil (NewHead/
// NewHeadWithShards' default) means no callback fires at all, preserving every
// existing caller's behavior. Self-locking (indexMu) - safe to call concurrently
// with real ingest traffic, though real callers set this once, right after
// construction, before any real traffic (matching how Cortex sets userTSDB.limiter
// post-construction for the identical "don't limit during reload" reason).
func (h *Head) SetSeriesLifecycleCallback(cb SeriesLifecycleCallback) {
	h.indexMu.Lock()
	defer h.indexMu.Unlock()
	h.lifecycleCallback = cb
}

// MinTime, MaxTime return the earliest/latest timestamp accepted by the head so
// far (in-order or OOO), or (math.MaxInt64, math.MinInt64) if no sample has
// ever been accepted - matching real tsdb.Head's own empty-head convention.
func (h *Head) MinTime() int64 { return h.minTime.Load() }
func (h *Head) MaxTime() int64 { return h.maxTime.Load() }

// updateMinMaxTime folds ts into the head-wide MinTime/MaxTime via lock-free CAS
// loops, called by every path that accepts a sample (float in-order/OOO, histogram,
// ST-zero) so those two accessors reflect the whole head regardless of sample type
// or which shard it landed in.
func (h *Head) updateMinMaxTime(ts int64) {
	for {
		cur := h.minTime.Load()
		if ts >= cur || h.minTime.CompareAndSwap(cur, ts) {
			break
		}
	}
	for {
		cur := h.maxTime.Load()
		if ts <= cur || h.maxTime.CompareAndSwap(cur, ts) {
			break
		}
	}
}

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
//
// Self-locking (see Head's doc comment): takes indexMu for the whole call (symbol/
// target interning and series dedup all need it regardless), plus the target shard's
// lock briefly, only when actually creating a new series.
func (h *Head) GetOrCreateSeries(target TargetLabels, metricName, localName, localLabel string) (uint32, error) {
	h.indexMu.Lock()
	defer h.indexMu.Unlock()

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

	// A genuinely new series - the one point PreCreation/PostCreation fire, not
	// on a dedup hit above. See SeriesLifecycleCallback's own doc comment for
	// why this exists and what it deliberately doesn't cover.
	var lbls labels.Labels
	if h.lifecycleCallback != nil {
		lbls = buildLabels(target, metricName, localName, localLabel)
		// PreCreation (e.g. userDB's) commonly calls back into NumSeries or
		// PostingsForMatchers, both of which need indexMu themselves - held
		// non-reentrantly, so it must be released across this call or every
		// such callback self-deadlocks (confirmed by a hanging test before
		// this fix). Re-check the dedup key on reacquire: another goroutine
		// may have created this exact series while we were unlocked.
		h.indexMu.Unlock()
		err := h.lifecycleCallback.PreCreation(lbls)
		h.indexMu.Lock()
		if err != nil {
			return 0, err
		}
		if ref, ok := h.seriesIndex[key]; ok {
			return ref, nil
		}
	}

	ref := h.nextRef
	shard, localIdx := h.shardFor(ref)
	shard.mu.Lock()
	got := shard.series.Create(targetID, nameID, localNameID, localRef, hasLocal)
	shard.mu.Unlock()
	if got != localIdx {
		// Would mean the global ref counter and a shard's own local numbering
		// diverged - only possible if something bypassed GetOrCreateSeries to
		// create a series directly, a programming error, not a runtime condition.
		panic("columnarhead: shard local index diverged from global ref accounting")
	}

	h.nextRef++
	h.seriesIndex[key] = ref
	h.namePostings[nameID] = append(h.namePostings[nameID], ref)

	if h.lifecycleCallback != nil {
		h.lifecycleCallback.PostCreation(lbls)
	}
	return ref, nil
}

// buildLabels reconstructs the full label set GetOrCreateSeries's own arguments
// represent - the same shape SeriesLabels reconstructs from an already-created
// series' ref, but usable BEFORE a ref exists yet (SeriesLifecycleCallback.
// PreCreation needs the labels before creation, not after). Omits an empty
// target label, matching SeriesLabels' own real-Prometheus-compatible
// "empty-valued label == absent" fix (CHECKLIST.md's Phase 7 step 5 note) - kept
// in sync deliberately, not by accident, since both reconstruct the identical
// shape from the identical inputs.
func buildLabels(target TargetLabels, metricName, localName, localLabel string) labels.Labels {
	b := labels.NewScratchBuilder(8)
	b.Add(labels.MetricName, metricName)
	addIfNotEmptyLabel(&b, labelCluster, target.Cluster)
	addIfNotEmptyLabel(&b, labelNamespace, target.Namespace)
	addIfNotEmptyLabel(&b, labelPod, target.Pod)
	addIfNotEmptyLabel(&b, labelContainer, target.Container)
	addIfNotEmptyLabel(&b, labelNode, target.Node)
	addIfNotEmptyLabel(&b, labelJob, target.Job)
	if localLabel != "" {
		b.Add(localName, localLabel)
	}
	b.Sort()
	return b.Labels()
}

func addIfNotEmptyLabel(b *labels.ScratchBuilder, name, value string) {
	if value != "" {
		b.Add(name, value)
	}
}

// LookupSeriesRef returns the series ref for (target, metricName, localName,
// localLabel) if it's already known, without creating anything - the self-locking
// counterpart to lookupTarget+lookupSeries for callers (storage.GetRef,
// AppendExemplar/UpdateMetadata's label-resolution fallback) that aren't already
// holding indexMu themselves.
func (h *Head) LookupSeriesRef(target TargetLabels, metricName, localName, localLabel string) (uint32, bool) {
	h.indexMu.RLock()
	defer h.indexMu.RUnlock()
	tRefs, ok := h.lookupTarget(target)
	if !ok {
		return 0, false
	}
	return h.lookupSeries(tRefs, metricName, localName, localLabel)
}

// lookupTarget returns target's symbol-ref tuple and whether it's already known,
// without creating anything - the read-only counterpart to GetOrCreateSeries's target
// resolution. Not self-locking - callers must already hold indexMu (LookupSeriesRef
// does; GetOrCreateSeries does).
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
// and whether it's already known, without creating anything. Not self-locking - see
// lookupTarget.
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
//
// Self-locking: takes ref's shard's write lock for the call - the one lock this
// touches, since ref is already resolved (no indexMu needed).
func (h *Head) Append(ref uint32, ts int64, v float64) error {
	shard, localIdx := h.shardFor(ref)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	action, err := h.appendable(shard, localIdx, ts, v)
	switch action {
	case appendReject:
		return err
	case appendSkip:
		return nil
	case appendOOO:
		shard.ooo.insert(localIdx, ts, v)
	default: // appendInOrder
		if err := shard.series.Append(localIdx, ts, v); err != nil {
			return err
		}
	}
	h.updateMinMaxTime(ts)
	if window := h.oooTimeWindow.Load(); window > 0 {
		shard.ooo.trim(localIdx, h.maxTime.Load()-window)
	}
	return nil
}

type appendAction int

const (
	appendInOrder appendAction = iota
	appendOOO
	appendSkip   // exact (ts, value) duplicate of the last in-order sample - a real, valid case (federation, retries), not an error
	appendReject // err is set to the real storage sentinel the caller should see
)

// appendable decides how (ts, v) relates to (shard, localIdx)'s existing in-order
// float stream, matching real Prometheus's own memSeries.appendable semantics
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
//
// Not self-locking - the caller (Append) already holds shard's lock.
func (h *Head) appendable(shard *seriesShard, localIdx uint32, ts int64, v float64) (appendAction, error) {
	lastTS, lastBits, ok := shard.series.LastSample(localIdx)
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
	maxTime := h.maxTime.Load()
	window := h.oooTimeWindow.Load()
	if window > 0 && ts >= maxTime-window {
		return appendOOO, nil
	}
	if window > 0 {
		return appendReject, storage.ErrTooOldSample
	}
	return appendReject, storage.ErrOutOfOrderSample
}

// SeriesLabels reconstructs ref's full label set: the six target labels, __name__,
// and the one optional extra label, in the shape splitLabels originally accepted -
// the read-side inverse of GetOrCreateSeries's write-side resolution.
//
// Not self-locking - callers must already hold indexMu (for symbols/targets) and
// ref's shard's lock (for series identity fields); Querier/ChunkQuerier hold both
// for their whole lifetime.
func (h *Head) SeriesLabels(ref uint32) labels.Labels {
	shard, localIdx := h.shardFor(ref)
	tRefs := h.targets.Get(shard.series.TargetID(localIdx))
	b := labels.NewScratchBuilder(8)
	b.Add(labels.MetricName, h.symbols.String(uint32(shard.series.NameID(localIdx))))
	// A target label that was never set on the original series interns to the
	// empty string (GetOrCreateSeries interns target.Cluster etc. as-is, with
	// no separate "absent" sentinel) - real Prometheus label-set semantics
	// treat an empty-valued label as equivalent to absent (Labels.Get returns
	// "" either way, and no matcher ever distinguishes the two), so it must
	// be OMITTED here, not added as a real, present, empty-string label pair.
	// splitLabels (appender.go) already accepts a series with some or all
	// target labels absent - a bug found via a genuinely minimal end-to-end
	// push (just __name__, no target labels) through the real ingester path,
	// CHECKLIST.md's Phase 7 step 5; every existing test in this package
	// happened to always set all six, so the asymmetry between what
	// splitLabels accepts and what this reconstructed was never exercised
	// until then.
	addIfNotEmpty := func(name string, symID uint32) {
		if v := h.symbols.String(symID); v != "" {
			b.Add(name, v)
		}
	}
	addIfNotEmpty(labelCluster, tRefs[0])
	addIfNotEmpty(labelNamespace, tRefs[1])
	addIfNotEmpty(labelPod, tRefs[2])
	addIfNotEmpty(labelContainer, tRefs[3])
	addIfNotEmpty(labelNode, tRefs[4])
	addIfNotEmpty(labelJob, tRefs[5])
	if shard.series.HasLocal(localIdx) {
		b.Add(h.symbols.String(uint32(shard.series.LocalName(localIdx))), h.symbols.String(uint32(shard.series.LocalRef(localIdx))))
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
//
// Not self-locking - see SeriesLabels.
func (h *Head) SeriesLabelValue(ref uint32, name string) string {
	shard, localIdx := h.shardFor(ref)
	switch name {
	case labels.MetricName:
		return h.symbols.String(uint32(shard.series.NameID(localIdx)))
	case labelCluster, labelNamespace, labelPod, labelContainer, labelNode, labelJob:
		tRefs := h.targets.Get(shard.series.TargetID(localIdx))
		return h.symbols.String(tRefs[targetLabelIndex(name)])
	default:
		if shard.series.HasLocal(localIdx) && h.symbols.String(uint32(shard.series.LocalName(localIdx))) == name {
			return h.symbols.String(uint32(shard.series.LocalRef(localIdx)))
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
// of range for this Head. Self-locking (indexMu - metadata isn't sharded, see Head's
// doc comment on why).
func (h *Head) SetMetadata(ref uint32, m metadata.Metadata) error {
	h.indexMu.Lock()
	defer h.indexMu.Unlock()
	if ref >= h.nextRef {
		return ErrSeriesNotFound
	}
	h.metadata.set(ref, m)
	return nil
}

// Metadata returns the metadata recorded for ref, if any. Self-locking (indexMu) -
// unlike the sharded per-series read methods, metadata isn't sharded, and this isn't
// currently called from within an active Querier's already-held locks, so it's safe
// (and simpler) for this one to lock itself rather than rely on a caller.
func (h *Head) Metadata(ref uint32) (metadata.Metadata, bool) {
	h.indexMu.RLock()
	defer h.indexMu.RUnlock()
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
//
// Self-locking: indexMu briefly (lastST isn't sharded - a small, low-frequency map),
// then ref's shard's write lock for the actual encode.
func (h *Head) SetSTZeroSample(ref uint32, t, st int64) error {
	h.indexMu.Lock()
	if st >= t {
		h.indexMu.Unlock()
		return ErrSTZeroSampleCollision
	}
	if last, ok := h.lastST[ref]; ok && st <= last {
		h.indexMu.Unlock()
		return ErrSTZeroSampleTooOld
	}
	h.lastST[ref] = st
	h.indexMu.Unlock()

	shard, localIdx := h.shardFor(ref)
	shard.mu.Lock()
	err := shard.series.Append(localIdx, st, 0)
	shard.mu.Unlock()
	if err != nil {
		return err
	}
	h.updateMinMaxTime(st)
	return nil
}

// AppendExemplar stores e for the series at ref. Does not validate ref against
// NumSeries() the way SetMetadata/Append do: an out-of-range ref just means the
// stored exemplar can never be retrieved by a real series, not a corruption risk
// (exemplarStorage indexes by ref value only, never dereferences it into SeriesStore).
// Self-locking (indexMu - exemplars aren't sharded, see Head's doc comment on why).
func (h *Head) AppendExemplar(ref uint32, e exemplar.Exemplar) {
	h.indexMu.Lock()
	defer h.indexMu.Unlock()
	h.exemplars.append(ref, e)
}

// Exemplars returns every currently retained exemplar for ref, oldest first.
// Self-locking (indexMu) - see Metadata's doc comment for the same reasoning.
func (h *Head) Exemplars(ref uint32) []exemplarEntry {
	h.indexMu.RLock()
	defer h.indexMu.RUnlock()
	return h.exemplars.forSeries(ref)
}

// AppendHistogram encodes one integer-count histogram sample for the series at ref.
// See HistogramStore's doc comment for what this does and does not support (stable
// schema/zero-threshold/span layout only, no custom buckets). Self-locking: ref's
// shard's write lock for the call.
func (h *Head) AppendHistogram(ref uint32, ts int64, hg *histogram.Histogram) error {
	shard, localIdx := h.shardFor(ref)
	shard.mu.Lock()
	err := shard.histograms.Append(localIdx, ts, hg)
	shard.mu.Unlock()
	if err != nil {
		return err
	}
	h.updateMinMaxTime(ts)
	return nil
}

// AppendFloatHistogram encodes one float-count histogram sample for the series at
// ref - the FloatHistogram counterpart to AppendHistogram. Self-locking: ref's
// shard's write lock for the call.
func (h *Head) AppendFloatHistogram(ref uint32, ts int64, hg *histogram.FloatHistogram) error {
	shard, localIdx := h.shardFor(ref)
	shard.mu.Lock()
	err := shard.histograms.AppendFloat(localIdx, ts, hg)
	shard.mu.Unlock()
	if err != nil {
		return err
	}
	h.updateMinMaxTime(ts)
	return nil
}

// HistogramIterator returns an iterator over ref's encoded histogram samples (either
// integer- or float-typed - see HasFloatHistogram/HistogramIterator's own doc
// comment on At vs AtFloat). Not self-locking - see SeriesLabels.
func (h *Head) HistogramIterator(ref uint32) *HistogramIterator {
	shard, localIdx := h.shardFor(ref)
	return shard.histograms.Iterator(localIdx)
}

// HasFloatHistogram reports whether ref's histogram samples are FloatHistogram-typed
// - meaningless (and false) if !HasHistogram(ref). Not self-locking - see
// SeriesLabels.
func (h *Head) HasFloatHistogram(ref uint32) bool {
	shard, localIdx := h.shardFor(ref)
	return shard.histograms.IsFloat(localIdx)
}

// Iterator returns an iterator over ref's encoded samples. Not self-locking - see
// SeriesLabels.
func (h *Head) Iterator(ref uint32) *Iterator {
	shard, localIdx := h.shardFor(ref)
	return shard.series.Iterator(localIdx)
}

// HasHistogram reports whether ref ever received a histogram sample - the read
// path's series-type check (querier.go/chunk_querier.go, deciding between a float
// and histogram iterator). Not self-locking - see SeriesLabels.
func (h *Head) HasHistogram(ref uint32) bool {
	shard, localIdx := h.shardFor(ref)
	return shard.histograms.Has(localIdx)
}

// OOOSamples returns ref's current out-of-order float sample buffer, oldest first -
// nil if ref has none (see ooo.go's oooStore.samples). Not self-locking - see
// SeriesLabels.
func (h *Head) OOOSamples(ref uint32) []oooSample {
	shard, localIdx := h.shardFor(ref)
	return shard.ooo.samples(localIdx)
}

// SeriesRefsForName returns every series ref with __name__ == metricName, and
// whether metricName is known at all - the read path for design doc §3.4's
// __name__-postings shortcut (see Querier.Select). A read-only lookup: unlike
// GetOrCreateSeries, an unknown metricName is never interned as a side effect of
// asking whether it exists (same reasoning as lookupTarget/lookupSeries). Not
// self-locking - callers must already hold indexMu (Querier/ChunkQuerier do, for
// their whole lifetime).
func (h *Head) SeriesRefsForName(metricName string) ([]uint32, bool) {
	nameID32, ok := h.symbols.Lookup(metricName)
	if !ok || nameID32 > math.MaxUint16 {
		return nil, false
	}
	refs, ok := h.namePostings[uint16(nameID32)]
	return refs, ok
}

// PostingsForMatchers returns the postings list for series matching ms, time-
// agnostic (not bounded to any [mint, maxt] range) - matching real
// tsdb.IndexReader.Postings-family semantics, which have no time dimension of
// their own. This is the narrow index-level query surface Cortex's
// tsdbStore.PostingsForMatchers (pkg/ingester, Phase 7) needs for its label-set/
// tracker cardinality counters, deliberately NOT a full tsdb.IndexReader (see that
// interface's own doc comment in ingester.go for why: only 4 of its 11 methods are
// ever actually used by real callers).
//
// Reuses Select's exact matcher-driven candidate-ref logic (querier.go's
// candidateRefs/refMatchesAll - the same __name__-postings shortcut, same
// per-candidate filtering) via a full-range Querier, rather than duplicating it -
// just wraps the resulting refs as index.Postings instead of a storage.SeriesSet.
// Self-locking: opens and closes its own Querier for the call, like AppendExemplar/
// Metadata do for indexMu - not a longer-lived cursor.
func (h *Head) PostingsForMatchers(_ context.Context, ms ...*labels.Matcher) (index.Postings, error) {
	q, err := h.Querier(math.MinInt64, math.MaxInt64)
	if err != nil {
		return nil, err
	}
	defer q.Close()
	hq := q.(*headQuerier)

	candidates, rest := hq.candidateRefs(ms)
	refs := make([]storage.SeriesRef, 0, len(candidates))
	for _, ref := range candidates {
		if refMatchesAll(h, ref, rest) {
			refs = append(refs, storage.SeriesRef(ref))
		}
	}
	return index.NewListPostings(refs), nil
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
// Self-locking: takes every shard's write lock in ascending order (the same fixed
// order Querier/ChunkQuerier use, so this can never deadlock against a concurrent
// query) - a real entry point for concurrent use, since a real compaction goroutine
// calling this runs concurrently with live append/query traffic. Does NOT take
// indexMu - Truncate only touches shard-local series data, never symbols/targets/
// dedup indexes.
func (h *Head) Truncate(mint int64) {
	for _, shard := range h.shards {
		shard.mu.Lock()
		n := uint32(shard.series.NumSeries())
		for ref := uint32(0); ref < n; ref++ {
			shard.series.Truncate(ref, mint)
			shard.histograms.Truncate(ref, mint)
		}
		shard.mu.Unlock()
	}
	// Advance minTime to at least mint, matching real tsdb.Head.truncateMemory's
	// own convention (vendor/.../tsdb/head.go: h.minTime.Store(mint)). Without
	// this, MinTime() never reflects a Truncate at all - it's otherwise only
	// ever moved forward by real appends (updateMinMaxTime) - so a caller that
	// relies on MinTime() shrinking after Truncate to know what's left to do
	// (e.g. a periodic auto-compaction loop deciding whether there's still a
	// compactable range) would recompute the SAME already-empty range forever.
	// Forward-only CAS, the same pattern updateMinMaxTime itself uses: never
	// moves minTime backward, and a genuine no-op if mint is behind the
	// current value already (including the empty-head sentinel case, where
	// mint <= math.MaxInt64 is always true and this correctly does nothing).
	for {
		cur := h.minTime.Load()
		if mint <= cur || h.minTime.CompareAndSwap(cur, mint) {
			break
		}
	}
}

// NumSeries, NumTargets, NumSymbols report the head's current cardinality.
// NumSeries self-locks indexMu briefly (nextRef lives there); NumTargets/NumSymbols
// are NOT self-locking - callers must already hold indexMu (matches how these were
// already used before sharding).
func (h *Head) NumSeries() int {
	h.indexMu.RLock()
	defer h.indexMu.RUnlock()
	return int(h.nextRef)
}
func (h *Head) NumTargets() int { return h.targets.NumTargets() }
func (h *Head) NumSymbols() int { return h.symbols.NumSymbols() }
