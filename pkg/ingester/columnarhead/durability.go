package columnarhead

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// This file is the Phase 5 spike design doc §7 asks for before building a
// conventional WAL: "the target-major columnar arena is already a better WAL than
// the WAL... if it can serve as the durable record directly, an entire redundant
// in-memory copy of every in-flight sample disappears." DurableHead tests that
// hypothesis directly, rather than assuming it.
//
// Finding, checked before writing any of this rather than assumed: three of Head's
// four core structures are ALREADY append-only in memory, with no in-place rewrite
// of old bytes at all - liveInterner's blob/offset (Intern only ever appends),
// TargetStore's refs (Create only ever appends), and SeriesStore's per-series
// identity fields (targetID/nameID/localName/localRef/hasLocal, set once at Create,
// never mutated again). Durability for those is close to free: flush the tail past
// the last-known-durable length, fsync, done - no separate WAL record encoding
// needed, the in-memory bytes ARE the durable bytes.
//
// The one genuine complication is SeriesStore's arena: growSlot moves a series to a
// bigger region and FREES its old one for a *different* series' later alloc() to
// reuse and overwrite - a plain "flush everything past a single arena-wide
// high-water mark" scheme cannot see that in-place overwrite. An earlier version of
// this file disabled free-list reuse entirely to sidestep the problem, at a real,
// measured memory cost (+14.6% arena size in one workload) - retracted once
// per-series tracking (flushedBytes/flushedSlotOff/flushedGeneration below) turned
// out to already handle reuse correctly on its own: each series' flush state is
// keyed by its OWN current slotOff and a generation counter (bumped by Truncate),
// so a reused slotOff is indistinguishable from a freshly-appended one - both just
// look like "this ref's current location differs from what was last durable,
// reflush from scratch." Proven, not assumed: TestDurableHeadSurvivesChainedReuse
// forces a slot to be occupied by three different series in sequence (one of them
// already flushed once before it moved into the reused slot) and confirms correct
// recovery; mutation-tested by removing the mismatch check and confirming it
// reproduces real data corruption after reload. Free-list reuse is therefore always
// enabled - NewHead/NewSeriesStore are used directly, no separate durable variant.
//
// Scope, stated plainly: floats only, matching Phase 2's own precedent - histograms,
// exemplars, metadata, and start-timestamps are NOT persisted here. A crash loses
// them even if this were wired into the real ingest path (which it isn't yet - this
// proves the mechanism, it is not itself the finished Phase 5 feature).
const (
	fileSymbolsBlob    = "symbols_blob.bin"
	fileSymbolsOffsets = "symbols_offsets.bin"
	fileTargets        = "targets.bin"
	fileArena          = "arena.bin"
	fileSeriesMeta     = "series_meta.bin"
)

// seriesMetaRecordSize is one series' fixed-width persisted record: targetID(4) +
// nameID(2) + localName(2) + localRef(2) + hasLocal(1) + bitOff(2) + nSamples(2) +
// slotOff(4) + slotCap(4) + val.lastBits(8) + val.leading(1) + val.trailing(1) +
// ts.lastTS(8) + ts.lastDelta(8). Unlike the identity fields (targetID etc.), bitOff/
// nSamples/slotOff/slotCap/val/ts mutate on every Append - persisted via a full
// rewrite of this (small, O(numSeries) not O(arena size)) table on every Flush,
// rather than tracked incrementally like the arena; simpler, and cheap at any
// realistic series count (500k series here is ~24 MB - see TestHeadAtScale for how
// fast a live head of that size already builds).
const seriesMetaRecordSize = 4 + 2 + 2 + 2 + 1 + 2 + 2 + 4 + 4 + 8 + 1 + 1 + 8 + 8

func encodeSeriesMetaRecord(s *SeriesStore, ref uint32, buf []byte) {
	binary.LittleEndian.PutUint32(buf[0:4], s.targetID[ref])
	binary.LittleEndian.PutUint16(buf[4:6], s.nameID[ref])
	binary.LittleEndian.PutUint16(buf[6:8], s.localName[ref])
	binary.LittleEndian.PutUint16(buf[8:10], s.localRef[ref])
	if s.hasLocal[ref] {
		buf[10] = 1
	} else {
		buf[10] = 0
	}
	binary.LittleEndian.PutUint16(buf[11:13], s.bitOff[ref])
	binary.LittleEndian.PutUint16(buf[13:15], s.nSamples[ref])
	binary.LittleEndian.PutUint32(buf[15:19], s.slotOff[ref])
	binary.LittleEndian.PutUint32(buf[19:23], s.slotCap[ref])
	binary.LittleEndian.PutUint64(buf[23:31], s.val[ref].lastBits)
	buf[31] = s.val[ref].leading
	buf[32] = s.val[ref].trailing
	binary.LittleEndian.PutUint64(buf[33:41], uint64(s.ts[ref].lastTS))
	binary.LittleEndian.PutUint64(buf[41:49], uint64(s.ts[ref].lastDelta))
}

func decodeSeriesMetaRecord(buf []byte) (targetID uint32, nameID, localName, localRef uint16, hasLocal bool, bitOff, nSamples uint16, slotOff, slotCap uint32, val valueState, ts tsState) {
	targetID = binary.LittleEndian.Uint32(buf[0:4])
	nameID = binary.LittleEndian.Uint16(buf[4:6])
	localName = binary.LittleEndian.Uint16(buf[6:8])
	localRef = binary.LittleEndian.Uint16(buf[8:10])
	hasLocal = buf[10] != 0
	bitOff = binary.LittleEndian.Uint16(buf[11:13])
	nSamples = binary.LittleEndian.Uint16(buf[13:15])
	slotOff = binary.LittleEndian.Uint32(buf[15:19])
	slotCap = binary.LittleEndian.Uint32(buf[19:23])
	val.lastBits = binary.LittleEndian.Uint64(buf[23:31])
	val.leading = buf[31]
	val.trailing = buf[32]
	ts.lastTS = int64(binary.LittleEndian.Uint64(buf[33:41]))
	ts.lastDelta = int64(binary.LittleEndian.Uint64(buf[41:49]))
	return
}

// DurableHead wraps a Head with on-disk persistence for its append-only structures.
// Not wired into the real ingest path - a standalone harness for measuring whether
// the underlying mechanism (see this file's package-level doc comment) is viable.
type DurableHead struct {
	*Head
	dir string

	blobFile, offsetFile, targetsFile, arenaFile, metaFile *os.File

	// High-water marks: how much of each append-only structure is already durable.
	// Units match what's being flushed - bytes for blob, element counts (multiplied
	// by 4 on write) for offset/targets.
	blobFlushed, offsetFlushed, targetsFlushed int

	// Per-series arena durability tracking - NOT a single arena-wide high-water
	// mark. A series' slot is allocated with spare capacity up front (e.g. 16
	// bytes on the first sample), and later samples fill in MORE of that SAME
	// already-allocated region without ever growing len(arena) - a single global
	// high-water mark would miss those in-place fills entirely (found by
	// TestDurableHeadFlushIsIncremental: a second Flush reported 0 new bytes
	// despite 5 real new samples, because they all landed within capacity
	// reserved - and counted as "flushed" - by the FIRST Flush). flushedBytes[ref]
	// is how many bytes of ref's CURRENT slot are already durable;
	// flushedSlotOff[ref] is which slotOff that count is relative to - a growSlot
	// move changes slotOff, at which point the new slot's bytes are entirely new
	// from the file's perspective (even though they're a copy of already-durable
	// data) and must be reflushed from scratch at their new location.
	//
	// flushedGeneration[ref] catches a second, distinct case a byte-count/slotOff
	// comparison alone cannot: SeriesStore.Truncate rewrites a series' bytes from
	// scratch AT THE SAME slotOff, and the resulting byte count can shrink or even
	// coincidentally match what was there before - found the same way as the
	// first case, by writing a test for exactly this interaction (Truncate then
	// Flush) rather than assuming it worked once the incremental-flush fix above
	// was in place.
	flushedBytes, flushedSlotOff, flushedGeneration []uint32

	// stopAutoFlush is set by StartAutoFlush and cleared by stopping it - Close
	// calls it automatically (see Close's doc comment) so a caller that forgets
	// to stop the background flusher before shutting down doesn't leave it
	// running against closed file handles.
	stopAutoFlush func()
}

// CreateDurableHead opens a brand-new DurableHead backed by files under dir (which
// must not already contain a prior durable head - use LoadDurableHead to resume
// one). Fails if any of the five files already exist, rather than silently
// overwriting a prior head's data.
func CreateDurableHead(dir string, expectedSeries, expectedTargets, expectedSymbols int) (*DurableHead, error) {
	for _, name := range []string{fileSymbolsBlob, fileSymbolsOffsets, fileTargets, fileArena, fileSeriesMeta} {
		if _, err := os.Stat(filepath.Join(dir, name)); err == nil {
			return nil, fmt.Errorf("columnarhead: %s already exists in %s - use LoadDurableHead", name, dir)
		}
	}
	dh := &DurableHead{Head: NewHead(expectedSeries, expectedTargets, expectedSymbols), dir: dir}
	var err error
	if dh.blobFile, err = os.OpenFile(filepath.Join(dir, fileSymbolsBlob), os.O_RDWR|os.O_CREATE, 0o644); err != nil {
		return nil, err
	}
	if dh.offsetFile, err = os.OpenFile(filepath.Join(dir, fileSymbolsOffsets), os.O_RDWR|os.O_CREATE, 0o644); err != nil {
		return nil, err
	}
	if dh.targetsFile, err = os.OpenFile(filepath.Join(dir, fileTargets), os.O_RDWR|os.O_CREATE, 0o644); err != nil {
		return nil, err
	}
	if dh.arenaFile, err = os.OpenFile(filepath.Join(dir, fileArena), os.O_RDWR|os.O_CREATE, 0o644); err != nil {
		return nil, err
	}
	if dh.metaFile, err = os.OpenFile(filepath.Join(dir, fileSeriesMeta), os.O_RDWR|os.O_CREATE, 0o644); err != nil {
		return nil, err
	}
	return dh, nil
}

// LoadDurableHead reconstructs a DurableHead from a directory previously written by
// CreateDurableHead/Flush - the "restart after a crash" half of the spike. Only
// data covered by a completed Flush is recovered; anything appended after the last
// Flush is correctly absent, not an error - that IS the durability boundary being
// tested.
//
// Reconstructs Head's derived indexes (targetIndex, seriesIndex, namePostings) by
// replaying the loaded target/series records once, the same key construction
// GetOrCreateSeries uses - these indexes are never themselves persisted, since
// they're fully redundant with what's already in targets.bin/series_meta.bin.
func LoadDurableHead(dir string) (*DurableHead, error) {
	blob, err := os.ReadFile(filepath.Join(dir, fileSymbolsBlob))
	if err != nil {
		return nil, err
	}
	offsetBytes, err := os.ReadFile(filepath.Join(dir, fileSymbolsOffsets))
	if err != nil {
		return nil, err
	}
	if len(offsetBytes)%4 != 0 {
		return nil, fmt.Errorf("columnarhead: %s size %d not a multiple of 4", fileSymbolsOffsets, len(offsetBytes))
	}
	targetBytes, err := os.ReadFile(filepath.Join(dir, fileTargets))
	if err != nil {
		return nil, err
	}
	if len(targetBytes)%4 != 0 {
		return nil, fmt.Errorf("columnarhead: %s size %d not a multiple of 4", fileTargets, len(targetBytes))
	}
	arena, err := os.ReadFile(filepath.Join(dir, fileArena))
	if err != nil {
		return nil, err
	}
	metaBytes, err := os.ReadFile(filepath.Join(dir, fileSeriesMeta))
	if err != nil {
		return nil, err
	}
	if len(metaBytes)%seriesMetaRecordSize != 0 {
		return nil, fmt.Errorf("columnarhead: %s size %d not a multiple of record size %d", fileSeriesMeta, len(metaBytes), seriesMetaRecordSize)
	}
	numSeries := len(metaBytes) / seriesMetaRecordSize
	numTargets := len(targetBytes) / 4 / targetFields

	li := newLiveInterner(len(offsetBytes)/4 - 1)
	li.blob = blob
	li.offset = li.offset[:0]
	for i := 0; i+4 <= len(offsetBytes); i += 4 {
		li.offset = append(li.offset, binary.LittleEndian.Uint32(offsetBytes[i:i+4]))
	}
	for id := 0; id < len(li.offset)-1; id++ {
		li.index[li.String(uint32(id))] = uint32(id)
	}

	ts := NewTargetStore(numTargets)
	for i := 0; i+4 <= len(targetBytes); i += 4 {
		ts.refs = append(ts.refs, binary.LittleEndian.Uint32(targetBytes[i:i+4]))
	}

	ss := NewSeriesStore(numSeries)
	ss.arena = arena
	for ref := 0; ref < numSeries; ref++ {
		rec := metaBytes[ref*seriesMetaRecordSize : (ref+1)*seriesMetaRecordSize]
		targetID, nameID, localName, localRef, hasLocal, bitOff, nSamples, slotOff, slotCap, val, tst := decodeSeriesMetaRecord(rec)
		ss.targetID = append(ss.targetID, targetID)
		ss.nameID = append(ss.nameID, nameID)
		ss.localName = append(ss.localName, localName)
		ss.localRef = append(ss.localRef, localRef)
		ss.hasLocal = append(ss.hasLocal, hasLocal)
		ss.bitOff = append(ss.bitOff, bitOff)
		ss.nSamples = append(ss.nSamples, nSamples)
		ss.generation = append(ss.generation, 0) // not persisted - see generation's doc comment; 0 is a safe baseline since flushedGeneration below starts at 0 too
		ss.slotOff = append(ss.slotOff, slotOff)
		ss.slotCap = append(ss.slotCap, slotCap)
		ss.val = append(ss.val, val)
		ss.ts = append(ss.ts, tst)
	}

	h := &Head{
		symbols:      li,
		targets:      ts,
		series:       ss,
		targetIndex:  make(map[[targetFields]uint32]uint32, numTargets),
		seriesIndex:  make(map[seriesKey]uint32, numSeries),
		namePostings: make(map[uint16][]uint32),
		metadata:     newSeriesMetadata(),
		lastST:       make(map[uint32]int64),
		exemplars:    newExemplarStorage(defaultExemplarCapacity),
		histograms:   NewHistogramStore(),
	}
	for id := uint32(0); id < uint32(numTargets); id++ {
		h.targetIndex[ts.Get(id)] = id
	}
	for ref := uint32(0); ref < uint32(numSeries); ref++ {
		key := seriesKey{
			targetID:  ss.TargetID(ref),
			nameID:    ss.NameID(ref),
			localName: ss.LocalName(ref),
			localRef:  ss.LocalRef(ref),
			hasLocal:  ss.HasLocal(ref),
		}
		h.seriesIndex[key] = ref
		h.namePostings[key.nameID] = append(h.namePostings[key.nameID], ref)
	}

	flushedBytes := make([]uint32, numSeries)
	flushedSlotOff := make([]uint32, numSeries)
	for ref := 0; ref < numSeries; ref++ {
		flushedBytes[ref] = (uint32(ss.bitOff[ref]) + 7) / 8
		flushedSlotOff[ref] = ss.slotOff[ref]
	}
	// flushedGeneration starts at all-zero, matching ss.generation's own reset to
	// 0 on reload (see the reconstruction loop above) - both start from the same
	// baseline, so the comparison in Flush is correct from the first post-reload
	// Flush onward.
	flushedGeneration := make([]uint32, numSeries)

	dh := &DurableHead{
		Head: h, dir: dir,
		blobFlushed: len(blob), offsetFlushed: len(li.offset), targetsFlushed: numTargets * targetFields,
		flushedBytes: flushedBytes, flushedSlotOff: flushedSlotOff, flushedGeneration: flushedGeneration,
	}
	if dh.blobFile, err = os.OpenFile(filepath.Join(dir, fileSymbolsBlob), os.O_RDWR, 0o644); err != nil {
		return nil, err
	}
	if dh.offsetFile, err = os.OpenFile(filepath.Join(dir, fileSymbolsOffsets), os.O_RDWR, 0o644); err != nil {
		return nil, err
	}
	if dh.targetsFile, err = os.OpenFile(filepath.Join(dir, fileTargets), os.O_RDWR, 0o644); err != nil {
		return nil, err
	}
	if dh.arenaFile, err = os.OpenFile(filepath.Join(dir, fileArena), os.O_RDWR, 0o644); err != nil {
		return nil, err
	}
	if dh.metaFile, err = os.OpenFile(filepath.Join(dir, fileSeriesMeta), os.O_RDWR, 0o644); err != nil {
		return nil, err
	}
	return dh, nil
}

// FlushStats reports what a Flush call actually wrote, for measuring the real
// "no redundant WAL copy" claim - new arena/blob/target bytes should track new
// samples/symbols/targets, not total live head size.
type FlushStats struct {
	NewBlobBytes, NewTargetBytes, NewArenaBytes int
	SeriesMetaBytes                             int // always a full rewrite - see seriesMetaRecordSize's doc comment
}

// Flush durably persists everything appended since the last Flush (or since
// creation) and fsyncs it. Takes Head's own write lock for the duration - a
// concurrent Append must not race a Flush reading the same slices.
func (dh *DurableHead) Flush() (FlushStats, error) {
	dh.mu.Lock()
	defer dh.mu.Unlock()

	var stats FlushStats

	newBlob := dh.symbols.blob[dh.blobFlushed:]
	if len(newBlob) > 0 {
		if _, err := dh.blobFile.WriteAt(newBlob, int64(dh.blobFlushed)); err != nil {
			return stats, fmt.Errorf("write %s: %w", fileSymbolsBlob, err)
		}
		stats.NewBlobBytes = len(newBlob)
		dh.blobFlushed = len(dh.symbols.blob)
	}

	if newOffsets := dh.symbols.offset[dh.offsetFlushed:]; len(newOffsets) > 0 {
		buf := make([]byte, len(newOffsets)*4)
		for i, v := range newOffsets {
			binary.LittleEndian.PutUint32(buf[i*4:], v)
		}
		if _, err := dh.offsetFile.WriteAt(buf, int64(dh.offsetFlushed*4)); err != nil {
			return stats, fmt.Errorf("write %s: %w", fileSymbolsOffsets, err)
		}
		dh.offsetFlushed = len(dh.symbols.offset)
	}

	if newTargets := dh.targets.refs[dh.targetsFlushed:]; len(newTargets) > 0 {
		buf := make([]byte, len(newTargets)*4)
		for i, v := range newTargets {
			binary.LittleEndian.PutUint32(buf[i*4:], v)
		}
		if _, err := dh.targetsFile.WriteAt(buf, int64(dh.targetsFlushed*4)); err != nil {
			return stats, fmt.Errorf("write %s: %w", fileTargets, err)
		}
		stats.NewTargetBytes = len(buf)
		dh.targetsFlushed = len(dh.targets.refs)
	}

	n := dh.series.NumSeries()
	for len(dh.flushedBytes) < n {
		dh.flushedBytes = append(dh.flushedBytes, 0)
		dh.flushedSlotOff = append(dh.flushedSlotOff, 0)
		dh.flushedGeneration = append(dh.flushedGeneration, 0)
	}
	for ref := 0; ref < n; ref++ {
		usedBytes := (uint32(dh.series.bitOff[ref]) + 7) / 8
		slotOff := dh.series.slotOff[ref]
		generation := dh.series.generation[ref]
		already := dh.flushedBytes[ref]
		if dh.flushedSlotOff[ref] != slotOff || dh.flushedGeneration[ref] != generation {
			// Either growSlot moved this series since its last flush (the new
			// slot's bytes are entirely new from the file's perspective, even
			// though they're a copy of already-durable data at the old location),
			// or Truncate rewrote it from scratch at the SAME slotOff (generation
			// bumped) - a byte-count comparison alone cannot detect the second
			// case, since truncation can shrink the count or even coincidentally
			// leave it unchanged while the actual bytes underneath are entirely
			// different. Either way, nothing at this slotOff is trustworthy
			// as "already flushed" - reflush the whole current range.
			already = 0
		}
		if usedBytes <= already {
			continue
		}
		newBytes := dh.series.arena[slotOff+already : slotOff+usedBytes]
		if _, err := dh.arenaFile.WriteAt(newBytes, int64(slotOff+already)); err != nil {
			return stats, fmt.Errorf("write %s: %w", fileArena, err)
		}
		stats.NewArenaBytes += len(newBytes)
		dh.flushedBytes[ref] = usedBytes
		dh.flushedSlotOff[ref] = slotOff
		dh.flushedGeneration[ref] = generation
	}
	metaBuf := make([]byte, n*seriesMetaRecordSize)
	for ref := 0; ref < n; ref++ {
		encodeSeriesMetaRecord(dh.series, uint32(ref), metaBuf[ref*seriesMetaRecordSize:(ref+1)*seriesMetaRecordSize])
	}
	if len(metaBuf) > 0 {
		if _, err := dh.metaFile.WriteAt(metaBuf, 0); err != nil {
			return stats, fmt.Errorf("write %s: %w", fileSeriesMeta, err)
		}
	}
	stats.SeriesMetaBytes = len(metaBuf)

	for _, f := range []*os.File{dh.blobFile, dh.offsetFile, dh.targetsFile, dh.arenaFile, dh.metaFile} {
		if err := f.Sync(); err != nil {
			return stats, fmt.Errorf("sync: %w", err)
		}
	}
	return stats, nil
}

// Compact reclaims space left behind by Head.Truncate: unlike a conventional WAL
// (multiple numbered segment files, old ones deleted after a checkpoint), this
// design has one arena file that only ever grows via Flush - Truncate shrinks the
// LIVE head, but nothing shrinks the DURABLE one to match, so disk usage grows
// forever even though live memory doesn't. Compact closes that gap by rebuilding
// the in-memory arena tightly (packing every series' current bytes back-to-back,
// dropping truncated/abandoned space and slot headroom - the same technique
// bench/05_compact_arena spiked in Phase 0, cited but not built here until now),
// then reusing Flush unmodified to write the new, smaller layout, then truncating
// the arena file down to match.
//
// Reusing Flush for the actual write is deliberate, not a shortcut: after
// rebuilding, every series' slotOff differs from what Flush last knew about, so
// the existing slotOff-mismatch detection (see flushedSlotOff's doc comment)
// already forces a correct full reflush of every series' current bytes at their
// new locations - no separate write path to keep in sync with Flush's.
//
// Real, stated cost: every series' slot becomes exactly as large as its current
// content, with zero spare headroom - the very next Append to any series
// immediately triggers a fresh growSlot, same tradeoff bench/05 already flagged
// for full compaction generally. Takes Head's write lock for the rebuild step
// only (Flush and the final truncate each take and release it again on their own),
// so Compact blocks concurrent Appenders/Queriers similarly to a Flush of
// comparable size, not for the whole Compact call end-to-end.
func (dh *DurableHead) Compact() (FlushStats, error) {
	dh.mu.Lock()
	old := dh.series
	newArena := make([]byte, 0, len(old.arena))
	for ref := 0; ref < old.NumSeries(); ref++ {
		usedBytes := (uint32(old.bitOff[ref]) + 7) / 8
		newOff := uint32(len(newArena))
		newArena = append(newArena, old.arena[old.slotOff[ref]:old.slotOff[ref]+usedBytes]...)
		old.slotOff[ref] = newOff
		old.slotCap[ref] = usedBytes
	}
	old.arena = newArena
	old.freeList = make(map[uint32][]uint32)
	dh.mu.Unlock()

	stats, err := dh.Flush()
	if err != nil {
		return stats, fmt.Errorf("compact: flush new layout: %w", err)
	}

	dh.mu.Lock()
	defer dh.mu.Unlock()
	if err := dh.arenaFile.Truncate(int64(len(dh.series.arena))); err != nil {
		return stats, fmt.Errorf("compact: truncate arena file: %w", err)
	}
	return stats, nil
}

// StartAutoFlush launches a background goroutine that calls Flush every interval,
// until the returned stop function is called (Close also stops it automatically -
// see Close's doc comment). onFlush, if non-nil, is invoked after every attempt
// (successful or not) - the caller's hook for logging/metrics/tests, since this
// package has no logger of its own. Calling StartAutoFlush again first stops any
// previously running one, rather than leaking it.
//
// The returned stop function is idempotent (via sync.Once) - both the caller and
// Close may end up calling it (e.g. an explicit Close followed by a deferred
// stop()), and only the first call should actually close anything.
//
// Real flush timing is a genuine tradeoff, not a free choice: Flush takes Head's
// write lock for its own duration (see Head's doc comment on why this is one
// coarse lock, not per-series ones), so a longer interval means more new data
// accumulates between flushes and each Flush call blocks concurrent
// Appenders/Queriers for longer - a shorter interval trades that for more frequent
// (individually cheaper) lock-holding and more fsync syscalls. See
// TestFlushBlocksAppendersUnderLoad for real, measured numbers at a realistic
// scale, not assumed ones.
func (dh *DurableHead) StartAutoFlush(interval time.Duration, onFlush func(FlushStats, error)) (stop func()) {
	if dh.stopAutoFlush != nil {
		dh.stopAutoFlush()
	}

	done := make(chan struct{})
	stopped := make(chan struct{})
	var once sync.Once
	stopFn := func() {
		once.Do(func() {
			close(done)
			<-stopped
		})
	}

	go func() {
		defer close(stopped)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				stats, err := dh.Flush()
				if onFlush != nil {
					onFlush(stats, err)
				}
			}
		}
	}()

	dh.stopAutoFlush = stopFn
	return stopFn
}

// Close closes every durable file handle without flushing - callers that want a
// clean shutdown must Flush() first; Close alone simulates a crash (whatever
// wasn't already flushed is lost), which is deliberately how the decisive test
// uses it. Stops any running StartAutoFlush loop first, so it never tries to
// Flush against a closed file handle.
func (dh *DurableHead) Close() error {
	if dh.stopAutoFlush != nil {
		dh.stopAutoFlush()
		dh.stopAutoFlush = nil
	}
	var err error
	for _, f := range []*os.File{dh.blobFile, dh.offsetFile, dh.targetsFile, dh.arenaFile, dh.metaFile} {
		if cerr := f.Close(); cerr != nil {
			err = errors.Join(err, cerr)
		}
	}
	return err
}
