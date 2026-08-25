package columnarhead

// liveInterner is a dynamic, find-or-create string interner: the live counterpart to
// the static, MPHF-backed SymbolTable. Real ingestion sees new label names/values
// continuously (pod names especially - they essentially never repeat once a pod is
// replaced), so a live head cannot rely on a structure with no Insert. This is the
// "small dynamic overlay for series created since the last build" design doc §3.3
// calls for - though "rebuild the static SymbolTable at compaction and reset this" is
// not implemented here; that rebuild-and-swap path is real future work.
//
// Backed by a plain Go map for the id index, which costs what bench/00_baseline and
// bench/03 already measured a Go map costs (~31-38 B/entry) - not the compact,
// MPHF-based representation. This is honestly the live-path cost until a rebuild path
// exists; see TestHeadAtScale for the real total, not an assumed one. Strings are still
// stored once each in a growing blob, same as SymbolTable, so duplication is avoided
// even before any MPHF is involved.
type liveInterner struct {
	blob   []byte
	offset []uint32 // offset[i]..offset[i+1] is symbol i's byte range in blob
	index  map[string]uint32
}

func newLiveInterner(expectedSymbols int) *liveInterner {
	return &liveInterner{
		offset: make([]uint32, 1, expectedSymbols+1), // offset[0] = 0
		index:  make(map[string]uint32, expectedSymbols),
	}
}

// Intern returns s's id, creating a new entry if s hasn't been seen before.
func (li *liveInterner) Intern(s string) uint32 {
	if id, ok := li.index[s]; ok {
		return id
	}
	id := uint32(len(li.offset) - 1)
	li.blob = append(li.blob, s...)
	li.offset = append(li.offset, uint32(len(li.blob)))
	li.index[s] = id
	return id
}

// Lookup returns s's id without creating one - unlike Intern, a read-only check. A
// pure lookup path (e.g. storage.GetRef, which must not create series as a side
// effect of checking whether one exists) needs this instead of Intern: calling Intern
// there would silently pollute the symbol table with an entry for every never-seen
// label value a caller merely asked about.
func (li *liveInterner) Lookup(s string) (uint32, bool) {
	id, ok := li.index[s]
	return id, ok
}

// String returns the interned string for id.
func (li *liveInterner) String(id uint32) string {
	return string(li.blob[li.offset[id]:li.offset[id+1]])
}

// NumSymbols returns the number of distinct strings interned so far.
func (li *liveInterner) NumSymbols() int {
	return len(li.index)
}

// BlobBytes returns the size of the interned string blob and offset index only - NOT
// the live Go map's cost, which this type has no way to report analytically. Measure
// the real total via runtime.MemStats, consistent with every other memory claim in
// this package (see TestHeadAtScale).
func (li *liveInterner) BlobBytes() int {
	return len(li.blob) + len(li.offset)*4
}
