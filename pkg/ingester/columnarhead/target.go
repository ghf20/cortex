package columnarhead

// targetFields is the number of symbol refs per target: cluster, namespace, pod,
// container, node, job. See columnar-head-design.md §3.1 - this is the shared label
// block responsible for the design's core memory claim, a measured 200:1 sharing ratio
// between series and targets on a realistic k8s workload (§2).
const targetFields = 6

// TargetStore is a flat, pointerless slab of target records: targetFields symbol refs
// per target, into whatever symbol table the caller resolved strings through (Head uses
// a liveInterner - see interner.go). Deduplication (finding an existing target for a
// repeated label set instead of creating a new one) is the caller's responsibility;
// TargetStore itself just appends and reads records by id, the same division of labor
// SeriesStore uses for series.
type TargetStore struct {
	refs []uint32 // targetFields uint32s per target, flat
}

// NewTargetStore returns an empty store with capacity preallocated for expectedTargets.
func NewTargetStore(expectedTargets int) *TargetStore {
	return &TargetStore{refs: make([]uint32, 0, expectedTargets*targetFields)}
}

// Create appends a new target record and returns its id.
func (t *TargetStore) Create(symbolRefs [targetFields]uint32) uint32 {
	id := uint32(len(t.refs) / targetFields)
	t.refs = append(t.refs, symbolRefs[:]...)
	return id
}

// Get returns the symbol refs for target id.
func (t *TargetStore) Get(id uint32) [targetFields]uint32 {
	var out [targetFields]uint32
	copy(out[:], t.refs[id*targetFields:id*targetFields+targetFields])
	return out
}

// NumTargets returns the number of target records created so far.
func (t *TargetStore) NumTargets() int {
	return len(t.refs) / targetFields
}

// SizeBytes returns the store's memory footprint.
func (t *TargetStore) SizeBytes() int {
	return len(t.refs) * 4
}
