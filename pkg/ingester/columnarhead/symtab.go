package columnarhead

// SymbolTable interns strings into a compact, deduplicated byte blob plus a real MPHF
// for O(1) string->id lookup and id->string decode - design doc §3.1's
// symbols/symOffsets/symMPHF layout, built on the MPHF in mphf.go.
//
// SymbolTable is the piece that actually performs the verification MPHF.Lookup cannot
// do on its own: comparing the candidate slot's stored string against the query before
// trusting it (see MPHF's type comment and TestMPHFUnknownKeyIsUnverified). Without
// this, an unknown label name/value could silently resolve to another symbol's id.
//
// Static, like the MPHF it wraps: there is no Insert. Rebuilding for a changed symbol
// set means calling BuildSymbolTable again from scratch.
type SymbolTable struct {
	blob   []byte
	offset []uint32 // offset[i]..offset[i+1] is symbol i's byte range in blob
	mphf   *MPHF
}

// BuildSymbolTable interns strs into a SymbolTable, deduplicating first. Unlike
// BuildMPHF, callers are not required to pre-dedupe: the actual use case (label names
// and values, repeated across every series that shares them) is exactly why this
// exists.
func BuildSymbolTable(strs []string) (*SymbolTable, error) {
	distinct := dedupeStrings(strs)
	if len(distinct) == 0 {
		return &SymbolTable{}, nil
	}
	mphf, err := BuildMPHF(distinct)
	if err != nil {
		return nil, err
	}
	// Order symbols by their MPHF slot so Lookup's verify step and String's decode
	// share one array, indexed identically - no separate slot->position map needed.
	ordered := make([]string, len(distinct))
	for _, s := range distinct {
		ordered[mphf.Lookup(s)] = s
	}
	blob, offset := packSymbols(ordered)
	return &SymbolTable{blob: blob, offset: offset, mphf: mphf}, nil
}

func dedupeStrings(strs []string) []string {
	seen := make(map[string]struct{}, len(strs))
	out := make([]string, 0, len(strs))
	for _, s := range strs {
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}

func packSymbols(ordered []string) ([]byte, []uint32) {
	offset := make([]uint32, len(ordered)+1)
	var total uint32
	for i, s := range ordered {
		offset[i] = total
		total += uint32(len(s))
	}
	offset[len(ordered)] = total
	blob := make([]byte, total)
	for i, s := range ordered {
		copy(blob[offset[i]:offset[i+1]], s)
	}
	return blob, offset
}

// NumSymbols returns the number of distinct symbols in the table.
func (t *SymbolTable) NumSymbols() uint32 {
	if t.mphf == nil {
		return 0
	}
	return t.mphf.NumKeys()
}

// Lookup returns s's id and true if s is one of the symbols the table was built over.
// This is the mandatory verification step (see the type comment): an id from the
// underlying MPHF alone cannot distinguish a known symbol from an unknown one.
func (t *SymbolTable) Lookup(s string) (uint32, bool) {
	if t.NumSymbols() == 0 {
		return 0, false
	}
	id := t.mphf.Lookup(s)
	if t.String(id) != s {
		return 0, false
	}
	return id, true
}

// String returns the symbol stored at id. id must be < NumSymbols() - always true for
// an id returned by Lookup; the caller is responsible for validity otherwise.
func (t *SymbolTable) String(id uint32) string {
	return string(t.blob[t.offset[id]:t.offset[id+1]])
}

// SizeBytes returns the table's total memory footprint: the blob, the offset array,
// and the underlying MPHF.
func (t *SymbolTable) SizeBytes() int {
	if t.mphf == nil {
		return 0
	}
	return len(t.blob) + len(t.offset)*4 + t.mphf.SizeBytes()
}
