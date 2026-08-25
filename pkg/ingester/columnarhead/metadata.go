package columnarhead

import "github.com/prometheus/prometheus/model/metadata"

// seriesMetadata holds Head's per-series metadata store: Type/Unit/Help, keyed by
// internal series ref. Small (metric-name-scale cardinality, not per-sample), so a
// plain Go map is the right structure here - no case for a compact/columnar layout
// the way samples or symbols need one.
type seriesMetadata struct {
	byRef map[uint32]metadata.Metadata
}

func newSeriesMetadata() *seriesMetadata {
	return &seriesMetadata{byRef: make(map[uint32]metadata.Metadata)}
}

func (sm *seriesMetadata) set(ref uint32, m metadata.Metadata) {
	sm.byRef[ref] = m
}

func (sm *seriesMetadata) get(ref uint32) (metadata.Metadata, bool) {
	m, ok := sm.byRef[ref]
	return m, ok
}
