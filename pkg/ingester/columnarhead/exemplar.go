package columnarhead

import (
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
)

// exemplarEntry is one stored exemplar plus which series it belongs to.
type exemplarEntry struct {
	seriesRef uint32
	ts        int64
	value     float64
	labels    map[string]string // defensively copied out of caller-owned exemplar.Labels
}

// exemplarStorage is a fixed-capacity ring buffer of exemplars across the whole Head -
// not per-series - mirroring Prometheus's own CircularExemplarStorage at a basic
// level: a bounded, best-effort ring, not unbounded retention. When full, the oldest
// entry is silently overwritten.
//
// See exemplar_querier.go for the real storage.ExemplarQuerier built over this -
// forSeries/all are its read primitives.
type exemplarStorage struct {
	entries []exemplarEntry
	next    int  // next write position
	filled  bool // true once entries has wrapped at least once
}

func newExemplarStorage(capacity int) *exemplarStorage {
	return &exemplarStorage{entries: make([]exemplarEntry, capacity)}
}

// append stores e for seriesRef, overwriting the oldest entry if the ring is full.
// Defensively copies e.Labels out into stable Go strings - it may be caller-owned,
// backed by memory the caller reuses after this call returns (the same category of
// hazard investigated earlier this session in pkg/cortexpb.CopyLabels, applied here
// deliberately rather than assumed safe).
func (es *exemplarStorage) append(seriesRef uint32, e exemplar.Exemplar) {
	if len(es.entries) == 0 {
		return
	}
	lbls := make(map[string]string, e.Labels.Len())
	e.Labels.Range(func(l labels.Label) { lbls[l.Name] = l.Value })

	es.entries[es.next] = exemplarEntry{seriesRef: seriesRef, ts: e.Ts, value: e.Value, labels: lbls}
	es.next++
	if es.next >= len(es.entries) {
		es.next = 0
		es.filled = true
	}
}

// forSeries returns every currently retained exemplar for seriesRef, oldest first.
// Older entries may be absent if the ring has wrapped past them.
func (es *exemplarStorage) forSeries(seriesRef uint32) []exemplarEntry {
	n := len(es.entries)
	if n == 0 {
		return nil
	}
	start, count := 0, es.next
	if es.filled {
		start, count = es.next, n
	}
	var out []exemplarEntry
	for i := 0; i < count; i++ {
		e := es.entries[(start+i)%n]
		if e.seriesRef == seriesRef {
			out = append(out, e)
		}
	}
	return out
}

// all returns every currently retained exemplar, oldest first, across every
// series - the read path storage.ExemplarQuerier's Select needs (see
// exemplar_querier.go), unlike forSeries' single-series scan.
func (es *exemplarStorage) all() []exemplarEntry {
	n := len(es.entries)
	if n == 0 {
		return nil
	}
	start, count := 0, es.next
	if es.filled {
		start, count = es.next, n
	}
	out := make([]exemplarEntry, count)
	for i := 0; i < count; i++ {
		out[i] = es.entries[(start+i)%n]
	}
	return out
}

// Len returns the number of exemplars currently retained (across all series).
func (es *exemplarStorage) Len() int {
	if es.filled {
		return len(es.entries)
	}
	return es.next
}
