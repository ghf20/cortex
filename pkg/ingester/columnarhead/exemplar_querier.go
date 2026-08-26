package columnarhead

import (
	"context"
	"slices"

	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
)

// ExemplarQuerier returns a storage.ExemplarQuerier over h's currently retained
// exemplars - the real implementation exemplarStorage's own doc comment deferred
// ("a real storage.ExemplarQuerier is separate, later work"), now built. Matches
// Cortex's tsdbStore.ExemplarQuerier(ctx context.Context) (storage.ExemplarQuerier,
// error) signature (Phase 7, ingester.go). ctx is accepted for signature
// conformance and unused, same as Appender's.
func (h *Head) ExemplarQuerier(_ context.Context) (storage.ExemplarQuerier, error) {
	return &exemplarQuerier{h: h}, nil
}

type exemplarQuerier struct {
	h *Head
}

var _ storage.ExemplarQuerier = (*exemplarQuerier)(nil)

// Select implements storage.ExemplarQuerier, matching real Prometheus's own
// CircularExemplarStorage.Select semantics exactly (vendor/.../tsdb/exemplar.go):
// within one matcher set, AND; between matcher sets, OR (storage.ExemplarQuerier's
// own documented contract). Results are sorted by series labels, one
// exemplar.QueryResult per matching series.
//
// exemplarStorage itself only tracks a series REF per entry, not its label set
// (unlike real CircularExemplarStorage, which stores the full label set inline per
// ring entry) - resolving each entry's series labels here needs Head.SeriesLabels,
// which is why this locks indexMu plus every shard, the same pattern
// Head.Querier's construction uses. Unlike Querier, there's no Close() to call:
// storage.ExemplarQuerier's contract is a single synchronous Select, not a
// longer-lived cursor, so the locks are acquired and released within this one call.
//
// A stated, pre-existing gap this inherits from exemplarStorage.append (not
// introduced here): HasTs is always reported true, since exemplarEntry never
// recorded the original exemplar.Exemplar's HasTs bit - a real exemplar appended
// with HasTs: false (no explicit timestamp, scrape timestamp used instead) is
// indistinguishable here from one with a genuine explicit timestamp.
func (q *exemplarQuerier) Select(start, end int64, matchers ...[]*labels.Matcher) ([]exemplar.QueryResult, error) {
	h := q.h
	h.indexMu.RLock()
	defer h.indexMu.RUnlock()
	for _, shard := range h.shards {
		shard.mu.RLock()
	}
	defer func() {
		for _, shard := range h.shards {
			shard.mu.RUnlock()
		}
	}()

	byRef := make(map[uint32][]exemplarEntry)
	var order []uint32
	for _, e := range h.exemplars.all() {
		if e.ts < start || e.ts > end {
			continue
		}
		if _, ok := byRef[e.seriesRef]; !ok {
			order = append(order, e.seriesRef)
		}
		byRef[e.seriesRef] = append(byRef[e.seriesRef], e)
	}

	ret := make([]exemplar.QueryResult, 0, len(order))
	for _, ref := range order {
		lbls := h.SeriesLabels(ref)
		if !matchesSomeMatcherSet(lbls, matchers) {
			continue
		}
		entries := byRef[ref]
		exs := make([]exemplar.Exemplar, len(entries))
		for i, e := range entries {
			exs[i] = exemplar.Exemplar{
				Labels: labelsFromMap(e.labels),
				Value:  e.value,
				Ts:     e.ts,
				HasTs:  true,
			}
		}
		ret = append(ret, exemplar.QueryResult{SeriesLabels: lbls, Exemplars: exs})
	}

	slices.SortFunc(ret, func(a, b exemplar.QueryResult) int {
		return labels.Compare(a.SeriesLabels, b.SeriesLabels)
	})
	return ret, nil
}

// matchesSomeMatcherSet reports whether lbls satisfies at least one matcher set
// (every matcher within that set matching) - the AND-within/OR-between semantics
// storage.ExemplarQuerier.Select's doc comment specifies, same helper name and
// shape as real Prometheus's own (vendor/.../tsdb/exemplar.go) for the identical
// reason.
func matchesSomeMatcherSet(lbls labels.Labels, matcherSets [][]*labels.Matcher) bool {
outer:
	for _, ms := range matcherSets {
		for _, m := range ms {
			if !m.Matches(lbls.Get(m.Name)) {
				continue outer
			}
		}
		return true
	}
	return false
}

// labelsFromMap rebuilds a sorted labels.Labels from exemplarEntry's defensively-
// copied map[string]string representation (see exemplarStorage.append).
func labelsFromMap(m map[string]string) labels.Labels {
	b := labels.NewScratchBuilder(len(m))
	for k, v := range m {
		b.Add(k, v)
	}
	b.Sort()
	return b.Labels()
}
