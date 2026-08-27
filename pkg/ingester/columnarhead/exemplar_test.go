package columnarhead

import (
	"fmt"
	"testing"

	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
)

func appendN(es *exemplarStorage, ref uint32, n int, startTS int64) {
	for i := 0; i < n; i++ {
		es.append(ref, exemplar.Exemplar{
			Ts:     startTS + int64(i),
			Value:  float64(i),
			Labels: labels.FromStrings("trace_id", fmt.Sprintf("t%d", i)),
		})
	}
}

// TestExemplarStorageResizeShrinkKeepsNewest confirms a shrink keeps the most
// recently appended entries (the ones a ring would keep anyway) and drops the
// oldest, rather than an arbitrary or reversed subset.
func TestExemplarStorageResizeShrinkKeepsNewest(t *testing.T) {
	es := newExemplarStorage(10)
	appendN(es, 1, 5, 100) // ts 100..104

	es.resize(2)

	got := es.all()
	if len(got) != 2 {
		t.Fatalf("Len after shrink = %d, want 2", len(got))
	}
	if got[0].ts != 103 || got[1].ts != 104 {
		t.Fatalf("kept entries = [%d, %d], want [103, 104] (the two newest)", got[0].ts, got[1].ts)
	}
	if es.Len() != 2 {
		t.Fatalf("Len() = %d, want 2", es.Len())
	}
}

// TestExemplarStorageResizeGrowKeepsEverything confirms a grow preserves every
// existing entry, just with more headroom for future appends.
func TestExemplarStorageResizeGrowKeepsEverything(t *testing.T) {
	es := newExemplarStorage(3)
	appendN(es, 1, 3, 100) // fills the ring exactly

	es.resize(10)

	got := es.all()
	if len(got) != 3 {
		t.Fatalf("Len after grow = %d, want 3 (nothing should be lost)", len(got))
	}
	for i, e := range got {
		if e.ts != int64(100+i) {
			t.Fatalf("entry %d ts = %d, want %d", i, e.ts, 100+i)
		}
	}

	// The new headroom must actually be usable - appending past the OLD
	// capacity (3) must not overwrite anything yet.
	appendN(es, 1, 3, 200) // ts 200,201,202 - total now 6, still under new cap 10
	if es.Len() != 6 {
		t.Fatalf("Len() after appending into new headroom = %d, want 6", es.Len())
	}
}

// TestExemplarStorageResizeToZeroDropsEverything confirms a resize to 0 (the
// "exemplars disabled" case columnarheadTSDBStore.ApplyConfig can produce) empties
// the ring and makes further appends a safe no-op, not a panic.
func TestExemplarStorageResizeToZeroDropsEverything(t *testing.T) {
	es := newExemplarStorage(5)
	appendN(es, 1, 5, 100)

	es.resize(0)

	if got := es.Len(); got != 0 {
		t.Fatalf("Len() after resize(0) = %d, want 0", got)
	}
	es.append(1, exemplar.Exemplar{Ts: 999, Value: 1, Labels: labels.EmptyLabels()})
	if got := es.Len(); got != 0 {
		t.Fatalf("Len() after appending into a zero-capacity ring = %d, want still 0", got)
	}
}

// TestExemplarStorageResizeNoopWhenUnchanged confirms resizing to the current
// capacity leaves existing entries untouched - the common case, since
// ApplyConfig runs on every periodic limits-refresh tick even when nothing
// changed.
func TestExemplarStorageResizeNoopWhenUnchanged(t *testing.T) {
	es := newExemplarStorage(5)
	appendN(es, 1, 3, 100)

	es.resize(5)

	got := es.all()
	if len(got) != 3 {
		t.Fatalf("Len after no-op resize = %d, want 3", len(got))
	}
	for i, e := range got {
		if e.ts != int64(100+i) {
			t.Fatalf("entry %d ts = %d, want %d", i, e.ts, 100+i)
		}
	}
}

// TestHeadSetExemplarCapacity confirms Head.SetExemplarCapacity reaches the
// underlying ring - the self-locking wrapper AppendExemplar/Exemplars already
// use, exercised end-to-end through Head rather than only at the
// exemplarStorage unit level.
func TestHeadSetExemplarCapacity(t *testing.T) {
	h := NewHead(1, 1, 1)
	h.SetExemplarCapacity(2)

	h.AppendExemplar(1, exemplar.Exemplar{Ts: 1, Value: 1, Labels: labels.EmptyLabels()})
	h.AppendExemplar(1, exemplar.Exemplar{Ts: 2, Value: 2, Labels: labels.EmptyLabels()})
	h.AppendExemplar(1, exemplar.Exemplar{Ts: 3, Value: 3, Labels: labels.EmptyLabels()}) // evicts ts=1

	got := h.Exemplars(1)
	if len(got) != 2 {
		t.Fatalf("Exemplars(1) returned %d entries, want 2 (capacity 2)", len(got))
	}
	if got[0].ts != 2 || got[1].ts != 3 {
		t.Fatalf("kept entries ts = [%d, %d], want [2, 3]", got[0].ts, got[1].ts)
	}
}
