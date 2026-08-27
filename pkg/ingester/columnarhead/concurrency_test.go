package columnarhead

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/tsdbutil"
)

// TestHeadShardWritesAreIndependent is the decisive test for Phase A's actual
// point (see CHECKLIST.md's locked-down locking design): two series in DIFFERENT
// shards must be appendable concurrently without blocking each other, even while
// one shard is under a live write lock. TestHeadConcurrentAppendQueryTruncateCompact
// already proves the sharded design doesn't crash or corrupt data under real
// contention; this test proves the more specific claim the sharding was built for -
// that a slow/blocked writer on one shard does NOT stall a writer on another - by
// holding one shard's lock directly and confirming a different shard's Append still
// completes immediately, while the SAME shard's Append (the control case) does not.
func TestHeadShardWritesAreIndependent(t *testing.T) {
	h := NewHeadWithShards(4, 1, 8, 2) // exactly 2 shards, so placement is deterministic
	tgt := TargetLabels{Cluster: "c", Namespace: "n", Pod: "p", Container: "co", Node: "no", Job: "j"}
	refA, err := h.GetOrCreateSeries(tgt, "series_a") // ref 0 -> shard 0
	if err != nil {
		t.Fatalf("GetOrCreateSeries(A): %v", err)
	}
	refB, err := h.GetOrCreateSeries(tgt, "series_b") // ref 1 -> shard 1
	if err != nil {
		t.Fatalf("GetOrCreateSeries(B): %v", err)
	}
	shardA, _ := h.shardFor(refA)
	shardB, _ := h.shardFor(refB)
	if shardA == shardB {
		t.Fatal("test setup broken: series A and B landed in the same shard - shardFor's round-robin assignment changed")
	}

	shardA.mu.Lock()

	// A different shard's Append must complete immediately - it must not be
	// blocked by shard A's held lock.
	doneB := make(chan error, 1)
	go func() { doneB <- h.Append(refB, 1700000000000, 1) }()
	select {
	case err := <-doneB:
		if err != nil {
			t.Fatalf("Append to series B (different shard): %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Append to series B blocked on shard A's held lock - shards are not actually independent")
	}

	// Control case: Append to series A itself (the SAME, still-locked shard) must
	// NOT complete while the lock is held - otherwise this test would only be
	// proving shardFor's arithmetic, not that the lock actually excludes anyone.
	doneA := make(chan error, 1)
	go func() { doneA <- h.Append(refA, 1700000000000, 1) }()
	select {
	case <-doneA:
		t.Fatal("Append to series A completed while shard A's lock was held - the lock isn't excluding concurrent writers")
	case <-time.After(100 * time.Millisecond):
		// Expected: still blocked.
	}

	shardA.mu.Unlock()
	if err := <-doneA; err != nil {
		t.Fatalf("Append to series A (after unlock): %v", err)
	}
}

// TestHeadConcurrentAppendQueryTruncateCompact is the decisive check for Head's
// locking: real concurrent traffic through every entry point Head's doc comment
// claims is safe (Appender, Querier, Truncate, and CompactHead, which is built on
// Querier) running simultaneously under -race, for long enough to actually exercise
// contention, not just a token handful of goroutines that happen not to overlap.
// With defaultNumShards=32 and numSeries=16, each series' writer goroutine lands in
// its own distinct shard here, so this incidentally already exercises real
// cross-shard writer traffic - see TestHeadShardWritesAreIndependent for the
// narrower, decisive proof that shards are genuinely independent, not just "didn't
// crash under load."
//
// Each series has exactly one writer goroutine, so despite arbitrary interleaving
// with readers/Truncate/CompactHead, that series' own final state is fully
// deterministic - the real correctness check at the end is exact equality, not just
// "didn't crash."
func TestHeadConcurrentAppendQueryTruncateCompact(t *testing.T) {
	h := NewHead(64, 8, 64)
	const numSeries = 16
	const samplesPerSeries = 300

	tgt := TargetLabels{Cluster: "c", Namespace: "n", Pod: "p", Container: "co", Node: "no", Job: "j"}
	refs := make([]uint32, numSeries)
	for i := 0; i < numSeries; i++ {
		ref, err := h.GetOrCreateSeries(tgt, fmt.Sprintf("series_%d", i))
		if err != nil {
			t.Fatalf("GetOrCreateSeries: %v", err)
		}
		refs[i] = ref
	}

	var writers sync.WaitGroup
	for i := 0; i < numSeries; i++ {
		writers.Add(1)
		go func(idx int) {
			defer writers.Done()
			// Through h.Appender(), not h.Append directly - Append is the "raw", not
			// individually locked, method (see Head's doc comment); Appender's
			// per-call wrapper is the actual safe entry point for concurrent writers.
			app := h.Appender(context.Background())
			extRef := toExternalRef(refs[idx])
			base := int64(1700000000000)
			for s := 0; s < samplesPerSeries; s++ {
				ts := base + int64(s)*15000
				v := float64(idx*samplesPerSeries + s)
				if _, err := app.Append(extRef, labels.EmptyLabels(), ts, v); err != nil {
					t.Errorf("Append(series %d, sample %d): %v", idx, s, err)
					return
				}
			}
		}(i)
	}

	stop := make(chan struct{})
	var readers sync.WaitGroup
	for r := 0; r < 4; r++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				q, err := h.Querier(math.MinInt64, math.MaxInt64)
				if err != nil {
					t.Errorf("Querier: %v", err)
					return
				}
				ss := q.Select(context.Background(), false, nil)
				for ss.Next() {
					it := ss.At().Iterator(nil)
					for it.Next() == chunkenc.ValFloat {
						it.At()
					}
					if err := it.Err(); err != nil {
						t.Errorf("iterator error: %v", err)
					}
				}
				if err := ss.Err(); err != nil {
					t.Errorf("SeriesSet.Err(): %v", err)
				}
				q.Close()
			}
		}()
	}

	// Truncate and CompactHead concurrently too - the actual point of this test,
	// not just Append racing Select. mint=math.MinInt64 means nothing is ever
	// actually dropped (there's no sample older than that), so this exercises the
	// write-lock path repeatedly without disturbing the deterministic final check
	// below.
	readers.Add(1)
	go func() {
		defer readers.Done()
		dir := t.TempDir()
		for i := 0; i < 5; i++ {
			h.Truncate(math.MinInt64)
			if _, err := CompactHead(h, math.MinInt64, math.MaxInt64, dir, 2*60*60*1000, testLogger()); err != nil {
				t.Errorf("CompactHead: %v", err)
				return
			}
		}
	}()

	writers.Wait()
	close(stop)
	readers.Wait()

	for i := 0; i < numSeries; i++ {
		it := h.Iterator(refs[i])
		count := 0
		for it.Next() {
			_, v := it.At()
			want := float64(i*samplesPerSeries + count)
			if v != want {
				t.Fatalf("series %d sample %d: v = %v, want %v", i, count, v, want)
			}
			count++
		}
		if count != samplesPerSeries {
			t.Fatalf("series %d: decoded %d samples after all writers finished, want %d", i, count, samplesPerSeries)
		}
	}
}

// TestHeadAppendHistogramAndCommitConcurrency ports real Prometheus's identically
// named regression test (prometheus/prometheus#15139): two goroutines race to
// create AND append the same brand-new series (ref=0, full labels every call) with
// an identical (ts, histogram) sample. One commit must create the series and store
// the sample; the other must see it as an exact duplicate and silently no-op -
// real Prometheus's bug was a corrupted duplicate-check under a race in its
// double-checked-locking series creation path. columnarhead's GetOrCreateSeries
// takes a single indexMu.Lock() for its entire create-or-lookup body (no
// double-checked locking to race), and the subsequent duplicate check
// (Head.appendable/appendableHistogram) runs under that same series' shard lock -
// structurally a different design, but worth verifying under -race directly rather
// than only by inspection.
func TestHeadAppendHistogramAndCommitConcurrency(t *testing.T) {
	cases := []struct {
		name     string
		appendFn func(app *headAppender, l labels.Labels) error
	}{
		{"integer histogram", func(app *headAppender, l labels.Labels) error {
			_, err := app.AppendHistogram(0, l, 1, tsdbutil.GenerateTestHistogram(1), nil)
			return err
		}},
		{"float histogram", func(app *headAppender, l labels.Labels) error {
			_, err := app.AppendHistogram(0, l, 1, nil, tsdbutil.GenerateTestFloatHistogram(1))
			return err
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := NewHead(8, 4, 8)
			const n = 2000
			labelsFor := func(i int) labels.Labels {
				return labels.FromStrings(labels.MetricName, "m",
					"cluster", "c", "namespace", "n", "pod", "p", "container", "co", "node", "no", "job", "j",
					"serial", strconv.Itoa(i))
			}

			var wg sync.WaitGroup
			wg.Add(2)
			race := func() {
				defer wg.Done()
				for i := 0; i < n; i++ {
					app := h.Appender(context.Background()).(*headAppender)
					if err := tc.appendFn(app, labelsFor(i)); err != nil {
						t.Errorf("append %d: %v", i, err)
						return
					}
					if err := app.Commit(); err != nil {
						t.Errorf("commit %d: %v", i, err)
						return
					}
				}
			}
			go race()
			go race()
			wg.Wait()

			if got := h.NumSeries(); got != n {
				t.Fatalf("NumSeries() = %d, want %d - each serial value must resolve to exactly one series across both racing goroutines", got, n)
			}
		})
	}
}
