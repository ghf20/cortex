package columnarhead

import (
	"context"
	"fmt"
	"math"
	"sync"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

// TestHeadConcurrentAppendQueryTruncateCompact is the decisive check for Head's
// locking: real concurrent traffic through every entry point Head's doc comment
// claims is safe (Appender, Querier, Truncate, and CompactHead, which is built on
// Querier) running simultaneously under -race, for long enough to actually exercise
// contention, not just a token handful of goroutines that happen not to overlap.
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
		ref, err := h.GetOrCreateSeries(tgt, fmt.Sprintf("series_%d", i), "", "")
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
