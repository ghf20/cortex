package ingester

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/promslog"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/test"
)

func newTestColumnarheadStore(t *testing.T, dir string, blockDuration int64) *columnarheadTSDBStore {
	t.Helper()
	s, err := newColumnarheadTSDBStore(dir, 8, 4, 32, blockDuration, blockDuration, nil, promslog.NewNopLogger(), nil)
	require.NoError(t, err)
	return s
}

func seriesLabels(name, pod string) labels.Labels {
	return labels.FromStrings(labels.MetricName, name, "cluster", "c", "namespace", "n", "pod", pod, "container", "co", "node", "no", "job", "j")
}

// queryAll collects every (metric name, timestamp, value) sample a store's
// Querier returns over [mint, maxt] - the decisive check throughout this file
// that head+block data is genuinely merged, not just individually present.
func queryAll(t *testing.T, s *columnarheadTSDBStore, mint, maxt int64) map[string][]sample {
	t.Helper()
	q, err := s.Querier(mint, maxt)
	require.NoError(t, err)
	defer q.Close()

	got := map[string][]sample{}
	ss := q.Select(context.Background(), false, nil, labels.MustNewMatcher(labels.MatchRegexp, labels.MetricName, ".+"))
	for ss.Next() {
		ser := ss.At()
		name := ser.Labels().Get(labels.MetricName)
		it := ser.Iterator(nil)
		for it.Next() == chunkenc.ValFloat {
			ts, v := it.At()
			got[name] = append(got[name], sample{ts, v})
		}
		require.NoError(t, it.Err())
	}
	require.NoError(t, ss.Err())
	return got
}

type sample struct {
	ts int64
	v  float64
}

// TestColumnarheadTSDBStoreQueryMergesHeadAndBlocks is the decisive test for the
// whole store: append samples, force a head compaction into a durable block, then
// confirm a query still sees ALL samples (the ones now durable in the block AND
// any appended afterward, still only in the head) as a single merged result - not
// two disjoint answers depending on which half of the store a caller happens to
// ask.
func TestColumnarheadTSDBStoreQueryMergesHeadAndBlocks(t *testing.T) {
	dir := t.TempDir()
	s := newTestColumnarheadStore(t, dir, 2*60*60*1000)
	defer s.Close()

	l := seriesLabels("up", "p")
	app := s.Appender(context.Background())
	base := int64(1700000000000)
	var before []sample
	for i := 0; i < 5; i++ {
		ts := base + int64(i)*15000
		v := float64(i)
		if _, err := app.Append(0, l, ts, v); err != nil {
			t.Fatalf("Append: %v", err)
		}
		before = append(before, sample{ts, v})
	}

	if err := s.CompactHeadRange(context.Background(), s.MinTime(), s.MaxTime()); err != nil {
		t.Fatalf("CompactHeadRange: %v", err)
	}
	if len(s.Blocks()) != 1 {
		t.Fatalf("Blocks() = %d, want 1 after CompactHeadRange", len(s.Blocks()))
	}
	// Head must have been truncated: the compacted range should no longer be
	// live in memory (NumSeries stays, per columnarhead's own documented
	// Truncate semantics, but there should be nothing left to iterate before
	// appending more).
	if got := queryAll(t, s, base-1, base+4*15000+1); len(got["up"]) != len(before) {
		t.Fatalf("post-compaction query (block only) = %v, want %v", got["up"], before)
	}

	// Append more, still in the live head only.
	var after []sample
	app2 := s.Appender(context.Background())
	for i := 5; i < 8; i++ {
		ts := base + int64(i)*15000
		v := float64(i)
		if _, err := app2.Append(0, l, ts, v); err != nil {
			t.Fatalf("Append (post-compact): %v", err)
		}
		after = append(after, sample{ts, v})
	}

	got := queryAll(t, s, base-1, base+7*15000+1)
	want := append(append([]sample(nil), before...), after...)
	require.Equal(t, want, got["up"])
}

// TestColumnarheadTSDBStoreReload confirms a store closed and reopened on the
// same directory rediscovers its blocks AND its durable head state correctly -
// the real "restart after a crash/restart" scenario, not just an in-process check.
func TestColumnarheadTSDBStoreReload(t *testing.T) {
	dir := t.TempDir()
	s := newTestColumnarheadStore(t, dir, 2*60*60*1000)

	l := seriesLabels("up", "p")
	app := s.Appender(context.Background())
	base := int64(1700000000000)
	var want []sample
	for i := 0; i < 5; i++ {
		ts := base + int64(i)*15000
		v := float64(i)
		if _, err := app.Append(0, l, ts, v); err != nil {
			t.Fatalf("Append: %v", err)
		}
		want = append(want, sample{ts, v})
	}
	if err := s.CompactHeadRange(context.Background(), s.MinTime(), s.MaxTime()); err != nil {
		t.Fatalf("CompactHeadRange: %v", err)
	}
	// Flush the durable head so the still-live portion (there is none here,
	// everything was compacted) and any bookkeeping survive too - a real
	// deployment would call this on a schedule; do it explicitly here.
	if _, err := s.head.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reloaded, err := newColumnarheadTSDBStore(dir, 8, 4, 32, 2*60*60*1000, 2*60*60*1000, nil, promslog.NewNopLogger(), nil)
	if err != nil {
		t.Fatalf("newColumnarheadTSDBStore (reload): %v", err)
	}
	defer reloaded.Close()

	if len(reloaded.Blocks()) != 1 {
		t.Fatalf("Blocks() after reload = %d, want 1", len(reloaded.Blocks()))
	}
	got := queryAll(t, reloaded, base-1, base+4*15000+1)
	require.Equal(t, want, got["up"])
}

// TestColumnarheadTSDBStoreCompactMergesBlocks is the decisive test for real
// block-level LEVELED merge compaction: produce several small, non-overlapping
// head-compacted blocks, then call Compact and confirm the block COUNT actually
// shrinks (a real merge happened, not just a no-op scan) while every sample
// remains queryable and correct.
func TestColumnarheadTSDBStoreCompactMergesBlocks(t *testing.T) {
	const blockDuration = 60 * 1000 // 1 minute blocks - small, so several fit in the test's time range
	dir := t.TempDir()
	// maxBlockDuration must exceed minBlockDuration for there to be a NEXT
	// compaction level to merge into at all - a single-level compactor
	// (min == max, like newTestColumnarheadStore's helper uses elsewhere in
	// this file) never merges anything, by real LeveledCompactor design, not
	// a bug: real *tsdb.DB.Open's own tsdb.ExponentialBlockRanges expansion is
	// exactly what creates that next level.
	s, err := newColumnarheadTSDBStore(dir, 8, 4, 32, blockDuration, 10*blockDuration, nil, promslog.NewNopLogger(), nil)
	require.NoError(t, err)
	defer s.Close()

	l := seriesLabels("up", "p")
	app := s.Appender(context.Background())
	base := int64(1700000000000)
	var want []sample
	// 5 head-range compactions, one block each, back to back in time - exactly
	// the shape LeveledCompactor.Plan should pick up as mergeable.
	for block := 0; block < 5; block++ {
		blockStart := base + int64(block)*blockDuration
		for i := 0; i < 3; i++ {
			ts := blockStart + int64(i)*15000
			v := float64(block*10 + i)
			if _, err := app.Append(0, l, ts, v); err != nil {
				t.Fatalf("Append: %v", err)
			}
			want = append(want, sample{ts, v})
		}
		if err := s.CompactHeadRange(context.Background(), blockStart, blockStart+blockDuration-1); err != nil {
			t.Fatalf("CompactHeadRange(block %d): %v", block, err)
		}
	}
	before := len(s.Blocks())
	if before < 2 {
		t.Fatalf("test setup broken: only %d blocks before Compact, need several to prove a merge happened", before)
	}

	if err := s.Compact(context.Background()); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	after := len(s.Blocks())
	if after >= before {
		t.Fatalf("Blocks() after Compact = %d, want fewer than %d (a merge should have reduced the count)", after, before)
	}
	t.Logf("block count %d -> %d after Compact (real LeveledCompactor merge)", before, after)

	got := queryAll(t, s, base-1, base+5*blockDuration)
	require.Equal(t, want, got["up"])
}

// TestColumnarheadTSDBStorePrunesDeletableBlocks confirms blocksToDelete's
// verdict is actually acted on: a block it marks deletable is closed and its
// directory removed from disk after Compact.
func TestColumnarheadTSDBStorePrunesDeletableBlocks(t *testing.T) {
	dir := t.TempDir()
	s, err := newColumnarheadTSDBStore(dir, 8, 4, 32, 2*60*60*1000, 2*60*60*1000, func(blocks []*tsdb.Block) map[ulid.ULID]struct{} {
		deletable := make(map[ulid.ULID]struct{}, len(blocks))
		for _, b := range blocks {
			deletable[b.Meta().ULID] = struct{}{}
		}
		return deletable
	}, promslog.NewNopLogger(), nil)
	require.NoError(t, err)
	defer s.Close()

	l := seriesLabels("up", "p")
	app := s.Appender(context.Background())
	base := int64(1700000000000)
	if _, err := app.Append(0, l, base, 1); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := s.CompactHeadRange(context.Background(), s.MinTime(), s.MaxTime()); err != nil {
		t.Fatalf("CompactHeadRange: %v", err)
	}
	blocks := s.Blocks()
	if len(blocks) != 1 {
		t.Fatalf("Blocks() = %d, want 1", len(blocks))
	}
	blockDir := filepath.Join(dir, blocks[0].Meta().ULID.String())
	if _, err := os.Stat(blockDir); err != nil {
		t.Fatalf("block dir missing before prune: %v", err)
	}

	if err := s.Compact(context.Background()); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if len(s.Blocks()) != 0 {
		t.Fatalf("Blocks() after Compact = %d, want 0 (blocksToDelete marked everything deletable)", len(s.Blocks()))
	}
	if _, err := os.Stat(blockDir); !os.IsNotExist(err) {
		t.Fatalf("block dir %s still exists after being marked deletable: %v", blockDir, err)
	}
}

// TestColumnarheadTSDBStoreCompactOOOHeadIsNoOp confirms CompactOOOHead is
// callable and harmless - the real behavior (OOO samples already included in
// every compacted block) is proven in columnarhead's own
// TestCompactHeadIncludesOOOSamples; this just checks the tsdbStore-level method
// itself doesn't error or panic.
func TestColumnarheadTSDBStoreCompactOOOHeadIsNoOp(t *testing.T) {
	dir := t.TempDir()
	s := newTestColumnarheadStore(t, dir, 2*60*60*1000)
	defer s.Close()
	if err := s.CompactOOOHead(context.Background()); err != nil {
		t.Fatalf("CompactOOOHead: %v", err)
	}
}

// TestColumnarheadTSDBStorePostingsForMatchers is a smoke test that the
// tsdbStore-level PostingsForMatchers wrapper reaches the real underlying head
// correctly - columnarhead's own TestPostingsForMatchers covers the matcher
// semantics in depth.
func TestColumnarheadTSDBStorePostingsForMatchers(t *testing.T) {
	dir := t.TempDir()
	s := newTestColumnarheadStore(t, dir, 2*60*60*1000)
	defer s.Close()

	l := seriesLabels("up", "p")
	app := s.Appender(context.Background())
	if _, err := app.Append(0, l, 1700000000000, 1); err != nil {
		t.Fatalf("Append: %v", err)
	}

	p, err := s.PostingsForMatchers(context.Background(), labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "up"))
	if err != nil {
		t.Fatalf("PostingsForMatchers: %v", err)
	}
	count := 0
	for p.Next() {
		count++
	}
	require.NoError(t, p.Err())
	if count != 1 {
		t.Fatalf("got %d postings, want 1", count)
	}
}

// TestIngester_UseColumnarHead is the decisive end-to-end test for the
// -blocks-storage.tsdb.use-columnar-head flag: a real *Ingester, constructed
// through its normal createTSDB path with the flag set, must actually wire in a
// *columnarheadTSDBStore (not silently fall back to the real *tsdb.DB), and real
// Push/Querier traffic through it must round-trip correctly - the same bar
// TestIngester_QueryStream holds the real backend to, just reached through
// userTSDB.Querier directly rather than the gRPC streaming path.
func TestIngester_UseColumnarHead(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	cfg.BlocksStorageConfig.TSDB.UseColumnarHead = true

	i, err := prepareIngesterWithBlocksStorage(t, cfg, prometheus.NewRegistry())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	test.Poll(t, 1*time.Second, ring.ACTIVE, func() any {
		return i.lifecycler.GetState()
	})

	ctx := user.InjectOrgID(context.Background(), userID)
	// All six fixed target labels set, not just __name__ - the shape
	// columnarhead's appender.splitLabels/Head.SeriesLabels round-trip is
	// actually built and tested for (see CHECKLIST.md's own note on the
	// separate, pre-existing gap: SeriesLabels currently always emits these
	// six labels even when empty, rather than omitting absent ones like real
	// Prometheus label-set semantics require - not exercised by this test,
	// which deliberately stays within the documented, tested shape).
	lbls := seriesLabels("foo", "p")
	req, _ := mockWriteRequest(t, lbls, 456, 123000)
	_, err = i.Push(ctx, req)
	require.NoError(t, err)

	db, err := i.getTSDB(userID)
	require.NoError(t, err)
	require.NotNil(t, db)

	// Confirm the columnar-head-backed store is actually the one wired in -
	// not silently falling back to the real *tsdb.DB, which would make every
	// other assertion here meaningless.
	_, ok := db.db.(*columnarheadTSDBStore)
	require.True(t, ok, "userTSDB.db is a %T, want *columnarheadTSDBStore", db.db)

	require.Equal(t, uint64(1), db.NumSeries())

	q, err := db.Querier(0, 200000)
	require.NoError(t, err)
	defer q.Close()

	ss := q.Select(ctx, false, nil, labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "foo"))
	require.True(t, ss.Next())
	series := ss.At()
	require.Equal(t, lbls, series.Labels())
	it := series.Iterator(nil)
	require.Equal(t, chunkenc.ValFloat, it.Next())
	ts, v := it.At()
	require.Equal(t, int64(123000), ts)
	require.Equal(t, float64(456), v)
	require.False(t, ss.Next())
}
