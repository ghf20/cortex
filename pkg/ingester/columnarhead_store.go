package ingester

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"sync"

	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/index"

	"github.com/cortexproject/cortex/pkg/ingester/columnarhead"
)

// columnarheadTSDBStore is a tsdbStore implementation backed by
// columnarhead.DurableHead - the alternative head implementation CHECKLIST.md's
// Phase 7 scoped. Unlike nativeTSDBStore (a thin adapter over *tsdb.DB, which
// already does everything internally), this type owns real responsibilities of
// its own: tracking the on-disk blocks columnarhead.CompactHead produces,
// merging the live head's query results with every open block's (real *tsdb.DB
// does the identical merge internally - db.Querier/db.ChunkQuerier, vendor/.../
// tsdb/db.go), and real block-level LEVELED merge compaction reusing
// tsdb.LeveledCompactor unmodified - the same "reuse the real machinery instead
// of reinventing it" precedent CompactHead itself already established for
// head->block compaction (see CHECKLIST.md's Phase 5a).
//
// Not yet wired into the ingester's real construction path (see CHECKLIST.md's
// Phase 7 recommended build order, step 5) - this type is complete and
// independently tested, but nothing in ingester.go constructs one yet.
type columnarheadTSDBStore struct {
	head   *columnarhead.DurableHead
	dir    string
	logger *slog.Logger

	compactor  *tsdb.LeveledCompactor
	chunkRange int64 // smallest configured block range - what CompactHeadRange writes head blocks at

	// blocksToDelete decides which currently-loaded blocks are safe to remove
	// (shipped + past retention, or superseded by a merge) - the same shape
	// tsdb.Options.BlocksToDelete already uses, so Cortex's own retention/
	// shipping policy (userTSDB.blocksToDelete for the real backend) can be
	// reused or adapted for this one without duplicating that logic here. nil
	// means "never delete anything," a safe default, not silently broken
	// behavior.
	blocksToDelete func([]*tsdb.Block) map[ulid.ULID]struct{}

	// cmtx serializes CompactHeadRange/Compact/CompactOOOHead against each
	// other, matching real *tsdb.DB's own db.cmtx - compaction is not safe to
	// run concurrently with itself (the block list and the compactor's on-disk
	// planning both assume a single active compaction at a time).
	cmtx sync.Mutex

	// mtx protects blocks - matching real *tsdb.DB's own db.mtx. Deliberately
	// separate from cmtx: a Querier only needs a brief RLock to snapshot the
	// current block list, not exclusion against the whole compaction process,
	// the same separation real *tsdb.DB draws between db.mtx and db.cmtx.
	mtx    sync.RWMutex
	blocks []*tsdb.Block // sorted by MinTime ascending, matching real *tsdb.DB.blocks
}

var _ tsdbStore = (*columnarheadTSDBStore)(nil)

// newColumnarheadTSDBStore opens or creates a columnarhead-backed store rooted
// at dir - the same directory layout convention as a real *tsdb.DB (durable head
// files and ULID-named block subdirectories side by side; see
// columnarhead.CreateDurableHead's own file set and this function's
// discoverColumnarheadBlocks). minBlockDuration/maxBlockDuration mirror
// tsdb.Options' identically-named fields, expanded into a leveled compactor's
// range set the exact same way real *tsdb.Open does (tsdb.ExponentialBlockRanges,
// trimmed to maxBlockDuration).
func newColumnarheadTSDBStore(
	dir string,
	expectedSeries, expectedTargets, expectedSymbols int,
	minBlockDuration, maxBlockDuration int64,
	blocksToDelete func([]*tsdb.Block) map[ulid.ULID]struct{},
	logger *slog.Logger,
	reg prometheus.Registerer,
) (*columnarheadTSDBStore, error) {
	if err := os.MkdirAll(dir, 0o777); err != nil {
		return nil, err
	}

	head, err := columnarhead.LoadDurableHead(dir)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("load durable head: %w", err)
		}
		head, err = columnarhead.CreateDurableHead(dir, expectedSeries, expectedTargets, expectedSymbols)
		if err != nil {
			return nil, fmt.Errorf("create durable head: %w", err)
		}
	}

	blocks, err := discoverColumnarheadBlocks(logger, dir)
	if err != nil {
		return nil, fmt.Errorf("discover existing blocks: %w", err)
	}

	rngs := tsdb.ExponentialBlockRanges(minBlockDuration, 10, 3)
	for i, v := range rngs {
		if v > maxBlockDuration {
			rngs = rngs[:i]
			break
		}
	}
	if len(rngs) == 0 {
		rngs = []int64{minBlockDuration}
	}

	compactor, err := tsdb.NewLeveledCompactorWithOptions(context.Background(), reg, logger, rngs, chunkenc.NewPool(), tsdb.LeveledCompactorOptions{
		// Same deliberate choice real Cortex's own *tsdb.DB construction makes
		// (ingester.go's tsdb.Open call) - let compactors handle overlaps
		// explicitly rather than assuming this backend never produces them.
		EnableOverlappingCompaction: false,
	})
	if err != nil {
		return nil, fmt.Errorf("create leveled compactor: %w", err)
	}

	return &columnarheadTSDBStore{
		head:           head,
		dir:            dir,
		logger:         logger,
		compactor:      compactor,
		chunkRange:     rngs[0],
		blocksToDelete: blocksToDelete,
		blocks:         blocks,
	}, nil
}

// discoverColumnarheadBlocks scans dir for existing ULID-named block
// subdirectories (a completed meta.json is what distinguishes a real block from
// an in-progress write or unrelated directory) and opens each via the real
// tsdb.OpenBlock - the equivalent of what real *tsdb.DB's own openBlocks does on
// startup, reimplemented here only because that helper is unexported.
func discoverColumnarheadBlocks(logger *slog.Logger, dir string) ([]*tsdb.Block, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var blocks []*tsdb.Block
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		if _, err := ulid.Parse(e.Name()); err != nil {
			continue // not a block directory
		}
		blockDir := filepath.Join(dir, e.Name())
		if _, err := os.Stat(filepath.Join(blockDir, "meta.json")); err != nil {
			continue // incomplete or unrelated directory, not a real block
		}
		b, err := tsdb.OpenBlock(logger, blockDir, chunkenc.NewPool(), nil)
		if err != nil {
			return nil, fmt.Errorf("open block %s: %w", e.Name(), err)
		}
		blocks = append(blocks, b)
	}
	sortBlocksByMinTime(blocks)
	return blocks, nil
}

func sortBlocksByMinTime(blocks []*tsdb.Block) {
	for i := 1; i < len(blocks); i++ {
		for j := i; j > 0 && blocks[j-1].Meta().MinTime > blocks[j].Meta().MinTime; j-- {
			blocks[j-1], blocks[j] = blocks[j], blocks[j-1]
		}
	}
}

func (s *columnarheadTSDBStore) Appender(ctx context.Context) storage.Appender {
	return s.head.Appender(ctx)
}

// Querier merges the live head's Querier with every open block that overlaps
// [mint, maxt], via storage.NewMergeQuerier - real *tsdb.DB.Querier's exact
// pattern (primaries = every querier, secondaries = nil, storage.ChainedSeriesMerge
// - vendor/.../tsdb/db.go). Unlike real *tsdb.DB, there's no isolation-driven
// querier-collides-with-truncation dance to replicate: this package has no
// isolation (see CHECKLIST.md's Phase 4 conclusion - Commit is a no-op, so
// there's no in-flight transaction a concurrent Truncate could invalidate a
// reader's view of).
func (s *columnarheadTSDBStore) Querier(mint, maxt int64) (_ storage.Querier, err error) {
	headQuerier, err := s.head.Querier(mint, maxt)
	if err != nil {
		return nil, err
	}
	queriers := []storage.Querier{headQuerier}
	defer func() {
		if err != nil {
			for _, q := range queriers {
				_ = q.Close()
			}
		}
	}()

	for _, b := range s.overlappingBlocks(mint, maxt) {
		q, err := tsdb.NewBlockQuerier(b, mint, maxt)
		if err != nil {
			return nil, fmt.Errorf("open querier for block %s: %w", b.Meta().ULID, err)
		}
		queriers = append(queriers, q)
	}
	return storage.NewMergeQuerier(queriers, nil, storage.ChainedSeriesMerge), nil
}

// ChunkQuerier is Querier's chunk-level counterpart - same merge pattern, same
// mergeFn real *tsdb.DB.ChunkQuerier uses (storage.NewCompactingChunkSeriesMerger
// over storage.ChainedSeriesMerge).
func (s *columnarheadTSDBStore) ChunkQuerier(mint, maxt int64) (_ storage.ChunkQuerier, err error) {
	headQuerier, err := s.head.ChunkQuerier(mint, maxt)
	if err != nil {
		return nil, err
	}
	queriers := []storage.ChunkQuerier{headQuerier}
	defer func() {
		if err != nil {
			for _, q := range queriers {
				_ = q.Close()
			}
		}
	}()

	for _, b := range s.overlappingBlocks(mint, maxt) {
		q, err := tsdb.NewBlockChunkQuerier(b, mint, maxt)
		if err != nil {
			return nil, fmt.Errorf("open chunk querier for block %s: %w", b.Meta().ULID, err)
		}
		queriers = append(queriers, q)
	}
	return storage.NewMergeChunkQuerier(queriers, nil, storage.NewCompactingChunkSeriesMerger(storage.ChainedSeriesMerge)), nil
}

func (s *columnarheadTSDBStore) overlappingBlocks(mint, maxt int64) []*tsdb.Block {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	var out []*tsdb.Block
	for _, b := range s.blocks {
		if b.OverlapsClosedInterval(mint, maxt) {
			out = append(out, b)
		}
	}
	return out
}

func (s *columnarheadTSDBStore) ExemplarQuerier(ctx context.Context) (storage.ExemplarQuerier, error) {
	return s.head.ExemplarQuerier(ctx)
}

func (s *columnarheadTSDBStore) Blocks() []*tsdb.Block {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return append([]*tsdb.Block(nil), s.blocks...)
}

func (s *columnarheadTSDBStore) Close() error {
	var err error
	s.mtx.Lock()
	for _, b := range s.blocks {
		if cerr := b.Close(); cerr != nil {
			err = errors.Join(err, cerr)
		}
	}
	s.blocks = nil
	s.mtx.Unlock()
	if cerr := s.head.Close(); cerr != nil {
		err = errors.Join(err, cerr)
	}
	return err
}

func (s *columnarheadTSDBStore) StartTime() (int64, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	if len(s.blocks) > 0 {
		return s.blocks[0].Meta().MinTime, nil
	}
	return s.head.MinTime(), nil
}

func (s *columnarheadTSDBStore) Dir() string {
	return s.dir
}

func (s *columnarheadTSDBStore) NumSeries() uint64 {
	return uint64(s.head.NumSeries())
}

func (s *columnarheadTSDBStore) MinTime() int64 {
	return s.head.MinTime()
}

func (s *columnarheadTSDBStore) MaxTime() int64 {
	return s.head.MaxTime()
}

func (s *columnarheadTSDBStore) PostingsForMatchers(ctx context.Context, ms ...*labels.Matcher) (index.Postings, error) {
	return s.head.PostingsForMatchers(ctx, ms...)
}

// ApplyConfig wires the one live-reconfigurable knob that has a real
// columnarhead equivalent so far - OutOfOrderTimeWindow (Head.SetOOOTimeWindow,
// built in Phase 4) - matching real *tsdb.DB.ApplyConfig's own field path
// (vendor/.../tsdb/db.go). Retention/MaxBytes reconfiguration, real *tsdb.DB's
// other ApplyConfig responsibility, has no columnarhead equivalent yet (no
// size-based retention has been built for this backend at all) - a stated gap,
// not a silent one.
func (s *columnarheadTSDBStore) ApplyConfig(conf *config.Config) error {
	oooTimeWindow := int64(0)
	if conf.StorageConfig.TSDBConfig != nil {
		oooTimeWindow = conf.StorageConfig.TSDBConfig.OutOfOrderTimeWindow
	}
	if oooTimeWindow < 0 {
		oooTimeWindow = 0
	}
	s.head.SetOOOTimeWindow(oooTimeWindow)
	return nil
}

// CompactHeadRange compacts [mint, maxt] into a new durable block (reusing
// columnarhead.CompactHead, Phase 5a) and truncates the live head for that same
// range once the block is durably written. Real *tsdb.DB.CompactHead only
// truncates its WAL here, not head memory (see vendor/.../tsdb/db.go) - real
// tsdb.Head reclaims memory separately, via mmap'd chunk garbage collection
// during db.reload(). columnarhead has no separate reclaim path: Head.Truncate
// is the ONLY mechanism that frees arena bytes, so tying it to every successful
// head compaction (forced or automatic) is the correct design for this backend,
// not a divergence from real semantics without reason.
func (s *columnarheadTSDBStore) CompactHeadRange(_ context.Context, mint, maxt int64) error {
	s.cmtx.Lock()
	defer s.cmtx.Unlock()
	// Deliberately does NOT prune via blocksToDelete here, matching real
	// *tsdb.DB.CompactHead's own asymmetry (vendor/.../tsdb/db.go): the
	// EXPORTED CompactHead only truncates the WAL, it never calls reloadBlocks
	// (retention pruning is exclusively a db.Compact()/auto-compact-loop
	// responsibility there, via db.reload()). See Compact's own doc comment.
	return s.compactHeadRangeLocked(mint, maxt)
}

// compactHeadRangeLocked is CompactHeadRange's body, factored out so Compact's
// own auto-compaction loop (which already holds cmtx) can call it directly
// without re-locking - the same shape real *tsdb.DB.compactHead (unexported,
// called by both the auto-compact loop and the exported CompactHead) has.
func (s *columnarheadTSDBStore) compactHeadRangeLocked(mint, maxt int64) error {
	blockDir, err := columnarhead.CompactHead(s.head.Head, mint, maxt, s.dir, s.chunkRange, s.logger)
	if err != nil {
		return fmt.Errorf("compact head range [%d,%d]: %w", mint, maxt, err)
	}
	if blockDir != "" {
		block, err := tsdb.OpenBlock(s.logger, blockDir, chunkenc.NewPool(), nil)
		if err != nil {
			return fmt.Errorf("open compacted block %s: %w", blockDir, err)
		}
		s.mtx.Lock()
		s.blocks = append(s.blocks, block)
		sortBlocksByMinTime(s.blocks)
		s.mtx.Unlock()
	}
	s.head.Truncate(maxt + 1)
	return nil
}

// CompactOOOHead is a genuine, verified no-op for this backend, not an
// unimplemented stub: real *tsdb.DB needs a separate OOO compaction pass
// (tsdb.NewOOOCompactionHead) because its out-of-order samples live in a
// physically separate OOOHeadChunkReader that ordinary head compaction never
// touches. columnarhead's OOO design is different by construction - a series'
// OOO buffer is merged into its in-order stream at READ time
// (headSeries.Iterator -> mergedIterator, ooo.go/querier.go), so every block
// CompactHead/CompactHeadRange has ever produced already includes OOO samples.
// TestCompactHeadIncludesOOOSamples (columnarhead package) verifies this
// directly rather than assuming it.
func (s *columnarheadTSDBStore) CompactOOOHead(context.Context) error {
	return nil
}

// Compact mirrors real *tsdb.DB.Compact's own two-phase shape (vendor/.../tsdb/
// db.go): first, repeatedly compact any head range that's safely in the past
// (the same compactable() 1.5x-chunk-range threshold real tsdb.Head uses) into
// durable blocks; then, real block-level LEVELED merge compaction, reusing
// tsdb.LeveledCompactor.Plan/.Compact unmodified against the real, standard
// blocks this backend already produces - the plan/compact ALGORITHM is real
// Prometheus code, not reimplemented, only the block-list bookkeeping around it
// (open/close/delete) is this backend's own.
func (s *columnarheadTSDBStore) Compact(ctx context.Context) error {
	s.cmtx.Lock()
	defer s.cmtx.Unlock()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if !s.compactable() {
			break
		}
		minTime := s.head.MinTime()
		blockMaxTime := rangeForColumnarheadTimestamp(minTime, s.chunkRange) - 1
		if err := s.compactHeadRangeLocked(minTime, blockMaxTime); err != nil {
			return fmt.Errorf("auto-compact head: %w", err)
		}
	}

	if err := s.compactBlocksLocked(ctx); err != nil {
		return err
	}
	return s.pruneBlocksLocked()
}

// compactable matches real tsdb.Head.compactable's exact threshold
// (vendor/.../tsdb/head.go): more than 1.5x the configured (smallest) block
// range between the head's min and max timestamp. An empty head (MinTime still
// the math.MaxInt64 sentinel) is never compactable.
func (s *columnarheadTSDBStore) compactable() bool {
	minTime, maxTime := s.head.MinTime(), s.head.MaxTime()
	if minTime == math.MaxInt64 {
		return false
	}
	return maxTime-minTime > s.chunkRange/2*3
}

// rangeForColumnarheadTimestamp matches real tsdb's own rangeForTimestamp
// (vendor/.../tsdb/db.go, unexported) - the exclusive end of the block-range
// bucket containing t.
func rangeForColumnarheadTimestamp(t, width int64) int64 {
	return (t/width)*width + width
}

// compactBlocksLocked performs real block-level LEVELED merge compaction,
// reusing tsdb.LeveledCompactor.Plan/.Compact exactly as real *tsdb.DB's own
// compactBlocks does (vendor/.../tsdb/db.go) - Plan decides which existing
// blocks should merge (the standard leveled/tiered algorithm, unmodified),
// Compact performs the merge and writes new block(s). The caller (Compact) must
// already hold cmtx.
func (s *columnarheadTSDBStore) compactBlocksLocked(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		// Same guard real compactBlocks uses: don't spend time merging blocks
		// if a head block is overdue - persisting the head takes priority.
		if s.compactable() {
			return nil
		}

		plan, err := s.compactor.Plan(s.dir)
		if err != nil {
			return fmt.Errorf("plan block compaction: %w", err)
		}
		if len(plan) == 0 {
			return nil
		}

		s.mtx.RLock()
		open := append([]*tsdb.Block(nil), s.blocks...)
		s.mtx.RUnlock()

		uids, err := s.compactor.Compact(s.dir, plan, open)
		if err != nil {
			return fmt.Errorf("compact blocks %v: %w", plan, err)
		}
		if err := s.reloadAfterMergeLocked(plan, uids); err != nil {
			return err
		}
	}
}

// reloadAfterMergeLocked replaces every block whose directory appears in
// mergedDirs (now superseded by the merge) with the newly written blocks
// identified by newULIDs - real *tsdb.DB.reloadBlocks' equivalent, simplified:
// this backend doesn't need reloadBlocks' full generality (crash-parent
// tracking, corruption handling across an arbitrary on-disk state) since it
// only ever calls this immediately after its OWN successful compactor.Compact
// call, with an exact, known set of inputs and outputs.
//
// Closing a merged-away block before deleting its directory is safe even if a
// query is still reading it: real *tsdb.Block.Close blocks on its own internal
// pendingReaders WaitGroup (vendor/.../tsdb/block.go) until in-flight readers
// finish - not something this code needs to reimplement.
func (s *columnarheadTSDBStore) reloadAfterMergeLocked(mergedDirs []string, newULIDs []ulid.ULID) error {
	merged := make(map[string]struct{}, len(mergedDirs))
	for _, d := range mergedDirs {
		merged[filepath.Base(d)] = struct{}{}
	}

	s.mtx.Lock()
	var kept []*tsdb.Block
	for _, b := range s.blocks {
		if _, ok := merged[b.Meta().ULID.String()]; !ok {
			kept = append(kept, b)
			continue
		}
		if err := b.Close(); err != nil {
			s.mtx.Unlock()
			return fmt.Errorf("close merged-away block %s: %w", b.Meta().ULID, err)
		}
		if err := os.RemoveAll(filepath.Join(s.dir, b.Meta().ULID.String())); err != nil {
			s.mtx.Unlock()
			return fmt.Errorf("delete merged-away block dir %s: %w", b.Meta().ULID, err)
		}
	}
	s.blocks = kept
	s.mtx.Unlock()

	for _, uid := range newULIDs {
		block, err := tsdb.OpenBlock(s.logger, filepath.Join(s.dir, uid.String()), chunkenc.NewPool(), nil)
		if err != nil {
			return fmt.Errorf("open merged block %s: %w", uid, err)
		}
		s.mtx.Lock()
		s.blocks = append(s.blocks, block)
		sortBlocksByMinTime(s.blocks)
		s.mtx.Unlock()
	}
	return nil
}

// pruneBlocksLocked consults blocksToDelete (if any) and removes whatever it
// marks deletable - the retention/shipped-block-age policy real *tsdb.DB
// applies via its own db.reloadBlocks on every reload (see tsdb.Options.
// BlocksToDelete's doc comment). Deliberately called once per public
// CompactHeadRange/Compact call rather than after every single internal reload
// step real *tsdb.DB triggers this from - close enough to the real cadence
// without replicating every one of its internal call sites. The caller must
// already hold cmtx.
func (s *columnarheadTSDBStore) pruneBlocksLocked() error {
	if s.blocksToDelete == nil {
		return nil
	}

	s.mtx.RLock()
	current := append([]*tsdb.Block(nil), s.blocks...)
	s.mtx.RUnlock()

	deletable := s.blocksToDelete(current)
	if len(deletable) == 0 {
		return nil
	}

	s.mtx.Lock()
	defer s.mtx.Unlock()
	var kept []*tsdb.Block
	for _, b := range s.blocks {
		if _, ok := deletable[b.Meta().ULID]; !ok {
			kept = append(kept, b)
			continue
		}
		if err := b.Close(); err != nil {
			return fmt.Errorf("close deletable block %s: %w", b.Meta().ULID, err)
		}
		if err := os.RemoveAll(filepath.Join(s.dir, b.Meta().ULID.String())); err != nil {
			return fmt.Errorf("delete block dir %s: %w", b.Meta().ULID, err)
		}
	}
	s.blocks = kept
	return nil
}
