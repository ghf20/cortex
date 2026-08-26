package columnarhead

import (
	"context"
	"log/slog"
	"path/filepath"

	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
)

// CompactHead writes every series in h with at least one sample in [mint, maxt] as a
// single durable TSDB block under dir, reusing Prometheus's own
// tsdb.CreateBlock/BlockWriter/LeveledCompactor machinery unmodified - see
// CHECKLIST.md's Phase 5a scoping for why this is reusable at all: CreateBlock takes
// []storage.Series, the same interface Head.Querier already produces, and stages into
// its own internal scratch *tsdb.Head rather than needing to wrap h itself as a
// concrete tsdb.Head (the thing that actually blocks full tsdbStore wiring).
//
// sortSeries=false here is deliberate, not an oversight: unlike a direct
// index.Writer.AddSeries caller (see querier.go's sortRefsByLabels, genuinely
// required there), this path is provably insensitive to input order. Verified, not
// assumed: an adversarial test (4 series created in reverse label-sorted order)
// round-trips correctly. LeveledCompactor never writes the block index straight from
// our input list - it re-derives a sorted enumeration from the scratch head via
// AllSortedPostings/headIndexReader.SortedPostings before writing anything
// (tsdb/compact.go's populateBlock), so pre-sorting our own output would only add
// cost (reconstructing every series' labels.Labels up front) for no benefit.
func CompactHead(h *Head, mint, maxt int64, dir string, chunkRange int64, logger *slog.Logger) (string, error) {
	q, err := h.Querier(mint, maxt)
	if err != nil {
		return "", err
	}
	defer q.Close()

	ss := q.Select(context.Background(), false, nil)
	var series []storage.Series
	for ss.Next() {
		series = append(series, ss.At())
	}
	if err := ss.Err(); err != nil {
		return "", err
	}

	blockDir, err := tsdb.CreateBlock(series, dir, chunkRange, logger)
	if err != nil {
		return "", err
	}
	// tsdb.CreateBlock's own BlockWriter.Flush returns a ZERO ulid.ULID (not an
	// error, and not an empty path - filepath.Join(dir, ulid.ULID{}.String())
	// is a real, non-empty string) when nothing was actually written to disk
	// (vendor/.../tsdb/blockwriter.go: "No block was produced. Caller is
	// responsible to check empty ulid.ULID based on its use case."). Every
	// existing caller in this package assumed blockDir == "" signals that case
	// (see compact_test.go's blockDir == "" checks) - a real, previously latent
	// gap, since every existing test happened to compact non-empty ranges.
	// Translate real Prometheus's zero-ULID convention into that expected ""
	// convention here, rather than returning a path to a block that was never
	// actually written.
	if filepath.Base(blockDir) == (ulid.ULID{}).String() {
		return "", nil
	}
	return blockDir, nil
}
