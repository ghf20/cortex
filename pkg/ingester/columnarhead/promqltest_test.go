package columnarhead

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/testutil"
)

// This file is the first PromQL-LEVEL test this package has ever had. Every
// other test here (unit, differential, real gRPC chunks) verifies "does
// columnarhead's stored/served data match real Prometheus's" - none of them
// verify "does a real PromQL query evaluated over that data produce the right
// ANSWER." Those are different bars: PromQL's own counter-reset detection,
// staleness handling, and range-vector windowing logic all run one layer
// above what's been tested so far. This wires the REAL, unmodified
// promql.Engine and Prometheus's own upstream promqltest acceptance suite
// (promqltest/testdata/*.test, the same files real Prometheus's own CI runs)
// directly against a bare columnarhead.Head, via storage.Storage.
//
// A real, known limitation going in, not discovered after the fact: this
// package's label-shape restriction (ErrUnsupportedLabelShape - the six
// target labels plus at most ONE extra label) rejects a real fraction of the
// suite's own `load` blocks outright (many .test files use 2-3 non-target
// labels, e.g. {job="api-server", instance="0", group="canary"}), which fails
// that whole file's subtest immediately - not a PromQL bug, a pre-existing,
// already-documented prototype scope limit unrelated to what this test
// exists to check. Left as-is rather than working around it (e.g. by
// stripping labels down to fit): a synthetic pass would prove nothing about
// real behavior. See CHECKLIST.md's Phase 6 for the honest per-file
// breakdown this surfaced.

// columnarStorage adapts a bare *Head to storage.Storage so the real
// promql.Engine can query it directly - test-only infrastructure, not needed
// anywhere in the real ingest path (which never runs PromQL itself; that's
// the querier/distributor's job in real Cortex, reached through
// columnarheadTSDBStore/tsdbStore instead, already covered by this session's
// other tests).
type columnarStorage struct {
	h *Head
}

func newColumnarStorage(testutil.T) storage.Storage {
	return &columnarStorage{h: NewHead(64, 8, 64)}
}

func (s *columnarStorage) Appender(ctx context.Context) storage.Appender {
	return s.h.Appender(ctx)
}

func (s *columnarStorage) Querier(mint, maxt int64) (storage.Querier, error) {
	return s.h.Querier(mint, maxt)
}

func (s *columnarStorage) ChunkQuerier(mint, maxt int64) (storage.ChunkQuerier, error) {
	return s.h.ChunkQuerier(mint, maxt)
}

func (s *columnarStorage) StartTime() (int64, error) {
	return s.h.MinTime(), nil
}

func (s *columnarStorage) Close() error {
	return nil
}

var _ storage.Storage = (*columnarStorage)(nil)

// TestPromQLAcceptanceSuite runs real Prometheus's own upstream PromQL
// acceptance test suite (promql/promqltest/testdata/*.test - the same files
// real Prometheus's own CI runs, embedded in the vendored promqltest package,
// not copied here) directly against columnarhead - the first time any PromQL
// EVALUATION (not just storage round-tripping) has touched this package. See
// this file's own package-level doc comment for the real, known label-shape
// limitation this surfaces on a fraction of the suite, and CHECKLIST.md's
// Phase 6 for the resulting per-file pass/fail breakdown.
func TestPromQLAcceptanceSuite(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	engine := promqltest.NewTestEngineWithOpts(t, promql.EngineOpts{
		Logger:                   logger,
		Reg:                      nil,
		MaxSamples:               promqltest.DefaultMaxSamplesPerQuery,
		Timeout:                  100 * time.Second,
		NoStepSubqueryIntervalFn: func(int64) int64 { return 60000 },
		EnableAtModifier:         true,
		EnableNegativeOffset:     true,
		EnablePerStepStats:       false,
		LookbackDelta:            0,
		EnableDelayedNameRemoval: true,
	})
	promqltest.RunBuiltinTestsWithStorage(t, engine, newColumnarStorage)
}
