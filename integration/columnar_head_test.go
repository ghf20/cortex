//go:build requires_docker

package integration

import (
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
)

// TestColumnarHeadIngestion is Phase 7's first real integration-suite coverage
// for the columnar head prototype (-blocks-storage.tsdb.use-columnar-head) -
// CHECKLIST.md flagged this as a real, checked (not assumed) gap: everything
// built through this branch had only ever run inside `go test` against
// in-process types (columnarhead.Head, columnarheadTSDBStore, or the real
// gRPC-layer TestIngester_UseColumnarHead_QueryStream, still an in-process
// Go test dialing its own in-process gRPC server) - the flag had never driven
// an actual running `cortex` binary through real HTTP remote-write + real
// HTTP query-API traffic before this. UseColumnarHead gates a hard if/else in
// ingester.go's userTSDB construction (real tsdb.DB vs columnarheadTSDBStore,
// no fallback path either way), so a passing round-trip here is exactly as
// meaningful as it looks - there's no silent-fallback failure mode to guard
// against separately.
func TestColumnarHeadIngestion(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	require.NoError(t, writeFileToSharedDir(s, "alertmanager_configs", []byte{}))

	minio := e2edb.NewMinio(9000, bucketName)
	require.NoError(t, s.StartAndWaitReady(minio))

	flags := mergeFlags(AlertmanagerLocalFlags(), BlocksStorageFlags(), map[string]string{
		"-blocks-storage.tsdb.use-columnar-head":        "true",
		"-blocks-storage.tsdb.enable-native-histograms": "true",
		"-alertmanager.web.external-url":                "http://localhost/alertmanager",
		// No consul dependency needed for a single-instance test - in-memory
		// ring is enough (getting_started_single_process_config_test.go's own
		// config file sets this the same way; set explicitly here since this
		// test builds its flag set directly instead of from a config file).
		"-ring.store": "inmemory",
	})

	cortex := e2ecortex.NewSingleBinary("cortex", flags, "")
	require.NoError(t, s.StartAndWaitReady(cortex))

	// Wait until Cortex has updated the ring state, matching the OOO native
	// histogram integration test's own precedent for single-binary mode.
	require.NoError(t, cortex.WaitSumMetrics(e2e.Equals(float64(512)), "cortex_ring_tokens_total"))

	c, err := e2ecortex.NewClient(cortex.HTTPEndpoint(), cortex.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	now := time.Now()

	// Plain float series, real K8s-shaped target labels plus one extra label -
	// exactly the shape this prototype's whole design targets.
	floatSeries, expectedFloatVector := e2e.GenerateSeries("test_float_series", now,
		prompb.Label{Name: "cluster", Value: "eks-prod-1"},
		prompb.Label{Name: "namespace", Value: "ns-7"},
		prompb.Label{Name: "pod", Value: "payments-api-1"},
		prompb.Label{Name: "job", Value: "cadvisor"},
	)
	res, err := c.Push(floatSeries)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	// Native histogram series - exercises HistogramStore end to end, not just
	// SeriesStore.
	histIdx := uint32(42)
	histSeries := e2e.GenerateHistogramSeries("test_histogram_series", now, histIdx, false,
		prompb.Label{Name: "cluster", Value: "eks-prod-1"},
		prompb.Label{Name: "namespace", Value: "ns-7"},
		prompb.Label{Name: "pod", Value: "payments-api-1"},
		prompb.Label{Name: "job", Value: "cadvisor"},
	)
	res, err = c.Push(histSeries)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	// Query the float series back through the real HTTP query API.
	result, err := c.Query("test_float_series", now)
	require.NoError(t, err)
	require.Equal(t, model.ValVector, result.Type())
	require.Equal(t, expectedFloatVector, result.(model.Vector))

	// Query the histogram series back - confirms the round trip through real
	// HTTP remote-write -> columnarhead ingestion -> real query-API histogram
	// JSON encoding, not just that the push itself succeeded.
	histResult, err := c.Query("test_histogram_series", now)
	require.NoError(t, err)
	require.Equal(t, model.ValVector, histResult.Type())
	histVector := histResult.(model.Vector)
	require.Len(t, histVector, 1)
	require.NotNil(t, histVector[0].Histogram, "expected a native histogram value back, got a plain float sample")
	require.Equal(t, model.LabelValue("test_histogram_series"), histVector[0].Metric[model.MetricNameLabel])
}
