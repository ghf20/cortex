package columnarhead

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
)

// benchHeadAtScale builds the same 500k-series, k8s-shaped head used throughout
// CHECKLIST.md (25,000 pods x 200 series/pod, 400 distinct metric names), for
// measuring Select's real cost - both the postings-accelerated path and the full-scan
// fallback, so the design doc's §3.4 postings claim (originally "4.79 ms linear scan...
// __name__ postings only: 20 MB") is checked against this implementation's own real
// numbers instead of staying an unverified, decade-old-feeling citation.
func benchHeadAtScale(b *testing.B) *Head {
	b.Helper()
	const (
		n            = 500_000
		seriesPerTgt = 200
		numMetrics   = 400
	)
	h := NewHead(n, n/seriesPerTgt, numMetrics+16)
	for i := 0; i < n; i++ {
		tgt := TargetLabels{
			Cluster:   "eks-prod-1",
			Namespace: "ns-7",
			Pod:       fmt.Sprintf("payments-api-7d9f8b6c4-%06x", i/seriesPerTgt),
			Container: "app",
			Node:      "ip-10-1-2-3.ec2.internal",
			Job:       "cadvisor",
		}
		metric := fmt.Sprintf("container_metric_name_number_%03d_total", i%numMetrics)
		if _, err := h.GetOrCreateSeries(tgt, metric); err != nil {
			b.Fatalf("series %d: %v", i, err)
		}
	}
	return h
}

// BenchmarkSelectWithNamePostings measures Select's real cost for the accelerated
// path: an exact __name__ matcher, hitting design doc §3.4's postings shortcut. At
// 500k series / 400 names, each name's postings list averages ~1,250 series.
func BenchmarkSelectWithNamePostings(b *testing.B) {
	h := benchHeadAtScale(b)
	q, err := h.Querier(math.MinInt64, math.MaxInt64)
	if err != nil {
		b.Fatalf("Querier: %v", err)
	}
	defer q.Close()
	m := labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "container_metric_name_number_042_total")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ss := q.Select(context.Background(), false, nil, m)
		for ss.Next() {
			_ = ss.At().Labels()
		}
	}
}

// BenchmarkSelectFullScan measures Select's cost for the fallback path: a regex
// __name__ matcher that can't use postings, forcing a scan over all 500k series -
// the "before" baseline the postings shortcut above is meant to improve on.
func BenchmarkSelectFullScan(b *testing.B) {
	h := benchHeadAtScale(b)
	q, err := h.Querier(math.MinInt64, math.MaxInt64)
	if err != nil {
		b.Fatalf("Querier: %v", err)
	}
	defer q.Close()
	m := labels.MustNewMatcher(labels.MatchRegexp, labels.MetricName, "container_metric_name_number_042_total")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ss := q.Select(context.Background(), false, nil, m)
		for ss.Next() {
			_ = ss.At().Labels()
		}
	}
}
