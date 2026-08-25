package columnarhead

import (
	"testing"
	"time"
)

// TestMPHFAtScale measures construction time and real bits/key at the same 500k-key
// scale used throughout CHECKLIST.md - correcting the Phase 0 fix (bench/00_baseline),
// which only ever measured a byte-size PLACEHOLDER (make([]byte, n*3/8+1), no hash
// function, no construction, no lookup) and used that to PROJECT a 10.06x memory ratio.
// This is the first time construction time is measured at all - flagged as an omission
// in Phase 0's own review notes ("neither MPHF construction time exercised... not free
// and isn't mentioned in the effort estimate").
func TestMPHFAtScale(t *testing.T) {
	if testing.Short() {
		t.Skip("500k-key MPHF construction; skipped in -short")
	}
	const n = 500_000
	keys := genKeys(n, 42)

	t0 := time.Now()
	m, err := BuildMPHF(keys)
	buildTime := time.Since(t0)
	if err != nil {
		t.Fatalf("BuildMPHF: %v", err)
	}

	bitsPerKey := float64(m.SizeBytes()) * 8 / n
	t.Logf("construction: %v for %d keys (%.2f us/key)", buildTime, n, float64(buildTime.Microseconds())/n)
	t.Logf("size: %d bytes total, %.2f bits/key (dispWidth=%d, numBuckets=%d, lambda=%d)",
		m.SizeBytes(), bitsPerKey, m.dispWidth, m.numBuckets, bucketLambda)
	t.Logf("comparison: bench/00_baseline's placeholder assumed 3 bits/key without building "+
		"anything; this MPHF's real measured figure is %.2f bits/key", bitsPerKey)

	// Loose bound: catches a construction regression (e.g. lambda/width blowing up)
	// without being brittle to the exact bit count, which depends on hash luck.
	if bitsPerKey > 16 {
		t.Errorf("bits/key = %.2f, expected single digits for lambda=%d - construction may "+
			"have degraded (unusually large displacement values)", bitsPerKey, bucketLambda)
	}

	// Bijection re-check at full scale, not just the 5000-key correctness test - cheap
	// insurance that scale itself doesn't surface something the smaller tests missed.
	seenSlots := make([]bool, n)
	for _, k := range keys {
		slot := m.Lookup(k)
		if seenSlots[slot] {
			t.Fatalf("collision at scale: slot %d produced by more than one key", slot)
		}
		seenSlots[slot] = true
	}
}

// TestMPHFLookupLatency compares real MPHF.Lookup latency against a Go map lookup at
// the same 500k-key scale, using go test -bench for statistically meaningful numbers -
// flagged in CHECKLIST.md as worth measuring, not just heap size ("plausibly a
// cold-path throughput win").
func BenchmarkMPHFLookup(b *testing.B) {
	const n = 500_000
	keys := genKeys(n, 7)
	m, err := BuildMPHF(keys)
	if err != nil {
		b.Fatalf("BuildMPHF: %v", err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m.Lookup(keys[i%n])
	}
}

func BenchmarkGoMapLookup(b *testing.B) {
	const n = 500_000
	keys := genKeys(n, 7)
	mp := make(map[string]uint32, n)
	for i, k := range keys {
		mp[k] = uint32(i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = mp[keys[i%n]]
	}
}
