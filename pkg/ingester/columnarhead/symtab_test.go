package columnarhead

import (
	"fmt"
	"testing"
)

func TestSymbolTableRoundTrip(t *testing.T) {
	strs := []string{"cluster", "namespace", "pod", "cluster", "container", "namespace", "job", "node"}
	st, err := BuildSymbolTable(strs)
	if err != nil {
		t.Fatalf("BuildSymbolTable: %v", err)
	}
	distinct := dedupeStrings(strs)
	if st.NumSymbols() != uint32(len(distinct)) {
		t.Fatalf("NumSymbols() = %d, want %d (deduped)", st.NumSymbols(), len(distinct))
	}
	for _, s := range distinct {
		id, ok := st.Lookup(s)
		if !ok {
			t.Fatalf("Lookup(%q) = false, want true (built symbol)", s)
		}
		if got := st.String(id); got != s {
			t.Fatalf("String(Lookup(%q)) = %q, want %q", s, got, s)
		}
	}
}

func TestSymbolTableUnknownRejected(t *testing.T) {
	st, err := BuildSymbolTable([]string{"cluster", "namespace", "pod"})
	if err != nil {
		t.Fatalf("BuildSymbolTable: %v", err)
	}
	if _, ok := st.Lookup("never_built"); ok {
		t.Fatal("Lookup on a string never built into the table returned ok=true")
	}
}

// TestSymbolTableVerificationCatchesMPHFCollision demonstrates the concrete case
// TestMPHFUnknownKeyIsUnverified (mphf_test.go) warns about: probes for an unknown
// string that the BARE MPHF assigns the same slot as a real symbol, then confirms
// SymbolTable.Lookup still correctly reports ok=false - proving the verify step in
// Lookup is load-bearing, not decorative.
func TestSymbolTableVerificationCatchesMPHFCollision(t *testing.T) {
	built := []string{"cluster", "namespace", "pod", "container", "node", "job", "le"}
	st, err := BuildSymbolTable(built)
	if err != nil {
		t.Fatalf("BuildSymbolTable: %v", err)
	}
	builtSet := make(map[string]bool, len(built))
	for _, s := range built {
		builtSet[s] = true
	}

	found := false
	for i := 0; i < 100000 && !found; i++ {
		probe := fmt.Sprintf("unknown_%d", i)
		if builtSet[probe] {
			continue
		}
		slot := st.mphf.Lookup(probe) // bypasses verification deliberately, test-only
		for _, s := range built {
			if st.mphf.Lookup(s) != slot {
				continue
			}
			// Bare MPHF collided with a real symbol's slot; the verified path must
			// still reject it.
			if _, ok := st.Lookup(probe); ok {
				t.Fatalf("SymbolTable.Lookup(%q) = true despite colliding with %q at "+
					"the MPHF layer - verification did not catch it", probe, s)
			}
			found = true
			break
		}
	}
	if !found {
		t.Skip("no bare-MPHF collision found in 100000 probes at this table size - " +
			"statistically expected occasionally, not a correctness failure")
	}
}

func TestSymbolTableEmpty(t *testing.T) {
	st, err := BuildSymbolTable(nil)
	if err != nil {
		t.Fatalf("BuildSymbolTable(nil): %v", err)
	}
	if st.NumSymbols() != 0 {
		t.Fatalf("NumSymbols() = %d, want 0", st.NumSymbols())
	}
	if _, ok := st.Lookup("anything"); ok {
		t.Fatal("Lookup on an empty table returned ok=true")
	}
}

// TestSymbolTableAtScale measures real distinct-symbol count and total size for a
// k8s-shaped label corpus consistent with the workload used throughout CHECKLIST.md
// (cluster/namespace/pod/container/node/job values plus metric names), not a synthetic
// string set.
func TestSymbolTableAtScale(t *testing.T) {
	if testing.Short() {
		t.Skip("large symbol table build; skipped in -short")
	}
	const (
		numPods    = 25_000 // 5M series / 200 per target, matching the design doc's workload
		numMetrics = 400
	)
	var strs []string
	for i := 0; i < numPods; i++ {
		strs = append(strs,
			fmt.Sprintf("payments-api-7d9f8b6c4-%06x", i),
			"eks-prod-1", "ns-7", "app", "cadvisor", "ip-10-1-2-3.ec2.internal", // repeat every iteration, deduped
		)
	}
	for i := 0; i < numMetrics; i++ {
		strs = append(strs, fmt.Sprintf("container_metric_name_number_%03d_total", i))
	}
	strs = append(strs, "0.005", "0.01", "0.025", "0.05", "0.1", "0.25", "0.5", "1", "2.5", "5", "10", "+Inf")

	st, err := BuildSymbolTable(strs)
	if err != nil {
		t.Fatalf("BuildSymbolTable: %v", err)
	}
	t.Logf("input: %d strings (with repeats), %d distinct symbols", len(strs), st.NumSymbols())
	t.Logf("size: %d bytes total (%.1f B/symbol)", st.SizeBytes(), float64(st.SizeBytes())/float64(st.NumSymbols()))

	for _, s := range dedupeStrings(strs) {
		id, ok := st.Lookup(s)
		if !ok || st.String(id) != s {
			t.Fatalf("round-trip failed for %q", s)
		}
	}
}
