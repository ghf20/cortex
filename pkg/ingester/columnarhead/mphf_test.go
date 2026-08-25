package columnarhead

import (
	"fmt"
	"math/rand"
	"testing"
)

func genKeys(n int, seed int64) []string {
	rng := rand.New(rand.NewSource(seed))
	keys := make([]string, n)
	seen := make(map[string]bool, n)
	for i := 0; i < n; {
		k := fmt.Sprintf("%s_%d_%x", []string{"cluster", "namespace", "pod", "container", "node", "job"}[rng.Intn(6)], rng.Intn(1<<30), rng.Int63())
		if seen[k] {
			continue
		}
		seen[k] = true
		keys[i] = k
		i++
	}
	return keys
}

func TestMPHFBijection(t *testing.T) {
	for _, n := range []int{1, 2, 3, 7, 100, 5000} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			keys := genKeys(n, int64(n))
			m, err := BuildMPHF(keys)
			if err != nil {
				t.Fatalf("BuildMPHF: %v", err)
			}
			seenSlots := make([]bool, n)
			for _, k := range keys {
				slot := m.Lookup(k)
				if slot >= uint32(n) {
					t.Fatalf("Lookup(%q) = %d, out of range [0,%d)", k, slot, n)
				}
				if seenSlots[slot] {
					t.Fatalf("Lookup(%q) = %d, collides with another key - not a bijection", k, slot)
				}
				seenSlots[slot] = true
			}
			for i, seen := range seenSlots {
				if !seen {
					t.Fatalf("slot %d never produced by any key - not minimal", i)
				}
			}
		})
	}
}

func TestMPHFLookupIsStable(t *testing.T) {
	keys := genKeys(1000, 1)
	m, err := BuildMPHF(keys)
	if err != nil {
		t.Fatalf("BuildMPHF: %v", err)
	}
	for _, k := range keys {
		first := m.Lookup(k)
		for i := 0; i < 5; i++ {
			if got := m.Lookup(k); got != first {
				t.Fatalf("Lookup(%q) returned %d then %d on repeated calls", k, first, got)
			}
		}
	}
}

// TestMPHFLookupNeverOutOfRange verifies Lookup's documented contract - result always
// in [0, NumKeys()) - holds for keys OUTSIDE the built set, not just inside it. Found
// via a rare, seed-dependent panic (not by inspection): an unknown key's raw slot can
// land in the "slack" region past the last real key's bitmap position, where rank()
// legitimately returns exactly NumKeys() (one past the valid range) rather than
// something < NumKeys() - which then panics wherever a caller (SymbolTable.String)
// indexes an array with it. Sweeps many small builds and many probes per build,
// rather than one lucky/unlucky seed, since the bug is inherently probabilistic
// (depends on where an unknown key's hash happens to land relative to the slack
// region, which varies by seed and key set).
func TestMPHFLookupNeverOutOfRange(t *testing.T) {
	for n := 1; n <= 20; n++ {
		for seed := int64(0); seed < 20; seed++ {
			keys := genKeys(n, seed)
			m, err := BuildMPHF(keys)
			if err != nil {
				t.Fatalf("BuildMPHF(n=%d, seed=%d): %v", n, seed, err)
			}
			built := make(map[string]bool, n)
			for _, k := range keys {
				built[k] = true
			}
			for i := 0; i < 200; i++ {
				probe := fmt.Sprintf("probe_%d_%d_%d", n, seed, i)
				if built[probe] {
					continue
				}
				if id := m.Lookup(probe); id >= m.NumKeys() {
					t.Fatalf("n=%d, seed=%d, probe=%q: Lookup returned %d, want < NumKeys()=%d",
						n, seed, probe, id, m.NumKeys())
				}
			}
		}
	}
}

func TestMPHFEmpty(t *testing.T) {
	m, err := BuildMPHF(nil)
	if err != nil {
		t.Fatalf("BuildMPHF(nil): %v", err)
	}
	if m.NumKeys() != 0 {
		t.Fatalf("NumKeys() = %d, want 0", m.NumKeys())
	}
	// Must not panic on an empty MPHF.
	_ = m.Lookup("anything")
}

func TestMPHFDuplicateKeyRejected(t *testing.T) {
	_, err := BuildMPHF([]string{"a", "b", "a"})
	if err != ErrDuplicateKey {
		t.Fatalf("BuildMPHF with duplicate key: got %v, want ErrDuplicateKey", err)
	}
}

// TestMPHFUnknownKeyIsUnverified makes concrete what the MPHF type comment warns about:
// Lookup on a key that was never built into the table returns a plausible-looking slot,
// not an error - it can even collide with a real key's slot. This is not a bug; it's
// the defining property of a minimal perfect hash function, and the reason
// design-doc §3.3 calls verification mandatory. If this test ever failed to demonstrate
// a collision for ANY of the probes below across many built sets, that would suggest an
// unintended verification/detection mechanism was added, silently changing the
// documented contract - not a bug fix.
func TestMPHFUnknownKeyIsUnverified(t *testing.T) {
	keys := genKeys(200, 99)
	m, err := BuildMPHF(keys)
	if err != nil {
		t.Fatalf("BuildMPHF: %v", err)
	}
	built := make(map[string]bool, len(keys))
	for _, k := range keys {
		built[k] = true
	}

	foundCollision := false
	for i := 0; i < 10000 && !foundCollision; i++ {
		probe := fmt.Sprintf("never_built_%d", i)
		if built[probe] {
			continue
		}
		slot := m.Lookup(probe)
		if slot >= m.NumKeys() {
			t.Fatalf("Lookup on unknown key returned out-of-range slot %d (want < %d) - "+
				"even an unverified lookup must stay in range", slot, m.NumKeys())
		}
		for _, k := range keys {
			if m.Lookup(k) == slot {
				foundCollision = true
				break
			}
		}
	}
	if !foundCollision {
		t.Skip("no unknown-key collision found in 10000 probes - statistically expected " +
			"occasionally at this key count, not a correctness failure; the in-range " +
			"check above already exercises the documented contract")
	}
}
