package columnarhead

import (
	"math/rand"
	"testing"
)

func TestBitsRoundTrip(t *testing.T) {
	// 200 writes up to 64 bits each; size generously so the test can't overflow on its own.
	arena := make([]byte, 2048)
	var off uint32
	type write struct {
		val uint64
		n   uint32
	}
	rng := rand.New(rand.NewSource(1))
	var writes []write
	for i := 0; i < 200; i++ {
		n := uint32(1 + rng.Intn(64))
		var val uint64
		if n == 64 {
			val = rng.Uint64()
		} else {
			val = rng.Uint64() & ((1 << n) - 1)
		}
		writes = append(writes, write{val, n})
	}

	for _, w := range writes {
		off = writeBits(arena, 0, off, w.val, w.n)
	}

	off = 0
	for i, w := range writes {
		got, newOff := readBits(arena, 0, off, w.n)
		if got != w.val {
			t.Fatalf("write %d: readBits(n=%d) = %d, want %d", i, w.n, got, w.val)
		}
		off = newOff
	}
}

func TestBitsIsolatedAcrossSlots(t *testing.T) {
	const slotBytes = 8
	arena := make([]byte, slotBytes*2)
	writeBits(arena, 0, 0, 0xFFFFFFFFFFFFFFFF, 64)
	writeBits(arena, slotBytes, 0, 0, 64)

	v0, _ := readBits(arena, 0, 0, 64)
	v1, _ := readBits(arena, slotBytes, 0, 64)
	if v0 != 0xFFFFFFFFFFFFFFFF {
		t.Fatalf("slot 0 corrupted: got %x", v0)
	}
	if v1 != 0 {
		t.Fatalf("slot 1 corrupted: got %x", v1)
	}
}
