package columnarhead

import "testing"

func TestVarbitBoundaries(t *testing.T) {
	// One value at, just inside, and just outside each bucket edge - this is exactly
	// the kind of off-by-one that bias-encoding (vs. raw two's complement) gets wrong
	// silently if the bucket range and the bit width don't match exactly.
	deltas := []int64{
		0,
		1, -1,
		63, 64, 65, -63, -64, -65,
		255, 256, 257, -255, -256, -257,
		2047, 2048, 2049, -2047, -2048, -2049,
		1 << 40, -(1 << 40),
	}
	arena := make([]byte, 4096)
	var off uint32
	for _, d := range deltas {
		off = writeVarbit(arena, 0, off, d)
	}
	off = 0
	for _, want := range deltas {
		got, newOff := readVarbit(arena, 0, off)
		if got != want {
			t.Fatalf("readVarbit: got %d, want %d", got, want)
		}
		off = newOff
	}
}

func TestTimestampRoundTrip(t *testing.T) {
	// Irregular real-world-shaped intervals: nominal 15s scrape with jitter, plus a gap.
	ts := []int64{
		1700000000000,
		1700000015023,
		1700000029987,
		1700000045000,
		1700000045000 + 120000, // scrape gap
		1700000180010,
	}
	arena := make([]byte, 4096)
	var st tsState
	var off uint32
	for i, v := range ts {
		off = writeTimestamp(arena, 0, off, v, &st, uint32(i))
	}

	var rst tsState
	off = 0
	for i, want := range ts {
		got, newOff := readTimestamp(arena, 0, off, &rst, uint32(i))
		if got != want {
			t.Fatalf("sample %d: got ts %d, want %d", i, got, want)
		}
		off = newOff
	}
}
