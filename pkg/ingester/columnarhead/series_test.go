package columnarhead

import (
	"math"
	"math/rand"
	"testing"
)

type sample struct {
	ts int64
	v  float64
}

func decodeAll(t *testing.T, s *SeriesStore, ref uint32) []sample {
	t.Helper()
	it := s.Iterator(ref)
	var got []sample
	for it.Next() {
		ts, v := it.At()
		got = append(got, sample{ts, v})
	}
	return got
}

func assertSamplesEqual(t *testing.T, got, want []sample) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d samples, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].ts != want[i].ts {
			t.Fatalf("sample %d: ts = %d, want %d", i, got[i].ts, want[i].ts)
		}
		gv, wv := got[i].v, want[i].v
		if math.Float64bits(gv) != math.Float64bits(wv) {
			t.Fatalf("sample %d: value = %v (%x), want %v (%x)", i, gv, math.Float64bits(gv), wv, math.Float64bits(wv))
		}
	}
}

func TestSeriesStoreRoundTrip_Patterns(t *testing.T) {
	cases := map[string][]sample{
		"single sample": {
			{1700000000000, 1.0},
		},
		"two samples": {
			{1700000000000, 1.0},
			{1700000015000, 2.0},
		},
		"constant value (KSM-style gauge)": {
			{1700000000000, 1},
			{1700000015000, 1},
			{1700000030000, 1},
			{1700000045000, 1},
			{1700000060000, 1},
		},
		"monotonic counter": {
			{1700000000000, 1000},
			{1700000015000, 1003},
			{1700000030000, 1006},
			{1700000045000, 1009},
		},
		"noisy gauge, jittered scrape": {
			{1700000000000, 1e6},
			{1700000015023, 1e6 + 40.1},
			{1700000029987, 1e6 + 79.3},
			{1700000045011, 1e6 + 121.9},
		},
		"large jump requiring new window": {
			{1700000000000, 1},
			{1700000015000, 1 << 20},
			{1700000030000, 3},
			{1700000045000, 1 << 40},
		},
	}
	for name, want := range cases {
		t.Run(name, func(t *testing.T) {
			s := NewSeriesStore(1)
			ref := s.Create(0, 0, nil)
			for _, sm := range want {
				if err := s.Append(ref, sm.ts, sm.v); err != nil {
					t.Fatalf("Append: %v", err)
				}
			}
			assertSamplesEqual(t, decodeAll(t, s, ref), want)
		})
	}
}

func TestSeriesStoreRoundTrip_Random(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	s := NewSeriesStore(1)
	ref := s.Create(0, 0, nil)

	var want []sample
	ts := int64(1700000000000)
	v := 0.0
	for i := 0; i < 50; i++ {
		ts += 15000 + int64(rng.Intn(2000)-1000) // jittered ~15s
		switch rng.Intn(3) {
		case 0:
			// unchanged
		case 1:
			v += rng.Float64() * 10
		case 2:
			v = rng.Float64() * 1e9
		}
		want = append(want, sample{ts, v})
		if err := s.Append(ref, ts, v); err != nil {
			t.Fatalf("Append at %d: %v", i, err)
		}
	}
	assertSamplesEqual(t, decodeAll(t, s, ref), want)
}

func TestSeriesStoreIsolatesSeries(t *testing.T) {
	s := NewSeriesStore(3)
	refA := s.Create(0, 0, nil)
	refB := s.Create(1, 1, nil)
	refC := s.Create(2, 2, nil)

	wantA := []sample{{1700000000000, 1}, {1700000015000, 2}}
	wantB := []sample{{1700000000000, 100}, {1700000015000, 100}, {1700000030000, 100}}
	wantC := []sample{{1700000000000, -5.5}}

	for _, sm := range wantA {
		if err := s.Append(refA, sm.ts, sm.v); err != nil {
			t.Fatal(err)
		}
	}
	for _, sm := range wantB {
		if err := s.Append(refB, sm.ts, sm.v); err != nil {
			t.Fatal(err)
		}
	}
	for _, sm := range wantC {
		if err := s.Append(refC, sm.ts, sm.v); err != nil {
			t.Fatal(err)
		}
	}

	assertSamplesEqual(t, decodeAll(t, s, refA), wantA)
	assertSamplesEqual(t, decodeAll(t, s, refB), wantB)
	assertSamplesEqual(t, decodeAll(t, s, refC), wantC)
}

// TestSeriesStoreGrowsAcrossManySamples appends far more samples than the 16-byte
// initial slot can hold, forcing many grow events (each copies the series' bits to a
// fresh arena region). This is the case that would surface a copy-offset bug or a
// growth event corrupting a neighboring series' still-live slot.
func TestSeriesStoreGrowsAcrossManySamples(t *testing.T) {
	s := NewSeriesStore(2)
	ref := s.Create(0, 0, nil)
	other := s.Create(1, 1, nil) // must survive ref's many grow events untouched

	var want, wantOther []sample
	ts := int64(1700000000000)
	otherTS := int64(1700000000000)
	for i := 0; i < 1000; i++ {
		ts += 15000
		v := float64(i) * 1.7 // irregular deltas, avoids the cheap 1-bit "unchanged" path
		if err := s.Append(ref, ts, v); err != nil {
			t.Fatalf("Append at sample %d: %v", i, err)
		}
		want = append(want, sample{ts, v})

		if i%10 == 0 {
			otherTS += 15000
			if err := s.Append(other, otherTS, 42); err != nil {
				t.Fatalf("Append(other) at sample %d: %v", i, err)
			}
			wantOther = append(wantOther, sample{otherTS, 42})
		}
	}
	if s.slotCap[ref] <= initialSlotBytes {
		t.Fatalf("slotCap[ref] = %d, expected growth past the %d-byte initial slot", s.slotCap[ref], initialSlotBytes)
	}
	assertSamplesEqual(t, decodeAll(t, s, ref), want)
	assertSamplesEqual(t, decodeAll(t, s, other), wantOther)
}

// TestSeriesStoreFreeListReuseIsZeroed specifically targets the free-list's most
// dangerous failure mode: writeBits ORs new bits into arena, so if alloc() ever handed
// back a reused region without zeroing it first, stale 1-bits left over from the
// previous occupant would silently corrupt the new series' encoding instead of failing
// loudly. Forces reuse by growing series until their initial 16B slots are freed, then
// creating a fresh series and confirming its very first (all-ones-adjacent) sample
// round-trips exactly.
func TestSeriesStoreFreeListReuseIsZeroed(t *testing.T) {
	s := NewSeriesStore(4)

	// Fill several series' initial slots with maximally "dirty" bit patterns (large
	// XOR deltas via alternating extreme values), then force each to grow, freeing its
	// 16B initial slot back to the free list.
	var refs []uint32
	for i := 0; i < 4; i++ {
		ref := s.Create(0, 0, nil)
		refs = append(refs, ref)
		vals := []float64{math.MaxFloat64, -math.MaxFloat64, math.SmallestNonzeroFloat64}
		ts := int64(1700000000000)
		for _, v := range vals {
			ts += 15000
			if err := s.Append(ref, ts, v); err != nil {
				t.Fatalf("series %d: %v", i, err)
			}
		}
	}
	if len(s.freeList[initialSlotBytes]) == 0 {
		t.Fatal("expected at least one 16B region freed by growth - test setup didn't force growth")
	}

	// A fresh series should now be handed one of those freed, dirty regions.
	newRef := s.Create(9, 9, nil)
	want := []sample{{1700000000000, 0}} // value 0 => raw bits all zero; any leftover 1-bit corrupts this
	if err := s.Append(newRef, want[0].ts, want[0].v); err != nil {
		t.Fatal(err)
	}
	assertSamplesEqual(t, decodeAll(t, s, newRef), want)

	// The older series must also still decode correctly - reuse must not have
	// clobbered a still-live region.
	for i, ref := range refs {
		it := s.Iterator(ref)
		count := 0
		for it.Next() {
			count++
		}
		if count != 3 {
			t.Fatalf("series %d: decoded %d samples after reuse elsewhere, want 3", i, count)
		}
	}
}

// TestSeriesStoreTruncate covers the core decode/re-encode path: samples older than
// mint are dropped, samples at or after mint survive bit-for-bit, and a neighboring
// series (sharing the same underlying arena) is untouched.
func TestSeriesStoreTruncate(t *testing.T) {
	s := NewSeriesStore(2)
	ref := s.Create(0, 0, nil)
	other := s.Create(1, 1, nil)

	all := []sample{
		{1700000000000, 1.5},
		{1700000015000, 2.5},
		{1700000030000, 3.5},
		{1700000045000, 4.5},
		{1700000060000, 5.5},
	}
	for _, sm := range all {
		if err := s.Append(ref, sm.ts, sm.v); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	wantOther := []sample{{1700000000000, 42}, {1700000015000, 43}}
	for _, sm := range wantOther {
		if err := s.Append(other, sm.ts, sm.v); err != nil {
			t.Fatalf("Append(other): %v", err)
		}
	}

	n := s.Truncate(ref, 1700000030000)
	if n != 3 {
		t.Fatalf("Truncate returned %d, want 3", n)
	}
	assertSamplesEqual(t, decodeAll(t, s, ref), all[2:])
	if s.nSamples[ref] != 3 {
		t.Fatalf("nSamples[ref] = %d, want 3", s.nSamples[ref])
	}
	assertSamplesEqual(t, decodeAll(t, s, other), wantOther)
}

// TestSeriesStoreTruncateToEmpty covers mint past every existing sample: the series
// stays allocated with zero samples rather than erroring or panicking.
func TestSeriesStoreTruncateToEmpty(t *testing.T) {
	s := NewSeriesStore(1)
	ref := s.Create(0, 0, nil)
	want := []sample{{1700000000000, 1}, {1700000015000, 2}}
	for _, sm := range want {
		if err := s.Append(ref, sm.ts, sm.v); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	if n := s.Truncate(ref, 1800000000000); n != 0 {
		t.Fatalf("Truncate returned %d, want 0", n)
	}
	if got := decodeAll(t, s, ref); len(got) != 0 {
		t.Fatalf("decoded %d samples after truncate-to-empty, want 0", len(got))
	}
	if s.NumSeries() != 1 {
		t.Fatalf("NumSeries() = %d, want 1 (series must stay allocated, not removed)", s.NumSeries())
	}
}

// TestSeriesStoreTruncateThenAppendMore checks that a truncated series' encoder state
// is genuinely reset, not left pointing at stale deltas: appending after a truncate
// must encode correctly relative to the new first retained sample, not the original
// (now-dropped) first sample.
func TestSeriesStoreTruncateThenAppendMore(t *testing.T) {
	s := NewSeriesStore(1)
	ref := s.Create(0, 0, nil)
	before := []sample{
		{1700000000000, 100},
		{1700000015000, 200},
		{1700000030000, 300},
	}
	for _, sm := range before {
		if err := s.Append(ref, sm.ts, sm.v); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	s.Truncate(ref, 1700000015000) // drops the first sample only

	more := []sample{{1700000045000, 400}, {1700000060000, 500}}
	for _, sm := range more {
		if err := s.Append(ref, sm.ts, sm.v); err != nil {
			t.Fatalf("Append after Truncate: %v", err)
		}
	}
	want := append(append([]sample{}, before[1:]...), more...)
	assertSamplesEqual(t, decodeAll(t, s, ref), want)
}

func TestSeriesStoreNumSeries(t *testing.T) {
	s := NewSeriesStore(0)
	if s.NumSeries() != 0 {
		t.Fatalf("NumSeries() = %d, want 0", s.NumSeries())
	}
	s.Create(0, 0, nil)
	s.Create(0, 0, nil)
	if s.NumSeries() != 2 {
		t.Fatalf("NumSeries() = %d, want 2", s.NumSeries())
	}
}
