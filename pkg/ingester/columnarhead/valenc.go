package columnarhead

import (
	"math"
	"math/bits"
)

// noWindow marks a value stream with no established leading/trailing-zero window yet.
const noWindow = 0xff

// valueState is the Gorilla XOR encoder/decoder state for one series' value stream.
type valueState struct {
	lastBits uint64
	leading  uint8
	trailing uint8
}

func newValueState() valueState {
	return valueState{leading: noWindow, trailing: noWindow}
}

// writeValue XOR-encodes v against st into arena at (base, off), updating st in place.
// first must be true only for a series' very first sample, which is written raw.
func writeValue(arena []byte, base, off uint32, v float64, st *valueState, first bool) uint32 {
	cur := math.Float64bits(v)
	if first {
		off = writeBits(arena, base, off, cur, 64)
		st.lastBits = cur
		return off
	}
	x := st.lastBits ^ cur
	if x == 0 {
		off = writeBits(arena, base, off, 0, 1)
		st.lastBits = cur
		return off
	}
	lead := uint32(bits.LeadingZeros64(x))
	trail := uint32(bits.TrailingZeros64(x))
	if lead > 31 {
		lead = 31
	}
	if st.leading != noWindow && lead >= uint32(st.leading) && trail >= uint32(st.trailing) {
		off = writeBits(arena, base, off, 2, 2)
		off = writeBits(arena, base, off, x>>st.trailing, 64-uint32(st.leading)-uint32(st.trailing))
	} else {
		off = writeBits(arena, base, off, 3, 2)
		off = writeBits(arena, base, off, uint64(lead), 5)
		sig := 64 - lead - trail
		off = writeBits(arena, base, off, uint64(sig-1), 6)
		off = writeBits(arena, base, off, x>>trail, sig)
		st.leading, st.trailing = uint8(lead), uint8(trail)
	}
	st.lastBits = cur
	return off
}

// readValue decodes the next value from arena at (base, off), mirroring writeValue.
func readValue(arena []byte, base, off uint32, st *valueState, first bool) (float64, uint32) {
	if first {
		bits64, newOff := readBits(arena, base, off, 64)
		st.lastBits = bits64
		return math.Float64frombits(bits64), newOff
	}
	ctrl0, off := readBits(arena, base, off, 1)
	if ctrl0 == 0 {
		return math.Float64frombits(st.lastBits), off
	}
	ctrl1, off := readBits(arena, base, off, 1)
	var x uint64
	if ctrl1 == 0 {
		n := 64 - uint32(st.leading) - uint32(st.trailing)
		sigBits, newOff := readBits(arena, base, off, n)
		x = sigBits << st.trailing
		off = newOff
	} else {
		leadBits, off1 := readBits(arena, base, off, 5)
		sigM1, off2 := readBits(arena, base, off1, 6)
		sig := uint32(sigM1) + 1
		lead := uint32(leadBits)
		trail := 64 - lead - sig
		sigBits, off3 := readBits(arena, base, off2, sig)
		x = sigBits << trail
		st.leading, st.trailing = uint8(lead), uint8(trail)
		off = off3
	}
	cur := st.lastBits ^ x
	st.lastBits = cur
	return math.Float64frombits(cur), off
}
