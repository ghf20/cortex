package columnarhead

// tsState is the delta-of-delta encoder/decoder state for one series' timestamp stream.
type tsState struct {
	lastTS    int64
	lastDelta int64
}

// writeTimestamp encodes ts into arena at (base, off), updating st. samplesSoFar is the
// number of samples already written for this series (0 = first sample, written raw;
// 1 = second sample, written as a raw delta; 2+ = delta-of-delta).
func writeTimestamp(arena []byte, base, off uint32, ts int64, st *tsState, samplesSoFar uint32) uint32 {
	switch samplesSoFar {
	case 0:
		off = writeBits(arena, base, off, uint64(ts), 64)
		st.lastTS = ts
		return off
	case 1:
		delta := ts - st.lastTS
		off = writeVarbit(arena, base, off, delta)
		st.lastDelta = delta
		st.lastTS = ts
		return off
	default:
		delta := ts - st.lastTS
		dod := delta - st.lastDelta
		off = writeVarbit(arena, base, off, dod)
		st.lastDelta = delta
		st.lastTS = ts
		return off
	}
}

// readTimestamp decodes the next timestamp from arena at (base, off), mirroring writeTimestamp.
func readTimestamp(arena []byte, base, off uint32, st *tsState, samplesSoFar uint32) (int64, uint32) {
	switch samplesSoFar {
	case 0:
		u, newOff := readBits(arena, base, off, 64)
		st.lastTS = int64(u)
		return st.lastTS, newOff
	case 1:
		delta, newOff := readVarbit(arena, base, off)
		st.lastDelta = delta
		st.lastTS += delta
		return st.lastTS, newOff
	default:
		dod, newOff := readVarbit(arena, base, off)
		st.lastDelta += dod
		st.lastTS += st.lastDelta
		return st.lastTS, newOff
	}
}

// writeVarbit bias-encodes a signed delta into a bucketed variable width, same scheme
// as Prometheus's chunkenc timestamp encoding:
//
//	0            -> '0'                   (1 bit)
//	[-63,64]     -> '10'   + 7 bits, bias 63
//	[-255,256]   -> '110'  + 9 bits, bias 255
//	[-2047,2048] -> '1110' + 12 bits, bias 2047
//	else         -> '1111' + 64 bits, raw two's complement
func writeVarbit(arena []byte, base, off uint32, d int64) uint32 {
	switch {
	case d == 0:
		return writeBits(arena, base, off, 0, 1)
	case -63 <= d && d <= 64:
		off = writeBits(arena, base, off, 0b10, 2)
		return writeBits(arena, base, off, uint64(d+63), 7)
	case -255 <= d && d <= 256:
		off = writeBits(arena, base, off, 0b110, 3)
		return writeBits(arena, base, off, uint64(d+255), 9)
	case -2047 <= d && d <= 2048:
		off = writeBits(arena, base, off, 0b1110, 4)
		return writeBits(arena, base, off, uint64(d+2047), 12)
	default:
		off = writeBits(arena, base, off, 0b1111, 4)
		return writeBits(arena, base, off, uint64(d), 64)
	}
}

// readVarbit mirrors writeVarbit.
func readVarbit(arena []byte, base, off uint32) (int64, uint32) {
	b0, off := readBits(arena, base, off, 1)
	if b0 == 0 {
		return 0, off
	}
	b1, off := readBits(arena, base, off, 1)
	if b1 == 0 {
		v, off := readBits(arena, base, off, 7)
		return int64(v) - 63, off
	}
	b2, off := readBits(arena, base, off, 1)
	if b2 == 0 {
		v, off := readBits(arena, base, off, 9)
		return int64(v) - 255, off
	}
	b3, off := readBits(arena, base, off, 1)
	if b3 == 0 {
		v, off := readBits(arena, base, off, 12)
		return int64(v) - 2047, off
	}
	v, off := readBits(arena, base, off, 64)
	return int64(v), off
}
