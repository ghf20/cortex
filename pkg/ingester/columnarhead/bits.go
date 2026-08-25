// Package columnarhead implements a pointerless, columnar alternative to
// prometheus/prometheus/tsdb.Head, described in columnar-head-design.md.
package columnarhead

// writeBits writes the low n bits of u (MSB-first) into arena, starting at bit offset
// off within the byte-slot starting at byte base. Returns the new offset (off+n). Does
// not bounds-check; callers must reserve room first.
func writeBits(arena []byte, base, off uint32, u uint64, n uint32) uint32 {
	for n > 0 {
		byteIdx := base + (off >> 3)
		bitIdx := off & 7
		space := 8 - bitIdx
		take := n
		if take > space {
			take = space
		}
		v := byte((u >> (n - take)) & ((1 << take) - 1))
		arena[byteIdx] |= v << (space - take)
		off += take
		n -= take
	}
	return off
}

// readBits reads n bits (MSB-first) from arena at bit offset off within the byte-slot
// starting at byte base, mirroring writeBits. Returns the value and the new offset.
func readBits(arena []byte, base, off, n uint32) (uint64, uint32) {
	var u uint64
	for n > 0 {
		byteIdx := base + (off >> 3)
		bitIdx := off & 7
		space := 8 - bitIdx
		take := n
		if take > space {
			take = space
		}
		shift := space - take
		v := (arena[byteIdx] >> shift) & ((1 << take) - 1)
		u = (u << take) | uint64(v)
		off += take
		n -= take
	}
	return u, off
}
