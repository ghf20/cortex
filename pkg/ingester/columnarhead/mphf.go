package columnarhead

import (
	"errors"
	"hash/maphash"
	"math/bits"
	"sort"
)

// ErrDuplicateKey is returned by BuildMPHF if the input contains a repeated key. Two
// byte-identical keys hash identically under every seed and every displacement, so a
// bucket containing both can never be resolved - checked upfront so construction fails
// immediately with a clear cause instead of exhausting every displacement/seed attempt
// first.
var ErrDuplicateKey = errors.New("columnarhead: duplicate key in MPHF input")

// errConstructionFailed is returned internally by tryBuild and retried with a new seed;
// BuildMPHF only surfaces it after exhausting maxSeedAttempts.
var errConstructionFailed = errors.New("columnarhead: MPHF construction did not converge")

// MPHF is a static minimal perfect hash function over a fixed key set, built via CHD
// (compress, hash, displace; Belazzougui, Botelho, Dietzfelbinger 2009). Lookup is O(1)
// and, for a key in the built set, returns a unique value in [0, n) - a bijection, no
// two keys share a slot.
//
// An MPHF has no way to detect an unknown key: Lookup on a key outside the built set
// returns AN ARBITRARY slot in [0, n), not an error - it may even coincide with a real
// key's slot. Callers MUST verify the returned slot against the real record before
// trusting it (design doc §3.3). This is not optional; see TestMPHFUnknownKeyIsUnverified.
//
// MPHF is static: there is no Insert. Rebuilding for a changed key set means calling
// BuildMPHF again from scratch (§3.3's "rebuild at head compaction, small dynamic
// overlay for series created since" - the overlay isn't implemented here).
//
// Construction targets numSlots = ceil(n * slotSlack) displacement slots, not exactly n:
// a first version that tried to place n keys into exactly n slots reliably failed to
// converge past a few thousand keys (verified - see git history), because the very last
// buckets placed have almost no free slots left to land in and their success
// probability collapses. Building into slightly more slots than keys, then recovering
// minimality via a rank/select structure over the occupied-slot bitmap (bit i set iff
// slot i holds a key), is the standard CHD fix and is what makes construction actually
// converge at the sizes this package is meant to run at.
type MPHF struct {
	seed       maphash.Seed
	numKeys    uint32
	numSlots   uint32 // > numKeys; construction target before rank/select recovers minimality
	numBuckets uint32
	dispWidth  uint32 // bits per bucket displacement value
	disp       []byte // bit-packed displacement values, numBuckets*dispWidth bits total

	occupied  []uint64 // bitmap over [0, numSlots): bit i set iff a key landed on slot i
	rankBlock []uint32 // cumulative popcount before each rankBlockWords-word block
}

// bucketLambda is the target average number of keys per bucket. CHD's construction
// cost and the resulting displacement bit-width both grow with bucket size, so this is
// the main construction-time/space tradeoff knob; 4 is the paper's suggested default.
const bucketLambda = 4

// slotSlack is how much larger the construction slot space is than the key count
// (numSlots = ceil(n * slotSlack)). 1.23 is the CHD paper's recommended default,
// balancing construction success probability against the rank/select structure's
// memory overhead - which is real and paid in the final size, not free slack.
const slotSlack = 1.23

// rankBlockWords is the granularity of the stored rank prefix-sum array: one uint32
// covers rankBlockWords 64-bit words. Larger blocks mean less stored overhead but more
// popcount work per Lookup (bounded, still O(1) - just a larger constant).
const rankBlockWords = 8

// maxDisplacementAttempts bounds how many displacement values one bucket tries before
// giving up on the current seed - generous but finite, so a pathological key set fails
// fast rather than spinning forever.
const maxDisplacementAttempts = 1 << 16

// maxSeedAttempts bounds how many global seeds BuildMPHF tries before giving up
// entirely. A single bucket repeatedly failing to converge under multiple independent
// seeds indicates something structural (e.g. undetected duplicates), not bad luck.
const maxSeedAttempts = 32

// BuildMPHF builds a minimal perfect hash function over keys. keys must be distinct;
// duplicates are detected upfront and reported as ErrDuplicateKey rather than causing
// construction to fail slowly and confusingly deep inside the algorithm.
func BuildMPHF(keys []string) (*MPHF, error) {
	n := uint32(len(keys))
	if n == 0 {
		return &MPHF{}, nil
	}
	if err := checkDuplicates(keys); err != nil {
		return nil, err
	}

	numBuckets := (n + bucketLambda - 1) / bucketLambda
	if numBuckets == 0 {
		numBuckets = 1
	}
	numSlots := uint32(float64(n) * slotSlack)
	if numSlots <= n {
		numSlots = n + 1
	}

	var lastErr error
	for attempt := 0; attempt < maxSeedAttempts; attempt++ {
		seed := maphash.MakeSeed()
		disp, occupied, err := tryBuild(keys, seed, n, numSlots, numBuckets)
		if err == nil {
			width := dispWidthFor(disp)
			rankBlock := buildRankBlocks(occupied)
			return &MPHF{
				seed:       seed,
				numKeys:    n,
				numSlots:   numSlots,
				numBuckets: numBuckets,
				dispWidth:  width,
				disp:       packDisplacements(disp, width),
				occupied:   occupied,
				rankBlock:  rankBlock,
			}, nil
		}
		lastErr = err
	}
	return nil, lastErr
}

func checkDuplicates(keys []string) error {
	seen := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		if _, ok := seen[k]; ok {
			return ErrDuplicateKey
		}
		seen[k] = struct{}{}
	}
	return nil
}

// tryBuild attempts one full CHD construction under the given seed, placing n keys
// into numSlots > n slots. Returns the per-bucket displacement values and the
// resulting occupied-slot bitmap (len(occupied)*64 >= numSlots bits) on success.
func tryBuild(keys []string, seed maphash.Seed, n, numSlots, numBuckets uint32) ([]uint32, []uint64, error) {
	hashes := make([]uint64, len(keys))
	bucketOf := make([]uint32, len(keys))
	bucketSize := make([]uint32, numBuckets)
	for i, k := range keys {
		h := maphash.String(seed, k)
		hashes[i] = h
		b := uint32(h % uint64(numBuckets))
		bucketOf[i] = b
		bucketSize[b]++
	}

	bucketStart := make([]uint32, numBuckets+1)
	for b := uint32(0); b < numBuckets; b++ {
		bucketStart[b+1] = bucketStart[b] + bucketSize[b]
	}
	members := make([]uint32, len(keys))
	cursor := append([]uint32(nil), bucketStart[:numBuckets]...)
	for i := range keys {
		b := bucketOf[i]
		members[cursor[b]] = uint32(i)
		cursor[b]++
	}

	order := make([]uint32, numBuckets)
	for b := range order {
		order[b] = uint32(b)
	}
	sort.Slice(order, func(a, c int) bool {
		return bucketSize[order[a]] > bucketSize[order[c]]
	})

	occupied := make([]uint64, (numSlots+63)/64)
	disp := make([]uint32, numBuckets)
	slotsBuf := make([]uint32, 0, 64)

	isSet := func(s uint32) bool { return occupied[s/64]&(1<<(s%64)) != 0 }
	set := func(s uint32) { occupied[s/64] |= 1 << (s % 64) }

	for _, b := range order {
		start, end := bucketStart[b], bucketStart[b+1]
		if start == end {
			continue
		}
		found := false
		for d := uint32(0); d < maxDisplacementAttempts; d++ {
			slotsBuf = slotsBuf[:0]
			ok := true
			for _, idx := range members[start:end] {
				s := mixSlot(hashes[idx], d, numSlots)
				if isSet(s) {
					ok = false
					break
				}
				dup := false
				for _, prior := range slotsBuf {
					if prior == s {
						dup = true
						break
					}
				}
				if dup {
					ok = false
					break
				}
				slotsBuf = append(slotsBuf, s)
			}
			if ok {
				for _, s := range slotsBuf {
					set(s)
				}
				disp[b] = d
				found = true
				break
			}
		}
		if !found {
			return nil, nil, errConstructionFailed
		}
	}
	return disp, occupied, nil
}

// mixSlot derives a slot in [0, n) from a key's base hash h and a bucket's displacement
// d. Different d values behave like independent hash functions of h, which is exactly
// what CHD's per-bucket displacement search needs.
func mixSlot(h uint64, d uint32, n uint32) uint32 {
	x := h ^ (uint64(d) * 0x9E3779B97F4A7C15)
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	x *= 0xc4ceb9fe1a85ec53
	x ^= x >> 33
	return uint32(x % uint64(n))
}

func dispWidthFor(disp []uint32) uint32 {
	var max uint32
	for _, d := range disp {
		if d > max {
			max = d
		}
	}
	w := uint32(bits.Len32(max))
	if w == 0 {
		w = 1
	}
	return w
}

func packDisplacements(disp []uint32, width uint32) []byte {
	totalBits := uint32(len(disp)) * width
	packed := make([]byte, (totalBits+7)/8)
	var off uint32
	for _, d := range disp {
		off = writeBits(packed, 0, off, uint64(d), width)
	}
	return packed
}

// buildRankBlocks precomputes, for every rankBlockWords-word block of occupied, the
// cumulative popcount of all bits strictly before that block - the coarse level of the
// two-level rank structure Lookup uses to turn a raw (non-minimal) slot into a minimal
// one in O(1).
func buildRankBlocks(occupied []uint64) []uint32 {
	numBlocks := (len(occupied) + rankBlockWords - 1) / rankBlockWords
	rankBlock := make([]uint32, numBlocks+1)
	var count uint32
	for blk := 0; blk < numBlocks; blk++ {
		rankBlock[blk] = count
		end := (blk + 1) * rankBlockWords
		if end > len(occupied) {
			end = len(occupied)
		}
		for w := blk * rankBlockWords; w < end; w++ {
			count += uint32(bits.OnesCount64(occupied[w]))
		}
	}
	rankBlock[numBlocks] = count
	return rankBlock
}

// rank returns the number of set bits in occupied strictly before position pos - i.e.
// the 0-indexed rank of pos among the set bits, which is exactly the minimal slot a raw
// CHD slot maps to (see the MPHF type comment). O(1) up to a rankBlockWords-sized
// constant number of extra word popcounts.
func (m *MPHF) rank(pos uint32) uint32 {
	word := pos / 64
	blk := word / rankBlockWords
	count := m.rankBlock[blk]
	for w := uint32(blk) * rankBlockWords; w < word; w++ {
		count += uint32(bits.OnesCount64(m.occupied[w]))
	}
	if bitInWord := pos % 64; bitInWord > 0 {
		mask := uint64(1)<<bitInWord - 1
		count += uint32(bits.OnesCount64(m.occupied[word] & mask))
	}
	return count
}

// NumKeys returns the number of keys the MPHF was built over.
func (m *MPHF) NumKeys() uint32 {
	return m.numKeys
}

// Lookup returns key's slot in [0, m.NumKeys()). For a key not in the built set, the
// result is an arbitrary value in that range - not an error, not necessarily distinct
// from a real key's slot. See the MPHF type comment: verification is the caller's
// responsibility.
func (m *MPHF) Lookup(key string) uint32 {
	if m.numKeys == 0 {
		return 0
	}
	h := maphash.String(m.seed, key)
	b := uint32(h % uint64(m.numBuckets))
	d := m.readDisp(b)
	raw := mixSlot(h, d, m.numSlots)
	return m.rank(raw)
}

func (m *MPHF) readDisp(bucket uint32) uint32 {
	off := bucket * m.dispWidth
	v, _ := readBits(m.disp, 0, off, m.dispWidth)
	return uint32(v)
}

// SizeBytes returns the MPHF's own memory footprint: the packed displacement array,
// the occupied-slot bitmap, and the rank prefix-sum array - everything Lookup needs.
// Does not include the keys, which an MPHF never stores.
func (m *MPHF) SizeBytes() int {
	return len(m.disp) + len(m.occupied)*8 + len(m.rankBlock)*4
}
