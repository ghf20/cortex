package columnarhead

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestFixedWidthRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	for _, width := range []uint32{1, 2, 5, 8, 9, 16, 17, 24, 31, 32} {
		t.Run(fmt.Sprintf("width_%d", width), func(t *testing.T) {
			n := 500
			vals := make([]uint32, n)
			max := uint64(1)<<width - 1
			for i := range vals {
				vals[i] = uint32(rng.Uint64() & max)
			}
			buf := packFixedWidth(vals, width)
			for i, want := range vals {
				got := unpackFixedWidth(buf, uint32(i), width)
				if got != want {
					t.Fatalf("width=%d, index=%d: got %d, want %d", width, i, got, want)
				}
			}
		})
	}
}

func TestFixedWidthEmpty(t *testing.T) {
	buf := packFixedWidth(nil, 5)
	if len(buf) < 8 {
		t.Fatalf("expected at least 8 bytes of padding even for empty input, got %d", len(buf))
	}
}
