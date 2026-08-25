package columnarhead

import "testing"

func TestTargetStoreRoundTrip(t *testing.T) {
	ts := NewTargetStore(2)
	a := ts.Create([targetFields]uint32{1, 2, 3, 4, 5, 6})
	b := ts.Create([targetFields]uint32{10, 20, 30, 40, 50, 60})

	if got := ts.Get(a); got != [targetFields]uint32{1, 2, 3, 4, 5, 6} {
		t.Fatalf("Get(a) = %v, want {1,2,3,4,5,6}", got)
	}
	if got := ts.Get(b); got != [targetFields]uint32{10, 20, 30, 40, 50, 60} {
		t.Fatalf("Get(b) = %v, want {10,20,30,40,50,60}", got)
	}
	if ts.NumTargets() != 2 {
		t.Fatalf("NumTargets() = %d, want 2", ts.NumTargets())
	}
}
