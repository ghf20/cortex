package columnarhead

import (
	"errors"
	"math"
	"testing"

	"github.com/prometheus/prometheus/storage"
)

// TestAmendSemantics ports three real Prometheus db_test.go tests exercising
// float-path amend/duplicate detection (TestDuplicateNaNDatapointNoAmendError,
// TestNonDuplicateNaNDatapointsCausesAmendError, TestSkippingInvalidValuesInSameTxn):
// an exact-bit-pattern duplicate at the same timestamp (including NaN, which is
// never == itself) is a silent no-op, a genuinely different value is rejected, and
// a rejected "amend" must not overwrite what's already stored. All three pass
// unmodified - Head.appendable's existing math.Float64bits(v) comparison already
// covers this (including NaN correctly, via bit-pattern rather than == comparison) -
// this just adds direct regression coverage at the Head level rather than relying
// on it being incidentally exercised elsewhere.
func TestAmendSemantics(t *testing.T) {
	tgt := TargetLabels{Cluster: "c", Namespace: "n", Pod: "p", Container: "co", Node: "no", Job: "j"}

	t.Run("duplicate NaN is not an amend error", func(t *testing.T) {
		h := NewHead(1, 1, 1)
		ref, _ := h.GetOrCreateSeries(tgt, "m")
		if err := h.Append(ref, 0, math.NaN()); err != nil {
			t.Fatalf("first NaN: %v", err)
		}
		if err := h.Append(ref, 0, math.NaN()); err != nil {
			t.Fatalf("duplicate NaN (same bit pattern) = %v, want nil", err)
		}
	})

	t.Run("non-duplicate NaN (different bit pattern) is an amend error", func(t *testing.T) {
		h := NewHead(1, 1, 1)
		ref, _ := h.GetOrCreateSeries(tgt, "m")
		if err := h.Append(ref, 0, math.Float64frombits(0x7ff0000000000001)); err != nil {
			t.Fatalf("first: %v", err)
		}
		if err := h.Append(ref, 0, math.Float64frombits(0x7ff0000000000002)); !errors.Is(err, storage.ErrDuplicateSampleForTimestamp) {
			t.Fatalf("different NaN bit pattern, same ts = %v, want storage.ErrDuplicateSampleForTimestamp", err)
		}
	})

	t.Run("skipping invalid value in same txn - only first value stored", func(t *testing.T) {
		h := NewHead(1, 1, 1)
		ref, _ := h.GetOrCreateSeries(tgt, "m")
		if err := h.Append(ref, 0, 1); err != nil {
			t.Fatalf("first: %v", err)
		}
		if err := h.Append(ref, 0, 2); err == nil {
			t.Fatalf("amended value at same ts = nil, want rejection")
		}
		it := h.Iterator(ref)
		if !it.Next() {
			t.Fatal("no sample stored")
		}
		_, v := it.At()
		if v != 1 {
			t.Fatalf("stored value = %v, want 1 (the amended value must not have overwritten it)", v)
		}
		if it.Next() {
			t.Fatal("more than one sample stored")
		}
	})
}
