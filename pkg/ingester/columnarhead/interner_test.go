package columnarhead

import "testing"

func TestLiveInternerDedup(t *testing.T) {
	li := newLiveInterner(4)
	a1 := li.Intern("cluster")
	b := li.Intern("namespace")
	a2 := li.Intern("cluster")

	if a1 != a2 {
		t.Fatalf("Intern(\"cluster\") returned %d then %d - not deduplicated", a1, a2)
	}
	if a1 == b {
		t.Fatalf("distinct strings got the same id %d", a1)
	}
	if li.NumSymbols() != 2 {
		t.Fatalf("NumSymbols() = %d, want 2", li.NumSymbols())
	}
	if got := li.String(a1); got != "cluster" {
		t.Fatalf("String(a1) = %q, want \"cluster\"", got)
	}
	if got := li.String(b); got != "namespace" {
		t.Fatalf("String(b) = %q, want \"namespace\"", got)
	}
}

func TestLiveInternerEmptyString(t *testing.T) {
	li := newLiveInterner(1)
	id := li.Intern("")
	if got := li.String(id); got != "" {
		t.Fatalf("String(Intern(\"\")) = %q, want \"\"", got)
	}
	// Interning "" again must return the same id, not create a second empty entry.
	if id2 := li.Intern(""); id2 != id {
		t.Fatalf("second Intern(\"\") = %d, want %d", id2, id)
	}
}
