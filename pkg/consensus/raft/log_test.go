package raft

import "testing"

// TestRaftLog_TermAt_HugeIndexReturnsFalseNotPanic is a regression test for a
// uint64->int conversion overflow in toSliceIndex: when raftIndex is a huge
// value (e.g. a caller computed nextIndex-1 with nextIndex==0, underflowing
// to near math.MaxUint64), raftIndex-firstIndex is itself a huge uint64.
// Converting that straight to int used to wrap around to a negative number,
// slipping past the `idx >= len(l.entries)` bounds check (never true for a
// negative idx) and panicking on the subsequent slice access instead of
// returning the "not found" result every other out-of-range call gets.
func TestRaftLog_TermAt_HugeIndexReturnsFalseNotPanic(t *testing.T) {
	l := NewRaftLog()
	l.Append(1, []byte("only-entry"))

	hugeIndex := ^uint64(0) // math.MaxUint64, as produced by a 0-1 underflow

	term, ok := l.TermAt(hugeIndex)
	if ok {
		t.Fatalf("expected TermAt(%d) to report not-found, got ok=true term=%d", hugeIndex, term)
	}
	if term != 0 {
		t.Fatalf("expected zero-value term on not-found, got %d", term)
	}
}

// TestRaftLog_Slice_HugeIndexReturnsNilNotPanic is the Slice-path counterpart
// to TestRaftLog_TermAt_HugeIndexReturnsFalseNotPanic - Slice shares the same
// toSliceIndex helper and so shares the same overflow exposure.
func TestRaftLog_Slice_HugeIndexReturnsNilNotPanic(t *testing.T) {
	l := NewRaftLog()
	l.Append(1, []byte("only-entry"))

	hugeIndex := ^uint64(0)

	res := l.Slice(hugeIndex)
	if res != nil {
		t.Fatalf("expected Slice(%d) to return nil for an out-of-range index, got %v", hugeIndex, res)
	}
}
