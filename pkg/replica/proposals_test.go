package replica

import (
	"errors"
	"testing"
)

func TestProposalTracker_SupersededProposalIsReported(t *testing.T) {
	pt := NewProposalTracker()
	ch := make(chan error, 1)
	pt.waiters[7] = proposalWaiter{term: 2, result: ch}

	// A different leader's entry (term 3) committed at index 7.
	pt.Notify(7, 3, nil)
	if err := <-ch; !errors.Is(err, ErrProposalSuperseded) {
		t.Fatalf("expected ErrProposalSuperseded, got %v", err)
	}
	if _, ok := pt.waiters[7]; ok {
		t.Fatalf("waiter not removed after Notify")
	}
}
