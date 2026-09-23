package raft

import "context"

// The apply pipeline delivers committed entries to the state machine.
//
//	commit advances ─▶ scheduleApplyLocked ─▶ applyQueue ─▶ applyLoop ─▶ applyCh ─▶ state machine
//	                                                                                     │
//	                     WaitApplied / Snapshot ◀── lastApplied ◀── ReportApplied ◀──────┘
//
// A single goroutine (applyLoop) is the only sender on applyCh, so messages
// arrive strictly in log order and a slow state machine never blocks the
// consensus path, which only appends to an in-memory queue. "Applied" means
// the state machine said so via ReportApplied - not merely that Raft queued it.

// applyItem is one queued delivery. No-ops advance lastApplied inside Raft
// instead of being shown to the state machine.
type applyItem struct {
	msg  ApplyMsg
	noop bool
}

// notifier lets goroutines wait for a condition guarded by rn.mu to change:
// waiters grab the current channel under the lock, and broadcast closes it
// (waking all of them) and installs a fresh one.
type notifier struct{ ch chan struct{} }

func newNotifier() notifier               { return notifier{ch: make(chan struct{})} }
func (n *notifier) broadcast()            { close(n.ch); n.ch = make(chan struct{}) }
func (n *notifier) wait() <-chan struct{} { return n.ch }

// scheduleApplyLocked queues every committed entry not yet dispatched.
func (rn *RaftNode) scheduleApplyLocked() {
	if rn.commitIndex <= rn.lastDispatched {
		return
	}
	entries := rn.log.SliceN(rn.lastDispatched+1, int(rn.commitIndex-rn.lastDispatched))
	items := make([]applyItem, 0, len(entries))
	for _, e := range entries {
		items = append(items, applyItem{
			msg: ApplyMsg{
				CommandValid: true,
				CommandIndex: e.Index,
				CommandTerm:  e.Term,
				Command:      e.Data,
			},
			noop: e.Type == EntryNoOp,
		})
		rn.lastDispatched = e.Index
	}
	rn.enqueueApplyLocked(items...)
}

// enqueueApplyLocked appends items in log order and wakes applyLoop.
func (rn *RaftNode) enqueueApplyLocked(items ...applyItem) {
	if len(items) == 0 {
		return
	}
	rn.applyQueue = append(rn.applyQueue, items...)
	select {
	case rn.applyNotify <- struct{}{}:
	default: // a wakeup is already pending; applyLoop will see these too
	}
}

// applyLoop drains the queue to applyCh, strictly in order.
func (rn *RaftNode) applyLoop() {
	defer rn.wg.Done()
	for {
		select {
		case <-rn.stopCh:
			return
		case <-rn.applyNotify:
		}

		rn.mu.Lock()
		items := rn.applyQueue
		rn.applyQueue = nil
		rn.mu.Unlock()

		for _, it := range items {
			if it.noop {
				// A no-op has no effect, so it is applied once everything
				// before it has been. Waiting keeps lastApplied meaning
				// "every entry up to here is reflected in the state machine".
				if rn.WaitApplied(context.Background(), it.msg.CommandIndex-1) != nil { // only fails on stop
					return
				}
				rn.ReportApplied(it.msg.CommandIndex)
				continue
			}
			select {
			case rn.applyCh <- it.msg:
			case <-rn.stopCh:
				return
			}
		}
	}
}

// ReportApplied is called by the state machine once it has finished applying
// the ApplyMsg at index (command or snapshot). Every consumer of applyCh must
// call it for every message it processes - including ones it rejects as
// malformed - or WaitApplied and Snapshot will (safely) stall at the last
// reported index. Reports at or below the current value are ignored.
func (rn *RaftNode) ReportApplied(index uint64) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	if index <= rn.lastApplied {
		return
	}
	rn.lastApplied = index
	rn.appliedSignal.broadcast()
}

// WaitApplied blocks until the state machine has reported applying at least
// index, or ctx is done. Callers use this after ReadIndex to know when it's
// safe to read the local state machine.
func (rn *RaftNode) WaitApplied(ctx context.Context, index uint64) error {
	for {
		rn.mu.Lock()
		if rn.lastApplied >= index {
			rn.mu.Unlock()
			return nil
		}
		wait := rn.appliedSignal.wait()
		rn.mu.Unlock()

		select {
		case <-wait:
		case <-ctx.Done():
			return ctx.Err()
		case <-rn.stopCh:
			return ErrStopped
		}
	}
}

// LastApplied returns the highest index the state machine has confirmed via
// ReportApplied.
func (rn *RaftNode) LastApplied() uint64 {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return rn.lastApplied
}
