package sharding

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"sync"
)

// RangeRouter manages the global routing map of sorted disjoint ranges.
// It is fully thread-safe for high-concurrency read-heavy client workloads.
type RangeRouter struct {
	mu     sync.RWMutex
	ranges []RangeDescriptor
}

// NewRangeRouter creates an empty routing table.
func NewRangeRouter() *RangeRouter {
	return &RangeRouter{
		ranges: make([]RangeDescriptor, 0),
	}
}

// UpdateTable validates and installs a new routing table atomically.
//
// Invariants enforced:
//  1. Each descriptor must be individually valid.
//  2. Descriptors are sorted strictly by StartKey.
//  3. Continuous Coverage: ranges[0].StartKey == "" (lower bound -Infinity)
//  4. No Gaps / No Overlaps: ranges[i].EndKey == ranges[i+1].StartKey
//  5. Total Upper Bound: ranges[last].EndKey == "" (+Infinity)
func (r *RangeRouter) UpdateTable(descs []RangeDescriptor) error {
	if len(descs) == 0 {
		return errors.New("sharding: cannot install empty routing table")
	}

	cloned := make([]RangeDescriptor, len(descs))
	for i, d := range descs {
		if err := d.Validate(); err != nil {
			return err
		}
		cloned[i] = d.Clone()
	}

	// 1. Sort lexicographically by StartKey
	sort.Slice(cloned, func(i, j int) bool {
		return bytes.Compare(cloned[i].StartKey, cloned[j].StartKey) < 0
	})

	// 2. Invariant Check: First range must start at "" (-Infinity)
	if len(cloned[0].StartKey) != 0 {
		return fmt.Errorf("%w: first range must start at \"\", got %q", ErrGapsInRoutingTable, cloned[0].StartKey)
	}

	// 3. Invariant Check: Adjacency and Continuity
	for i := 0; i < len(cloned)-1; i++ {
		curr := cloned[i]
		next := cloned[i+1]

		// An internal range cannot have an empty EndKey (+Infinity belongs only to the last range)
		if len(curr.EndKey) == 0 {
			return fmt.Errorf("%w: internal range %d has unbounded +Infinity EndKey", ErrOverlappingRanges, curr.RangeID)
		}

		cmp := bytes.Compare(curr.EndKey, next.StartKey)
		if cmp < 0 {
			return fmt.Errorf("%w: gap between range %d [%q, %q) and range %d [%q, %q)",
				ErrGapsInRoutingTable, curr.RangeID, curr.StartKey, curr.EndKey, next.RangeID, next.StartKey, next.EndKey)
		}
		if cmp > 0 {
			return fmt.Errorf("%w: overlap between range %d and range %d", ErrOverlappingRanges, curr.RangeID, next.RangeID)
		}
	}

	// 4. Invariant Check: Final range must extend to +Infinity ("")
	last := cloned[len(cloned)-1]
	if len(last.EndKey) != 0 {
		return fmt.Errorf("%w: final range %d must have empty EndKey (+Infinity), got %q", ErrGapsInRoutingTable, last.RangeID, last.EndKey)
	}

	r.mu.Lock()
	r.ranges = cloned
	r.mu.Unlock()

	return nil
}

// FindRange identifies the RangeDescriptor responsible for key in O(log N) time.
func (r *RangeRouter) FindRange(key []byte) (RangeDescriptor, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.ranges) == 0 {
		return RangeDescriptor{}, ErrRangeNotFound
	}

	// Binary search: find the smallest index where ranges[idx].EndKey > key
	idx := sort.Search(len(r.ranges), func(i int) bool {
		endKey := r.ranges[i].EndKey
		if len(endKey) == 0 {
			return true // EndKey == "" represents +Infinity
		}
		return bytes.Compare(endKey, key) > 0
	})

	if idx < len(r.ranges) {
		candidate := r.ranges[idx]
		if candidate.Contains(key) {
			return candidate, nil
		}
	}

	return RangeDescriptor{}, ErrRangeNotFound
}

// UpdateLeader updates the cached LeaderID for a given range.
func (r *RangeRouter) UpdateLeader(rangeID uint64, leaderID uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	for i := range r.ranges {
		if r.ranges[i].RangeID == rangeID {
			r.ranges[i].LeaderID = leaderID
			return true
		}
	}
	return false
}

// GetAllRanges returns a snapshot of all active ranges in sorted order.
func (r *RangeRouter) GetAllRanges() []RangeDescriptor {
	r.mu.RLock()
	defer r.mu.RUnlock()

	res := make([]RangeDescriptor, len(r.ranges))
	for i, rd := range r.ranges {
		res[i] = rd.Clone()
	}
	return res
}
