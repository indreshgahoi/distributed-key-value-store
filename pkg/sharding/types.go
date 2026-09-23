// Package sharding implements range-based keyspace partitioning and Multi-Raft routing.
package sharding

import (
	"bytes"
	"errors"
	"fmt"
)

var (
	// ErrRangeNotFound is returned when a key does not fall within any registered range.
	ErrRangeNotFound = errors.New("sharding: key does not map to any active range")

	// ErrInvalidRangeBounds is returned when StartKey >= EndKey (and EndKey is not +Infinity).
	ErrInvalidRangeBounds = errors.New("sharding: StartKey must be strictly less than EndKey")

	// ErrGapsInRoutingTable is returned when ranges do not cover the entire keyspace continuously.
	ErrGapsInRoutingTable = errors.New("sharding: routing table contains gaps or non-continuous bounds")

	// ErrOverlappingRanges is returned when two ranges overlap.
	ErrOverlappingRanges = errors.New("sharding: routing table contains overlapping ranges")
)

// RangeDescriptor defines the boundary invariants for a continuous key interval.
//
// Invariant: The range represents the half-open interval [StartKey, EndKey).
// An empty slice ([]byte("")) for EndKey represents positive infinity (+Infinity).
// An empty slice ([]byte("")) for StartKey represents negative infinity (-Infinity).
type RangeDescriptor struct {
	// RangeID is the globally unique identifier for this consensus range.
	RangeID uint64 `json:"range_id"`

	// StartKey is the inclusive lower bound.
	StartKey []byte `json:"start_key"`

	// EndKey is the exclusive upper bound (empty slice means +Infinity).
	EndKey []byte `json:"end_key"`

	// Peers identifies the cluster node IDs hosting replicas of this range.
	Peers []uint64 `json:"peers"`

	// LeaderID caches the current active leader for this range to optimize client routing.
	LeaderID uint64 `json:"leader_id"`
}

// Contains asserts whether key falls within [StartKey, EndKey).
func (rd RangeDescriptor) Contains(key []byte) bool {
	// key must be >= StartKey
	if bytes.Compare(key, rd.StartKey) < 0 {
		return false
	}
	// If EndKey is empty, it covers everything up to +Infinity
	if len(rd.EndKey) == 0 {
		return true
	}
	// key must be strictly < EndKey
	return bytes.Compare(key, rd.EndKey) < 0
}

// Validate verifies that the individual range bounds are mathematically well-formed.
func (rd RangeDescriptor) Validate() error {
	if rd.RangeID == 0 {
		return errors.New("sharding: RangeID must be greater than 0")
	}
	if len(rd.Peers) == 0 {
		return errors.New("sharding: Range must have at least one replica peer")
	}
	// If EndKey is not +Infinity, ensure StartKey < EndKey
	if len(rd.EndKey) > 0 && bytes.Compare(rd.StartKey, rd.EndKey) >= 0 {
		return fmt.Errorf("%w: [%q, %q)", ErrInvalidRangeBounds, rd.StartKey, rd.EndKey)
	}
	return nil
}

// Clone returns a deep copy of the RangeDescriptor.
func (rd RangeDescriptor) Clone() RangeDescriptor {
	c := RangeDescriptor{
		RangeID:  rd.RangeID,
		StartKey: bytes.Clone(rd.StartKey),
		EndKey:   bytes.Clone(rd.EndKey),
		Peers:    make([]uint64, len(rd.Peers)),
		LeaderID: rd.LeaderID,
	}
	copy(c.Peers, rd.Peers)
	return c
}
