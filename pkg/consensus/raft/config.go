package raft

import (
	"errors"
	"fmt"
	"time"
)

// Config encapsulates operational parameters and timing invariants for a Raft node.
type Config struct {
	// NodeID uniquely identifies this node within the cluster (must be > 0).
	NodeID uint64

	// Peers is the full list of all cluster member node IDs (including this node).
	Peers []uint64

	// HeartbeatInterval defines how frequently the Leader broadcasts empty AppendEntries
	// heartbeats to suppress follower elections.
	// Invariant: Must be significantly smaller than ElectionTimeoutMin (typically 3x to 5x smaller).
	HeartbeatInterval time.Duration

	// ElectionTimeoutMin is the lower bound of the randomized follower election timer.
	ElectionTimeoutMin time.Duration

	// ElectionTimeoutMax is the upper bound of the randomized follower election timer.
	// Invariant: Max must be strictly greater than Min to prevent split votes.
	ElectionTimeoutMax time.Duration

	// RPCTimeout sets the network deadline for outbound RequestVote and AppendEntries calls.
	RPCTimeout time.Duration
}

// DefaultConfig returns a production-tested configuration suitable for intra-datacenter networks.
func DefaultConfig(nodeID uint64, peers []uint64) Config {
	return Config{
		NodeID:             nodeID,
		Peers:              peers,
		HeartbeatInterval:  50 * time.Millisecond,
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		RPCTimeout:         40 * time.Millisecond,
	}
}

// Validate verifies that the configuration satisfies the mathematical safety invariants of Raft.
func (c *Config) Validate() error {
	if c.NodeID == 0 {
		return errors.New("raft: NodeID must be greater than 0")
	}
	if len(c.Peers) < 3 {
		return errors.New("raft: cluster must contain at least 3 peers to establish quorum")
	}
	if c.HeartbeatInterval <= 0 {
		return errors.New("raft: HeartbeatInterval must be positive")
	}
	if c.ElectionTimeoutMin <= c.HeartbeatInterval {
		return fmt.Errorf("raft: ElectionTimeoutMin (%v) must be strictly greater than HeartbeatInterval (%v)",
			c.ElectionTimeoutMin, c.HeartbeatInterval)
	}
	if c.ElectionTimeoutMax <= c.ElectionTimeoutMin {
		return fmt.Errorf("raft: ElectionTimeoutMax (%v) must be greater than ElectionTimeoutMin (%v)",
			c.ElectionTimeoutMax, c.ElectionTimeoutMin)
	}
	if c.RPCTimeout <= 0 {
		return errors.New("raft: RPCTimeout must be positive")
	}
	return nil
}
