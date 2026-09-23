# Milestone 3: Multi-Raft and Range Sharding

## Why Milestone 3? (The Problem Multi-Raft Solves)

- **Throughput Bottleneck:** Every write in the entire database must pass through one single Leader.
- **Storage Ceiling:** Every node in the cluster stores 100% of the dataset. A cluster cannot store 10 TB if nodes have 2 TB drives.
- **Unnecessary Contention:** Unrelated keys (`account:Ram` and `order:999`) contend for the exact same log and sequential commit queue.

```text
Single-Raft (Milestone 2):
All Keys ────────► [ Single Raft Leader ] ────────► Replicates 100% of data to all 3 nodes
                   (One CPU core / One disk queue)

Multi-Raft (Milestone 3):
Keys ["", "m")     ──────► [ Range 1 Leader (Node 1) ] ──► Replicated across Raft Group 1
Keys ["m", +∞)     ──────► [ Range 2 Leader (Node 2) ] ──► Replicated across Raft Group 2
(Parallel execution: Writes to Range 1 and Range 2 happen concurrently on separate nodes/cores!)
```

## Step-by-Step Implementation Roadmap

```text
Phase 3.1: Range Descriptors & The Range Router (pkg/sharding/router.go)
           └── Model: Continuous, disjoint [StartKey, EndKey) intervals.
           └── Router: O(log N) binary search routing table mapping Key -> RangeID.
           └── Deliverable: Router unit test verifying exact boundary conditions.
                                  │
                                  ▼
Phase 3.2: Multi-Raft Node & RPC Multiplexing (pkg/sharding/multi_node.go)
           └── Co-location: Run multiple independent Raft consensus groups on one node.
           └── Multiplexing: Add RangeID to the wire RPC so one TCP port serves all ranges.
           └── Deliverable: 3-node cluster running 2 concurrent Raft groups in parallel.
                                  │
                                  ▼
Phase 3.3: Dynamic Range Splitting via Consensus (pkg/sharding/split.go)
           └── The Split Invariant: Range Leader proposes SplitCommand through Raft.
           └── Atomic Execution: On commit, split parent into Left and Right ranges.
           └── Deliverable: Automated test showing range splitting when key count > threshold.
                                  │
                                  ▼
Phase 3.4: Server Integration & Multi-Range Client Routing (cmd/kv-server)
           └── Routing Proxy: Client requests to any node auto-route to the range leader.
           └── Deliverable: End-to-end multi-range cluster smoke test.
```

## Phase 3.1: Core Concepts to Understand First

Before writing code for Phase 3.1, there are two strict invariants to know.

### 1. The Disjoint Interval Invariant

The global keyspace is divided into sorted half-open intervals:

$$\text{Range}_i = [StartKey_i, EndKey_i)$$

- `StartKey`: Inclusive lower bound.
- `EndKey`: Exclusive upper bound. An empty byte slice `[]byte("")` as `EndKey` represents positive infinity ($+\infty$).

Invariant: for any two adjacent ranges:

$$EndKey_i == StartKey_{i+1}$$

There can be zero gaps and zero overlaps anywhere in the database.

### 2. Range Routing Resolution in $O(\log N)$

Given an arbitrary search key $K$, the router executes a binary search over the sorted slice of ranges. A key matches $\text{Range}_i$ if and only if:

$$StartKey_i \le K < EndKey_i$$
