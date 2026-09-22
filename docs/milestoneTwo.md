## Milestone 2: Single Group Raft Consensus
```text
+-----------------------------------------------------------------------+
| Application / KV Client API                                           |
+-----------------------------------------------------------------------+
                                   │ Propose(command)
                                   ▼
+-----------------------------------------------------------------------+
| Milestone 2: Single-Group Raft Engine (pkg/raft)                      |
| - Consensus State Machine (Follower, Candidate, Leader)               |
| - Replicated Write-Ahead Log (Term, Index, Command)                   |
| - Quorum Agreement Engine (RequestVote, AppendEntries)                |
+-----------------------------------------------------------------------+
                                   │ Commit notification via applyCh
                                   ▼
+-----------------------------------------------------------------------+
| Milestone 1: Local MVCC Storage Engine (pkg/storage/mvcc)             |
| - Put(key, val, commitTS)                                             |
| - Delete(key, commitTS)                                               |
+-----------------------------------------------------------------------+
```
1. Why Do We Need Milestone 2? (The Core Problem)  
In Milestone 1, we built an MVCC storage engine on a single machine. While it handles snapshot isolation, historical versions, and tombstones, it has two fundamental single-node limits:  
1. Zero Fault Tolerance: If the server loses power or corrupts its disk, all data is lost.
2. Single Point of Failure (SPOF): If the node crashes, all client reads and writes halt.
To survive hardware failures, we must replicate data across multiple independent machines (e.g., 3 or 5 nodes).

However, multi-node replication introduces the Consensus Problem: 
* If Node 1 accepts balance = ₹100 and Node 2 simultaneously accepts balance = ₹150, which write occurred first? 
* If a network cable between nodes is severed, how do nodes prevent two leaders from accepting conflicting writes (Split-Brain)?
The Solution: Raft (Majority Quorum Replication) 
Raft solves this by electing a single Leader through Majority Quorum ($Q = \lfloor N/2 \rfloor + 1$):
* In a 3-node cluster, a quorum requires $\lfloor 3/2 \rfloor + 1 = 2$ nodes.  
* Any two majorities in a cluster of size $N$ must overlap by at least one node: $$(2 \text{ nodes}) + (2 \text{ nodes}) = 4 > 3 \implies \text{At least one common node}$$  
* This mathematical overlap guarantees that:
   1. There can never be two valid leaders in the same term.
   2. A newly elected leader is guaranteed to contain every log entry committed by previous terms.
________________


2. The Core State Machine (Roles & Transitions)
Every node in the cluster exists in one of three mutually exclusive roles:
```text
               Times out, starts election
             ┌─────────────────────────────┐
             ▼                             │
      +--------------+  Times out, new el  +---------------+
      |              | ------------------> |               |
      |   Follower   |                     |   Candidate   |
      |              | <------------------ |               |
      +--------------+   Steps down:       +---------------+
             ▲           Discovers higher          │
             │           term or Leader            │ Receives votes from
             │                                     │ majority of cluster
             │           Steps down:               ▼
             │           Discovers higher  +---------------+
             └──────────────────────────── |    Leader     |
                         term              +---------------+
```

Role Invariants
1. Follower:
   * Purely reactive. Accepts RPCs from leaders and candidates.
   * If it receives no communication within its Election Timeout (randomized between $150\text{ ms} - 300\text{ ms}$), it assumes the leader is dead, increments its CurrentTerm, transitions to Candidate, and calls an election.
2. Candidate:
   * Votes for itself and broadcasts RequestVote RPCs to all peers.
   * If it gathers votes from a majority ($\ge \lfloor N/2 \rfloor + 1$), it transitions to Leader.
   * If it receives an AppendEntries RPC from a legitimate leader with a term $\ge$ its own, it steps down to Follower.
   * If the election times out without a winner (split vote), it increments CurrentTerm and starts a new election.
3. Leader:
   * Serves all client writes.
   * Appends client commands to its local log and replicates them via AppendEntries.
   * Sends periodic empty AppendEntries heartbeats (every $50\text{ ms}$) to reset followers' election timers.
________________


3. Physical Protocol Definition
Raft uses two core Remote Procedure Calls (RPCs).
RPC 1: RequestVote  
Invoked by candidates to gather votes during an election.
Request:
```text
+-------------------+---------------------------------------------------+
| Term (uint64)     | Candidate's current term                          |
| CandidateID (int) | ID of candidate requesting vote                   |
| LastLogIndex (int)| Index of candidate's last log entry               |
| LastLogTerm (uint64)| Term of candidate's last log entry              |
+-------------------+---------------------------------------------------+


Response:
+-------------------+---------------------------------------------------+
| Term (uint64)     | Current term of recipient (for candidate to update)|
| VoteGranted (bool)| True means candidate received vote                |
+-------------------+---------------------------------------------------+
```

The "Up-to-Date Log" Voting Rule (Election Safety)
A voter will deny its vote if the candidate's log is less up-to-date than the voter's own log:  

1. If the candidate and voter have logs ending in different terms, the log with the higher term is more up-to-date.  
2. If both logs end in the same term, whichever log has the higher LastLogIndex (longer log) is more up-to-date.  
________________


RPC 2: AppendEntries  
Invoked by the leader to replicate log entries and serve as a periodic heartbeat. 
```text
Request:
+-----------------------+-----------------------------------------------+
| Term (uint64)         | Leader's current term                         |
| LeaderID (int)        | ID of the leader                              |
| PrevLogIndex (int)    | Index of log entry immediately preceding new  |
| PrevLogTerm (uint64)  | Term of PrevLogIndex entry                    |
| Entries ([]LogEntry)  | Log entries to store (empty for heartbeat)    |
| LeaderCommit (int)    | Leader's CommitIndex                          |
+-----------------------+-----------------------------------------------+


Response:
+-----------------------+-----------------------------------------------+
| Term (uint64)         | Current term of recipient                     |
| Success (bool)        | True if follower matched PrevLogIndex/Term    |
| MatchIndex (int)      | Highest index replicated on this follower     |
+-----------------------+-----------------------------------------------+
```

The Log Matching Invariant
1. If two entries in different logs have the same index and term, they store the same command.  
2. If two entries in different logs have the same index and term, then their logs are identical in all preceding entries.  
________________


4. Ground-Zero Execution Walkthrough: Replicating a Write
```text
Client                Leader (Node 1)             Follower (Node 2)         Follower (Node 3)
  │                          │                            │                         │
  ├─ 1. Put("Ram", ₹100) ───►│                            │                         │
  │                          ├─ 2. Append to local log    │                         │
  │                          │     [Index: 1, Term: 1]    │                         │
  │                          │                            │                         │
  │                          ├─ 3a. AppendEntries RPC ───►│                         │
  │                          │      (Index: 1, Term: 1)   ├─ Append to local log    │
  │                          │                            │                         │
  │                          ├─ 3b. AppendEntries RPC ─────────────────────────────►│ (Dropped /
  │                          │      (Index: 1, Term: 1)   │                         │  Unresponsive)
  │                          │                            │                         │
  │                          │◄─ 4. Success (MatchIdx: 1)─┤                         │
  │                          │                            │                         │
  │                          ├─ 5. Quorum Reached (2/3)!  │                         │
  │                          │     Leader commitIndex = 1 │                         │
  │                          ├─ 6. Apply to local MVCC    │                         │
  │                          │                            │                         │
  │◄─ 7. Return Success ─────┤                            │                         │
  │                          │                            │                         │
  │                          │                            │                         │
  │   --- Later (Next write or periodic heartbeat) ---   │                         │
  │                          │                            │                         │
  │                          ├─ 8. AppendEntries ────────►│                         │
  │                          │     (LeaderCommit = 1)     ├─ 9. Follower updates    │
  │                          │                            │     commitIndex = 1 and │
  │                          │                            │     applies to its MVCC │
```

5. Failure Modes &  Edge Cases
Edge Case 1:  (Never Commit Previous-Term Entries Directly by Counting Replicas)
* The Trap: A leader crashes. A new leader comes up and replicates an uncommitted log entry created by an older term onto a majority of nodes.
* The Rule: A leader can NEVER mark an entry from a PREVIOUS term as committed simply by counting replicas. It must commit at least one entry from its current term by counting replicas. Once an entry from the current term is committed, all prior entries are committed indirectly by the Log Matching Property.
Edge Case 2: Split Votes
* If three candidates start an election at the exact same millisecond in a 3-node cluster, each node might vote for itself ($1-1-1$). No majority is reached.
* Mitigation: Randomized Election Timeouts (e.g., Node 1 waits $160\text{ ms}$, Node 2 waits $240\text{ ms}$, Node 3 waits $290\text{ ms}$). Node 1 will time out first, initiate an election, and gather votes before peers wake up.
Edge Case 3: Decoupling Consensus from State Machine Application
* Replicating a log entry across network sockets must not block on disk operations or MVCC updates.
* Raft uses an asynchronous channel (applyCh). When commitIndex advances, committed commands are pushed to applyCh in sequential order, and an independent worker applies them to Milestone 1's MVCCStore.

## 6. Testing & Verification

### Automated test suite (`pkg/consensus/raft`)

The tests run against an in-process `SimulatedNetwork` (`raft_test.go`) rather than real sockets, so partitions, drops, and reconnects are deterministic and don't depend on OS-level networking timing:

| Test | Verifies |
|---|---|
| `TestRaft_DeterministicLeaderElection` | A fresh 3-node cluster converges on exactly one Leader within 1s, at a term > 0. |
| `TestRaft_QuorumReplicationAndMVCCCommit` | A command proposed on the Leader replicates via quorum, advances `commitIndex`, and is applied to every node's Layer 2 MVCC store. |
| `TestRaft_MinorityPartitionNonBlocking` | With one follower disconnected (2/3 quorum remaining), the Leader still commits writes — a minority partition never blocks progress. |
| `TestRaft_LeaderFailureAndFailover` | Disconnecting the Leader triggers a new election with a strictly higher term; new writes continue under the new Leader; on reconnect, the stale Leader adopts the higher term, steps down to Follower, and catches up its log. |
| `TestRaft_LogConflictTruncation` | An entry written by an isolated (stale) Leader that never reached quorum is correctly overwritten once a legitimately-elected new Leader's conflicting entry replicates — the Log Matching Invariant (§3) in practice. |
| `TestRaft_ConcurrentProposalsAndRace` | 5 goroutines × 20 concurrent proposals converge to identical log length on every node, clean under `-race`. |

Two benchmarks measure the hot paths directly (`raft_bench_test.go`):
- `BenchmarkRaft_LogAppend` — raw in-memory log append, no network involved.
- `BenchmarkRaft_SequentialProposals` — full propose → quorum replicate → commit → apply latency on a 3-node cluster.
- `BenchmarkRaft_ConcurrentProposals` — proposal throughput under concurrent write pressure. **Must be run with a fixed `-benchtime` (e.g. `-benchtime=2000x`)** — left to Go's default adaptive scaling, it proposes enough unique keys to exhaust the test cluster's fixed-size Layer 0 arena and panics the whole binary (a known Layer 0 limitation: no reclamation, see [bench/README.md](../bench/README.md)).

### Running locally

```bash
# Full raft test suite, verbose
go test -v ./pkg/consensus/raft/...

# Race detector
go test -race ./pkg/consensus/raft/...

# Benchmarks
go test -bench=BenchmarkRaft_LogAppend -benchmem -run='^$' ./pkg/consensus/raft/...
go test -bench=BenchmarkRaft_SequentialProposals -benchmem -run='^$' ./pkg/consensus/raft/...
go test -bench=BenchmarkRaft_ConcurrentProposals -benchmem -benchtime=2000x -run='^$' ./pkg/consensus/raft/...
```

To see the consensus engine running for real rather than under simulation, build the `kv-server` daemon and run a live 3-node cluster on localhost — see [README: Run KV Server](../README.md#run-kv-server) for the exact commands and a `/status` polling loop to watch leader election and term convergence happen live.

For a scripted version of that walkthrough, run [`test_cluster.sh`](../test_cluster.sh) from the repo root:

```bash
./test_cluster.sh
```

It builds `kv-server`, boots a real 3-node cluster over actual TCP/HTTP (not the `SimulatedNetwork` the tests above use), and checks leader election, write replication across all three nodes, and that a follower correctly redirects writes with HTTP 307 — cleaning up all processes on exit regardless of pass/fail. See [README: Automated cluster smoke test](../README.md#automated-cluster-smoke-test) for what each of its four checks does.

This category of test matters specifically because it's the only one exercising the real transport layer: the `--peers` address-parsing bug once present in `main.go` (storing the wrong split segment as each peer's address) was invisible to the in-process `SimulatedNetwork` tests above, since those never touch `TCPTransport`, `net/rpc`, or flag parsing at all.