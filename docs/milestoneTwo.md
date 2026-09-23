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
| `TestRaft_InstallSnapshotToLaggingFollower` | A follower disconnected for 20 leader proposals is reconnected after the leader compacts its log; the leader detects the follower's `nextIndex` is behind the compaction horizon and sends `InstallSnapshot` instead of individual entries, and the follower's MVCC store ends up with the correct state. Verification polls with a timeout rather than a fixed sleep — see note below. |
| `TestStorage_KVStorage_PersistenceAndTruncation` | `KVStorage` (the `Storage` implementation over Layer 0) persists `HardState` and log entries, correctly truncates conflicting entries on overwrite, and `CreateSnapshot` compacts entries before the watermark so they return `ErrCompacted`. |
| `TestRaft_PreVoteBlocksTermInflationWhileIsolated` | A node disconnected through 600ms of isolation (several election-timeout cycles) never advances `currentTerm` — `startRealElectionLocked` (the only place `currentTerm++` happens) is only reached after winning a Pre-Vote quorum, which an isolated node can never do. |
| `TestRaft_PreVoteProtectsStableLeaderOnReconnect` | The same isolation-then-reconnect scenario, but asserting on the *Leader* instead: it stays Leader at the same term throughout, for a full second after the isolated node reconnects. This is the exact scenario that made `TestRaft_InstallSnapshotToLaggingFollower` flaky (see below) — now a dedicated regression test. |
| `TestStorage_TidwallStorage_CrashReplayAndCompaction` | `TidwallStorage` (the real disk-backed `Storage` implementation, `storage_tidwall.go`) persists `HardState` and log entries across a close+reopen of the same directory, correctly truncates conflicting entries on overwrite, and compacts via `CreateSnapshot`. Storage-layer only — doesn't touch `RaftNode`. |
| `TestRaft_NodeReplaysLogFromStorageOnRestart` | The integration-level counterpart: boots a `RaftNode` over a `TidwallStorage` directory that already has durably-persisted entries from a "previous process," and asserts the new node's **in-memory** `rn.log`/`commitIndex` actually reflect them. `TestStorage_TidwallStorage_CrashReplayAndCompaction` alone doesn't prove this — the storage layer can be perfectly correct while `RaftNode` never bothers to read it back on boot, which is exactly the bug this test caught (see the note below). |

Eight benchmarks measure the hot paths directly (`raft_bench_test.go`, `raft_snapshot_bench_test.go`, `raft_prevote_bench_test.go`, `storage_tidwall_bench_test.go`):
- `BenchmarkRaft_LogAppend` — raw in-memory log append, no network involved.
- `BenchmarkRaft_SequentialProposals` — full propose → quorum replicate → commit → apply latency on a 3-node cluster, now including `KVStorage` persistence on every step.
- `BenchmarkRaft_ConcurrentProposals` — proposal throughput under concurrent write pressure. **Must be run with a fixed `-benchtime` (e.g. `-benchtime=600x`)** — left to Go's default adaptive scaling, it proposes enough unique keys to exhaust the test cluster's fixed-size Layer 0 arena and panics the whole binary (a known Layer 0 limitation: no reclamation, see [bench/README.md](../bench/README.md)). This cap dropped from 2000x to 600x once persistence landed, since every `AppendEntries` now also writes into the raft node's own storage arena on top of the MVCC arena.
- `BenchmarkMVCC_SnapshotExport` — Layer 2 export throughput streaming 10,000 live keys.
- `BenchmarkMVCC_SnapshotRestore` — ingestion throughput replaying a 10,000-key snapshot into a fresh store.
- `BenchmarkRaft_LeaderElection` — time-to-first-leader on a fresh 3-node cluster. None of the other benchmarks touch the election path at all (they measure steady-state proposing after a leader already exists), and Pre-Vote's entire cost is one extra RPC round-trip specifically in that path, so this is the one that actually reflects it. ~95ms/op, dominated by the randomized 60-120ms election timeout itself.
- `BenchmarkTidwallStorage_Append` — single-entry `Save()` against the real disk-backed `TidwallStorage`. ~13ms/op — this is what an actual fsync costs, versus the microsecond-scale numbers `BenchmarkRaft_SequentialProposals` shows for the in-memory `KVStorage` it uses instead. Not a regression; a different, deliberately slower, actually-durable backend.
- `BenchmarkTidwallStorage_SequentialRead` — 10-entry range reads against a pre-populated 10,000-entry `TidwallStorage` log under concurrent load. ~900ns/op.

**A note on `TestRaft_NodeReplaysLogFromStorageOnRestart` and the two-part persistence bug it caught:** getting crash recovery actually working took two separate fixes in `NewRaftNode`, not one, and the gap between them is a good example of why storage-layer unit tests and node-level integration tests catch different things.

1. `RaftNode.log` (the in-memory operational log `broadcastAppendEntriesLocked`/`checkAdvanceCommitIndexLocked`/etc. actually read from) was never populated from `storage.Entries()` on boot — it always started at just the snapshot boundary, regardless of what was durably on disk. `TestStorage_TidwallStorage_CrashReplayAndCompaction` passing the whole time didn't catch this, because it only proves the storage layer itself is correct in isolation - nothing in that test ever constructs a `RaftNode` and checks whether it actually reads storage back. Fixed by calling `storage.LastIndex()`/`storage.Entries()` in `NewRaftNode` and replaying the result via `rn.log.TruncateAndAppend`.
2. Fixing #1 alone still wasn't enough: replaying into `rn.log` restores Raft's own bookkeeping, but the actual data lives in Layer 2 (the MVCC store), which only gets written to via `applyCh` — and nothing was re-driving that pipeline on boot for entries already committed (per the restored `HardState`) but not yet applied. `commitIndex` would correctly read `5` after a restart, `lastApplied` would still read `0`, and nothing would ever reconcile the two. Caught not by the Go-level test (which only checks `rn.log`/`commitIndex`, not the state machine) but by [`test_crash_recovery.sh`](../test_crash_recovery.sh) — write keys, `SIGKILL` all 3 nodes, restart, `GET` returned 404 for everything despite the Raft layer having correctly recovered. Fixed by calling `rn.scheduleApplyLocked()` in `NewRaftNode` whenever `commitIndex > lastApplied` after the replay.

**A note on `TestRaft_InstallSnapshotToLaggingFollower` and Pre-Vote:** this test was flaky (~20% failure rate) until Pre-Vote was implemented (see [§9](#9-the-pre-vote-protocol-raft-96) below) and its final assertion changed from a single fixed sleep-then-check to a poll with a generous timeout. Root cause: while `followerID` was disconnected, it kept timing out and incrementing its own term in isolation — every attempt failed immediately since it couldn't reach anyone, but the term itself kept climbing. The instant it reconnected, if its own election timer fired before it received a heartbeat, it broadcast `RequestVote` at that inflated term, and `HandleRequestVote` adopted any higher term unconditionally — even from a candidate whose short, stale log could never actually win the vote — forcing the healthy, currently-serving Leader to step down right as it was supposed to be delivering the snapshot.

Implementing Pre-Vote fixed this at the root (see `TestRaft_PreVoteBlocksTermInflationWhileIsolated`/`TestRaft_PreVoteProtectsStableLeaderOnReconnect` above), but two bugs surfaced while validating it, both in the leader-stickiness check `HandleRequestVote` uses to reject Pre-Votes from a node that hasn't proven it can reach a quorum:
1. **`rn.lastHeartBeat` was written once, at construction, and never updated.** A follower's stickiness window was therefore only real for the first `ElectionTimeoutMin` after the process started — after that, `time.Since(rn.lastHeartBeat)` only grew, so the check was permanently `false` regardless of how recently a real heartbeat had actually arrived. Fixed by updating `rn.lastHeartBeat` inside `HandleAppendEntries` whenever a legitimate heartbeat is processed.
2. **A Leader never receives `AppendEntries`** (it only sends them), so fix #1 alone didn't help the Leader's own copy of `lastHeartBeat` — it stayed frozen at construction time for as long as the node led, meaning the Leader itself would eventually grant a Pre-Vote to its own challenger. Fixed by treating `rn.role == RoleLeader` as inherently active in the stickiness check — a functioning Leader doesn't need a timer to know it's active.

Both were caught empirically: `TestRaft_PreVoteProtectsStableLeaderOnReconnect` failed ~12% of the time before fix #2 (25-run and 40-run samples), and 0/40 after.

### Running locally

```bash
# Full raft test suite, verbose
go test -v ./pkg/consensus/raft/...

# Race detector
go test -race ./pkg/consensus/raft/...

# Benchmarks
go test -bench=BenchmarkRaft_LogAppend -benchmem -run='^$' ./pkg/consensus/raft/...
go test -bench=BenchmarkRaft_SequentialProposals -benchmem -run='^$' ./pkg/consensus/raft/...
go test -bench=BenchmarkRaft_ConcurrentProposals -benchmem -benchtime=600x -run='^$' ./pkg/consensus/raft/...
go test -bench=BenchmarkMVCC_SnapshotExport -benchmem -run='^$' ./pkg/consensus/raft/...
go test -bench=BenchmarkMVCC_SnapshotRestore -benchmem -run='^$' ./pkg/consensus/raft/...
go test -bench=BenchmarkRaft_LeaderElection -benchmem -run='^$' ./pkg/consensus/raft/...
go test -bench=BenchmarkTidwallStorage_Append -benchmem -run='^$' ./pkg/consensus/raft/...
go test -bench=BenchmarkTidwallStorage_SequentialRead -benchmem -run='^$' ./pkg/consensus/raft/...
```

To see the consensus engine running for real rather than under simulation, build the `kv-server` daemon and run a live 3-node cluster on localhost — see [README: Run KV Server](../README.md#run-kv-server) for the exact commands and a `/status` polling loop to watch leader election and term convergence happen live.

For a scripted version of that walkthrough, run [`test_cluster.sh`](../test_cluster.sh) from the repo root:

```bash
./test_cluster.sh
```

It builds `kv-server`, boots a real 3-node cluster over actual TCP/HTTP (not the `SimulatedNetwork` the tests above use), and checks leader election, write replication across all three nodes, and that a follower correctly redirects writes with HTTP 307 — cleaning up all processes on exit regardless of pass/fail. See [README: Automated cluster smoke test](../README.md#automated-cluster-smoke-test) for what each of its four checks does.

This category of test matters specifically because it's the only one exercising the real transport layer: the `--peers` address-parsing bug once present in `main.go` (storing the wrong split segment as each peer's address) was invisible to the in-process `SimulatedNetwork` tests above, since those never touch `TCPTransport`, `net/rpc`, or flag parsing at all.

For the crash-recovery scenario specifically — does data survive if every node dies? — run [`test_crash_recovery.sh`](../test_crash_recovery.sh):

```bash
./test_crash_recovery.sh
```

It writes to a live cluster, `SIGKILL`s all three processes, restarts them from the same `data/` directories, and verifies the data and cluster are both still there. See [README: Crash-recovery smoke test](../README.md#crash-recovery-smoke-test). This is the test that caught the second of the two persistence bugs described above — `TestRaft_NodeReplaysLogFromStorageOnRestart` alone wasn't enough to catch it.

## 7. Storage Durability & Log Persistence

See [milestoneTwoDurableChoice.md](milestoneTwoDurableChoice.md) for the full ADR: why a dedicated Raft WAL is kept separate from Layer 2's MVCC store, the alternatives considered, and current implementation status (both Layer 2's file snapshot and Raft's own disk WAL, `TidwallStorage`, are real and built).

### Why In-Memory Consensus Is Unsafe
In the baseline Raft engine, state (`currentTerm`, `votedFor`, `log[]`) lives purely in volatile memory. If a node crashes and reboots:  

1. **Term Amnesia:** It resets `currentTerm` to 0.
2. **Double-Voting Bug (Election Safety Violation):** If Node 1 voted for Node 2 in Term 5, crashes, restarts with `votedFor = 0`, and receives a vote request from Node 3 in Term 5, it will vote a second time in the same term. This can lead to **two leaders being elected in the exact same term**, violating the core safety property of Raft.

```text
CRASH WITHOUT PERSISTENCE:
Node 1 votes for Node 2 in Term 5 ──────► Node 1 crashes
                                              │ reboots with votedFor = 0
Node 1 votes for Node 3 in Term 5 ◄────── Node 1 receives vote request
────────────────────────────────────────────────────────────────────────
RESULT: Both Node 2 and Node 3 achieve majority! TWO LEADERS IN TERM 5!
```
The Persistence Invariant (Raft §5.2)  
Before a node responds to any RequestVote or AppendEntries RPC, it must atomically flush three fields to stable storage:  

```go
currentTerm: Latest term this server has observed.

votedFor: Candidate ID that received this server's vote in the current term.

log[]: Log entries (command, index, and term).
```

### The Pluggable Storage Pattern (CockroachDB / Pebble Model)  
Rather than writing custom file serialization that rewrites the entire log on every mutation ($O(N)$ disk write amplification), we use a pluggable Storage interface. The intent, matching CockroachDB/Pebble, is for Raft state to live directly inside an embedded LSM engine using key prefixes: 

HardState: !raft:hs -> {Term, Vote, Commit}  
Log Entries: !raft:log:<BigEndianIndex> -> Serialized LogEntry  
Snapshot Horizon: !raft:snap -> SnapshotMeta

**Current state — this is not yet durable.** `KVStorage` implements that key-prefix scheme correctly, but the `raw.ByteEngine` it's built on is Layer 0's `SkipListEngine`, which is entirely in-memory (`Arena.buf` is a plain `make([]byte, capacity)` — no file, no mmap, nothing reaches disk). The backend-comparison diagram below shows "KVStorage (Pebble)" as the target design; the actual backend in use today is the in-memory skip list, not Pebble. Concretely: `Save()` is called at every point §5.2 requires, so the *protocol* is followed correctly within a process's lifetime — but if the process dies, everything `Save()` ever wrote dies with it. `InitialState()` returns `Term=0, Vote=0` on every restart, not "whatever it was before." Layer 0 gaining a real on-disk backend (the `pebble.go` in the original package layout, never built — see the Layer 0 checklist in the [README](../README.md)) is a prerequisite for this section's durability claims to hold across a real crash, not just an in-process failure.

### 8. Log Compaction & Snapshotting (InstallSnapshot RPC)  
The Unbounded Log Problem
An append-only log cannot grow indefinitely. If a key is updated 1,000,000 times:

It consumes excessive disk/memory space.  

Replaying 1,000,000 entries on startup takes minutes.    

Log Compaction HorizonWhen the log reaches a threshold size, the node takes a point-in-time snapshot of the state machine up to index $K$, then discards all log entries $\le K$  
```text
Before Compaction:
Log Index: [ 0 ]

After Compacting up to Index 5 (lastIncludedIndex=5, lastIncludedTerm=2):
Snapshot File: Contains state machine state as of Index 5.
In-Memory Log: [ Index: 5 (Sentinel) ] [ Index: 6 ] [ Index: 7 ]
                 ▲
                 └─ Sentinel entry representing the snapshot boundary
```
The InstallSnapshot Protocol  
When a follower is partitioned or lagging so far behind that the leader has already compacted the entries the follower needs (nextIndex[peer] <= firstIndex), the leader cannot use AppendEntries. It sends an InstallSnapshot RPC to fast-forward the follower directly to the snapshot horizon.
```text
+--------------------------------------+
                           |   Raft Consensus State Machine       |
                           |   (Leader Election, Replication)     |
                           +--------------------------------------+
                                              │
                         calls atomic Save()  │  queries Entries(), Term()
                                              ▼
                           +--------------------------------------+
                           |       raft.Storage (Interface)       |
                           +--------------------------------------+
                                      ▲                ▲
               ┌──────────────────────┴───────┐        └──────────────────────┐
               │                              │                               │
+------------------------------+ +------------------------------+ +------------------------------+
| Backend 1: MemStorage        | | Backend 2: KVStorage (Pebble)| | Backend 3: SegmentedFileWAL  |
| - Fast unit tests            | | - CockroachDB pattern        | | - etcd/wal or tidwall/wal    |
| - Microbenchmarking          | | - Atomic WriteBatch over L0  | | - Pre-allocated disk chunks  |
| - In-memory SkipList         | | - Integrated with MVCC disk  | | - Raw disk platters          |
+------------------------------+ +------------------------------+ +------------------------------+
```
```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                          Layer 2: MVCC Engine                               │
└─────────────────────────────────────────────────────────────────────────────┘
          │                                                  ▲
          │ 1. Unapplied/Committed log > Threshold           │ 6. If follower lags,
          │    (e.g., 5,000 entries or 32 MB)                │    delivers Snapshot
          │                                                  │    via applyCh
          ▼ 2. Calls ExportSnapshot(watermarkTS)             │
┌────────────────────────────────────────────────────────────┴────────────────┐
│                   Compactor / Snapshot Controller (Daemon)                  │
│  - Dumps active MVCC dataset to bytes                                       │
│  - Tracks lastAppliedIndex and lastAppliedTerm                              │
└─────────────────────────────────────────────────────────────────────────────┘
          │
          │ 3. Handshake: raftNode.Snapshot(appliedIndex, appliedTerm, data)
          ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                       Milestone 2: Raft Consensus Engine                    │
│  - 4. storage.CreateSnapshot(meta, data)                                    │
│  - 5. raftLog.CompactLog(appliedIndex, appliedTerm)                         │
│       (Entries <= appliedIndex are purged from RAM and disk)                │
└─────────────────────────────────────────────────────────────────────────────┘
```
## 9. The Pre-Vote Protocol (Raft §9.6)
The Problem: How an Isolated Follower Destabilizes the Cluster
Imagine a 3-node cluster:

Node 1: Leader (Term 1)

Node 2: Follower (Term 1)

Node 3: Follower (Term 1)

Now, network cables to Node 3 are severed (Node 3 is partitioned on an island).
```text
Healthy Quorum (Taking Writes):          Isolated Island:
+----------------+  +----------------+   +-------------------+
| Node 1 (Leader)|  | Node 2 (Follow)|   | Node 3 (Follower) |
| Term: 1        |  | Term: 1        |   | (Disconnected)    |
+----------------+  +----------------+   +-------------------+
        ▲                   ▲                      │
        └────── Quorum ─────┘                      │ 1. Election timer times out!
                                                   │ 2. Bumps term: 1 -> 2 -> ... -> 50
                                                   ▼ (Cannot get votes, keeps bumping)
```
- The Inflation Loop: While partitioned, Node 3 never receives heartbeats. Its election timer expires repeatedly. Every ~200 ms, Node 3 transitions to Candidate and increments its term:$$1 \rightarrow 2 \rightarrow 3 \rightarrow \dots \rightarrow \mathbf{50}$$
- The Reconnection: The network heals, and Node 3 reconnects to the cluster.  
- The Disruption: Node 3 broadcasts RequestVote with Term = 51.  
- The Disaster:Node 1 (the legitimate Leader) and Node 2 receive Node 3’s message:

```go
if args.Term > rn.currentTerm { // 51 > 1
    rn.currentTerm = args.Term
    rn.role = RoleFollower // ❌ LEADER IS INSTANTLY DEPOSED!
    rn.votedFor = 0
}
```

The Service Outage:

- Node 1 immediately abdicates leadership.
- Can Node 3 become the new leader? NO, because its log is stale (it missed all writes while disconnected, so its up-to-date log check fails).
- Result: Client writes freeze cluster-wide while the nodes waste time running unnecessary elections to re-elect Node 1 or Node 2 at Term 52.


How Production Systems Solve This: The Pre-Vote Protocol (Raft §9.6)  
- Tier-1 distributed systems (etcd, CockroachDB, TiKV, and Ongaro’s Raft dissertation §9.6) solve this by introducing an additional phase: **Pre-Vote**.  
- A disconnected node is forbidden from incrementing its term or deposing a healthy leader unless it first proves it can win an election.
```text
Standard Raft (Vulnerable):
Follower Times Out ────────► Increments Term ────────► Sends RequestVote (Deposes Leader)
                                     ▲
                             Term inflated here!


                         Production Raft with Pre-Vote:
Follower Times Out ────────► Sends PreVote (Trial Run) ───┬─► Majority Grants? ──► Increments Term
                             (Does NOT bump term!)        │
                                                          └─► Denied? ──────────► Back to Follower
```
How the Pre-Vote Protocol Works in 3 Rules
Rule 1: A Trial Election (RolePreCandidate)
When a follower's election timer expires, it does not become a RoleCandidate and does not increment currentTerm.
Instead, it becomes a RolePreCandidate and asks peers:

"If I were to start an election for currentTerm + 1, would you vote for me?"

Rule 2: The Leader-Lease Protection on Followers
When Node 1 or Node 2 receives a PreVote from Node 3:
They will DENY the pre-vote if:

They have received a heartbeat from a valid leader within their own ElectionTimeoutMin.

OR the candidate's log is less up-to-date than their own.

Because Node 1 and Node 2 are actively in contact with each other, they know the current leader is healthy. They reject Node 3's pre-vote:

"No, we have an active leader and heard from it 20ms ago. We will not vote for you."

Rule 3: The Result
Node 3 receives 0 votes out of 2. It fails the pre-vote.

Node 3’s term never increments.

Node 1 remains the Leader, undisturbed.

When the leader's next periodic heartbeat arrives at Node 3, Node 3 realizes Node 1 is alive and transitions back to RoleFollower.