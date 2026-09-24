# Architecture FAQ — design review and interview preparation

Questions a principal-level reviewer is likely to ask about this system, with answers grounded in the code. Read [architecture.md](architecture.md) first; this document goes deeper on the *why*.

How to use it: cover each answer and explain it aloud in your own words, then open the referenced code and check that you could rebuild the reasoning from scratch. The **Follow-up** lines are where interviews usually go next. Several answers end in a trade-off or an open problem, on purpose: a principal answer says what a design costs, not just what it does.

**Contents**

1. [System design and trade-offs](#1-system-design-and-trade-offs)
2. [Raft correctness](#2-raft-correctness)
3. [Reads and linearizability](#3-reads-and-linearizability)
4. [Durability and crash recovery](#4-durability-and-crash-recovery)
5. [Storage engine and MVCC](#5-storage-engine-and-mvcc)
6. [Client semantics](#6-client-semantics)
7. [Verification](#7-verification)
8. [Performance](#8-performance)
9. [Failure modes and operations](#9-failure-modes-and-operations)
10. [Limits, and what comes next](#10-limits-and-what-comes-next)

---

## 1. System design and trade-offs

### Q1. Walk me through the architecture in two minutes.

Three storage layers sit under a consensus layer and a service layer:

- **Layer 0 (`raw`):** an ordered byte map, a lock-free skiplist on an arena.
- **Layer 1 (`codec`):** a key encoding in which byte order means "user key ascending, then version descending".
- **Layer 2 (`mvcc`):** a versioned store.
- **Raft:** replicates a log of commands.
- **`kv-server`:** applies committed commands to the store and serves HTTP.

The only durable state is Raft's: the log, the vote and term, and snapshots. The store lives in memory and is rebuilt on restart from the latest snapshot plus the log after it. Each layer gives the one above it exactly one guarantee (the table in [architecture.md §1](architecture.md#1-the-system-in-one-picture)).

### Q2. Why is the MVCC store in memory rather than on disk?

It's a deliberate staging decision. Raft already provides durability (snapshot plus WAL), so the store only has to be *rebuildable*, not persistent itself. That kept the correctness-critical surface small while the consensus layer was being hardened.

The cost is real: live data must fit in `--memtable-bytes` (64 MiB by default), and restart time grows with the log since the last snapshot. The next step is an LSM tree, where a full memtable is flushed to immutable SSTables. The arena-backed skiplist is exactly the memtable an LSM needs, so none of this work is wasted.

### Q3. Why one mutex in `RaftNode` instead of finer-grained locking or an actor loop?

The protocol logic is sequential by nature. One lock turns each handler into a critical section you can reason about like single-threaded code. The rule that makes this work is that network I/O always happens on other goroutines with the lock released. When a reply comes back, the handler re-takes the lock and re-checks that the role and term are unchanged, because anything may have happened during the round trip.

Contention isn't the bottleneck: the lock is held for microseconds, and fsync dominates latency.

**Follow-up: what about etcd's design?** etcd's `raft` package is a pure, deterministic state machine. The caller drives it with `Tick`, `Step` and `Ready`, and it does no I/O or threading. That design is far better for deterministic simulation testing, because you control time and message order completely. Here, adopting it is the natural next step if randomized testing isn't enough. See Q30.

### Q4. Why net/rpc over TCP instead of gRPC?

It's standard library and dependency-free, and it's sufficient for learning and correctness work. Its weaknesses are known and contained in `transport.go`:
- A call can't be cancelled, so a timed-out call drops the connection to force a redial.
- There's no streaming, which matters for large snapshots (Q27).
- Every message is gob-encoded, including all entry payloads.

The `NetworkTransport` interface keeps swapping to gRPC a local change.

### Q5. Where are the module boundaries, and why there?

- **Raft never interprets commands.** It moves opaque bytes. Its only contract with the state machine is the `ApplyMsg` stream plus `ReportApplied`.
- **The state machine owns the meaning of commands** and is the store's only writer.
- **Storage is an interface** (`Storage`). There are two implementations with the same contract: on-disk (`TidwallStorage`) and over a Layer 0 engine (`KVStorage`, used by tests to simulate crash and restart without real disks).

Each boundary sits where a different correctness argument begins: consensus safety, state machine determinism, and durability.

---

## 2. Raft correctness

### Q6. How do you guarantee at most one leader per term?

A node grants at most one real vote per term, and makes that vote durable *before* replying (`HandleRequestVote` → `persistLocked`). A candidate needs a majority, and any two majorities share at least one node, so two candidates can't both win the same term.

The persistence is the part people get wrong. If a node forgot its vote in a crash, it could vote again in the same term for a different candidate, and you'd get two leaders.

### Q7. How does a new leader know it holds every committed entry?

The Election Restriction (§5.4.1, `candidateLogUpToDateLocked`): a node only votes for a candidate whose log is at least as up to date as its own. "Up to date" means a higher last term, or the same last term and an equal or longer log. A committed entry is on a majority, the winner got votes from a majority, and those two majorities overlap. So the winner's log is at least as up to date as a node holding the entry, which means it holds the entry too.

Removing this check was one of the mutation tests. The chaos test caught it, with different commands applied at the same index.

### Q8. Explain Figure 8. Why can't a leader commit an entry just because a majority stores it?

An entry from an **earlier term** can be on a majority and still be overwritten. Suppose leader S1 in term 2 replicates entry *e* to a majority and then crashes before committing it. A node whose log ends in term 3 can still win an election, because term 3 is newer than term 2, and it will overwrite *e*.

So `advanceCommitIndexLocked` only commits by counting replicas for **current-term** entries. Earlier entries become committed indirectly, as a side effect of committing a later current-term entry.

**Follow-up: how did you test it?** Deterministically, in `TestRaft_EarlierTermEntryNotCommittedByCounting`. The randomized chaos test did **not** catch a mutation that removed this rule. The election no-op (Q9) makes the dangerous window very narrow. Admitting that, and closing it with a targeted test, is a stronger answer than claiming the chaos test covers everything.

### Q9. Why does a new leader append a no-op?

It serves two purposes, both flowing from Figure 8:
1. A new leader can't advance `commitIndex` over earlier-term entries until it commits something from its own term. Without the no-op, a quiet cluster could leave committed-in-practice entries unconfirmed indefinitely.
2. Read Index is only correct once the leader's commit index is current (Q14).

The no-op commits both immediately after the election. It is applied inside Raft and never delivered to the state machine: `applyLoop` waits until everything before it is applied, then marks it applied (Q12).

### Q10. What does Pre-Vote solve, and what does "leader stickiness" add?

Without Pre-Vote, a node cut off from the cluster keeps timing out, incrementing its term, and starting elections. When it reconnects, its higher term forces the healthy leader to step down, an outage caused by the node that was broken.

Pre-Vote (§9.6) adds a trial round that changes no state: "would you vote for me in term T+1?" Only a quorum of yes answers leads to a real election.

Leader stickiness (`grantPreVoteLocked`) rejects pre-votes while the receiver has recently heard from a live leader. If the leader is alive, the one asking is the node that's cut off.

### Q11. What bug did the WAL-truncation fix address, and why is it subtle?

On a follower, the in-memory log truncated only at a real conflict. But the code handed the leader's *raw* `args.Entries` to `Storage.Save`, and Save replaces everything from `entries[0].Index` onward. Now suppose a stale, reordered AppendEntries carrying entries 1–2 arrives after entries 1–5 were already acknowledged. Memory keeps 1–5, but the disk is cut back to 1–2. After a crash, entries the leader had counted toward a commit quorum are gone.

It's subtle because the in-memory logic was correct and every test passed. The bug lived in the gap between two components that each looked correct. The fix: `TruncateAndAppend` returns exactly the suffix it wrote, and only that suffix is persisted. The `Storage.Save` contract is now documented to match.

### Q12. Why must "applied" be reported by the state machine rather than tracked by Raft?

Raft can only know it *queued* an entry. Earlier code advanced `lastApplied` at queue time, which broke both of its consumers:
- **Linearizable reads** (`WaitApplied`) could read a store that hadn't caught up.
- **Snapshots** could be labelled with an index whose effects the snapshot data didn't include. Compacting at that index then deleted entries that existed nowhere else.

Now there are two cursors. `lastDispatched` is internal. `lastApplied` advances only when the state machine calls `ReportApplied`. Snapshot also refuses an index beyond `lastApplied` (`ErrSnapshotAheadOfApplied`), so this class of bug fails safely instead of silently.

### Q13. How is apply order guaranteed?

Exactly one goroutine sends on `applyCh` (`applyLoop`), draining an ordered queue. Earlier code started a goroutine per commit batch and per snapshot, so the Go scheduler decided delivery order. Snapshots go through the same queue, so a snapshot can never overtake (or be overtaken by) the commands around it.

---

## 3. Reads and linearizability

### Q14. How does a read stay linearizable without going through the log?

Read Index (§6.4, `read_index.go`) has three steps:
1. **Wait until the leader has committed an entry from its own term** (the no-op), so its `commitIndex` includes everything committed before it took office. Record `commitIndex` as the read index.
2. **Confirm leadership:** send a round of heartbeats and require a quorum to answer in the current term. A deposed leader in a minority partition fails this step.
3. **Wait until the state machine has applied** at least the read index, then read locally.

Any write acknowledged before the read began has index ≤ the read index, so the read sees it.

### Q15. Why not leader leases? They avoid the heartbeat round trip.

Leases make correctness depend on bounded clock drift. The leader assumes no other leader can exist until its lease expires, and that assumption breaks if clocks run at different speeds, or if a process pauses (GC, VM migration) past the lease. Read Index is correct under arbitrary timing, so it's the right default.

A lease is a legitimate optimization to add later, but only behind an explicit clock-drift bound and ideally a monotonic clock source. That tension is exactly what the Hybrid Logical Clock work in Milestone 4 will revisit.

### Q16. What does `consistency=stale` give you, precisely?

Nothing beyond "some prefix of committed history". It reads the local state machine with no leadership or catch-up check. It works on any node and costs no network round trip, and it can be arbitrarily behind (for example, on a partitioned follower). It isn't even monotonic across nodes: a client switching nodes can see time go backwards. The end-to-end scripts use it only to check per-node replication. Bounded-staleness follower reads would need the follower to learn a safe read point from the leader.

---

## 4. Durability and crash recovery

### Q17. What exactly is on disk, and what is the commit point?

```text
raft/metadata.json      term, vote, snapshot bounds, which WAL dir and snapshot file are live
raft/snap-<index>.dat   latest snapshot
raft/wal-<base>/        tidwall/wal segments
```

`metadata.json` is the single commit point. A multi-file change writes its new files completely first, then replaces the metadata atomically (write a temp file, fsync it, rename it, fsync the directory). A crash at any point leaves the old state or the new state, never a mix. Files nothing references are deleted on the next open.

### Q18. Why fsync the directory after the rename?

The rename is a change to the *directory*. Without fsyncing the directory, a power loss can drop the rename even though the call returned success, leaving the old file (or no file) in place. The earlier code fsynced files but never directories.

### Q19. Why does the WAL have a base offset?

`tidwall/wal` requires an empty log to start at index 1. When a follower installs a snapshot at index *S* beyond the end of its own log, the next entry is *S+1*, which that library can't write into its existing log. So the follower starts a fresh WAL where WAL index *i* holds Raft index *i + base*, with *base = S*.

The switch to the new WAL is committed by the metadata write, so a crash mid-switch is safe. Before this fix, the follower never persisted the installed snapshot at all, and its durable log was silently unwritable afterwards.

### Q20. Walk me through a full-cluster `SIGKILL` and restart.

1. Each node reopens storage. Leftover temp files and unreferenced directories are removed.
2. `NewRaftNode` restores the term and vote. The persisted commit index is only a lower-bound hint (Q21).
3. It delivers the latest snapshot to `applyCh` first, then every entry up to that hint. The empty store is rebuilt through the normal ordered path.
4. An election happens. The new leader's no-op commits and carries the commit index forward over everything that was committed before the crash.

`test/e2e/crash_recovery_test.sh` does this against real processes with `--snapshot-every=3`. The logs show every node restoring a snapshot and then replaying the log.

### Q21. Why is the commit index persisted only as a hint?

Commit is volatile state in the Raft paper, since it can always be relearned from the leader. Persisting it on every change would add an fsync per commit. So it's refreshed only when term or vote is written anyway. On restart it's a safe lower bound: it was true at some point, and commit only ever increases. The node replays up to it immediately and learns the rest from the leader.

### Q22. What happens when the disk fails?

The node **halts** (fail-stop). If a Save fails, the node stops replying, closes `Done()`, and `Err()` reports the cause. `kv-server` exits. The alternative, continuing after a failed write, means acknowledging votes and entries the disk doesn't hold, which silently breaks every invariant in §2.

Halting a minority of nodes is safe, because the rest of the cluster carries on.

**Follow-up: what about fsync errors on Linux?** After a failed fsync, the kernel may already have dropped the dirty pages, so retrying the fsync can "succeed" without the data being written (the 2018 PostgreSQL fsyncgate incident). Fail-stop, followed by recovery from the log and from peers, is the only safe response. That makes it a design requirement, not merely a conservative choice.

---

## 5. Storage engine and MVCC

### Q23. Why a lock-free skiplist on an arena?

- **Reads never block.** Writers publish with compare-and-swap, and readers just follow pointers.
- **The Go GC has almost nothing to trace.** The arena is one `[]byte`, and nodes, keys and values are offsets inside it, not pointers. Millions of entries don't turn into millions of objects the GC must scan.

Point reads benchmark at about 26 ns with zero allocations.

**The costs:**
- The arena can't free individual entries.
- Memory is bounded (the skiplist returns `ErrArenaFull` when full, rather than panicking).
- Correctness depends on careful atomics.

Two subtle bugs were fixed in the atomics:
- A value's offset and length were stored separately, so a reader could see a new length with an old offset. They're now packed into one 64-bit word.
- Nodes weren't 8-byte aligned, and unaligned 64-bit atomic operations fault on 32-bit ARM.

### Q24. How is arena memory reclaimed if nothing can be freed?

Compaction by copy. `Store.Compact(watermark)` copies only the versions still visible at or above the watermark into a fresh engine, then swaps it in atomically. Readers are never blocked, since they keep using the engine they started on. Writers pause only while the replacement is built.

It's triggered at 75% usage, with hysteresis so a store whose live data sits near the threshold doesn't recompact on every write. It also runs as a retry when a write doesn't fit. If live data alone exceeds the budget, applying the entry fails and the node stops, because skipping a committed entry would make the replicas diverge.

### Q25. Why is the MVCC version the Raft log index?

The log index is already a total order that every replica agrees on. So every replica writes identical versions with no clock involved. Earlier code mixed two timestamp domains: `index*10` for writes and wall-clock nanoseconds for reads and garbage collection. That works by accident until something depends on comparing the two.

**Follow-up: what about multiple ranges?** Each range has its own log *and its own store* (`pkg/sharding`), so versions never collide: a range's versions only live in that range's store. What's missing is *ordering across* ranges: index 7 in range 1 and index 7 in range 2 aren't comparable, so nothing can read a consistent snapshot of two ranges or commit a transaction spanning them. That needs a shared timestamp source, Milestone 4's Hybrid Logical Clock, before cross-range transactions (Milestone 5).

**Follow-up: why a store per range rather than one per node?** Because a snapshot replaces its store's entire contents. With a shared store, installing range 1's snapshot erased range 2's data — an early version of the multi-range code had exactly that bug, now pinned by `TestMultiRaft_RangeSnapshotRestoreIsIsolated`. Per-range stores also give fail-isolation: a range that can't apply halts alone.

### Q26. Why must snapshot restore *replace* the store instead of merging?

A snapshot contains only live data; deleted keys are simply absent. Merging it into a follower that still holds an old version of a key deleted before the snapshot would bring that key back. So restore loads into a fresh engine, verifies the checksum, and swaps. Replacing costs no more than merging did: the fresh engine replaces the allocation a merge would have needed anyway, and restore benchmarks about 16% *faster* than the original merge (buffered reads).

The format has a magic header, an end marker and a CRC-32C, so a truncated or corrupt snapshot is rejected and the store is left untouched.

### Q27. What are the weaknesses of the current snapshot design?

- The whole snapshot is **held in memory** and sent **in one RPC**. For large states you want chunked streaming, with an offset so a transfer can resume after a disconnect.
- Taking a snapshot **blocks applies** while it's exported, because it runs on the applier goroutine. That's what makes it consistent, but it's a latency spike at scale. The fix is a point-in-time view of the store, which the MVCC design already supports by reading at a fixed timestamp.
- `RaftNode.Snapshot` holds `rn.mu` while the snapshot fsyncs.

---

## 6. Client semantics

### Q28. When does a write return success, and what does a 409 or 504 mean?

**Success** means the command was *applied* at the index `Propose` returned, **and** in the same term. A different term at that index means a leadership change replaced the entry, and the client gets **409** (`ErrProposalSuperseded`).

The proposal tracker holds its lock across `Propose` and waiter registration. Without that, a very fast commit could notify before the waiter existed, and a successful write would time out.

**504** means the outcome is unknown: the entry may or may not commit later.

### Q29. So are retries safe? Is this exactly-once?

No, and saying so plainly is the principal answer. A client that retries after a 504 can apply its write **twice**. For puts and deletes, which are idempotent on their final value, that's usually harmless. For anything non-idempotent (increments, compare-and-set, transactions) it's a bug.

Exactly-once needs client sessions, as described in the Raft dissertation §6.3:
- Each client gets an ID and numbers its requests.
- The state machine stores each client's last sequence number and response *as part of the replicated state*, so the table is identical on all replicas and survives snapshots.
- A duplicate returns the cached response instead of being applied again.
- Sessions need expiry, and the expiry must itself be deterministic across replicas.

---

## 7. Verification

### Q30. How do you know this is correct?

Five layers of tests, each aimed at a different kind of bug:
- **Unit tests** for each component's contract.
- **Regression tests** for every bug found, each of which failed before its fix.
- **Deterministic protocol tests** for scenarios too rare to hit at random (Figure 8, conflict hints, fail-stop, boot order).
- **A randomized fault-injection test.** It runs 5 nodes with 5% message drops, duplicates and lost replies, 0–5 ms delays that reorder messages, partitions (including one that cuts off the leader with a single follower), and crash-restarts (including of the leader). A fault is injected every 20–120 ms, and it checks the safety invariants throughout.
- **End-to-end scripts** against real processes.

A 10-seed soak ran about 30,000 acknowledged writes, 116 crashes and 202 partitions, with no violations.

**Follow-up: what's the gap?** It's randomized, not deterministic. A seed reproduces the fault schedule but not the goroutine interleaving. And it checks invariants, not full linearizability of client histories. The next steps are Porcupine over recorded histories, and eventually a deterministic simulator in the style of FoundationDB and TigerBeetle, which works best with the pure-state-machine design from Q3.

### Q31. How do you know the chaos test would catch a bug at all?

I tested the test. Known bugs were injected one at a time:

| Injected bug | Result |
|---|---|
| Original WAL-truncation bug | Caught on every seed |
| Election Restriction removed | Caught |
| Figure-8 rule removed | Not caught, so a deterministic test was added |

The key finding: **the first version of the harness caught none of the protocol bugs.** Random crashes rarely create divergent logs. Adding faults that target the leader (crash it, isolate it with one follower) is what gave the harness teeth. "The tests pass" means little until you've watched them fail.

### Q32. Why did the WAL bug survive the original test suite?

The tests exercised each component in isolation, and each component was correct in isolation. The bug needed a specific *interaction* (a reordered, stale RPC after a fresh one) plus a crash to become visible, and nothing in the suite produced that combination. That's the general argument for fault injection: it explores interactions nobody thought to write down.

---

## 8. Performance

### Q33. Where does write latency go?

The fsync. One append on real disk costs about 6.4 ms, while the in-memory log append is about 100 ns and the Raft processing around it is microseconds. The earlier design did two fsyncs per Save (metadata plus WAL) even when the HardState hadn't changed, including on every heartbeat. Skipping the unchanged metadata write halved append latency (13.3 ms → 6.4 ms, measured with `benchstat`).

### Q34. How would you raise throughput by 10× or more?

In rough order of payoff:
1. **Batching and group commit:** one fsync covers many proposals. This matters most, since fsync dominates.
2. **Pipelining:** don't wait for one AppendEntries round trip before sending the next.
3. **Parallelize the leader's disk write with replication.** The leader can send to followers while it fsyncs locally (Raft dissertation §10.2.1). Its own copy counts toward the quorum only once durable.
4. **A cheaper wire format** than gob.
5. **More ranges (Multi-Raft)**, which spreads load across groups.

Today every `Propose` triggers its own broadcast.

**The principal-level answer is how you'd prove it:** measure first. That means an end-to-end benchmark (YCSB-style, p50/p99, across real processes), a baseline against etcd on the same hardware, and one change at a time with `benchstat`.

### Q35. Did anything get slower, and was that acceptable?

- **Snapshot restore first appeared 30% slower, and that turned out to be the benchmark, not the code.** The benchmark created a 64 MiB destination store each iteration, and restore then built a second fresh engine and threw the first away. It paid for two arenas per restore, and the doubled memory eventually got the process killed on a loaded machine. Measured properly, one fresh engine per restore, it's about 16% faster than the original merge. The lesson worth telling: a surprising benchmark result deserves the same suspicion as a surprising test result.
- **The skiplist read path regressed 36%** in an intermediate version: `Get` stopped returning early when it found the key. `benchstat` caught it before the commit and it was fixed. That's why the benchmark baselines are committed.

---

## 9. Failure modes and operations

### Q36. What happens during a network partition?

The majority side elects a leader, if it doesn't already have one, and keeps serving. A leader on the minority side:
- keeps believing it's the leader, but can't commit, so its writes time out with 504;
- fails the quorum check on linearizable reads, which get 503;
- steps down as soon as it hears a higher term.

Minority-side followers can't elect a leader, and Pre-Vote stops them inflating their terms. When the partition heals, the minority's uncommitted entries are overwritten, and those clients got 504 ("outcome unknown"), never a false success.

### Q37. What about a slow disk, or one slow node?

A slow *follower* doesn't slow commits, because the quorum forms without it. A slow *leader* disk slows everything, since every proposal waits for the leader's fsync (Q34 item 3 is the mitigation). A dead peer doesn't delay heartbeats to the others, because each peer has its own connection and a dial to one never blocks another. The earlier code shared one lock across all dials.

### Q38. How would you operate this in production? What's missing?

- **Metrics:** commit latency, apply lag (commit index minus last applied), leader changes, fsync time, snapshot sizes and timings. Today there's only `/status`.
- **Membership changes** to replace a failed node without downtime (Q39).
- **Alerting on halted nodes,** since fail-stop only helps if someone notices.
- **CI** running the race detector and the chaos test on every change.

---

## 10. Limits, and what comes next

### Q39. How would you add membership changes?

Use single-server changes (Raft dissertation §4.1): add or remove one node at a time, as a special log entry. A node starts using a new configuration as soon as the entry is in its log, not when it commits.

Because the old and new majorities differ by one node, they always overlap, which keeps elections safe. Supporting pieces:
- **A learner (non-voting) phase,** so a new node catches up before it counts toward the quorum.
- **Leader transfer,** for removing the current leader.
- **Guarding against disruptive removed servers,** which the existing leader-stickiness logic already largely covers.

Joint consensus (§6 of the Raft paper) allows arbitrary changes in one step, but it's more complex. This is the most important missing feature, because Milestone 3's range rebalancing depends on it.

### Q40. What would you do differently if you started over?

- Build the Raft core as a **pure state machine** (etcd-style `Ready`) from day one, to enable deterministic simulation.
- Write the **fault-injection harness first**, before the features. Every serious bug found here would have surfaced earlier.
- Specify every **storage contract** (what "durable", "applied" and "replace" mean) in its interface doc before implementing it. Two of the worst bugs were contract mismatches between components that were each correct on their own.

### Q41. What are the known limitations, stated plainly?

- Membership is static.
- The store is bounded by memory.
- Snapshots are sent whole in one RPC.
- There's no exactly-once client session.
- There's no linearizability checker.
- Testing is randomized, not deterministic.
- Proposals aren't batched.
- There are no metrics.
- Multi-Raft is partial: ranges can be hosted and served, but not yet split or moved, and there's no cross-range ordering (Q25).

The full list is in [architecture.md §8](architecture.md#8-known-limitations-and-next-steps). Knowing your system's limits, and the order you'd fix them in, is itself what the interviewer is testing.
