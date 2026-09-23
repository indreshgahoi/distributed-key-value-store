# ADR: Dual-Engine Storage Architecture

## Status

**Decided:** Choice A (Clean Separation / Dual-Engine Architecture).

**Implementation status:** done. Component 1 landed as `pkg/consensus/raft/storage_tidwall.go`'s `TidwallStorage` (§7.4's decision: `tidwall/wal` for entries, a separate fsync'd `metadata.json` for `HardState`) and is wired into `cmd/kv-server/main.go`. Component 2 (Layer 2's file-based MVCC snapshot) was already built. §5's crash-recovery protocol is now real, end-to-end-tested behavior (`TestRaft_NodeReplaysLogFromStorageOnRestart`, `TestStorage_TidwallStorage_CrashReplayAndCompaction`, and the black-box [`test_crash_recovery.sh`](../test_crash_recovery.sh)), not just a target — with one caveat: `RaftNode.Snapshot()`'s 30-second ticker is independent of a crash happening in between, so "zero data loss" means zero loss of anything `Save()` durably wrote, not zero loss of anything ever proposed (an in-flight proposal that never reached `Save()` before a crash is, correctly, lost — see §6's Gate 1).

---

## 1. Context & the core dilemma

In designing a distributed, transactional key-value store backed by Raft consensus, two distinct persistence workloads exist on every node:

**A. The consensus log (Raft storage / "the recipe")**
- **Nature:** strictly sequential, append-only mutation records (`LogEntry`).
- **Lifespan:** short-lived and disposable — once an entry is committed by quorum, applied to the state machine, and included in a snapshot, it's dead weight and can be deleted.
- **Access pattern:** ~100% sequential appends. Reads happen only during crash-recovery reboots or when catching up a partitioned follower.

**B. The state machine (key-value MVCC store / "the cake")**
- **Nature:** cumulative, multi-versioned historical data.
- **Lifespan:** permanent until explicitly deleted by users or pruned by MVCC watermark garbage collection.
- **Access pattern:** high-frequency random point lookups, temporal range scans (`Scan("a", "z")`), and LSM-tree compactions. 95%+ of client query traffic hits this engine.

---

## 2. Evaluation of alternatives

| Dimension | Choice A: Dual-Engine (selected) | Choice B: Unified shared engine | Choice C: In-memory KV + Raft WAL |
|---|---|---|---|
| **Architecture** | Two separate engines (dedicated Raft WAL + dedicated KV store) | Single shared Pebble/RocksDB instance with system key prefixes | In-memory SkipList + durable Raft WAL on disk |
| **Write isolation** | Total isolation — heavy user queries never block Raft heartbeats | User queries and Raft logs compete for write buffers and disk channels | Fast in-memory user writes; disk I/O only on the Raft WAL |
| **Compaction** | Trivial deletion of old WAL segments, no LSM churn | Large log deletions trigger heavy background LSM compactions | In-memory GC + periodic snapshot exports to disk |
| **Adopted by** | etcd, TiKV (Raft-Engine + RocksDB) | CockroachDB (unified Pebble) | Redis Sentinel, etcd in-memory mode |

---

## 3. Decision: Choice A (dual-engine architecture)

We have chosen Choice A: clean separation / dual-engine architecture.

```text
                                [ Physical Machine / Node ]
                                             │
            ┌────────────────────────────────┴────────────────────────────────┐
            ▼                                                                 ▼
    [ Raft Consensus Engine ]                                     [ Layer 2: MVCC Store ]
            │                                                                 │
            ▼ (Sequential appends only)                                       ▼ (Random seeks, MVCC scans)
    +-------------------------------+                             +-------------------------------+
    | Storage Engine 1:             |                             | Storage Engine 2:             |
    | Dedicated Raft Disk WAL       |                             | Ordered KV / LSM Engine       |
    | (pkg/consensus/raft/storage)  |                             | (pkg/storage/raw & mvcc)      |
    +-------------------------------+                             +-------------------------------+
```

**Key architectural reasons:**

1. **Elimination of tail-latency contention.** Raft's heartbeat and commit latencies dictate cluster stability. In a unified engine, an expensive user range scan or large LSM compaction can cause write-stall pauses on disk, triggering false election timeouts. Choice A guarantees Raft dedicated disk channels.
2. **Specialized engine tuning.** The Raft WAL is tuned purely for sequential `writev` + `fsync` throughput. The KV engine is tuned for block cache hit-ratios, bloom filters, and skip-list concurrency.
3. **Zero codec & namespace collisions.** Raft system keys (`!raft:hs`, `!raft:log:<index>`) never share storage with user keys, eliminating any risk of user range scans encountering unescaped internal metadata.
4. **Clean Multi-Raft scaling (Milestone 3).** Co-locating thousands of ranges on a node becomes much easier when range consensus logs are multiplexed into a dedicated WAL subsystem without impacting user table files.

---

## 4. Component boundaries & contracts

**Component 1: Raft storage** (`pkg/consensus/raft/storage.go`)
- **Scope:** owns `currentTerm`, `votedFor`, and the replicated `LogEntry` stream.
- **Contract:** implements the `Storage` interface:
  - `Save(HardState, []LogEntry)` — atomically persists consensus metadata and appends entries.
  - `Entries(low, high, maxBytes)` — reads a log slice for peer replication.
  - `Term(index)` — returns the term at `index`.
  - `CreateSnapshot(meta, data)` / `ApplySnapshot(meta, data)` — prunes compacted entries ≤ `lastIncludedIndex`.
- **Status:** interface and call sites are complete (`Save` is invoked at every point Raft §5.2 requires). The concrete implementation, `KVStorage`, currently persists into an in-memory `raw.SkipListEngine` rather than a disk WAL — see the Status section above.

**Component 2: State machine store** (`pkg/storage/mvcc`)
- **Scope:** owns user keys, versioned values, tombstone flags, and read watermarks.
- **Contract:** implements the `MVCCStore` interface:
  - `Put(key, val, ts)` / `Delete(key, ts)` — writes versioned records.
  - `Get(key, ts)` / `Scan(start, end, ts)` — point and range snapshot reads.
  - `ExportSnapshot(w, ts)` / `RestoreSnapshot(r)` — streams baseline snapshots for Raft compaction.
- **Status:** implemented and disk-backed today via `SaveSnapshotToFile`/`LoadSnapshotFromFile` (`pkg/storage/mvcc/snapshot.go`), on a 30-second ticker in `cmd/kv-server/main.go`. Current file naming is a flat `data_node<N>.snap` in the working directory — simpler than the `data/node_X/mvcc.snap` layout implied below, which is the target once the storage layout is formalized alongside Component 1.

---

## 5. Crash recovery protocol

Node restart reconstructs state deterministically:

$$\text{Durable State at Boot} = \text{Snapshot}(K) + \sum_{i=K+1}^{\text{CommitIndex}} \text{Replay}(\text{LogEntry}_i)$$

1. **Phase 1 (base restore):** Layer 2 loads the baseline snapshot from `data/node_X/mvcc.snap` into memory.
2. **Phase 2 (WAL discovery):** Raft opens its dedicated WAL directory, `data/node_X/raft_wal/` (`NewTidwallStorage`).
3. **Phase 3 (replay loop):** `RaftNode.NewRaftNode` reads `storage.LastIndex()`/`storage.Entries()` and replays everything durably persisted beyond the snapshot boundary back into the in-memory log, then re-applies anything already committed (per the restored `HardState`) but not yet reflected in the state machine, by driving `scheduleApplyLocked` on boot.
4. **Phase 4 (normal operation):** the node rejoins the cluster fully synchronized.

**This is now real, tested behavior**, not a target — `TestRaft_NodeReplaysLogFromStorageOnRestart` covers Phase 3's log replay in isolation, and [`test_crash_recovery.sh`](../test_crash_recovery.sh) covers all four phases end-to-end against the real `kv-server` binary (write, `SIGKILL` all 3 nodes, restart, verify). Getting Phase 3 right took two separate fixes, both caught by tests rather than assumed correct: replaying into `rn.log` alone restores Raft's own bookkeeping but not the state machine's data — a distinct, easy-to-miss second step (re-driving `scheduleApplyLocked`) is required, or the Raft layer recovers cleanly while Layer 2 silently comes back empty.

One honest caveat on "zero data loss": it means zero loss of anything `Save()` durably wrote (per §6's Gate 1 - quorum commit), not zero loss of literally everything ever proposed. A write that was in flight (proposed but not yet durably saved, or saved on this node but not yet part of a quorum-committed entry) when the crash hit is correctly not recoverable - that's what quorum replication across the other nodes is for, not this node's local WAL.

---

## 6. Log Deletion Safety: The 3-Gate Invariant

In distributed systems, premature deletion leads to silent data corruption. To guarantee a log entry is truly "dead weight" and 100% safe to discard from disk, the engine must enforce a strict 3-gate invariant — an entry may only be truncated once it has passed **all three**, in order:

```text
Log Entry Lifecycle (must pass all 3 gates in order):
=========================================================================================>
[ Gate 1: Quorum Commit ] ──► [ Gate 2: State Machine Apply ] ──► [ Gate 3: Disk Snapshot ] ──► [ TRUNCATE / PURGE ]
  index <= commitIndex          index <= lastApplied                index <= lastIncludedIndex   (now safe to delete!)
```

**Why all three gates are mandatory:**

1. **Gate 1 — Quorum commit** (`index <= commitIndex`)
   If violated: deleting uncommitted entries would discard data that other peers may have already decided on.
2. **Gate 2 — State machine applied** (`index <= lastApplied`)
   If violated: if an entry is committed by Raft but the state machine (Layer 2 MVCC) hasn't executed it yet, deleting the entry means the command never executes. The data is lost.
3. **Gate 3 — Included in a durable disk snapshot** (`index <= lastIncludedIndex`)
   This is the critical gate. Even if an entry was applied to the in-memory SkipList, if the process crashes before an updated snapshot is written to disk, that in-memory state is wiped. If the log entry was already deleted from disk, the node cannot replay it on reboot. **You can never truncate entries ahead of the latest durable snapshot.**

**The mathematical deletion boundary:**

$$\text{Max Safe Deletion Index} = \text{Snapshot}.\text{LastIncludedIndex}$$

Whenever a snapshot finishes writing and fsyncing to disk, it establishes a new compaction horizon. Every entry at or below that horizon is provably redundant, because the snapshot file now contains the cumulative result of all those operations.

**Status:** Gates 1 and 2 (`commitIndex`, `lastApplied`) are enforced correctly today — `checkAdvanceCommitIndexLocked` and `scheduleApplyLocked` in `pkg/consensus/raft/node.go` never let one outrun the other. Gate 3 is enforced correctly *relative to the in-memory log* (`RaftLog.CompactLog` only runs from `RaftNode.Snapshot`, which is only called with an already-exported snapshot's index) — but since neither the snapshot nor the log Gate 3 protects is actually durable yet (§5 above), the invariant currently only protects against corrupting *in-memory* state, not a crash mid-compaction. It becomes a true disk-safety guarantee once §7 below is implemented.

---

## 7. RFC: Raft WAL Backend Selection

### 7.1 Problem statement

Writing a custom segmented WAL with file rolling, CRC32 record headers, and head/tail truncation is a well-trodden problem. Three off-the-shelf Go options were evaluated as the concrete implementation behind Component 1 (§4) — the thing `KVStorage` should persist into instead of an in-memory `raw.SkipListEngine`.

### 7.2 Options considered

**Option 1 — [`tidwall/wal`](https://github.com/tidwall/wal)**
A segmented, append-only log written in pure Go, zero third-party dependencies.

- `TruncateFront(index)` — O(1) space reclamation: identifies closed segment files entirely below `index` and `os.Remove()`s them outright, rather than the tombstone-and-compact-later behavior of an LSM/B-tree.
- `TruncateBack(index)` — O(1) rollback of uncommitted entries when a new leader's log conflicts with ours (Log Matching Invariant §3).
- Segments default to 20MB and roll automatically.
- `Options.NoSync` (default `false`, i.e. fsync **on**) governs durability per the package docs: *"NoSync disables fsync after writes. This is less durable and puts the log at risk of data loss when there's a server crash."* Default behavior is what Raft §5.2 requires.

**Option 2 — [`go.etcd.io/bbolt`](https://github.com/etcd-io/bbolt)** (the `hashicorp/raft-boltdb` pattern)
Used by Consul, Vault, and Nomad. An embedded single-file B+-tree; log entries stored as `key = BigEndian(index)` in a bucket, HardState in a second bucket in the same file.

- Compaction deletes keys below the horizon, but BoltDB's freelist-based delete does **not** shrink the file on disk immediately — the reclaimed pages are reused for future writes, not returned to the OS, unlike Option 1's immediate `os.Remove()`.

**Option 3 — [`go.etcd.io/etcd/server/v3/wal`](https://github.com/etcd-io/etcd)**
What etcd and Kubernetes actually run: pre-allocated 64MB segments, length-prefixed records with CRC32-IEEE checksums, explicit snapshot barrier records. Pulls in etcd's internal logging and protobuf dependencies — heavy for a single-purpose adapter.

### 7.3 Verifying the proposed `tidwall/wal` adapter

Before deciding, the conceptual adapter sketched for Option 1 was checked against `tidwall/wal`'s actual public API (`pkg.go.dev/github.com/tidwall/wal`), rather than accepting the sketch as-is. Two gaps surfaced:

1. **`HardState` is never actually persisted.** `tidwall/wal`'s entire API is index-sequential entries (`Write`, `WriteBatch`, `Read`, `TruncateFront`, `TruncateBack`) — there is no side-channel for small mutable metadata like `{Term, Vote, Commit}`. The sketched adapter's `Save(hs HardState, entries []LogEntry)` loops over `entries` and writes them, but never writes `hs` anywhere. Since `Term`/`Vote` are exactly the fields whose loss causes the double-voting bug §7 spent a full section explaining, an adapter that silently drops them on every call reintroduces that bug rather than fixing it. **`tidwall/wal` needs to be paired with a second, independent durable store for `HardState`** — it doesn't provide one itself.
2. **The write loop should be `WriteBatch`, not per-entry `Write`.** Per the docs, `Sync()`'s doc comment ("not necessary when `NoSync` is false") implies each `Write`/`WriteBatch` call fsyncs on its own when syncing is enabled. Looping `Write()` once per entry means one fsync per entry; `Save()` in this codebase is routinely called with multiple entries at once (e.g. `HandleAppendEntries` persisting a whole batch of replicated entries). The adapter should collect entries into a `wal.Batch` and call `WriteBatch` once, to get one fsync per `Save()` call instead of N.

Two more things worth flagging honestly rather than repeating as fact: the claim that `tidwall/wal` is "used by many Go distributed systems, Raft implementations, and message queues" could not be substantiated from the library's own README — no adopters are listed there, so treat that specific claim as unverified until checked against something more concrete than marketing copy. Likewise, no crash-recovery / corrupt-tail-truncation behavior on `Open` is documented in what's publicly visible — worth a targeted test (kill `-9` mid-`Write`, then `Open` and check `LastIndex()`) before relying on it, not an assumption.

### 7.4 Decision

**Conditionally accept `tidwall/wal`** for the log-entry portion of Component 1, with two required amendments to the sketch in §7.3, not as originally proposed:

1. Persist `HardState` through a **second, independent mechanism** — not through the WAL. The simplest option that stays consistent with this codebase's existing conventions: a small `hardstate_node<N>.json` file, rewritten via the same temp-file + `fsync` + atomic-rename pattern already used by `SaveSnapshotToFile` (`pkg/storage/mvcc/snapshot.go`). `HardState` is a handful of bytes, so a full-file rewrite per change is cheap; the correctness property that matters is that the rewrite is atomic (no reader ever observes a half-written file), which the existing rename-based pattern already guarantees elsewhere in this codebase.
2. Batch entries into one `wal.Batch` + one `WriteBatch()` call per `Save()` invocation, not a per-entry `Write()` loop.

This was chosen over Option 2 (bbolt) specifically because it matches this ADR's own stated design goal in §3.2 — "the Raft WAL is tuned purely for sequential `writev` + `fsync` throughput" — better than a general-purpose B+-tree does; and it was chosen over Option 3 (etcd's own `wal`) purely on dependency weight, which was never in question. The trade-off accepted by *not* choosing bbolt is losing the one property bbolt would have given for free: a single atomic transaction spanning both `HardState` and log entries. Splitting the two into independent files means reasoning carefully about relative write ordering on every `Save()` call (which one is written first matters for what state a crash between the two writes leaves behind) — a real, non-trivial cost of this decision, not a detail to gloss over when this gets implemented.

### 7.5 Open questions before implementation

- Confirm `tidwall/wal`'s actual crash-recovery behavior empirically (kill mid-write, reopen, check state) rather than assuming it — see §7.3.
- Decide and document the write ordering between the `HardState` file and the WAL on each `Save()` call, and what a crash between the two writes leaves a restarting node believing.
- Set `Options.AllowEmpty: true` if the log should ever be fully truncatable up to its current tip (e.g. compacting immediately after a snapshot with no new entries since) — the default refuses to let a `TruncateFront`/`TruncateBack` empty the log entirely.
```text
data/node_1/raft_storage/
├── 00000000000000000001.wal    (Segment 1: automatically managed by tidwall/wal)
├── 00000000000000010001.wal    (Segment 2: automatically rolled at 20MB)
├── metadata.json               (Durable HardState: term, vote, commit, snapshot bounds)
└── state.snap                  (Durable Layer 2 MVCC snapshot)
```
