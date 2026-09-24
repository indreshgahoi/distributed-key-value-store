# Distributed Key-Value Store

A distributed, transactional key-value database built from scratch in Go, layer by layer — each layer independently testable and providing a specific guarantee to the layer above it.

- **[docs/architecture.md](docs/architecture.md)** — start here: how the system works end to end, the invariants and where each is enforced, durability and recovery, and how correctness is verified (including fault-injection testing and mutation checks that prove the tests catch real bugs).
- [docs/faq.md](docs/faq.md) — design-review FAQ: 41 questions a senior reviewer would ask (correctness, durability, reads, verification, performance, limits), each answered with the code that implements it.
- [docs/concepts.md](docs/concepts.md) — a from-first-principles primer on *why* each layer (replication, consensus, partitioning, MVCC, clock skew, 2PC) exists.

## Roadmap

| # | Milestone | Status |
|---|---|---|
| 1 | Local MVCC Storage Engine (Single Node) | ✅ Done |
| 2 | Single Group Raft Consensus | ✅ Done |
| 3 | Multi-Raft and Range Sharding | 🚧 In progress |
| 4 | Hybrid Logical Clock (HLC) | ⬜ Not started |
| 5 | Multi-Range 2PC (Percolator Model) | ⬜ Not started |

Full roadmap notes: [docs/milestones.md](docs/milestones.md).

### Milestone 1 breakdown — Local MVCC Storage Engine

- [x] **Layer 0 — Raw ordered byte storage engine** (`pkg/storage/raw`): lock-free concurrent skip list over an arena allocator
  - [x] Arena-based bump allocator (no per-node GC pressure)
  - [x] `Put` / `Get` / `Delete` / `Iterator` (`Seek`, `First`, `Next`)
  - [x] Lock-free concurrent writes via CAS retry loops
  - [ ] On-disk backend
- [x] **Layer 1 — Binary encoding & serialization** (`pkg/storage/codec`): memcmp-safe key escaping, inverted-timestamp packing
  - [x] `EncodeKeyAppend` / `DecodeKey` — zero-collision key escaping + bit-inverted timestamp for descending version order
  - [x] `EncodeValueAppend` / `DecodeValue` — OpType header (Put/Delete) + zero-copy value payload
  - [x] Zero-allocation hot path via caller-supplied buffers
- [x] **Layer 2 — MVCC protocol & snapshot engine** (`pkg/storage/mvcc`): versioned Put, tombstone Delete, snapshot reads
  - [x] `Put` / `Delete` — versioned writes via Layer 1 encoding
  - [x] `Get` — snapshot point-read, tombstone-aware, resolves newest version at or before `readTS`
  - [x] `Scan` — snapshot range scan, deduplicates shadowed versions, skips tombstones
  - [x] `CompactBelowWatermark` — watermark-based GC that purges versions shadowed below a safe read timestamp

Design notes for this milestone: [docs/milestoneOne.md](docs/milestoneOne.md).

### Milestone 2 breakdown — Single Group Raft Consensus

- [x] **Leader election** (`election.go`): randomized timeouts, durable votes, the Election Restriction
  - [x] **Pre-Vote** (Raft §9.6) with leader stickiness: a partitioned node can't inflate its term and depose a healthy leader on reconnect
  - [x] **No-op on election** (§8): a new leader commits an entry from its own term before serving reads
- [x] **Log replication** (`replication.go`): quorum commit restricted to current-term entries (Figure 8), bounded-size `AppendEntries`
  - [x] **Fast log backtracking** (§5.3): followers return conflict hints so the leader skips a term per round trip
- [x] **Ordered apply pipeline** (`apply.go`): one goroutine delivers committed entries in log order; `lastApplied` advances only when the state machine reports it applied an entry (`ReportApplied`)
- [x] **Linearizable reads** (`read_index.go`): Read Index (§6.4) — current-term commit + quorum leadership check + wait for the state machine
- [x] **Durable storage** (`storage_tidwall.go`): WAL via `tidwall/wal`, `metadata.json` as the single atomic commit point (fsync file + directory), versioned on-disk format, crash-leftover cleanup — see the design record [docs/milestoneTwoDurableChoice.md](docs/milestoneTwoDurableChoice.md) and its update in [docs/architecture.md §5](docs/architecture.md#5-durability-and-recovery)
- [x] **Snapshots** (`snapshot.go`): log compaction; `InstallSnapshot` (one in flight per peer) durably installed on followers, keeping or discarding the log suffix per §7; on boot the snapshot is replayed to the state machine before the log
- [x] **Fail-stop on storage errors**: a node that can't persist halts instead of acknowledging state its disk doesn't hold
- [x] **Network transport** (`transport.go`): TCP `net/rpc`, one connection per peer (a dead peer never delays heartbeats to others)
- [x] **Node daemon** (`cmd/kv-server`): HTTP API with leader redirects, single-writer state machine, fail-stop and graceful shutdown
- [x] **Verification**: unit + regression tests for every bug found, deterministic protocol tests, a randomized fault-injection (chaos) test, and end-to-end scripts that `SIGKILL` a real cluster — see [docs/architecture.md §7](docs/architecture.md#7-how-correctness-is-verified)

Design notes and the original test-case catalog: [docs/milestoneTwo.md](docs/milestoneTwo.md).

### Milestone 3 breakdown — Multi-Raft and Range Sharding

- [x] **Phase 3.1 — Range descriptors & the range router** (`pkg/sharding`): `O(log N)` binary-search routing over sorted, disjoint `[StartKey, EndKey)` intervals
  - [x] `RangeDescriptor` — range bounds, replica peers, cached leader; `Contains`/`Validate`/`Clone`
  - [x] `RangeRouter.UpdateTable` — atomically installs a new routing table, rejecting any gap or overlap in keyspace coverage
  - [x] `RangeRouter.FindRange` — `O(log N)` binary-search lookup, thread-safe for concurrent read-heavy client workloads
- [x] **Phase 3.2 — Multi-Raft node & RPC multiplexing** (`pkg/sharding`): many independent range replicas on one node
  - [x] Each range is a complete replica (`pkg/replica`): its own Raft group, store, state machine and proposal tracker — so one range's snapshot can never touch another's data, and a failing range halts alone
  - [x] One listener and one pooled connection per node pair carry every range's RPCs (`transport.go`, `pkg/rpcclient`); an RPC for an unknown range can't tear down the shared connection
  - [x] `Put` / `Delete` return once applied; `Get` is a linearizable per-range Read Index read; writes routed to the wrong range are rejected at apply time
  - [x] Tested in-process and over real TCP, including a lagging node catching up via `InstallSnapshot` through the multiplexed transport
- [ ] **Phase 3.3 — Dynamic range splitting via consensus** (`pkg/sharding/split.go`): `SplitCommand` proposed and committed through Raft
- [ ] **Phase 3.4 — Server integration & multi-range client routing** (`cmd/kv-server`): requests to any node auto-route to the correct range leader

Design notes and roadmap: [docs/milestoneThree.md](docs/milestoneThree.md).

## Package structure

```text
distributed-key-value-store/
├── cmd/kv-server/                 # Node daemon
│   ├── main.go                    # wiring, fail-stop, graceful shutdown
│   ├── config.go                  # flags + validation
│   └── api.go                     # HTTP API: /put /delete /get /status, leader redirect
├── pkg/
│   ├── consensus/raft/            # Milestone 2: Raft (file-by-file map in docs/architecture.md §4)
│   │   ├── node.go                # RaftNode, recovery, event loop, persistence, fail-stop
│   │   ├── election.go            # Pre-Vote, elections, votes, becoming leader (+ no-op)
│   │   ├── replication.go         # Propose, AppendEntries, fast backtracking, commit
│   │   ├── snapshot.go            # compaction, InstallSnapshot
│   │   ├── apply.go               # ordered apply pipeline, ReportApplied / WaitApplied
│   │   ├── read_index.go          # linearizable reads
│   │   ├── log.go                 # in-memory log with snapshot sentinel
│   │   ├── storage.go             # Storage contract
│   │   ├── storage_tidwall.go     # on-disk Storage (WAL + atomic metadata)
│   │   ├── storage_kv.go          # Storage over a Layer 0 engine (used by tests)
│   │   ├── transport.go, rpc.go   # TCP RPC and message types
│   │   ├── chaos_test.go          # randomized fault-injection test
│   │   └── *_test.go              # unit, regression, and protocol tests
│   ├── replica/                   # KV state machine run by one Raft group: command format,
│   │                              #   single-writer applier (snapshots, compaction), proposal tracker
│   ├── rpcclient/                 # per-peer pooled net/rpc connections, shared by all transports
│   ├── sharding/                  # 🚧 Milestone 3
│   │   ├── types.go, router.go    # range descriptors + O(log N) router
│   │   ├── multi_node.go          # MultiRaftNode: per-range replicas, Put/Delete/Get routed by key
│   │   └── transport.go           # range-tagged RPCs multiplexed over one connection per peer
│   └── storage/
│       ├── raw/                   # Layer 0: lock-free skiplist on an arena allocator
│       ├── codec/                 # Layer 1: order-preserving key/value encoding
│       └── mvcc/                  # Layer 2: versioned store, checksummed snapshots, compaction
├── test/e2e/                      # end-to-end tests against real processes (see its README)
│   ├── cluster_test.sh            # smoke test over real HTTP/TCP
│   └── crash_recovery_test.sh     # SIGKILL every node, restart, verify data survived
└── Makefile                       # make test | test-race | chaos | e2e | bench | lint | ci
```

## Getting started

### Prerequisites

- Go 1.27+

### Build

```bash
go build ./...
```

### Build KV Server
```bash
go build -o kv-server ./cmd/kv-server
```
### Run the KV server

Each node persists its Raft state under `data/node_<id>/raft/` by default (`--data-dir` to change the base, or `--node-dir` to override the path entirely) — layout in [docs/architecture.md §5](docs/architecture.md#5-durability-and-recovery). Add `--http-peers=1=127.0.0.1:9001,2=127.0.0.1:9002,3=127.0.0.1:9003` so followers can redirect clients to the leader (`307` + `Location`); without it they answer `503` with the leader's ID.

Terminal 1
```bash
./kv-server \
  --id=1 \
  --raft-addr=127.0.0.1:8001 \
  --http-addr=:9001 \
  --peers=1=127.0.0.1:8001,2=127.0.0.1:8002,3=127.0.0.1:8003 \
  --heartbeat-interval=40ms \
  --election-timeout-min=120ms \
  --election-timeout-max=240ms
```

Terminal 2
```bash
./kv-server \
  --id=2 \
  --raft-addr=127.0.0.1:8002 \
  --http-addr=:9002 \
  --peers=1=127.0.0.1:8001,2=127.0.0.1:8002,3=127.0.0.1:8003 \
  --heartbeat-interval=40ms \
  --election-timeout-min=120ms \
  --election-timeout-max=240ms
```

Terminal 3
```bash
./kv-server \
  --id=3 \
  --raft-addr=127.0.0.1:8003 \
  --http-addr=:9003 \
  --peers=1=127.0.0.1:8001,2=127.0.0.1:8002,3=127.0.0.1:8003 \
  --heartbeat-interval=40ms \
  --election-timeout-min=120ms \
  --election-timeout-max=240ms
```

Use it:
```bash
curl -s -X POST localhost:9001/put -d '{"key":"greeting","value":"hello"}'   # write (on the leader)
curl -s 'localhost:9001/get?key=greeting'                                     # linearizable read (leader)
curl -s 'localhost:9002/get?key=greeting&consistency=stale'                   # local read, any node
curl -s -X POST localhost:9001/delete -d '{"key":"greeting"}'                 # delete
for port in 9001 9002 9003; do curl -s localhost:$port/status; done           # role, term, commit/applied index
```

### Automated cluster smoke test

[`cluster_test.sh`](test/e2e/cluster_test.sh) automates the manual 3-terminal walkthrough above into a single black-box script: it builds `kv-server`, boots a 3-node cluster in the background, and exercises it over real HTTP/TCP end-to-end (not the in-process simulated network the `pkg/consensus/raft` tests use).

```bash
make e2e   # or: test/e2e/cluster_test.sh
```

What it checks, in order:
1. **Leader election** — polls `/status` on all three ports (after a fixed 1s warm-up) and fails if no node reports `is_leader:true`.
2. **Write** — `POST /put` on the leader returns `"status":"committed"` (only once the write is applied).
3. **Replication** — a local (`consistency=stale`) read on every node returns the value.
4. **Linearizable reads** — served by the leader; `curl -L` via a follower follows its redirect.
5. **Follower redirect** — `POST /put` on a follower returns `307` pointing at the leader.

Each run works in its own temporary directory (binary, node data, logs) and cleans up on exit, even if an assertion fails partway through, so it never touches the working tree. `KEEP_WORK_DIR=1` keeps it for debugging.

### Crash-recovery smoke test

[`crash_recovery_test.sh`](test/e2e/crash_recovery_test.sh) is the direct answer to "if all my nodes die, can the data survive?" It writes to a live 3-node cluster, `SIGKILL`s all three processes (no graceful shutdown — simulating a real crash), restarts them over the same data directories, and verifies the data and cluster are still there.

```bash
test/e2e/crash_recovery_test.sh
```

It runs with `--snapshot-every=3`, so recovery exercises both paths: each node's empty in-memory store is rebuilt from its latest snapshot, then from the WAL entries after it.

This is a smoke test, not a substitute for `pkg/consensus/raft`'s test suite — it has no partition/failover coverage and only checks the happy path over a real network stack. Its value is specifically that it exercises the parts the in-process simulated-network tests can't: the actual `cmd/kv-server` binary, its flag parsing, the real `TCPTransport`/`net/rpc` wiring, and the HTTP client API — which is exactly where the peer-address bug (main.go's `--peers` parsing) and the heartbeat-interval unit bug were originally found.

### Run tests

Each tier has one command (`make help` lists them):

```bash
make test        # unit + protocol tests, short chaos run
make test-race   # full Go suite under the race detector
make chaos       # fault-injection soak (CHAOS_SEEDS=10 CHAOS_SECONDS=6 by default)
make e2e         # real processes over TCP/HTTP, incl. SIGKILL-all-nodes recovery
make bench       # all benchmarks; compare with benchstat (bench/README.md)
make ci          # lint + test-race + e2e
```

Unit and protocol tests live next to the code they test (`*_test.go`), as is idiomatic Go; only tests that need real processes live in [`test/e2e/`](test/e2e/). The equivalent raw commands:

```bash
# Full unit test suite, verbose
go test -v ./...

# Race detector across all packages (includes a short chaos run)
go test -race ./...

# Longer randomized fault-injection soak; a failure prints its seed
RAFT_CHAOS_SEEDS=10 RAFT_CHAOS_SECONDS=6 go test -run TestRaft_Chaos -v ./pkg/consensus/raft/
RAFT_CHAOS_SEED=<seed> go test -run TestRaft_Chaos -v ./pkg/consensus/raft/   # replay one

# Benchmarks: throughput and allocations
go test -bench=BenchmarkSkipList_ConcurrentReads -benchmem -run='^$' -v ./pkg/storage/raw/...
go test -bench=BenchmarkCodec_ZeroAllocEncode -benchmem -run='^$' -v ./pkg/storage/codec/...
go test -bench=BenchmarkMVCC_SnapshotPointGet -benchmem -run='^$' -v ./pkg/storage/mvcc/...
go test -bench=BenchmarkRaft_LogAppend -benchmem -run='^$' -v ./pkg/consensus/raft/...
go test -bench=BenchmarkRaft_SequentialProposals -benchmem -run='^$' -v ./pkg/consensus/raft/...
# BenchmarkRaft_ConcurrentProposals needs a fixed -benchtime — default adaptive
# scaling fills the test cluster's fixed-size storage arena. See below.
go test -bench=BenchmarkRaft_ConcurrentProposals -benchmem -benchtime=600x -run='^$' -v ./pkg/consensus/raft/...
go test -bench=BenchmarkMVCC_SnapshotExport -benchmem -run='^$' -v ./pkg/consensus/raft/...
go test -bench=BenchmarkMVCC_SnapshotRestore -benchmem -run='^$' -v ./pkg/consensus/raft/...
go test -bench=BenchmarkRaft_LeaderElection -benchmem -run='^$' -v ./pkg/consensus/raft/...
go test -bench=BenchmarkTidwallStorage_Append -benchmem -run='^$' -v ./pkg/consensus/raft/...
go test -bench=BenchmarkTidwallStorage_SequentialRead -benchmem -run='^$' -v ./pkg/consensus/raft/...
go test -bench=BenchmarkRangeRouter_FindRange_1000Ranges -benchmem -run='^$' -v ./pkg/sharding/...
go test -bench=BenchmarkRangeRouter_FindRange_10000Ranges -benchmem -run='^$' -v ./pkg/sharding/...
```

```bash
# Sharding (Milestone 3) — routing, then multi-range replicas in-process and over TCP
go test -v ./pkg/sharding/...
go test -race -run TestRangeRouter_ConcurrentReadsAndUpdates ./pkg/sharding/...
```

Raft-specific test-case catalog (what each test verifies) and a from-first-principles Raft primer: [docs/milestoneTwo.md](docs/milestoneTwo.md#6-testing--verification).

## Benchmarks

Benchmark baselines are committed under [bench/](bench/) so throughput and allocation counts can be diffed across commits with `benchstat` instead of relying on memory. See [bench/README.md](bench/README.md) for how to update a baseline and compare it against history.

| Layer | Baseline | Detail |
|---|---|---|
| 0 — `raw` | 0 B/op, 0 allocs/op (concurrent `Get`) | [bench/BenchmarkSkipList_ConcurrentReads.txt](bench/BenchmarkSkipList_ConcurrentReads.txt) |
| 1 — `codec` | 0 B/op, 0 allocs/op (`EncodeKeyAppend` with reused buffer) | [bench/BenchmarkCodec_ZeroAllocEncode.txt](bench/BenchmarkCodec_ZeroAllocEncode.txt) |
| 2 — `mvcc` | ~200 ns/op, 96 B/op, 4 allocs/op (snapshot `Get` resolving the newest of 10 versions) | [bench/BenchmarkMVCC_SnapshotPointGet.txt](bench/BenchmarkMVCC_SnapshotPointGet.txt) |
| raft — log append | ~110 ns/op, 0 allocs/op (in-memory, no network) | [bench/BenchmarkRaft_LogAppend.txt](bench/BenchmarkRaft_LogAppend.txt) |
| raft — sequential propose | ~1.26 ms/op (full propose → quorum → commit → apply, incl. `KVStorage` persistence) | [bench/BenchmarkRaft_SequentialProposals.txt](bench/BenchmarkRaft_SequentialProposals.txt) |
| raft — concurrent propose | ~29 µs/op under contention (capped at 600 iterations, see note) | [bench/BenchmarkRaft_ConcurrentProposals.txt](bench/BenchmarkRaft_ConcurrentProposals.txt) |
| mvcc — snapshot export | ~0.9 ms/op (streaming 10,000 live keys, buffered + CRC-32C) | [bench/BenchmarkMVCC_SnapshotExport.txt](bench/BenchmarkMVCC_SnapshotExport.txt) |
| mvcc — snapshot restore | ~9.3 ms/op, 68 MB/op (10,000 keys into a fresh 64 MiB engine, checksum-verified, then swapped in — replaces state rather than merging) | [bench/BenchmarkMVCC_SnapshotRestore.txt](bench/BenchmarkMVCC_SnapshotRestore.txt) |
| raft — leader election | ~85 ms/op (time-to-first-leader on a fresh cluster, incl. the Pre-Vote round-trip) | [bench/BenchmarkRaft_LeaderElection.txt](bench/BenchmarkRaft_LeaderElection.txt) |
| raft — WAL append (fsync'd) | ~6.4 ms/op (one fsync per `Save()`; unchanged HardState is no longer rewritten — was ~13 ms with two) | [bench/BenchmarkTidwallStorage_Append.txt](bench/BenchmarkTidwallStorage_Append.txt) |
| raft — WAL sequential read | ~950 ns/op (10-entry range read under concurrent load) | [bench/BenchmarkTidwallStorage_SequentialRead.txt](bench/BenchmarkTidwallStorage_SequentialRead.txt) |

Layer 2's `Get` isn't zero-allocation like the layers below it — it allocates a seek-key buffer, a key-decode scratch buffer, and clones the returned payload to protect the caller from the storage engine's internal memory. That's expected at this layer, not a regression.

`BenchmarkRaft_ConcurrentProposals` must be run with a fixed `-benchtime` (e.g. `-benchtime=600x`) — see [bench/README.md](bench/README.md) for why.

## License

[MIT](LICENSE)
