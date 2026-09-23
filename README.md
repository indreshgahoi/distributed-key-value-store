# Distributed Key-Value Store

A distributed, transactional key-value database built from scratch in Go, layer by layer — each layer independently testable and providing a specific guarantee to the layer above it.

- **[docs/architecture.md](docs/architecture.md)** — start here: how the system works end to end, the invariants and where each is enforced, durability and recovery, and how correctness is verified (including fault-injection testing and mutation checks that prove the tests catch real bugs).
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
- [ ] **Phase 3.2 — Multi-Raft node & RPC multiplexing** (`pkg/sharding/multi_node.go`): co-locate multiple independent Raft groups on one node
- [ ] **Phase 3.3 — Dynamic range splitting via consensus** (`pkg/sharding/split.go`): `SplitCommand` proposed and committed through Raft
- [ ] **Phase 3.4 — Server integration & multi-range client routing** (`cmd/kv-server`): requests to any node auto-route to the correct range leader

Design notes and roadmap: [docs/milestoneThree.md](docs/milestoneThree.md).

## Package structure

```text
distributed-key-value-store/
├── cmd/kv-server/                 # Node daemon
│   ├── main.go                    # wiring, fail-stop, graceful shutdown
│   ├── config.go                  # flags + validation
│   ├── api.go                     # HTTP API: /put /delete /get /status, leader redirect
│   ├── proposals.go               # waits for a write to be *applied*, not just proposed
│   ├── statemachine.go            # single writer: applies entries, snapshots, compacts the store
│   └── command.go                 # replicated command format
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
│   ├── sharding/                  # 🚧 Milestone 3: range descriptors + O(log N) router
│   └── storage/
│       ├── raw/                   # Layer 0: lock-free skiplist on an arena allocator
│       ├── codec/                 # Layer 1: order-preserving key/value encoding
│       └── mvcc/                  # Layer 2: versioned store, checksummed snapshots, compaction
├── test_cluster.sh                # end-to-end smoke test over real HTTP/TCP
└── test_crash_recovery.sh         # SIGKILL every node, restart, verify data survived
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

[`test_cluster.sh`](test_cluster.sh) automates the manual 3-terminal walkthrough above into a single black-box script: it builds `kv-server`, boots a 3-node cluster in the background, and exercises it over real HTTP/TCP end-to-end (not the in-process simulated network the `pkg/consensus/raft` tests use).

```bash
./test_cluster.sh
```

What it checks, in order:
1. **Leader election** — polls `/status` on all three ports (after a fixed 1s warm-up) and fails if no node reports `is_leader:true`.
2. **Write** — `POST /put` on the leader returns `"status":"committed"` (only once the write is applied).
3. **Replication** — a local (`consistency=stale`) read on every node returns the value.
4. **Linearizable reads** — served by the leader; `curl -L` via a follower follows its redirect.
5. **Follower redirect** — `POST /put` on a follower returns `307` pointing at the leader.

It always cleans up after itself (`trap cleanup EXIT`): kills all three background server processes and removes the built binary and `data/` directory, even if an assertion fails partway through and the script exits early via `set -e`.

### Crash-recovery smoke test

[`test_crash_recovery.sh`](test_crash_recovery.sh) is the direct answer to "if all my nodes die, can the data survive?" It writes to a live 3-node cluster, `SIGKILL`s all three processes (no graceful shutdown — simulating a real crash), restarts them from the same on-disk `data/` directories, and verifies the data and cluster are still there.

```bash
./test_crash_recovery.sh
```

It runs with `--snapshot-every=3`, so recovery exercises both paths: each node's empty in-memory store is rebuilt from its latest snapshot, then from the WAL entries after it.

This is a smoke test, not a substitute for `pkg/consensus/raft`'s test suite — it has no partition/failover coverage and only checks the happy path over a real network stack. Its value is specifically that it exercises the parts the in-process simulated-network tests can't: the actual `cmd/kv-server` binary, its flag parsing, the real `TCPTransport`/`net/rpc` wiring, and the HTTP client API — which is exactly where the peer-address bug (main.go's `--peers` parsing) and the heartbeat-interval unit bug were originally found.

### Run tests

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
# Sharding (Milestone 3, Phase 3.1) — routing correctness + concurrency, in isolation
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
| mvcc — snapshot restore | ~14 ms/op (10,000 keys into a fresh engine, verified, then swapped in — replaces state rather than merging) | [bench/BenchmarkMVCC_SnapshotRestore.txt](bench/BenchmarkMVCC_SnapshotRestore.txt) |
| raft — leader election | ~85 ms/op (time-to-first-leader on a fresh cluster, incl. the Pre-Vote round-trip) | [bench/BenchmarkRaft_LeaderElection.txt](bench/BenchmarkRaft_LeaderElection.txt) |
| raft — WAL append (fsync'd) | ~6.4 ms/op (one fsync per `Save()`; unchanged HardState is no longer rewritten — was ~13 ms with two) | [bench/BenchmarkTidwallStorage_Append.txt](bench/BenchmarkTidwallStorage_Append.txt) |
| raft — WAL sequential read | ~950 ns/op (10-entry range read under concurrent load) | [bench/BenchmarkTidwallStorage_SequentialRead.txt](bench/BenchmarkTidwallStorage_SequentialRead.txt) |

Layer 2's `Get` isn't zero-allocation like the layers below it — it allocates a seek-key buffer, a key-decode scratch buffer, and clones the returned payload to protect the caller from the storage engine's internal memory. That's expected at this layer, not a regression.

`BenchmarkRaft_ConcurrentProposals` must be run with a fixed `-benchtime` (e.g. `-benchtime=600x`) — see [bench/README.md](bench/README.md) for why.

## License

[MIT](LICENSE)
