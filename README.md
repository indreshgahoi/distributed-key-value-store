# Distributed Key-Value Store

A distributed, transactional key-value database built from scratch in Go, layer by layer — each layer independently testable and providing a specific guarantee to the layer above it. See [docs/concepts.md](docs/concepts.md) for a from-first-principles primer on *why* each layer (replication, consensus, partitioning, MVCC, clock skew, 2PC) exists.

## Roadmap

| # | Milestone | Status |
|---|---|---|
| 1 | Local MVCC Storage Engine (Single Node) | ✅ Done |
| 2 | Single Group Raft Consensus | 🚧 In progress |
| 3 | Multi-Raft and Range Sharding | ⬜ Not started |
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

- [x] **Leader election** (`pkg/consensus/raft`): randomized election timeouts, term-based voting, majority quorum
  - [x] `RequestVote` / `HandleRequestVote` — candidate solicitation with Election Safety (up-to-date-log check)
  - [x] Leader heartbeats via periodic empty `AppendEntries` to suppress follower elections
  - [x] **Pre-Vote** (Raft §9.6): a two-phase election (trial round, then real) so an isolated node can't inflate its term or disrupt a healthy Leader on reconnect — see [docs/milestoneTwo.md §9](docs/milestoneTwo.md#9-the-pre-vote-protocol-raft-96)
- [x] **Log replication** (`pkg/consensus/raft`): quorum-based commit, Log Matching Invariant
  - [x] `AppendEntries` / `HandleAppendEntries` — replication with conflict detection and truncation
  - [x] Commits only current-term entries by counting replicas (Raft Figure 8 safety)
  - [x] Async `applyCh` decouples consensus from MVCC state-machine application
- [x] **Network transport** (`pkg/consensus/raft/transport.go`): TCP RPC via `net/rpc`, per-peer connection reuse
- [x] **Node daemon** (`cmd/kv-server`): HTTP client API (`/put`, `/get`, `/status`) fronting a Raft-replicated MVCC store
- [x] **Log persistence — interface & write path** (`pkg/consensus/raft/storage.go`): pluggable `Storage` interface; `KVStorage` calls `Save()` at every point Raft §5.2 requires (before granting a vote, before a real election, on every `AppendEntries`) — but see the caveat below
  - [ ] **Actual crash durability**: `KVStorage` currently sits on Layer 0's `raw.SkipListEngine`, which is in-memory only (no on-disk backend — see the Layer 0 checklist above). So today, if a node's process dies, `Save()`'s writes die with it; a restart comes back with `Term=0, Vote=0`, a fresh empty log, and no memory of ever having voted. The interface is correctly shaped for real durability (matches etcd/raft's own storage abstraction), but needs a disk-backed `raw.ByteEngine` underneath before that's actually true. Details: [docs/milestoneTwo.md §7](docs/milestoneTwo.md#7-storage-durability--log-persistence)
- [x] **Snapshotting & log compaction**: `RaftNode.Snapshot` compacts the in-memory and persisted log once Layer 2 checkpoints; `InstallSnapshot` RPC catches up a follower whose required entries were already compacted away

Design notes and the full test-case catalog: [docs/milestoneTwo.md](docs/milestoneTwo.md).

## Package structure

```text
distributed-key-value-store/
├── cmd/
│   └── kv-server/                 # ✅ Node daemon: HTTP client API + Raft-replicated MVCC store
│       └── main.go
├── pkg/
│   ├── consensus/
│   │   └── raft/                  # ✅ Milestone 2: single-group Raft consensus
│   │       ├── node.go            # ✅ RaftNode: election, replication, RPC handlers
│   │       ├── log.go             # ✅ RaftLog, Propose, GetState, Stop
│   │       ├── config.go          # ✅ Config + timing-invariant validation
│   │       ├── types.go           # ✅ NodeRole, LogEntry, RPC arg/reply types
│   │       ├── transport.go       # ✅ TCPTransport over net/rpc (incl. InstallSnapshot)
│   │       ├── storage.go         # ✅ Storage interface + KVStorage (durable HardState/log over Layer 0)
│   │       ├── snapshot.go        # ✅ InstallSnapshot RPC arg/reply types
│   │       ├── raft_test.go       # ✅ Election, replication, partition, failover, race tests
│   │       ├── raft_bench_test.go # ✅ Log append & proposal throughput benchmarks
│   │       ├── raft_snapshot_test.go       # ✅ Lagging-follower catch-up via InstallSnapshot
│   │       ├── raft_snapshot_bench_test.go # ✅ MVCC snapshot export/restore benchmarks
│   │       ├── storage_test.go             # ✅ KVStorage persistence & compaction test
│   │       ├── raft_prevote_test.go        # ✅ Pre-Vote: no term inflation while isolated, stable leader survives reconnect
│   │       └── raft_prevote_bench_test.go  # ✅ Leader-election latency benchmark
│   │
│   ├── storage/
│   │   ├── raw/                   # ✅ Layer 0: raw ordered byte storage engine
│   │   │   ├── engine.go          # ✅ Interfaces: ByteEngine, Iterator
│   │   │   ├── skiplist.go        # ✅ Lock-free concurrent skip list + arena allocator
│   │   │   └── skiplist_test.go   # ✅ Unit, ordering, seek, and concurrency/race tests
│   │   │
│   │   ├── codec/                 # ✅ Layer 1: binary encoding & serialization
│   │   │   ├── key.go             # ✅ Memcmp-safe key escaping, ^ts packing
│   │   │   ├── value.go           # ✅ Value encoding (OpType headers, payloads)
│   │   │   └── codec_test.go      # ✅ Ordering invariants, round-trip, benchmark
│   │   │
│   │   └── mvcc/                  # ✅ Layer 2: MVCC protocol & snapshot engine
│   │       ├── engine.go          # ✅ MVCCStore interface, KeyValue, sentinel errors
│   │       ├── mvcc.go            # ✅ Put/Delete/Get/Scan over Layer 0 + Layer 1
│   │       ├── mvcc_test.go       # ✅ Isolation, tombstone, and scan-dedup tests + benchmark
│   │       ├── gc.go              # ✅ Watermark-based compaction (CompactBelowWatermark)
│   │       ├── gc_test.go         # ✅ Compaction correctness test
│   │       └── snapshot.go        # ✅ Export/import + atomic file save/load for Raft snapshots
│   │
│   └── common/                    # planned
│       └── errors.go              # Domain-specific errors (KeyNotFound, StaleWrite)
├── go.mod
└── go.sum
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
### RUN KV SERVER
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

status:
```bash
for port in 9001 9002 9003; do
  echo -n "Port $port: "
  curl -s http://localhost:$port/status
  echo ""
done
```

### Automated cluster smoke test

[`test_cluster.sh`](test_cluster.sh) automates the manual 3-terminal walkthrough above into a single black-box script: it builds `kv-server`, boots a 3-node cluster in the background, and exercises it over real HTTP/TCP end-to-end (not the in-process simulated network the `pkg/consensus/raft` tests use).

```bash
./test_cluster.sh
```

What it checks, in order:
1. **Leader election** — polls `/status` on all three ports once (after a fixed 1s warm-up) and fails if no node reports `is_leader:true`.
2. **Write proposal** — `POST /put` on the leader and asserts the response is `"status":"proposed"`.
3. **Replication** — `GET /get` on all three ports and asserts every node returns the same written value.
4. **Follower redirect** — `POST /put` on a follower and asserts it's rejected with HTTP `307` (not the leader, so it must not accept writes).

It always cleans up after itself (`trap cleanup EXIT`): kills all three background server processes and removes the built binary, even if an assertion fails partway through and the script exits early via `set -e`.

This is a smoke test, not a substitute for `pkg/consensus/raft`'s test suite — it has no partition/failover coverage and only checks the happy path over a real network stack. Its value is specifically that it exercises the parts the in-process simulated-network tests can't: the actual `cmd/kv-server` binary, its flag parsing, the real `TCPTransport`/`net/rpc` wiring, and the HTTP client API — which is exactly where the peer-address bug (main.go's `--peers` parsing) and the heartbeat-interval unit bug were originally found.

### Run tests

```bash
# Full unit test suite, verbose
go test -v ./...

# Race detector across all packages
go test -race ./...

# Benchmarks: throughput and allocations
go test -bench=BenchmarkSkipList_ConcurrentReads -benchmem -run='^$' -v ./pkg/storage/raw/...
go test -bench=BenchmarkCodec_ZeroAllocEncode -benchmem -run='^$' -v ./pkg/storage/codec/...
go test -bench=BenchmarkMVCC_SnapshotPointGet -benchmem -run='^$' -v ./pkg/storage/mvcc/...
go test -bench=BenchmarkRaft_LogAppend -benchmem -run='^$' -v ./pkg/consensus/raft/...
go test -bench=BenchmarkRaft_SequentialProposals -benchmem -run='^$' -v ./pkg/consensus/raft/...
# BenchmarkRaft_ConcurrentProposals needs a fixed -benchtime — default adaptive
# scaling exhausts the test cluster's arena and panics the binary. See below.
go test -bench=BenchmarkRaft_ConcurrentProposals -benchmem -benchtime=600x -run='^$' -v ./pkg/consensus/raft/...
go test -bench=BenchmarkMVCC_SnapshotExport -benchmem -run='^$' -v ./pkg/consensus/raft/...
go test -bench=BenchmarkMVCC_SnapshotRestore -benchmem -run='^$' -v ./pkg/consensus/raft/...
go test -bench=BenchmarkRaft_LeaderElection -benchmem -run='^$' -v ./pkg/consensus/raft/...
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
| raft — sequential propose | ~1.25 ms/op, 68 allocs/op (full propose → quorum → commit → apply, now incl. `KVStorage` persistence) | [bench/BenchmarkRaft_SequentialProposals.txt](bench/BenchmarkRaft_SequentialProposals.txt) |
| raft — concurrent propose | ~10-80 µs/op under contention (capped at 600 iterations, see note) | [bench/BenchmarkRaft_ConcurrentProposals.txt](bench/BenchmarkRaft_ConcurrentProposals.txt) |
| mvcc — snapshot export | ~1.3 ms/op, ~1.37 MB/op (streaming 10,000 live keys) | [bench/BenchmarkMVCC_SnapshotExport.txt](bench/BenchmarkMVCC_SnapshotExport.txt) |
| mvcc — snapshot restore | ~11 ms/op, ~68 MB/op (replaying 10,000 keys into a fresh store) | [bench/BenchmarkMVCC_SnapshotRestore.txt](bench/BenchmarkMVCC_SnapshotRestore.txt) |
| raft — leader election | ~95 ms/op (time-to-first-leader on a fresh cluster, incl. the Pre-Vote round-trip) | [bench/BenchmarkRaft_LeaderElection.txt](bench/BenchmarkRaft_LeaderElection.txt) |

Layer 2's `Get` isn't zero-allocation like the layers below it — it allocates a seek-key buffer, a key-decode scratch buffer, and clones the returned payload to protect the caller from the storage engine's internal memory. That's expected at this layer, not a regression.

`BenchmarkRaft_ConcurrentProposals` must be run with a fixed `-benchtime` (e.g. `-benchtime=600x`) — see [bench/README.md](bench/README.md) for why.

## License

[MIT](LICENSE)
