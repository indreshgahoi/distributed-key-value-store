# Distributed Key-Value Store

A distributed, transactional key-value database built from scratch in Go, layer by layer — each layer independently testable and providing a specific guarantee to the layer above it. See [docs/concepts.md](docs/concepts.md) for a from-first-principles primer on *why* each layer (replication, consensus, partitioning, MVCC, clock skew, 2PC) exists.

## Roadmap

| # | Milestone | Status |
|---|---|---|
| 1 | Local MVCC Storage Engine (Single Node) | ✅ Done |
| 2 | Single Group Raft Consensus | ⬜ Not started |
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
  - [ ] `GC` — watermark-based compaction of old versions

Design notes for this milestone: [docs/milestoneOne.md](docs/milestoneOne.md).

## Package structure

```text
distributed-key-value-store/
├── cmd/
│   └── kv-server/                 # planned: entrypoint daemon for running a node
│       └── main.go
├── pkg/
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
│   │       └── mvcc_test.go       # ✅ Isolation, tombstone, and scan-dedup tests + benchmark
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
```

## Benchmarks

Benchmark baselines are committed under [bench/](bench/) so throughput and allocation counts can be diffed across commits with `benchstat` instead of relying on memory. See [bench/README.md](bench/README.md) for how to update a baseline and compare it against history.

| Layer | Baseline | Detail |
|---|---|---|
| 0 — `raw` | 0 B/op, 0 allocs/op (concurrent `Get`) | [bench/BenchmarkSkipList_ConcurrentReads.txt](bench/BenchmarkSkipList_ConcurrentReads.txt) |
| 1 — `codec` | 0 B/op, 0 allocs/op (`EncodeKeyAppend` with reused buffer) | [bench/BenchmarkCodec_ZeroAllocEncode.txt](bench/BenchmarkCodec_ZeroAllocEncode.txt) |
| 2 — `mvcc` | ~200 ns/op, 96 B/op, 4 allocs/op (snapshot `Get` resolving the newest of 10 versions) | [bench/BenchmarkMVCC_SnapshotPointGet.txt](bench/BenchmarkMVCC_SnapshotPointGet.txt) |

Layer 2's `Get` isn't zero-allocation like the layers below it — it allocates a seek-key buffer, a key-decode scratch buffer, and clones the returned payload to protect the caller from the storage engine's internal memory. That's expected at this layer, not a regression.
