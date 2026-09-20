# Distributed Key-Value Store

A distributed, transactional key-value database built from scratch in Go, layer by layer — each layer independently testable and providing a specific guarantee to the layer above it. See [docs/concepts.md](docs/concepts.md) for a from-first-principles primer on *why* each layer (replication, consensus, partitioning, MVCC, clock skew, 2PC) exists.

## Roadmap

| # | Milestone | Status |
|---|---|---|
| 1 | Local MVCC Storage Engine (Single Node) | 🚧 In progress |
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
- [ ] **Layer 1 — Binary encoding & serialization** (`pkg/storage/codec`): memcmp-safe key escaping, timestamp packing
- [ ] **Layer 2 — MVCC protocol & snapshot engine** (`pkg/storage/mvcc`): versioned Put, tombstone Delete, snapshot reads, GC

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
│   │   ├── codec/                 # planned: Layer 1 binary encoding & serialization
│   │   │   ├── key.go             # Memcmp-safe key escaping, ^ts packing
│   │   │   ├── key_test.go
│   │   │   ├── value.go           # Value encoding (OpType headers, payloads)
│   │   │   └── value_test.go
│   │   │
│   │   └── mvcc/                  # planned: Layer 2 MVCC protocol & snapshot engine
│   │       ├── engine.go          # MVCC interface (Put, Get, Delete, Scan)
│   │       ├── reader.go          # Snapshot iterator & point-get implementation
│   │       ├── writer.go          # Versioned Put, tombstone Delete, WriteBatch
│   │       ├── gc.go              # Watermark-based compaction & garbage collection
│   │       └── mvcc_test.go
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
go test -v ./pkg/storage/raw/...

# Race detector — verifies thread-safety under concurrent load
go test -race -run TestSkipList_ConcurrentRaceContention -v ./pkg/storage/raw/...

# Benchmark: concurrent read throughput and allocations
go test -bench=BenchmarkSkipList_ConcurrentReads -benchmem -run='^$' -v ./pkg/storage/raw/...
```

## Benchmarks

Benchmark baselines are committed under [bench/](bench/) so throughput and allocation counts can be diffed across commits with `benchstat` instead of relying on memory. See [bench/README.md](bench/README.md) for how to update a baseline and compare it against history.

Current read-path baseline: 0 B/op, 0 allocs/op for concurrent `Get` — see [bench/BenchmarkSkipList_ConcurrentReads.txt](bench/BenchmarkSkipList_ConcurrentReads.txt).

## Known issues

Tracked transparently as they're found through testing:

- [x] **Fixed** — torn read on concurrent value updates: `Get` and the iterator's `Value()` now use `atomic.LoadUint32` on `valOffset`/`valLen`, matching the writer's atomic stores. Caught by `-race` in `TestSkipList_ConcurrentRaceContention`.
- [ ] **Open** — `Put`'s node-linking loop writes a newly inserted node's forward pointers to a Go-heap copy instead of the arena-resident node, which truncates list traversal after the first insert. Reproduces via `TestSkipList_TotalLexicographicalOrder`.
- [ ] **Open** — `Arena.alloc` panics instead of returning an error when the arena is exhausted, and there's no compaction/flush strategy yet to bound memtable size. Reproduces under sustained concurrent writes in `TestSkipList_ConcurrentRaceContention` (without `-race`).
