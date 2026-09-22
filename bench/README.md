# Benchmarks

This directory holds committed benchmark baselines so read/write performance can be diffed across commits, not just eyeballed in a terminal that scrolls away.

## Updating a baseline

Run with `-count=5` (or more) so the output carries enough samples for statistical comparison, and pipe straight into the tracked file named after the benchmark:

```bash
go test -bench=<BenchmarkName> -benchmem -count=5 -run='^$' ./pkg/<package>/... | tee bench/<BenchmarkName>.txt
```

e.g.:
```bash
go test -bench=BenchmarkSkipList_ConcurrentReads -benchmem -count=5 -run='^$' ./pkg/storage/raw/... | tee bench/BenchmarkSkipList_ConcurrentReads.txt
go test -bench=BenchmarkCodec_ZeroAllocEncode -benchmem -count=5 -run='^$' ./pkg/storage/codec/... | tee bench/BenchmarkCodec_ZeroAllocEncode.txt
```

Commit the updated file alongside the code change that motivated it, so the two land together in history.

## Comparing against the last committed baseline

[`benchstat`](https://pkg.go.dev/golang.org/x/perf/cmd/benchstat) reads two `go test -bench` output files and reports whether the delta between them is statistically significant (not just noise):

```bash
go install golang.org/x/perf/cmd/benchstat@latest

# baseline = last commit's numbers, current = your working tree
git show HEAD:bench/<BenchmarkName>.txt > /tmp/baseline.txt
go test -bench=<BenchmarkName> -benchmem -count=5 -run='^$' ./pkg/<package>/... > /tmp/current.txt

benchstat /tmp/baseline.txt /tmp/current.txt
```

Or compare any two historical points directly:

```bash
git show <old-sha>:bench/<BenchmarkName>.txt > /tmp/old.txt
git show <new-sha>:bench/<BenchmarkName>.txt > /tmp/new.txt
benchstat /tmp/old.txt /tmp/new.txt
```

## Current baselines

| File | Benchmark | Covers |
|---|---|---|
| [`BenchmarkSkipList_ConcurrentReads.txt`](BenchmarkSkipList_ConcurrentReads.txt) | `BenchmarkSkipList_ConcurrentReads` | Concurrent `Get` throughput and allocations under `GOMAXPROCS` parallel readers (Layer 0) |
| [`BenchmarkCodec_ZeroAllocEncode.txt`](BenchmarkCodec_ZeroAllocEncode.txt) | `BenchmarkCodec_ZeroAllocEncode` | `EncodeKeyAppend` throughput and allocations when reusing a pre-sized buffer (Layer 1) |
| [`BenchmarkMVCC_SnapshotPointGet.txt`](BenchmarkMVCC_SnapshotPointGet.txt) | `BenchmarkMVCC_SnapshotPointGet` | Snapshot `Get` throughput and allocations, resolving the newest of 10 stored versions (Layer 2) |
| [`BenchmarkRaft_LogAppend.txt`](BenchmarkRaft_LogAppend.txt) | `BenchmarkRaft_LogAppend` | Raw in-memory replicated log append throughput (no network, no arena) |
| [`BenchmarkRaft_SequentialProposals.txt`](BenchmarkRaft_SequentialProposals.txt) | `BenchmarkRaft_SequentialProposals` | End-to-end propose → quorum replicate → commit → apply latency on a 3-node in-process cluster |
| [`BenchmarkRaft_ConcurrentProposals.txt`](BenchmarkRaft_ConcurrentProposals.txt) | `BenchmarkRaft_ConcurrentProposals` | Proposal throughput under concurrent goroutine write pressure — **must be run with `-benchtime=2000x`**, see note below |

Notes:
- `BenchmarkRaft_ConcurrentProposals` proposes unique keys as fast as `b.RunParallel` allows. Left to Go's default adaptive `-benchtime` (scale `b.N` until ~1s), it reliably exhausts the 16MB test-cluster arena and **panics the whole test binary** (`raw: arena out of memory` — Layer 0's bump allocator has no reclamation and errors via `panic`, not a returned error). Always pin the iteration count explicitly:
  ```bash
  go test -bench=BenchmarkRaft_ConcurrentProposals -benchmem -benchtime=2000x -count=5 -run='^$' ./pkg/consensus/raft/...
  ```
- `SkipListEngine.Get` returns a slice pointing directly into the arena's backing array (no per-call copy — that's what makes 0 allocs/op possible). Callers that need to retain a value past a subsequent write to that key must copy it themselves; the zero-allocation guarantee is a property of the read path, not a promise that the returned bytes are immutable forever.
- `EncodeKeyAppend`'s 0 allocs/op depends on the caller reusing a buffer sized via `EncodedKeyLen` and re-slicing it (`buf[:0]`) between calls, as the benchmark does — passing `nil` or an undersized `dst` will allocate.
- `mvcc.Store.Get` is not zero-allocation: it allocates a fresh seek-key buffer per call, a key-decode scratch buffer, and clones the returned payload via `slices.Clone` before handing it back to the caller (so the caller never holds a reference into Layer 0's internal arena). The 4 allocs/op baseline reflects that by design, not a bug to fix.
