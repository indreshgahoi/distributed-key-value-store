# Benchmarks

This directory holds committed benchmark baselines so read/write performance can be diffed across commits, not just eyeballed in a terminal that scrolls away.

## Updating a baseline

Run with `-count=5` (or more) so the output carries enough samples for statistical comparison, and pipe straight into the tracked file:

```bash
go test -bench=BenchmarkSkipList_ConcurrentReads -benchmem -count=5 -run='^$' ./pkg/storage/raw/... | tee bench/BenchmarkSkipList_ConcurrentReads.txt
```

Commit the updated file alongside the code change that motivated it, so the two land together in history.

## Comparing against the last committed baseline

[`benchstat`](https://pkg.go.dev/golang.org/x/perf/cmd/benchstat) reads two `go test -bench` output files and reports whether the delta between them is statistically significant (not just noise):

```bash
go install golang.org/x/perf/cmd/benchstat@latest

# baseline = last commit's numbers, current = your working tree
git show HEAD:bench/BenchmarkSkipList_ConcurrentReads.txt > /tmp/baseline.txt
go test -bench=BenchmarkSkipList_ConcurrentReads -benchmem -count=5 -run='^$' ./pkg/storage/raw/... > /tmp/current.txt

benchstat /tmp/baseline.txt /tmp/current.txt
```

Or compare any two historical points directly:

```bash
git show <old-sha>:bench/BenchmarkSkipList_ConcurrentReads.txt > /tmp/old.txt
git show <new-sha>:bench/BenchmarkSkipList_ConcurrentReads.txt > /tmp/new.txt
benchstat /tmp/old.txt /tmp/new.txt
```

## Current baselines

| File | Benchmark | Covers |
|---|---|---|
| [`BenchmarkSkipList_ConcurrentReads.txt`](BenchmarkSkipList_ConcurrentReads.txt) | `BenchmarkSkipList_ConcurrentReads` | Concurrent `Get` throughput and allocations under `GOMAXPROCS` parallel readers |

Note: `Get` returns a slice pointing directly into the arena's backing array (no per-call copy — that's what makes 0 allocs/op possible). Callers that need to retain a value past a subsequent write to that key must copy it themselves; the zero-allocation guarantee is a property of the read path, not a promise that the returned bytes are immutable forever.
