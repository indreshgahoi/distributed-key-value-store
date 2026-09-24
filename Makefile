# One command per test tier. `make help` lists them.

GO ?= go
CHAOS_SEEDS ?= 10
CHAOS_SECONDS ?= 6

.PHONY: help build test test-race chaos e2e bench lint ci clean

help: ## Show this help
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-10s %s\n", $$1, $$2}'

build: ## Build the kv-server binary into bin/
	$(GO) build -o bin/kv-server ./cmd/kv-server

test: ## Unit, protocol, and a short fault-injection run (-short)
	$(GO) test -short ./...

test-race: ## The full Go test suite under the race detector
	$(GO) test -race -count=1 ./...

chaos: ## Fault-injection soak (override CHAOS_SEEDS / CHAOS_SECONDS; replay one with RAFT_CHAOS_SEED=<n>)
	RAFT_CHAOS_SEEDS=$(CHAOS_SEEDS) RAFT_CHAOS_SECONDS=$(CHAOS_SECONDS) \
		$(GO) test -count=1 -run TestRaft_Chaos -v ./pkg/consensus/raft/

e2e: ## End-to-end tests against real processes (test/e2e)
	test/e2e/cluster_test.sh
	test/e2e/crash_recovery_test.sh

bench: ## All benchmarks (5 samples each; compare with benchstat, see bench/README.md)
	$(GO) test -run '^$$' -bench . -benchmem -count=5 ./pkg/storage/... ./pkg/sharding/...
	$(GO) test -run '^$$' -benchmem -count=5 \
		-bench '^Benchmark(Raft_(LogAppend|SequentialProposals|LeaderElection)|MVCC_SnapshotExport|TidwallStorage)' ./pkg/consensus/raft/
	# Own process: each restore allocates a fresh 64 MiB engine, so it must not
	# share a heap with the memory-heavy benchmarks above.
	$(GO) test -run '^$$' -bench '^BenchmarkMVCC_SnapshotRestore$$' -benchmem -count=5 ./pkg/consensus/raft/
	$(GO) test -run '^$$' -bench '^BenchmarkRaft_ConcurrentProposals$$' -benchmem -benchtime=600x -count=5 ./pkg/consensus/raft/

lint: ## gofmt and go vet
	@test -z "$$(gofmt -l cmd pkg)" || { echo "gofmt needed:"; gofmt -l cmd pkg; exit 1; }
	$(GO) vet ./...

ci: lint test-race e2e ## What CI should run

clean: ## Remove build output
	rm -rf bin
