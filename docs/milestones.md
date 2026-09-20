# Roadmap

Building a distributed transactional key-value database from scratch can feel daunting, so we follow a proven approach: build it layer by layer, where each layer is independently testable and provides a specific guarantee to the layer above it.

| # | Milestone | Status |
|---|---|---|
| 1 | Local MVCC Storage Engine (Single Node) | 🚧 In progress |
| 2 | Single Group Raft Consensus | ⬜ Not started |
| 3 | Multi-Raft and Range Sharding | ⬜ Not started |
| 4 | Hybrid Logical Clock (HLC) | ⬜ Not started |
| 5 | Multi-Range 2PC (Percolator Model) | ⬜ Not started |

See [milestoneOne.md](milestoneOne.md) for the Milestone 1 layer breakdown, and the top-level [README](../README.md) for current build status within that milestone.

### Technology

Go 1.27, standard library only — no external dependencies yet.
