# End-to-end tests

Black-box tests that build the real `kv-server` binary, start a 3-node cluster as separate processes, and drive it over real TCP and HTTP. They cover what the in-process Go tests can't: flag parsing, the TCP transport, the HTTP API, and recovery of real processes from real disks.

Unit, protocol and fault-injection tests live next to the code they test (`*_test.go`), as is idiomatic Go. Only tests that need real processes live here.

| Script | What it checks |
|---|---|
| `cluster_test.sh` | Election, a committed write, replication to every node, linearizable reads (on the leader and via a follower's redirect), write redirects |
| `crash_recovery_test.sh` | Write, `SIGKILL` every node, restart over the same data, verify the data survived through snapshot restore + log replay, and the cluster still accepts writes |
| `lib.sh` | Shared helpers: build, start/crash the cluster, find the leader, report failures |

```bash
make e2e                          # both, from the repo root
test/e2e/crash_recovery_test.sh   # one
KEEP_WORK_DIR=1 test/e2e/cluster_test.sh   # keep node data and logs for debugging
```

Each run works in its own temporary directory, so it never touches the working tree. It needs `curl`, and ports 8001-8003 and 9001-9003 free.
