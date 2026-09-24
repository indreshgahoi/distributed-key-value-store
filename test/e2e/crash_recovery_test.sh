#!/usr/bin/env bash
# End-to-end crash-recovery test: does data survive if every node dies?
#
# Writes to a live 3-node cluster, SIGKILLs all three processes (a hard crash:
# no graceful shutdown, no deferred cleanup), restarts them over the SAME data
# directories, and checks the data survived and the cluster still works. The
# in-memory store starts empty on restart, so everything it holds afterwards
# was rebuilt by Raft: the latest snapshot first, then the WAL entries after
# it. --snapshot-every=3 makes the 5 writes below produce a snapshot, so both
# recovery paths run.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

EXTRA_FLAGS=(--snapshot-every=3)

# check_keys_on_all_nodes <phase> verifies crash_test_key_1..5 on every node.
check_keys_on_all_nodes() {
  for port in "${HTTP_PORTS[@]}"; do
    for i in 1 2 3 4 5; do
      res=$(curl -s "http://localhost:$port/get?consistency=stale&key=crash_test_key_$i")
      [[ "$res" == *"value_$i"* ]] || fail "$1, port $port, key $i: got '$res'"
    done
  done
}

build_server

echo "==> Phase 1: Starting a fresh 3-node cluster..."
start_cluster "${EXTRA_FLAGS[@]}"
LEADER_PORT=$(find_leader) || fail "no leader elected on initial boot"
echo "==> Leader is on port :$LEADER_PORT"

echo "==> Phase 2: Writing 5 keys before any crash..."
for i in 1 2 3 4 5; do
  res=$(put "$LEADER_PORT" "crash_test_key_$i" "value_$i")
  [[ "$res" == *'"status":"committed"'* ]] || fail "write $i rejected: $res"
done
sleep 0.5
check_keys_on_all_nodes "pre-crash"
echo "PASS: all 5 keys committed and replicated to all 3 nodes."

echo "==> Phase 3: Hard crash - SIGKILL all 3 nodes..."
stop_cluster
echo "==> All 3 nodes killed without graceful shutdown."

echo "==> Phase 4: Restarting all 3 nodes over the same data directories..."
start_cluster "${EXTRA_FLAGS[@]}"
NEW_LEADER_PORT=$(find_leader) || fail "no leader elected after restart"
echo "==> New leader is on port :$NEW_LEADER_PORT"

echo "==> Phase 5: Verifying all 5 pre-crash keys survived on all 3 nodes..."
sleep 0.5
check_keys_on_all_nodes "post-crash"
grep -q "restored snapshot" "$WORK_DIR"/n1.log "$WORK_DIR"/n2.log "$WORK_DIR"/n3.log ||
  fail "no node restored from a snapshot, so the snapshot recovery path was not exercised"
echo "PASS: all 5 keys survived (recovered via snapshot restore + log replay)."

echo "==> Phase 6: The recovered cluster accepts and replicates new writes..."
res=$(put "$NEW_LEADER_PORT" "post_crash_key" "still_alive")
[[ "$res" == *'"status":"committed"'* ]] || fail "post-restart write rejected: $res"
sleep 0.5
for port in "${HTTP_PORTS[@]}"; do
  res=$(curl -s "http://localhost:$port/get?consistency=stale&key=post_crash_key")
  [[ "$res" == *"still_alive"* ]] || fail "post-restart write not replicated to port $port: $res"
done
echo "PASS: recovered cluster accepts and replicates new writes."

echo ""
echo "All crash-recovery end-to-end tests PASSED."
