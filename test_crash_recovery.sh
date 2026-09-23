#!/usr/bin/env bash
# Black-box crash-recovery test: does data survive if every node dies?
#
# Writes to a live 3-node cluster, SIGKILLs all three processes (simulating a
# hard crash - no graceful shutdown, no deferred cleanup), restarts them from
# the SAME on-disk data directories, and verifies the data is still there and
# the cluster is still usable. Deliberately does NOT wait for the 30s
# snapshot ticker - recovery here depends entirely on Raft's WAL replay on
# boot (pkg/consensus/raft/node.go's NewRaftNode reading storage.Entries()
# back into the in-memory log), which is the path that matters most: it's
# what covers everything written since the last snapshot.
set -euo pipefail

echo "==> Building kv-server binary..."
go build -o kv-server ./cmd/kv-server

# Explicit PID tracking (not `jobs -p`): this script starts the cluster
# twice (initial boot, then post-crash restart) in the same shell, and
# `jobs -p` proved unreliable across that kill+wait+relaunch cycle - it
# occasionally missed PIDs, leaking live kv-server processes past the
# script's own exit. Tracking exact PIDs from $! is unambiguous.
PIDS=()

cleanup() {
  echo "==> Cleaning up..."
  for pid in "${PIDS[@]:-}"; do
    kill -9 "$pid" 2>/dev/null || true
  done
  pkill -9 -f './kv-server' 2>/dev/null || true # belt and suspenders
  rm -f kv-server n1.log n2.log n3.log
  rm -rf data
}
trap cleanup EXIT

# Start fresh: don't let a previous run's on-disk state affect this one.
rm -rf data

PEERS="1=127.0.0.1:8001,2=127.0.0.1:8002,3=127.0.0.1:8003"
FLAGS=(--peers="$PEERS" --heartbeat-interval=30ms --election-timeout-min=100ms --election-timeout-max=200ms)

start_cluster() {
  PIDS=()
  ./kv-server --id=1 --raft-addr=127.0.0.1:8001 --http-addr=:9001 "${FLAGS[@]}" >>n1.log 2>&1 &
  PIDS+=("$!")
  ./kv-server --id=2 --raft-addr=127.0.0.1:8002 --http-addr=:9002 "${FLAGS[@]}" >>n2.log 2>&1 &
  PIDS+=("$!")
  ./kv-server --id=3 --raft-addr=127.0.0.1:8003 --http-addr=:9003 "${FLAGS[@]}" >>n3.log 2>&1 &
  PIDS+=("$!")
}

# Polls /status on all three ports until exactly one reports is_leader:true,
# or the deadline passes. Prints the leader's port on success.
find_leader() {
  local deadline=$((SECONDS + 5))
  while [ "$SECONDS" -lt "$deadline" ]; do
    for port in 9001 9002 9003; do
      status=$(curl -s "http://localhost:${port}/status" 2>/dev/null || true)
      if [[ "$status" == *"\"is_leader\":true"* ]]; then
        echo "$port"
        return 0
      fi
    done
    sleep 0.1
  done
  return 1
}

echo "==> Phase 1: Starting a fresh 3-node cluster..."
start_cluster
sleep 1

LEADER_PORT=$(find_leader) || { echo "FAIL: no leader elected on initial boot"; exit 1; }
echo "==> Leader is on port :${LEADER_PORT}"

echo "==> Phase 2: Writing 5 keys before any crash..."
for i in 1 2 3 4 5; do
  res=$(curl -s -X POST "http://localhost:${LEADER_PORT}/put" \
    -H "Content-Type: application/json" \
    -d "{\"key\":\"crash_test_key_${i}\",\"value\":\"value_${i}\"}")
  if [[ "$res" != *"\"status\":\"proposed\""* ]]; then
    echo "FAIL: write $i rejected: $res"
    exit 1
  fi
done
sleep 0.5
echo "PASS: 5 keys proposed successfully."

echo "==> Verifying all 5 keys are readable on all 3 nodes before the crash..."
for port in 9001 9002 9003; do
  for i in 1 2 3 4 5; do
    res=$(curl -s "http://localhost:${port}/get?key=crash_test_key_${i}")
    if [[ "$res" != *"value_${i}"* ]]; then
      echo "FAIL pre-crash on port $port, key $i: $res"
      exit 1
    fi
  done
done
echo "PASS: all 5 keys confirmed on all 3 nodes pre-crash."

echo "==> Phase 3: Simulating a hard crash - SIGKILL on all 3 node processes..."
for pid in "${PIDS[@]}"; do
  kill -9 "$pid" 2>/dev/null || true
done
wait 2>/dev/null || true
sleep 0.5
echo "==> All 3 node processes killed without graceful shutdown."

echo "==> Phase 4: Restarting all 3 nodes from the same on-disk data directories..."
start_cluster
sleep 1.5

NEW_LEADER_PORT=$(find_leader) || {
  echo "FAIL: no leader elected after restart"
  echo "--- node logs (last 20 lines each) ---"
  tail -n 20 n1.log n2.log n3.log
  exit 1
}
echo "==> New leader is on port :${NEW_LEADER_PORT}"

echo "==> Phase 5: Verifying all 5 pre-crash keys survived on all 3 nodes..."
failed=0
for port in 9001 9002 9003; do
  for i in 1 2 3 4 5; do
    res=$(curl -s "http://localhost:${port}/get?key=crash_test_key_${i}")
    if [[ "$res" != *"value_${i}"* ]]; then
      echo "FAIL post-crash on port $port, key $i: got '$res'"
      failed=1
    fi
  done
done

if [ "$failed" -eq 1 ]; then
  echo ""
  echo "FAIL: data did not fully survive the crash."
  echo "--- node logs (last 30 lines each) ---"
  tail -n 30 n1.log n2.log n3.log
  exit 1
fi
echo "PASS: all 5 keys survived a hard crash of all 3 nodes and are readable post-restart."

echo "==> Phase 6: Verifying the recovered cluster still accepts and replicates new writes..."
res=$(curl -s -X POST "http://localhost:${NEW_LEADER_PORT}/put" \
  -H "Content-Type: application/json" \
  -d '{"key":"post_crash_key","value":"still_alive"}')
if [[ "$res" != *"\"status\":\"proposed\""* ]]; then
  echo "FAIL: post-restart write rejected: $res"
  exit 1
fi
sleep 0.5
for port in 9001 9002 9003; do
  res=$(curl -s "http://localhost:${port}/get?key=post_crash_key")
  if [[ "$res" != *"still_alive"* ]]; then
    echo "FAIL: post-restart write not replicated to port $port: $res"
    exit 1
  fi
done
echo "PASS: recovered cluster accepts and replicates new writes."

echo ""
echo "All crash-recovery black-box tests PASSED."
echo "Data survived a hard SIGKILL of all 3 nodes and a full cluster restart."
