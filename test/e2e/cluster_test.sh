#!/usr/bin/env bash
# End-to-end smoke test: a real 3-node cluster over real TCP and HTTP.
# Checks election, a committed write, replication to every node,
# linearizable reads (on the leader and via a follower's redirect), and that
# followers redirect writes to the leader.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

build_server
echo "==> Starting 3-node cluster..."
start_cluster
LEADER_PORT=$(find_leader) || fail "no leader elected"
echo "==> Leader is on port :$LEADER_PORT"

echo "==> Test 1: Write to the leader..."
res=$(put "$LEADER_PORT" "test:balance" "₹1000")
[[ "$res" == *'"status":"committed"'* ]] || fail "write rejected: $res"
echo "PASS: Write committed."

sleep 0.3

echo "==> Test 2: The write replicated to all 3 nodes (local stale reads)..."
for port in "${HTTP_PORTS[@]}"; do
  res=$(curl -s "http://localhost:$port/get?consistency=stale&key=test:balance")
  [[ "$res" == *"₹1000"* ]] || fail "port $port: $res"
done
echo "PASS: Data replicated and consistent across all 3 nodes."

echo "==> Test 3: Linearizable read from the leader, and via a follower's redirect..."
res=$(curl -s "http://localhost:$LEADER_PORT/get?key=test:balance")
[[ "$res" == *"₹1000"* ]] || fail "linearizable read on leader: $res"
for port in "${HTTP_PORTS[@]}"; do
  res=$(curl -s -L "http://localhost:$port/get?key=test:balance")
  [[ "$res" == *"₹1000"* ]] || fail "linearizable read via port $port: $res"
done
echo "PASS: Linearizable reads served by the leader (followers redirect)."

echo "==> Test 4: A follower redirects writes to the leader..."
for port in "${HTTP_PORTS[@]}"; do
  [ "$port" != "$LEADER_PORT" ] && FOLLOWER_PORT=$port && break
done
result=$(curl -s -o /dev/null -w "%{http_code} %{redirect_url}" -X POST "http://localhost:$FOLLOWER_PORT/put" \
  -H "Content-Type: application/json" -d '{"key":"test:reject","value":"1"}')
[[ "$result" == "307 http://127.0.0.1:$LEADER_PORT/put" ]] ||
  fail "expected HTTP 307 to the leader (:$LEADER_PORT), got: $result"
echo "PASS: Follower redirects writes to the leader with HTTP 307."

echo ""
echo "All cluster end-to-end tests PASSED."
