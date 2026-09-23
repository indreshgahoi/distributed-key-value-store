#!/usr/bin/env bash
set -euo pipefail

echo "==> Building kv-server binary..."
go build -o kv-server ./cmd/kv-server

cleanup() {
  echo "==> Shutting down cluster processes..."
  kill $(jobs -p) 2>/dev/null || true
  rm -f kv-server
  rm -rf data
}
trap cleanup EXIT

# Nodes persist real state to ./data/node_<id>/raft/ (WAL + snapshots).
# Clear any leftovers from a prior run first, so this stays a clean, isolated
# smoke test rather than silently depending on state a previous run left behind.
rm -rf data

FLAGS=(--http-peers=1=127.0.0.1:9001,2=127.0.0.1:9002,3=127.0.0.1:9003
       --heartbeat-interval=30ms --election-timeout-min=100ms --election-timeout-max=200ms)

echo "==> Starting 3-node cluster in background..."
./kv-server --id=1 --raft-addr=127.0.0.1:8001 --http-addr=:9001 \
  "${FLAGS[@]}" > /dev/null 2>&1 &
PID1=$!

./kv-server --id=2 --raft-addr=127.0.0.1:8002 --http-addr=:9002 \
  "${FLAGS[@]}" > /dev/null 2>&1 &
PID2=$!

./kv-server --id=3 --raft-addr=127.0.0.1:8003 --http-addr=:9003 \
  "${FLAGS[@]}" > /dev/null 2>&1 &
PID3=$!

echo "==> Waiting for cluster leader election..."
sleep 1

LEADER_PORT=""
for port in 9001 9002 9003; do
  STATUS=$(curl -s "http://localhost:${port}/status" || true)
  if [[ "$STATUS" == *"\"is_leader\":true"* ]]; then
    LEADER_PORT=$port
    break
  fi
done

if [ -z "$LEADER_PORT" ]; then
  echo "FAIL: No leader elected."
  exit 1
fi
echo "==> Leader is running on port :${LEADER_PORT}"

echo "==> Test 1: Propose write to leader..."
WRITE_RES=$(curl -s -X POST "http://localhost:${LEADER_PORT}/put" \
  -H "Content-Type: application/json" \
  -d '{"key":"test:balance","value":"₹1000"}')

if [[ "$WRITE_RES" != *"\"status\":\"committed\""* ]]; then
  echo "FAIL: Write proposal rejected: $WRITE_RES"
  exit 1
fi
echo "PASS: Write proposed successfully."

sleep 0.3

echo "==> Test 2: Verify the write replicated to all 3 nodes (local stale reads)..."
for port in 9001 9002 9003; do
  READ_RES=$(curl -s "http://localhost:${port}/get?consistency=stale&key=test:balance")
  if [[ "$READ_RES" != *"₹1000"* ]]; then
    echo "FAIL on port $port: $READ_RES"
    exit 1
  fi
done
echo "PASS: Data replicated and consistent across all 3 nodes."

echo "==> Test 3: Linearizable read from the leader, and via a follower's redirect..."
READ_RES=$(curl -s "http://localhost:${LEADER_PORT}/get?key=test:balance")
if [[ "$READ_RES" != *"₹1000"* ]]; then
  echo "FAIL: linearizable read on leader: $READ_RES"
  exit 1
fi
for port in 9001 9002 9003; do
  READ_RES=$(curl -s -L "http://localhost:${port}/get?key=test:balance")
  if [[ "$READ_RES" != *"₹1000"* ]]; then
    echo "FAIL: linearizable read via port $port: $READ_RES"
    exit 1
  fi
done
echo "PASS: Linearizable reads served by the leader (followers redirect)."

echo "==> Test 4: Follower redirects writes to the leader..."
FOLLOWER_PORT=""
for port in 9001 9002 9003; do
  if [ "$port" != "$LEADER_PORT" ]; then
    FOLLOWER_PORT=$port
    break
  fi
done

RESULT=$(curl -s -o /dev/null -w "%{http_code} %{redirect_url}" -X POST "http://localhost:${FOLLOWER_PORT}/put" \
  -H "Content-Type: application/json" \
  -d '{"key":"test:reject","value":"1"}')

if [[ "$RESULT" != "307 http://127.0.0.1:${LEADER_PORT}/put" ]]; then
  echo "FAIL: Expected HTTP 307 to the leader (:${LEADER_PORT}), got: $RESULT"
  exit 1
fi
echo "PASS: Follower redirects writes to the leader with HTTP 307."

echo ""
echo "All main black-box tests PASSED successfully."