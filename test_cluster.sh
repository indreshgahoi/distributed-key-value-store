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

# Nodes now persist real state to ./data/node_<id>/ (Raft WAL + MVCC snapshot).
# Clear any leftovers from a prior run first, so this stays a clean, isolated
# smoke test rather than silently depending on state a previous run left behind.
rm -rf data

echo "==> Starting 3-node cluster in background..."
./kv-server --id=1 --raft-addr=127.0.0.1:8001 --http-addr=:9001 \
  --heartbeat-interval=30ms --election-timeout-min=100ms --election-timeout-max=200ms > /dev/null 2>&1 &
PID1=$!

./kv-server --id=2 --raft-addr=127.0.0.1:8002 --http-addr=:9002 \
  --heartbeat-interval=30ms --election-timeout-min=100ms --election-timeout-max=200ms > /dev/null 2>&1 &
PID2=$!

./kv-server --id=3 --raft-addr=127.0.0.1:8003 --http-addr=:9003 \
  --heartbeat-interval=30ms --election-timeout-min=100ms --election-timeout-max=200ms > /dev/null 2>&1 &
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

if [[ "$WRITE_RES" != *"\"status\":\"proposed\""* ]]; then
  echo "FAIL: Write proposal rejected: $WRITE_RES"
  exit 1
fi
echo "PASS: Write proposed successfully."

sleep 0.3

echo "==> Test 2: Verify read across all 3 nodes..."
for port in 9001 9002 9003; do
  READ_RES=$(curl -s "http://localhost:${port}/get?key=test:balance")
  if [[ "$READ_RES" != *"₹1000"* ]]; then
    echo "FAIL on port $port: $READ_RES"
    exit 1
  fi
done
echo "PASS: Data replicated and consistent across all 3 nodes."

echo "==> Test 3: Follower rejection test..."
FOLLOWER_PORT=""
for port in 9001 9002 9003; do
  if [ "$port" != "$LEADER_PORT" ]; then
    FOLLOWER_PORT=$port
    break
  fi
done

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST "http://localhost:${FOLLOWER_PORT}/put" \
  -H "Content-Type: application/json" \
  -d '{"key":"test:reject","value":"1"}')

if [ "$HTTP_CODE" != "307" ]; then
  echo "FAIL: Expected HTTP 307 from follower, got $HTTP_CODE"
  exit 1
fi
echo "PASS: Follower correctly rejected write with HTTP 307."

echo ""
echo "All main black-box tests PASSED successfully."