# Shared helpers for the end-to-end tests. Source it; don't run it.
#
# Every test runs in its own throwaway directory (binary, node data, logs),
# so it never touches the repository's working tree and a failed run leaves
# nothing behind.

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/kv-e2e.XXXXXX")"
PIDS=()

RAFT_PEERS="1=127.0.0.1:8001,2=127.0.0.1:8002,3=127.0.0.1:8003"
HTTP_PEERS="1=127.0.0.1:9001,2=127.0.0.1:9002,3=127.0.0.1:9003"
HTTP_PORTS=(9001 9002 9003)
BASE_FLAGS=(--peers="$RAFT_PEERS" --http-peers="$HTTP_PEERS"
            --heartbeat-interval=30ms --election-timeout-min=100ms --election-timeout-max=200ms)

cleanup() {
  stop_cluster
  if [ "${KEEP_WORK_DIR:-0}" = 1 ]; then
    echo "==> Kept $WORK_DIR (node data and logs)"
  else
    rm -rf "$WORK_DIR"
  fi
}
trap cleanup EXIT

build_server() {
  echo "==> Building kv-server..."
  (cd "$REPO_ROOT" && go build -o "$WORK_DIR/kv-server" ./cmd/kv-server)
}

# start_cluster [extra flags...] boots nodes 1-3 over the same data
# directories each time, so calling it again after a crash is a restart.
start_cluster() {
  PIDS=()
  for id in 1 2 3; do
    "$WORK_DIR/kv-server" --id="$id" --raft-addr="127.0.0.1:800$id" --http-addr=":900$id" \
      --data-dir="$WORK_DIR/data" "${BASE_FLAGS[@]}" "$@" >>"$WORK_DIR/n$id.log" 2>&1 &
    PIDS+=("$!")
  done
}

# stop_cluster SIGKILLs the nodes: no graceful shutdown, like a real crash.
# Only PIDs this test started are touched.
stop_cluster() {
  for pid in "${PIDS[@]:-}"; do
    kill -9 "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  PIDS=()
}

# find_leader prints the leader's HTTP port, polling for up to 5s.
find_leader() {
  local deadline=$((SECONDS + 5))
  while [ "$SECONDS" -lt "$deadline" ]; do
    for port in "${HTTP_PORTS[@]}"; do
      if [[ "$(curl -s "http://localhost:$port/status" 2>/dev/null || true)" == *'"is_leader":true'* ]]; then
        echo "$port"
        return 0
      fi
    done
    sleep 0.1
  done
  return 1
}

put() { # put <port> <key> <value>
  curl -s -X POST "http://localhost:$1/put" -H "Content-Type: application/json" \
    -d "{\"key\":\"$2\",\"value\":\"$3\"}"
}

fail() {
  echo "FAIL: $*"
  echo "--- node logs (last 20 lines each) ---"
  tail -n 20 "$WORK_DIR"/n*.log 2>/dev/null || true
  exit 1
}
