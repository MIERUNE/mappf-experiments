#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

trap 'jobs -pr | xargs -r kill 2>/dev/null || true; wait' INT TERM EXIT

NUM_NODES="${NUM_NODES:-3}"
LOG_FILE="${LOG_FILE:-log.txt}"
BASE_HTTP_PORT=8080
BASE_INTERNAL_HTTP_PORT=9090
BASE_GOSSIP_PORT=7946

if [[ ! "$NUM_NODES" =~ ^[0-9]+$ ]] || (( NUM_NODES < 1 )); then
  echo "NUM_NODES must be a positive integer" >&2
  exit 1
fi

cd "$ROOT_DIR"
cargo build -p ishikari
: > "$LOG_FILE"

seed_addrs=()
for ((i = 0; i < NUM_NODES; i++)); do
  http_port=$((BASE_HTTP_PORT + i))
  internal_http_port=$((BASE_INTERNAL_HTTP_PORT + i))
  gossip_port=$((BASE_GOSSIP_PORT + i))
  gossip_bind="[::1]:${gossip_port}"
  args=(
    target/debug/ishikari
    --http-port "$http_port"
    --internal-http-port "$internal_http_port"
    --gossip-bind "$gossip_bind"
  )
  if (( ${#seed_addrs[@]} > 0 )); then
    args+=(--gossip-seeds "$(IFS=,; echo "${seed_addrs[*]}")")
  fi
  echo "starting node-${i}: http_port=${http_port} internal_http_port=${internal_http_port} gossip_bind=${gossip_bind} log=${LOG_FILE}"
  "${args[@]}" 2>&1 | sed -u "s/^/[node-${i}] /" | tee -a "$LOG_FILE" &
  seed_addrs+=("$gossip_bind")
done

wait
