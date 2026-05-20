#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

NUM_NODES="${NUM_NODES:-3}"
BASE_HTTP_PORT="${BASE_HTTP_PORT:-8080}"
BASE_INTERNAL_PORT="${BASE_INTERNAL_PORT:-9090}"
BASE_GOSSIP_PORT="${BASE_GOSSIP_PORT:-7946}"
HTTP_HOST="${HTTP_HOST:-127.0.0.1}"
GOSSIP_HOST="${GOSSIP_HOST:-127.0.0.1}"
ADVERTISE_HOST="${ADVERTISE_HOST:-127.0.0.1}"
if [[ -z "${STYLE_URL_TEMPLATE:-}" ]]; then
  STYLE_URL_TEMPLATE='carto=https://basemaps.cartocdn.com/{style_id}/style.json'
fi
if [[ -z "${TILESET_URL_TEMPLATE:-}" ]]; then
  TILESET_URL_TEMPLATE='https://tileset-provider.example.test/tilesets/{tileset_id}/tileset.json'
fi
LOG_FILE="${LOG_FILE:-$ROOT_DIR/target/dev-cluster/biei-cluster.log}"
CACHE_DIR="${CACHE_DIR:-$ROOT_DIR/target/dev-cluster/maplibre-cache}"
CORES="${CORES:-2}"
RUST_LOG="${RUST_LOG:-info}"

usage() {
  cat <<EOF
Usage: [VAR=...] bash scripts/dev-cluster.sh

Starts a local biei cluster on one machine.

Environment:
  NUM_NODES             number of biei nodes (default: 3)
  BASE_HTTP_PORT        first public HTTP port (default: 8080)
  BASE_INTERNAL_PORT    first cluster-internal port (/_internal/*, metrics,
                        peer forwarding; default: 9090)
  BASE_GOSSIP_PORT      first UDP gossip port (default: 7946)
  HTTP_HOST             bind host for HTTP listeners (default: 127.0.0.1)
  GOSSIP_HOST           bind host for gossip listeners (default: 127.0.0.1)
  ADVERTISE_HOST        HTTP host published to peers (default: 127.0.0.1)
  STYLE_URL_TEMPLATE    style.json URL template
  TILESET_URL_TEMPLATE  tileset.json URL template
  LOG_FILE              combined log path (default: target/dev-cluster/biei-cluster.log)
  CACHE_DIR             per-node MapLibre ambient cache directory
  CORES                 --cores passed to each node (default: 2)
  RUST_LOG              tracing filter (default: info)
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ ! "$NUM_NODES" =~ ^[0-9]+$ ]] || (( NUM_NODES < 1 )); then
  echo "NUM_NODES must be a positive integer" >&2
  exit 1
fi
if [[ ! "$BASE_HTTP_PORT" =~ ^[0-9]+$ ]] || (( BASE_HTTP_PORT < 1 || BASE_HTTP_PORT > 65535 )); then
  echo "BASE_HTTP_PORT must be in 1..65535" >&2
  exit 1
fi
if [[ ! "$BASE_INTERNAL_PORT" =~ ^[0-9]+$ ]] || (( BASE_INTERNAL_PORT < 1 || BASE_INTERNAL_PORT > 65535 )); then
  echo "BASE_INTERNAL_PORT must be in 1..65535" >&2
  exit 1
fi
if [[ ! "$BASE_GOSSIP_PORT" =~ ^[0-9]+$ ]] || (( BASE_GOSSIP_PORT < 1 || BASE_GOSSIP_PORT > 65535 )); then
  echo "BASE_GOSSIP_PORT must be in 1..65535" >&2
  exit 1
fi
if (( BASE_HTTP_PORT + NUM_NODES - 1 > 65535 )); then
  echo "HTTP port range exceeds 65535" >&2
  exit 1
fi
if (( BASE_INTERNAL_PORT + NUM_NODES - 1 > 65535 )); then
  echo "internal port range exceeds 65535" >&2
  exit 1
fi
if (( BASE_GOSSIP_PORT + NUM_NODES - 1 > 65535 )); then
  echo "gossip port range exceeds 65535" >&2
  exit 1
fi

pids=()
cleanup() {
  trap - INT TERM EXIT
  if (( ${#pids[@]} > 0 )); then
    echo "stopping ${#pids[@]} biei node(s)..." >&2
    kill "${pids[@]}" 2>/dev/null || true
    wait "${pids[@]}" 2>/dev/null || true
  fi
}
trap cleanup INT TERM EXIT

mkdir -p "$(dirname "$LOG_FILE")" "$CACHE_DIR"
: > "$LOG_FILE"

cd "$ROOT_DIR"
cargo build -p biei

seed_addrs=()
for ((i = 0; i < NUM_NODES; i++)); do
  http_port=$((BASE_HTTP_PORT + i))
  internal_port=$((BASE_INTERNAL_PORT + i))
  gossip_port=$((BASE_GOSSIP_PORT + i))
  node_id="biei-${i}"
  bind_addr="${HTTP_HOST}:${http_port}"
  # Peers forward /_internal/* to the internal port, so advertise that — never
  # the public port (which no longer serves /_internal/*).
  internal_advertise_addr="${ADVERTISE_HOST}:${internal_port}"
  gossip_bind="${GOSSIP_HOST}:${gossip_port}"
  cache_path="${CACHE_DIR}/${node_id}.sqlite"

  args=(
    "$ROOT_DIR/target/debug/biei"
    --cluster
    --node-id "$node_id"
    --http-bind "$bind_addr"
    --internal-port "$internal_port"
    --internal-advertise-addr "$internal_advertise_addr"
    --gossip-bind "$gossip_bind"
    --style-templates "$STYLE_URL_TEMPLATE"
    --tileset-url-template "$TILESET_URL_TEMPLATE"
    --maplibre-cache-path "$cache_path"
    --cores "$CORES"
  )

  if (( ${#seed_addrs[@]} > 0 )); then
    args+=(--gossip-seeds "$(IFS=,; echo "${seed_addrs[*]}")")
  fi

  echo "starting ${node_id}: http=http://${bind_addr} internal=http://${HTTP_HOST}:${internal_port} internal_advertise=${internal_advertise_addr} gossip=${gossip_bind} log=${LOG_FILE}"
  RUST_LOG="$RUST_LOG" "${args[@]}" 2>&1 \
    | sed -u "s/^/[${node_id}] /" \
    | tee -a "$LOG_FILE" &
  pids+=("$!")
  seed_addrs+=("$gossip_bind")
done

cat <<EOF

biei dev cluster started.
  nodes:       ${NUM_NODES}
  first node:  http://${HTTP_HOST}:${BASE_HTTP_PORT}
  preview:     http://${HTTP_HOST}:${BASE_HTTP_PORT}/carto/gl/voyager-gl-style/preview
  live:        http://${HTTP_HOST}:${BASE_HTTP_PORT}/livez
  ready:       http://${HTTP_HOST}:${BASE_HTTP_PORT}/readyz
  metrics:     http://${HTTP_HOST}:${BASE_INTERNAL_PORT}/_internal/metrics
  log:         ${LOG_FILE}

Press Ctrl-C to stop all nodes.
EOF

wait
