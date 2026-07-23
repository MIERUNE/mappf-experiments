#!/usr/bin/env bash
# Linux/Docker E2E for the production Biei image. Starts two real renderer
# processes, waits for chitchat convergence, and requires an ingress request on
# node 0 to render through node 1's internal HTTP endpoint.
set -euo pipefail

ROOT_DIR="$(unset CDPATH; cd -- "$(dirname -- "$0")/../../.." && pwd)"
BIEI_IMAGE="${BIEI_IMAGE:-biei:dev}"
FIXTURE_PORT="${BIEI_FIXTURE_PORT:-18081}"
NODE0_PUBLIC_PORT="${BIEI_NODE0_PUBLIC_PORT:-18080}"
NODE1_PUBLIC_PORT="${BIEI_NODE1_PUBLIC_PORT:-18082}"
NODE0_INTERNAL_PORT="${BIEI_NODE0_INTERNAL_PORT:-19090}"
NODE1_INTERNAL_PORT="${BIEI_NODE1_INTERNAL_PORT:-19091}"
HOST_ALIAS="${BIEI_E2E_HOST_ALIAS:-host.docker.internal}"
NETWORK_SUBNET="${BIEI_E2E_SUBNET:-172.29.0.0/24}"
NODE0_IP="${BIEI_NODE0_IP:-172.29.0.2}"
NODE1_IP="${BIEI_NODE1_IP:-172.29.0.3}"
NETWORK="biei-e2e-$$"
NODE_PREFIX="biei-e2e-$$"
WORK_DIR="$(mktemp -d)"
FIXTURE_PID=""
CONTAINERS=()
HOST_GATEWAY_ARGS=()
if [[ "$HOST_ALIAS" == "host.docker.internal" ]]; then
  HOST_GATEWAY_ARGS=(--add-host host.docker.internal:host-gateway)
fi

cleanup() {
  local status=$?
  trap - EXIT
  if (( status != 0 )); then
    printf '\nBiei cluster E2E failed; container logs follow.\n' >&2
    for container in "${CONTAINERS[@]}"; do
      docker logs "$container" >&2 || true
    done
    if [[ -f "$WORK_DIR/fixture-server.log" ]]; then
      printf '\nFixture server log:\n' >&2
      sed -n '1,200p' "$WORK_DIR/fixture-server.log" >&2 || true
    fi
  fi
  if (( ${#CONTAINERS[@]} > 0 )); then
    docker rm -f "${CONTAINERS[@]}" >/dev/null 2>&1 || true
  fi
  if [[ -n "$FIXTURE_PID" ]]; then
    kill "$FIXTURE_PID" >/dev/null 2>&1 || true
    wait "$FIXTURE_PID" 2>/dev/null || true
  fi
  docker network rm "$NETWORK" >/dev/null 2>&1 || true
  rm -rf "$WORK_DIR"
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

wait_for_status() {
  local url=$1
  local expected=$2
  local attempts=${3:-60}
  local code
  for ((attempt = 1; attempt <= attempts; attempt++)); do
    code="$(curl -g -s -o /dev/null -w '%{http_code}' --max-time 1 "$url" || true)"
    if [[ "$code" == "$expected" ]]; then
      return 0
    fi
    sleep 1
  done
  printf 'timed out waiting for %s to return %s (last=%s)\n' "$url" "$expected" "$code" >&2
  return 1
}

expect_status() {
  local url=$1
  local expected=$2
  local code
  code="$(curl -g -s -o /dev/null -w '%{http_code}' --max-time 5 "$url" || true)"
  if [[ "$code" != "$expected" ]]; then
    printf '%s returned %s; expected %s\n' "$url" "$code" "$expected" >&2
    return 1
  fi
}

wait_for_metric() {
  local url=$1
  local pattern=$2
  local attempts=${3:-60}
  local body=""
  for ((attempt = 1; attempt <= attempts; attempt++)); do
    body="$(curl -fsS --max-time 2 "$url" || true)"
    if grep -Eq "$pattern" <<<"$body"; then
      return 0
    fi
    sleep 1
  done
  printf 'timed out waiting for metric %s at %s\n' "$pattern" "$url" >&2
  return 1
}

start_node() {
  local index=$1
  local ip=$2
  local public_port=$3
  local internal_port=$4
  local seed=${5:-}
  local seed_env=()
  if [[ -n "$seed" ]]; then
    seed_env=(-e "BIEI_GOSSIP_SEEDS=$seed")
  fi
  docker run -d \
    --name "${NODE_PREFIX}-${index}" \
    --network "$NETWORK" \
    --ip "$ip" \
    ${HOST_GATEWAY_ARGS[@]+"${HOST_GATEWAY_ARGS[@]}"} \
    --read-only \
    --tmpfs /tmp:rw,noexec,nosuid,size=64m \
    --tmpfs /var/cache/biei:rw,noexec,nosuid,size=64m \
    -p "127.0.0.1:${public_port}:8080" \
    -p "127.0.0.1:${internal_port}:9090" \
    -e BIEI_CLUSTER=true \
    -e BIEI_REQUIRE_GOSSIP_BOOTSTRAP=true \
    -e "BIEI_NODE_ID=biei-${index}" \
    -e BIEI_HTTP_BIND=0.0.0.0:8080 \
    -e BIEI_INTERNAL_PORT=9090 \
    -e "BIEI_INTERNAL_ADVERTISE_ADDR=${ip}:9090" \
    -e "BIEI_GOSSIP_BIND=${ip}:7946" \
    -e "BIEI_GOSSIP_ADVERTISE_ADDR=${ip}:7946" \
    ${seed_env[@]+"${seed_env[@]}"} \
    -e BIEI_CORES=1 \
    -e "BIEI_STYLE_TEMPLATES=smoke=http://${HOST_ALIAS}:${FIXTURE_PORT}/{style_id}.json" \
    -e "BIEI_MLN_RESOURCE_PRIVATE_HOSTS=${HOST_ALIAS}" \
    -e BIEI_RENDER_OUTPUT_CACHE_BYTES=16777216 \
    -e BIEI_MLN_RESOURCE_CACHE_BYTES=16777216 \
    "$BIEI_IMAGE"
}

cd "$ROOT_DIR"
for index in $(seq 0 15); do
  cp .github/fixtures/biei/empty-style.json "$WORK_DIR/style-${index}.json"
done
python3 -m http.server "$FIXTURE_PORT" --bind 0.0.0.0 \
  --directory "$WORK_DIR" >"$WORK_DIR/fixture-server.log" 2>&1 &
FIXTURE_PID=$!
wait_for_status "http://127.0.0.1:${FIXTURE_PORT}/style-0.json" 200 15

docker network create --driver bridge --subnet "$NETWORK_SUBNET" "$NETWORK" >/dev/null
CONTAINERS+=("$(start_node 0 "$NODE0_IP" "$NODE0_PUBLIC_PORT" "$NODE0_INTERNAL_PORT")")
CONTAINERS+=("$(start_node 1 "$NODE1_IP" "$NODE1_PUBLIC_PORT" "$NODE1_INTERNAL_PORT" "${NODE0_IP}:7946")")

wait_for_status "http://127.0.0.1:${NODE0_PUBLIC_PORT}/readyz" 200
wait_for_status "http://127.0.0.1:${NODE1_PUBLIC_PORT}/readyz" 200
wait_for_metric \
  "http://127.0.0.1:${NODE0_INTERNAL_PORT}/_internal/metrics" \
  'biei_membership_size\{node="biei-0",state="live"\} 2'
wait_for_metric \
  "http://127.0.0.1:${NODE1_INTERNAL_PORT}/_internal/metrics" \
  'biei_membership_size\{node="biei-1",state="live"\} 2'

# Public and internal listener responsibilities must remain disjoint.
expect_status "http://127.0.0.1:${NODE0_PUBLIC_PORT}/_internal/metrics" 404
expect_status "http://127.0.0.1:${NODE0_INTERNAL_PORT}/smoke/style-0/static/0,0,1/64x64.png" 404

# Each cold style receives an independent HRW placement. Try a bounded set and
# require at least one node-0 ingress to complete through node 1.
FORWARDED_STYLE=""
for index in $(seq 0 15); do
  curl -g -fsS --show-error --max-time 30 \
    "http://127.0.0.1:${NODE0_PUBLIC_PORT}/smoke/style-${index}/static/0,0,1/64x64.png" \
    --output "$WORK_DIR/render.png"
  metrics="$(curl -fsS --max-time 5 "http://127.0.0.1:${NODE0_INTERNAL_PORT}/_internal/metrics")"
  if grep -Eq 'biei_forwards_total\{outcome="success"\} [1-9][0-9]*' <<<"$metrics"; then
    FORWARDED_STYLE="style-${index}"
    break
  fi
done
if [[ -z "$FORWARDED_STYLE" ]]; then
  printf 'no peer-forwarded render observed across the bounded style set\n' >&2
  exit 1
fi

test "$(od -An -tx1 -N8 "$WORK_DIR/render.png" | tr -d ' \n')" = 89504e470d0a1a0a
file "$WORK_DIR/render.png" | grep -F '64 x 64'
wait_for_metric \
  "http://127.0.0.1:${NODE1_INTERNAL_PORT}/_internal/metrics" \
  'biei_tasks_completed_total\{route_tier="tier2_hrw_bl",scope="forwarded"\} [1-9][0-9]*' \
  10

# Exercise another encoder through the now-warm peer node.
curl -g -fsS --show-error --max-time 30 \
  "http://127.0.0.1:${NODE1_PUBLIC_PORT}/smoke/${FORWARDED_STYLE}/static/0,0,1/64x64.webp" \
  --output "$WORK_DIR/render.webp"
test "$(od -An -tx1 -N4 "$WORK_DIR/render.webp" | tr -d ' \n')" = 52494646
test "$(dd if="$WORK_DIR/render.webp" bs=1 skip=8 count=4 status=none | od -An -tx1 | tr -d ' \n')" = 57454250

printf 'PASS: two-node membership converged and peer rendering completed via %s\n' "$FORWARDED_STYLE"
