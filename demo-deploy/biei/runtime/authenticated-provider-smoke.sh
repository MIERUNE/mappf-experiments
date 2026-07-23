#!/usr/bin/env bash
# Composed delivery-auth E2E for the production Biei and Ishikari images.
# A broad token warms Ishikari's resource cache and Biei's rendered-output
# cache. A weaker token may read/render the style namespace but not the
# referenced tileset namespace, and therefore must never receive the broad
# token's rendered bytes.
set -euo pipefail

ROOT_DIR="$(unset CDPATH; cd -- "$(dirname -- "$0")/../../.." && pwd)"
BIEI_IMAGE="${BIEI_IMAGE:-biei:dev}"
ISHIKARI_IMAGE="${ISHIKARI_IMAGE:-ishikari:dev}"
FIXTURE_PORT="${AUTH_E2E_FIXTURE_PORT:-18181}"
BIEI_PUBLIC_PORT="${AUTH_E2E_BIEI_PUBLIC_PORT:-18180}"
ISHIKARI_PUBLIC_PORT="${AUTH_E2E_ISHIKARI_PUBLIC_PORT:-18182}"
HOST_ALIAS="${AUTH_E2E_HOST_ALIAS:-host.docker.internal}"
NETWORK="mmpf-auth-e2e-$$"
PREFIX="mmpf-auth-e2e-$$"
WORK_DIR="$(mktemp -d)"
FIXTURE_PID=""
CONTAINERS=()
HOST_GATEWAY_ARGS=()
if [[ "$HOST_ALIAS" == "host.docker.internal" ]]; then
  # Linux CI needs this explicit mapping. Docker Desktop supplies equivalent
  # host routing; Rancher Desktop callers set
  # AUTH_E2E_HOST_ALIAS=host.lima.internal.
  HOST_GATEWAY_ARGS=(--add-host host.docker.internal:host-gateway)
fi

cleanup() {
  local status=$?
  trap - EXIT
  if (( status != 0 )); then
    printf '\nAuthenticated provider E2E failed; container logs follow.\n' >&2
    for container in "${CONTAINERS[@]}"; do
      printf '\n%s:\n' "$container" >&2
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
  local code=""
  for ((attempt = 1; attempt <= attempts; attempt++)); do
    code="$(curl -g -s -o /dev/null -w '%{http_code}' --max-time 1 "$url" || true)"
    if [[ "$code" == "$expected" ]]; then
      return 0
    fi
    sleep 1
  done
  printf 'timed out waiting for endpoint to return %s (last=%s)\n' "$expected" "$code" >&2
  return 1
}

expect_status() {
  local label=$1
  local url=$2
  local expected=$3
  local code
  code="$(curl -g -s -o /dev/null -w '%{http_code}' --max-time 10 "$url" || true)"
  if [[ "$code" != "$expected" ]]; then
    printf '%s returned %s; expected %s\n' "$label" "$code" "$expected" >&2
    return 1
  fi
}

wait_for_metric() {
  local url=$1
  local pattern=$2
  local attempts=${3:-30}
  local body=""
  for ((attempt = 1; attempt <= attempts; attempt++)); do
    body="$(curl -fsS --max-time 2 "$url" || true)"
    if grep -Eq "$pattern" <<<"$body"; then
      return 0
    fi
    sleep 1
  done
  printf 'timed out waiting for metric %s\n' "$pattern" >&2
  return 1
}

cd "$ROOT_DIR"

# Generate a visible one-point MVT inside a minimal PMTiles v3 archive, two
# styles, and the shared registry snapshot. Keeping the digest construction in
# this fixture aligned with mmpf-auth makes the opaque credential suffixes
# replaceable without checking raw secrets into the repository.
python3 - "$WORK_DIR" <<'PY'
import gzip
import hashlib
import json
import pathlib
import struct
import sys

root = pathlib.Path(sys.argv[1])
(root / "auth").mkdir()
(root / "styles").mkdir()
(root / "tilesets").mkdir()

def varint(value):
    encoded = bytearray()
    while value >= 0x80:
        encoded.append((value & 0x7f) | 0x80)
        value >>= 7
    encoded.append(value)
    return bytes(encoded)

def uint_field(number, value):
    return varint(number << 3) + varint(value)

def bytes_field(number, value):
    return varint((number << 3) | 2) + varint(len(value)) + value

# Vector-tile feature: one point at the center of a 4096-unit tile.
geometry = b"".join(varint(value) for value in (9, 4096, 4096))
feature = uint_field(1, 1) + uint_field(3, 1) + bytes_field(4, geometry)
layer = (
    bytes_field(1, b"fixture")
    + bytes_field(2, feature)
    + uint_field(5, 4096)
    + uint_field(15, 2)
)
mvt = bytes_field(3, layer)
tile = gzip.compress(mvt, mtime=0)

# One root-directory entry for tile id 0 at data offset 0.
directory = b"".join(varint(value) for value in (1, 0, 1, len(tile), 1))
header_size = 127
leaf = b"leaf"
root_offset = header_size
leaf_offset = root_offset + len(directory)
data_offset = leaf_offset + len(leaf)
archive = bytearray(16 * 1024)
archive[:7] = b"PMTiles"
archive[7] = 3
for offset, value in (
    (8, root_offset),
    (16, len(directory)),
    (24, leaf_offset),
    (32, 0),
    (40, leaf_offset),
    (48, len(leaf)),
    (56, data_offset),
    (64, len(archive) - data_offset),
    (72, 1),
    (80, 1),
    (88, 1),
):
    archive[offset:offset + 8] = struct.pack("<Q", value)
archive[96] = 1  # clustered
archive[97] = 1  # internal compression: none
archive[98] = 2  # tile compression: gzip
archive[99] = 1  # tile type: MVT
archive[100] = 0
archive[101] = 0
archive[root_offset:leaf_offset] = directory
archive[leaf_offset:data_offset] = leaf
archive[data_offset:data_offset + len(tile)] = tile
(root / "tilesets" / "fixture.pmtiles").write_bytes(archive)

auth_style = {
    "version": 8,
    "sources": {"fixture": {"type": "vector", "url": "/fixture"}},
    "layers": [{
        "id": "visible-point",
        "type": "circle",
        "source": "fixture",
        "source-layer": "fixture",
        "paint": {"circle-radius": 20, "circle-color": "#ff0000"},
    }],
}
blank_style = {"version": 8, "sources": {}, "layers": []}
(root / "styles" / "auth-style.json").write_text(json.dumps(auth_style))
(root / "styles" / "blank-style.json").write_text(json.dumps(blank_style))

def credential_sha256(registry_id, credential):
    domain = b"mmpf-object-store-auth-v1\0"
    registry = registry_id.encode()
    secret = credential.encode()
    digest = hashlib.sha256()
    digest.update(domain)
    digest.update(len(registry).to_bytes(8, "big"))
    digest.update(registry)
    digest.update(len(secret).to_bytes(8, "big"))
    digest.update(secret)
    return digest.hexdigest()

snapshot = {
    "schema_version": 1,
    "registry_id": "public",
    "revision": 1,
    "credentials": [
        {
            "credential_sha256": credential_sha256("public", "broad"),
            "principal_id": "auth-e2e-broad",
            "enabled": True,
            "namespaces": ["fixture", "smoke"],
            "actions": ["read", "render.static"],
            "allow_missing_origin": True,
        },
        {
            "credential_sha256": credential_sha256("public", "style-only"),
            "principal_id": "auth-e2e-style-only",
            "enabled": True,
            "namespaces": ["smoke"],
            "actions": ["read", "render.static"],
            "allow_missing_origin": True,
        },
    ],
}
(root / "auth" / "current.json").write_text(json.dumps(snapshot))
PY
chmod -R a+rX "$WORK_DIR"

python3 -m http.server "$FIXTURE_PORT" --bind 0.0.0.0 \
  --directory "$WORK_DIR/styles" >"$WORK_DIR/fixture-server.log" 2>&1 &
FIXTURE_PID=$!
wait_for_status "http://127.0.0.1:${FIXTURE_PORT}/auth-style.json" 200 15

docker network create "$NETWORK" >/dev/null

ISHIKARI_CONTAINER="${PREFIX}-ishikari"
CONTAINERS+=("$ISHIKARI_CONTAINER")
docker run -d \
  --name "$ISHIKARI_CONTAINER" \
  --network "$NETWORK" \
  --network-alias ishikari \
  ${HOST_GATEWAY_ARGS[@]+"${HOST_GATEWAY_ARGS[@]}"} \
  --read-only \
  --tmpfs /tmp:rw,noexec,nosuid,size=64m \
  -v "$WORK_DIR:/fixtures:ro" \
  -p "127.0.0.1:${ISHIKARI_PUBLIC_PORT}:8080" \
  -e ISKR_NODE_ID=auth-e2e-ishikari \
  -e ISKR_HTTP_PORT=8080 \
  -e ISKR_INTERNAL_HTTP_PORT=9090 \
  -e ISKR_AUTH_REGISTRIES=public=file:///fixtures/auth/ \
  -e ISKR_TILESET_SOURCES=/fixtures/tilesets \
  -e "ISKR_STYLE_TEMPLATES=smoke=http://${HOST_ALIAS}:${FIXTURE_PORT}/{style_id}.json" \
  "$ISHIKARI_IMAGE" >/dev/null

wait_for_status "http://127.0.0.1:${ISHIKARI_PUBLIC_PORT}/readyz" 200

BIEI_CONTAINER="${PREFIX}-biei"
CONTAINERS+=("$BIEI_CONTAINER")
docker run -d \
  --name "$BIEI_CONTAINER" \
  --network "$NETWORK" \
  --read-only \
  --tmpfs /tmp:rw,noexec,nosuid,size=64m \
  --tmpfs /var/cache/biei:rw,noexec,nosuid,size=64m \
  -v "$WORK_DIR:/fixtures:ro" \
  -p "127.0.0.1:${BIEI_PUBLIC_PORT}:8080" \
  -e BIEI_NODE_ID=auth-e2e-biei \
  -e BIEI_HTTP_BIND=0.0.0.0:8080 \
  -e BIEI_CORES=1 \
  -e BIEI_AUTH_REGISTRIES=public=file:///fixtures/auth/ \
  -e BIEI_AUTH_PROVIDER_ORIGIN=http://ishikari:8080 \
  -e 'BIEI_STYLE_TEMPLATES=http://ishikari:8080/styles/{style_id}/style.json' \
  -e BIEI_MLN_RESOURCE_PRIVATE_HOSTS=ishikari \
  -e BIEI_RENDER_OUTPUT_CACHE_BYTES=16777216 \
  -e BIEI_MLN_RESOURCE_CACHE_BYTES=16777216 \
  "$BIEI_IMAGE" >/dev/null

wait_for_status "http://127.0.0.1:${BIEI_PUBLIC_PORT}/readyz" 200

BIEI_BASE="http://127.0.0.1:${BIEI_PUBLIC_PORT}/smoke"
ISHIKARI_BASE="http://127.0.0.1:${ISHIKARI_PUBLIC_PORT}"
BROAD_QUERY="access_token=public.broad"
WEAK_QUERY="access_token=public.style-only"

expect_status "unauthenticated Biei render" \
  "${BIEI_BASE}/auth-style/static/0,0,0/256x256.png" 401
expect_status "weaker token style access" \
  "${ISHIKARI_BASE}/styles/smoke/auth-style/style.json?${WEAK_QUERY}" 200

curl -g -fsS --show-error --max-time 30 \
  "${BIEI_BASE}/auth-style/static/0,0,0/256x256.png?${BROAD_QUERY}" \
  --output "$WORK_DIR/broad.png"
curl -g -fsS --show-error --max-time 30 \
  "${BIEI_BASE}/blank-style/static/0,0,0/256x256.png?${BROAD_QUERY}" \
  --output "$WORK_DIR/blank.png"

test "$(od -An -tx1 -N8 "$WORK_DIR/broad.png" | tr -d ' \n')" = 89504e470d0a1a0a
if cmp -s "$WORK_DIR/broad.png" "$WORK_DIR/blank.png"; then
  printf 'authorized tileset did not affect rendered bytes\n' >&2
  exit 1
fi

# The broad request above populated Ishikari's shared tile cache. Authorization
# for the current request must still run before that cache.
expect_status "weaker token cached tile access" \
  "${ISHIKARI_BASE}/tilesets/fixture/0/0/0?${WEAK_QUERY}" 403

# This is the composed non-interference assertion. The weaker token is valid for
# Biei and the style namespace, but not the referenced tileset. It may fail the
# render or produce a degraded image; it must never receive the broad image.
weak_code="$(curl -g -sS --max-time 30 \
  -o "$WORK_DIR/weak-response" -w '%{http_code}' \
  "${BIEI_BASE}/auth-style/static/0,0,0/256x256.png?${WEAK_QUERY}" || true)"
if [[ "$weak_code" == "000" ]]; then
  printf 'weaker render did not produce an HTTP response\n' >&2
  exit 1
fi
if [[ "$weak_code" == "200" ]] && cmp -s "$WORK_DIR/weak-response" "$WORK_DIR/broad.png"; then
  printf 'weaker token received the broad token render\n' >&2
  exit 1
fi

# A denied weaker request must neither replace nor poison the authorized entry.
curl -g -fsS --show-error --max-time 30 \
  "${BIEI_BASE}/auth-style/static/0,0,0/256x256.png?${BROAD_QUERY}" \
  --output "$WORK_DIR/broad-again.png"
cmp "$WORK_DIR/broad.png" "$WORK_DIR/broad-again.png"
wait_for_metric \
  "http://127.0.0.1:${BIEI_PUBLIC_PORT}/_internal/metrics" \
  'biei_render_output_cache_total\{[^}]*outcome="hit"[^}]*\} [1-9][0-9]*'

printf 'PASS: authenticated Biei -> Ishikari render preserved cache non-interference\n'
