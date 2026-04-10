#!/usr/bin/env bash
# Smoke test for the Ishikari demo: confirms the public provider routes serve
# through the Gateway and that the cluster-internal surface is NOT publicly
# reachable. Exits non-zero if any hard check fails.
#
# Usage:
#   bash demo-deploy/smoke.sh
#   ISHIKARI_HOST=ishikari-demo.mierune.dev SCHEME=https bash demo-deploy/smoke.sh
#   ISHIKARI_HOST=localhost:8080 SCHEME=http bash demo-deploy/smoke.sh   # port-forward
set -uo pipefail

SCHEME="${SCHEME:-https}"
ISHIKARI_HOST="${ISHIKARI_HOST:-ishikari-demo.mierune.dev}"
TIMEOUT="${TIMEOUT:-25}"
# A tileset key and style id that exist in the demo bucket; override for your data.
TILESET="${TILESET:-mierune/omt}"
STYLE="${STYLE:-mierune/jp_mierune_streets}"
TILE="${TILE:-8/227/100}"
GLYPH_FONT="${GLYPH_FONT:-Noto%20Sans%20CJK%20JP%20Regular}"
GLYPH_RANGE="${GLYPH_RANGE:-0-255}"

base="${SCHEME}://${ISHIKARI_HOST}"
fail=0

check() { # name expected-code url
  local code
  code=$(curl -g -s -o /dev/null -w '%{http_code}' --max-time "$TIMEOUT" "$3" 2>/dev/null)
  if [ "$code" = "$2" ]; then
    printf 'OK   %-34s %s\n' "$1" "$code"
  else
    printf 'FAIL %-34s got %s want %s  (%s)\n' "$1" "$code" "$2" "$3"
    fail=$((fail + 1))
  fi
}

check_type() { # name expected-code expected-content-type-prefix url
  local tmp code content_type
  tmp="$(mktemp)"
  code=$(curl -g -s -D "$tmp" -o /dev/null -w '%{http_code}' --max-time "$TIMEOUT" "$4" 2>/dev/null)
  content_type=$(awk 'BEGIN{IGNORECASE=1} /^content-type:/ {sub(/\r$/, "", $0); print $2; exit}' "$tmp")
  rm -f "$tmp"
  if [ "$code" = "$2" ] && [[ "$content_type" == "$3"* ]]; then
    printf 'OK   %-34s %s %s\n' "$1" "$code" "$content_type"
  else
    printf 'FAIL %-34s got %s %s want %s %s  (%s)\n' "$1" "$code" "${content_type:-<none>}" "$2" "$3" "$4"
    fail=$((fail + 1))
  fi
}

echo "== ishikari smoke: ${base} =="
# Public provider routes (catch-all -> :8080).
check_type "tilejson"        200 "application/json" "${base}/tilesets/${TILESET}"
check_type "style.json"      200 "application/json" "${base}/styles/${STYLE}/style.json"
check_type "tile"            200 "application/vnd.mapbox-vector-tile" "${base}/tilesets/${TILESET}/${TILE}"
check_type "glyph"           200 "application/x-protobuf" "${base}/fonts/${GLYPH_FONT}/${GLYPH_RANGE}.pbf"
check_type "sprite.json"     200 "application/json" "${base}/styles/${STYLE}/sprite.json"
check_type "sprite.png"      200 "image/png" "${base}/styles/${STYLE}/sprite.png"
check "readyz"          200 "${base}/readyz"
check "livez"           200 "${base}/livez"
# Internal surface must NOT be reachable through the Gateway.
check "internal-metrics blocked" 404 "${base}/_internal/metrics"
check "internal-cluster blocked" 404 "${base}/_internal/cluster"

if [ "$fail" -eq 0 ]; then
  echo "== ishikari smoke: PASS =="
else
  echo "== ishikari smoke: ${fail} FAILED =="
fi
exit "$fail"
