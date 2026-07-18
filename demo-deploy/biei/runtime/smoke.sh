#!/usr/bin/env bash
# End-to-end smoke test for the consolidated demo: the biei renderer plus the
# ishikari provider it renders through. Confirms public render/provider routes
# serve through the Gateway and that the cluster-internal surface is NOT publicly
# reachable. Exits non-zero if any hard check fails.
#
# Usage:
#   bash demo-deploy/biei/runtime/smoke.sh
#   BIEI_HOST=biei-demo.mierune.dev ISHIKARI_HOST=ishikari-demo.mierune.dev \
#     SCHEME=https bash demo-deploy/biei/runtime/smoke.sh
#   BIEI_HOST=localhost:8080 SCHEME=http \
#     bash demo-deploy/biei/runtime/smoke.sh   # port-forward
#
# Optional: set BIEI_ADDLAYER_URL to a full addlayer static URL to also exercise
# the addlayer path (syntax/tileset depends on your deployment, so it is opt-in).
set -uo pipefail

SCHEME="${SCHEME:-https}"
BIEI_HOST="${BIEI_HOST:-biei-demo.mierune.dev}"
ISHIKARI_HOST="${ISHIKARI_HOST:-ishikari-demo.mierune.dev}"
TIMEOUT="${TIMEOUT:-30}"
# Style ids / tileset key present in the demo; override for your data.
STYLE="${STYLE:-carto/voyager-gl-style}"
MIERUNE_STYLE="${MIERUNE_STYLE:-mierune/jp_mierune_streets}"
TILESET="${TILESET:-mierune/omt}"
TILE="${TILE:-8/227/100}"
GLYPH_FONT="${GLYPH_FONT:-Noto%20Sans%20CJK%20JP%20Regular}"
GLYPH_RANGE="${GLYPH_RANGE:-0-255}"

biei="${SCHEME}://${BIEI_HOST}"
ishikari="${SCHEME}://${ISHIKARI_HOST}"
fail=0
soft=0

check() { # name expected-code url
  local code
  code=$(curl -g -s -o /dev/null -w '%{http_code}' --max-time "$TIMEOUT" "$3" 2>/dev/null)
  if [ "$code" = "$2" ]; then
    printf 'OK   %-30s %s\n' "$1" "$code"
  else
    printf 'FAIL %-30s got %s want %s  (%s)\n' "$1" "$code" "$2" "$3"
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
    printf 'OK   %-30s %s %s\n' "$1" "$code" "$content_type"
  else
    printf 'FAIL %-30s got %s %s want %s %s  (%s)\n' "$1" "$code" "${content_type:-<none>}" "$2" "$3" "$4"
    fail=$((fail + 1))
  fi
}

softcheck() { # name expected-code url   (warns instead of failing)
  local code
  code=$(curl -g -s -o /dev/null -w '%{http_code}' --max-time "$TIMEOUT" "$3" 2>/dev/null)
  if [ "$code" = "$2" ]; then
    printf 'OK   %-30s %s\n' "$1" "$code"
  else
    printf 'WARN %-30s got %s want %s  (%s)\n' "$1" "$code" "$2" "$3"
    soft=$((soft + 1))
  fi
}

echo "== provider (ishikari): ${ishikari} =="
check_type "ishikari tilejson"    200 "application/json" "${ishikari}/tilesets/${TILESET}"
check_type "ishikari style.json"  200 "application/json" "${ishikari}/styles/${MIERUNE_STYLE}/style.json"
check_type "ishikari tile"        200 "application/vnd.mapbox-vector-tile" "${ishikari}/tilesets/${TILESET}/${TILE}"
check_type "ishikari glyph"       200 "application/x-protobuf" "${ishikari}/fonts/${GLYPH_FONT}/${GLYPH_RANGE}.pbf"
check_type "ishikari sprite.json" 200 "application/json" "${ishikari}/styles/${MIERUNE_STYLE}/sprite.json"
check_type "ishikari sprite.png"  200 "image/png" "${ishikari}/styles/${MIERUNE_STYLE}/sprite.png"
check "ishikari _internal blocked" 404 "${ishikari}/_internal/metrics"

echo "== renderer (biei): ${biei} =="
check "biei readyz"         200 "${biei}/readyz"
check "biei livez"          200 "${biei}/livez"
check "biei _internal blocked" 404 "${biei}/_internal/metrics"
# Render paths (biei -> ishikari -> private GCS).
check_type "biei static (bbox)" 200 "image/webp" "${biei}/${STYLE}/static/[139.6,35.6,139.9,35.8]/512x384.webp"
check_type "biei tile"          200 "image/webp" "${biei}/${STYLE}/8/227/100.webp"
check_type "biei preview"       200 "text/html" "${biei}/${MIERUNE_STYLE}/preview"

# addlayer is opt-in: its query syntax/tileset depend on the deployment.
if [ -n "${BIEI_ADDLAYER_URL:-}" ]; then
  echo "== addlayer (opt-in) =="
  softcheck "biei addlayer" 200 "${BIEI_ADDLAYER_URL}"
fi

echo
if [ "$fail" -eq 0 ]; then
  echo "== demo smoke: PASS (${soft} soft warnings) =="
else
  echo "== demo smoke: ${fail} FAILED (${soft} soft warnings) =="
fi
exit "$fail"
