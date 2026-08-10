#!/usr/bin/env bash
# Post-rollout warm-up for the biei demo.
#
# Issues one representative render per configured target through the PUBLIC
# endpoint, so ordinary cluster routing (HRW) places each profile on its owner
# node/worker — this script never targets a specific pod. Serial and bounded, so
# it does not amplify provider traffic or thrash worker slots during a rollout.
#
# Why a real render, not just a style/TileJSON prefetch: the dominant cold cost
# is first-touch in-render resource I/O (glyphs, sprites, source tiles) plus
# native style setup, none of which a metadata prefetch warms. Only an actual
# representative render loads them. See specs/biei-spec.md §4.5 (the request
# deadline is larger than the routing SLA precisely so this cold render
# completes instead of timing out and retiring its actor).
#
# This is an EXTERNAL, best-effort job. It is NOT part of pod readiness, and a
# warm-up miss has no effect on liveness — provider trouble must not keep new
# capacity from serving. Run it AFTER a rollout settles.
#
# Targets are explicit (style + render mode + scale + representative position)
# because native warmth is per-(style, mode, scale) profile: "warm this style"
# alone is underspecified. Keep the target count at or below the cluster's total
# warm-slot budget (roughly pods x renderer-slots) or warming just evicts what it
# warmed a moment ago. Only anonymous, publicly-renderable styles belong here.
#
# Usage:
#   bash demo-deploy/biei/runtime/warmup.sh
#   SCHEME=http BIEI_HOST=localhost:8080 bash demo-deploy/biei/runtime/warmup.sh
#   MEASURE=1 bash demo-deploy/biei/runtime/warmup.sh   # render twice: cold+warm
#
# Exit code is always 0: a warm-up is advisory and must not fail a rollout gate.
set -uo pipefail

SCHEME="${SCHEME:-https}"
BIEI_HOST="${BIEI_HOST:-biei-demo.mierune.dev}"
TIMEOUT="${TIMEOUT:-30}"
MEASURE="${MEASURE:-0}"
biei="${SCHEME}://${BIEI_HOST}"

# Representative position shared by every static target: central Tokyo, z11.
# Output size does not affect profile identity (style/mode/scale), so one size
# is enough to warm each profile.
POS="static/139.767,35.681,11,0,0/512x384.png"

# Explicit, bounded target list — one static profile per demo style. Format is
# "label|render-path"; the path is exactly what an anonymous client requests.
TARGETS=(
  "carto/voyager-gl-style|styles/carto/voyager-gl-style/${POS}"
  "carto/positron-gl-style|styles/carto/positron-gl-style/${POS}"
  "carto/dark-matter-gl-style|styles/carto/dark-matter-gl-style/${POS}"
  "mierune/jp_mierune_streets|styles/mierune/jp_mierune_streets/${POS}"
  "mierune/jp_mierune_dark|styles/mierune/jp_mierune_dark/${POS}"
  "mierune/jp_mierune_gray|styles/mierune/jp_mierune_gray/${POS}"
)

render() { # url -> "code time_total"
  curl -g -s -o /dev/null -w '%{http_code} %{time_total}' --max-time "$TIMEOUT" "$1"
}

started=0
completed=0
failed=0

echo "warm-up: ${#TARGETS[@]} targets via ${biei} (serial, cluster-routed, best-effort)"
for entry in "${TARGETS[@]}"; do
  label="${entry%%|*}"
  url="${biei}/${entry#*|}"
  started=$((started + 1))
  read -r code t1 < <(render "$url")
  if [ "$code" = "200" ]; then
    completed=$((completed + 1))
    if [ "$MEASURE" = "1" ]; then
      read -r _ t2 < <(render "$url")
      printf 'warmed   %-30s cold=%ss warm=%ss\n' "$label" "$t1" "$t2"
    else
      printf 'warmed   %-30s %ss\n' "$label" "$t1"
    fi
  else
    failed=$((failed + 1))
    printf 'MISS     %-30s code %s after %ss\n' "$label" "$code" "$t1"
  fi
done

echo "started=${started} completed=${completed} failed=${failed}"
exit 0
