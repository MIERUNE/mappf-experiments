#!/usr/bin/env bash
# Regression checks for the portable CPU-only autoscaling example.
set -euo pipefail

root="$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)"
rendered="$(KUBECONFIG=/dev/null kubectl kustomize "$root/k8s/overlays/gke")"
hpa="$(printf '%s\n' "$rendered" | awk '
  BEGIN { RS = "---" }
  /kind: HorizontalPodAutoscaler/ { print; found = 1; exit }
  END { if (!found) exit 1 }
')"

metrics="$(printf '%s\n' "$hpa" | sed -n '/^  metrics:/,/^  scaleTargetRef:/p')"
scale_down="$(printf '%s\n' "$hpa" | sed -n '/^    scaleDown:/,/^    scaleUp:/p')"
scale_up="$(printf '%s\n' "$hpa" | sed -n '/^    scaleUp:/,$p')"

expect() {
  local text="$1"
  local pattern="$2"
  local description="$3"
  if ! grep -Eq "$pattern" <<<"$text"; then
    printf 'FAIL: %s\n' "$description" >&2
    exit 1
  fi
}

reject() {
  local text="$1"
  local pattern="$2"
  local description="$3"
  if grep -Eq "$pattern" <<<"$text"; then
    printf 'FAIL: %s\n' "$description" >&2
    exit 1
  fi
}

expect_policy() {
  local section="$1"
  local direction="$2"
  local policy_type="$3"
  local value="$4"
  local period="$5"
  if ! awk -v wanted_type="$policy_type" -v wanted_value="$value" -v wanted_period="$period" '
    /- periodSeconds:/ { period = $3; type = ""; value = "" }
    /type:/ { type = $2 }
    /value:/ {
      value = $2
      if (type == wanted_type && value == wanted_value && period == wanted_period) found = 1
    }
    END { exit(found ? 0 : 1) }
  ' <<<"$section"; then
    printf 'FAIL: %s must include %s %s/%ss policy\n' \
      "$direction" "$policy_type" "$value" "$period" >&2
    exit 1
  fi
}

# Keep the example usable without a Prometheus/custom-metrics adapter.
expect "$hpa" 'minReplicas: 2' 'HPA must retain the two-pod cost floor'
expect "$metrics" 'name: cpu' 'HPA must scale on CPU'
expect "$metrics" 'type: Resource' 'HPA must use a standard resource metric'
expect "$metrics" 'averageUtilization: 50' 'HPA must retain 50% CPU headroom'
reject "$metrics" 'type: (External|Object|Pods)' 'HPA must not require custom metrics'

# Preserve fast burst expansion and the full provider retry window on scale-in.
expect "$scale_up" 'stabilizationWindowSeconds: 0' 'scale-up must not be stabilized'
expect "$scale_up" 'selectPolicy: Max' 'scale-up must choose the fastest allowed policy'
expect_policy "$scale_up" scale-up Percent 100 15
expect_policy "$scale_up" scale-up Pods 2 15
expect "$scale_down" 'stabilizationWindowSeconds: 600' \
  'scale-down must protect warm pods for the provider retry window'
expect_policy "$scale_down" scale-down Pods 1 60

printf 'PASS: portable CPU HPA policies are explicit\n'
