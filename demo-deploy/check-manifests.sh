#!/usr/bin/env bash
# Render every checked-in deployment composition and assert cluster contracts
# that Kustomize can otherwise render while silently wiring incorrectly.
set -euo pipefail

root="$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)"

for overlay in \
  "$root" \
  "$root/biei/runtime/k8s/base" \
  "$root/biei/runtime/k8s/overlays/local" \
  "$root/biei/runtime/k8s/overlays/gke" \
  "$root/ishikari/runtime/k8s/base" \
  "$root/ishikari/runtime/k8s/overlays/gke" \
  "$root/platform"
do
  KUBECONFIG=/dev/null kubectl kustomize "$overlay" >/dev/null
done

# Preserve the more detailed service-specific policy checks.
bash "$root/biei/runtime/check-hpa.sh"
bash "$root/ishikari/runtime/check-network-policy.sh"

rendered="$(KUBECONFIG=/dev/null kubectl kustomize "$root")"

document() {
  local kind="$1"
  local name="$2"
  awk -v kind="$kind" -v name="$name" '
    BEGIN { RS = "---" }
    $0 ~ "(^|\\n)kind: " kind "(\\n|$)" &&
      $0 ~ "(^|\\n)  name: " name "(\\n|$)" {
        sub(/^[[:space:]]+/, "")
        sub(/[[:space:]]+$/, "")
        print
        found = 1
        exit
      }
    END { if (!found) exit 1 }
  ' <<<"$rendered"
}

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

for app in biei ishikari; do
  deployment="$(document Deployment "$app")"
  gossip_service="$(document Service "$app-gossip")"
  public_service="$(document Service "$app")"
  hpa="$(document HorizontalPodAutoscaler "$app")"
  pdb="$(document PodDisruptionBudget "$app")"

  # The HPA, rather than repeated `kubectl apply`, owns the live replica count.
  reject "$deployment" '^  replicas:' "$app GKE Deployment must omit spec.replicas"
  expect "$hpa" 'minReplicas: 2' "$app HPA must retain the two-pod floor"
  expect "$hpa" "name: $app" "$app HPA must target its Deployment"
  expect "$pdb" 'maxUnavailable: 1' "$app PDB must allow only one voluntary disruption"
  expect "$deployment" 'topologyKey: kubernetes.io/hostname' \
    "$app pods must prefer separate nodes"
  expect "$deployment" 'whenUnsatisfiable: ScheduleAnyway' \
    "$app topology preference must not block scheduling"

  # Public HTTP, peer HTTP, and gossip are separate listener contracts.
  expect "$deployment" 'containerPort: 8080' "$app must listen publicly on TCP 8080"
  expect "$deployment" 'containerPort: 9090' "$app must listen internally on TCP 9090"
  expect "$deployment" 'containerPort: 7946' "$app must listen for gossip on UDP 7946"
  expect "$deployment" 'protocol: UDP' "$app gossip container port must remain UDP"

  expect "$gossip_service" 'clusterIP: None' "$app gossip Service must remain headless"
  expect "$gossip_service" 'publishNotReadyAddresses: true' \
    "$app bootstrap discovery must include not-ready peers"
  expect "$gossip_service" 'targetPort: gossip' "$app gossip Service must target gossip"
  expect "$gossip_service" 'protocol: UDP' "$app gossip Service must remain UDP"

  expect "$public_service" 'targetPort: http' "$app public Service must target HTTP"
  reject "$public_service" 'targetPort: (internal|gossip)' \
    "$app public Service must not expose peer or gossip listeners"
done

printf 'PASS: all deployment compositions render and preserve cluster contracts\n'
