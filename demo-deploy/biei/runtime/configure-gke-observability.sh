#!/usr/bin/env bash
# Apply the cost-conscious observability profile used by the GKE demo.
# Keeps mandatory system metrics and application Prometheus scrapes, while
# dropping unused high-volume GKE metric packages and known INFO-only node
# noise. Run again safely after cluster recreation or configuration changes.
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-mappf-experiment}"
CLUSTER="${CLUSTER:-mappf}"
REGION="${REGION:-asia-northeast1}"

upsert_exclusion() { # name description filter
  local name="$1"
  local description="$2"
  local filter="$3"
  local operation

  if gcloud logging sinks describe _Default \
    --project="${PROJECT_ID}" \
    --format=json \
    | jq -e --arg name "${name}" '.exclusions[]? | select(.name == $name)' \
      >/dev/null; then
    operation="--update-exclusion"
  else
    operation="--add-exclusion"
  fi

  gcloud logging sinks update _Default \
    --project="${PROJECT_ID}" \
    "${operation}=name=${name},description=${description},filter=${filter}"
}

# These node services write syslog PRIORITY=6 (informational) at very high
# volume. Preserve notice/warning/error priorities from the same log ids.
upsert_exclusion \
  poc-gke-node-info-noise \
  "PoC drops informational GKE node runtime logs" \
  'resource.type="k8s_node" AND jsonPayload.PRIORITY="6" AND (log_id("gcfsd") OR log_id("gcfs-snapshotter") OR log_id("kubelet") OR log_id("container-runtime") OR log_id("kube-node-configuration") OR log_id("kube-node-installation"))'

# Port 1 remains available for boot/kernel diagnostics. Port 3 is a verbose
# container-runtime stream and the debug port is intentionally non-operational.
upsert_exclusion \
  poc-verbose-serial-console \
  "PoC drops verbose serial port 3 and debug streams" \
  'resource.type="gce_instance" AND (log_id("serialconsole.googleapis.com/serial_port_3_output") OR log_id("serialconsole.googleapis.com/serial_port_debug_output"))'

# HPA resource metrics come from Kubernetes metrics APIs, not these Cloud
# Monitoring packages. Managed Prometheus remains enabled for the explicit
# biei/ishikari PodMonitoring resources.
gcloud container clusters update "${CLUSTER}" \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --monitoring=SYSTEM \
  --enable-managed-prometheus \
  --quiet

# GKE's CLI advertises both flags for cluster updates, but Autopilot retains
# advanced datapath metrics and rejects disabling image streaming. Keep the
# features there and rely on package reduction plus narrow INFO exclusions;
# Standard clusters can disable both.
autopilot=$(gcloud container clusters describe "${CLUSTER}" \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --format='value(autopilot.enabled)')
if [[ "${autopilot}" == "True" ]]; then
  echo "Autopilot retains datapath metrics and image streaming; INFO logs are excluded."
else
  # gcloud models these feature families as mutually exclusive update
  # operations, so keep them as separate idempotent calls.
  gcloud container clusters update "${CLUSTER}" \
    --project="${PROJECT_ID}" \
    --region="${REGION}" \
    --disable-dataplane-v2-metrics \
    --quiet

  gcloud container clusters update "${CLUSTER}" \
    --project="${PROJECT_ID}" \
    --region="${REGION}" \
    --no-enable-image-streaming \
    --quiet
fi
