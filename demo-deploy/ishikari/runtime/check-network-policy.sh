#!/usr/bin/env bash
# Assert the rendered GKE ingress trust boundary without requiring a cluster.
set -euo pipefail

root="$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)"
rendered="$(KUBECONFIG=/dev/null kubectl kustomize "$root/k8s/overlays/gke")"
policy="$(printf '%s\n' "$rendered" | awk '
  BEGIN { RS = "---" }
  /kind: NetworkPolicy/ && /name: ishikari-internal-boundary/ {
    sub(/^[[:space:]]+/, "")
    sub(/[[:space:]]+$/, "")
    print
    found = 1
    exit
  }
  END { if (!found) exit 1 }
')"
expected="$(cat <<'EOF'
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: ishikari-internal-boundary
  namespace: map-demo
spec:
  ingress:
  - ports:
    - port: 8080
      protocol: TCP
  - from:
    - podSelector:
        matchLabels:
          app: ishikari
    ports:
    - port: 9090
      protocol: TCP
    - port: 7946
      protocol: UDP
  - from:
    - namespaceSelector:
        matchLabels:
          kubernetes.io/metadata.name: gke-gmp-system
      podSelector:
        matchLabels:
          app.kubernetes.io/name: collector
    ports:
    - port: 9090
      protocol: TCP
  podSelector:
    matchLabels:
      app: ishikari
  policyTypes:
  - Ingress
EOF
)"

if [[ "$policy" != "$expected" ]]; then
  printf 'FAIL: rendered ishikari NetworkPolicy does not match the reviewed trust boundary\n' >&2
  printf '%s\n' '--- expected ---' "$expected" '--- actual ---' "$policy" >&2
  exit 1
fi

printf '%s\n' \
  'PASS: public/probe TCP 8080 is allowed' \
  'PASS: Ishikari peers may use internal TCP 9090 and gossip UDP 7946' \
  'PASS: managed Prometheus collectors may scrape TCP 9090' \
  'PASS: all other ingress to Ishikari pods is denied'
