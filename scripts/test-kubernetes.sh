#!/bin/sh
# Run the Kubernetes manifests on a throwaway kind cluster against a real
# bucket, and check a ledger while the autoscaler adds pods, a pod is killed,
# and the autoscaler removes pods again.
#
#   DDB_S3_ENDPOINT, DDB_S3_BUCKET, DDB_S3_ACCESS_KEY_ID, DDB_S3_SECRET_ACCESS_KEY
#   DURABLE_DB_IMAGE   image to test (default durable-db:dev, loaded from the local Docker)
#   KEEP=1             leave the cluster running afterwards
#
# The cluster has its own kubeconfig; yours is not read or changed.
set -eu
repo=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
: "${DDB_S3_ENDPOINT:?}" "${DDB_S3_BUCKET:?}" "${DDB_S3_ACCESS_KEY_ID:?}" "${DDB_S3_SECRET_ACCESS_KEY:?}"
image=${DURABLE_DB_IMAGE:-durable-db:dev}
cluster=durable-db-test
ns=durable-db
ledger_seconds=${LEDGER_SECONDS:-240}
work=$(mktemp -d)
export KUBECONFIG="$work/kubeconfig"

kind=${KIND:-$repo/.tools/bin/kind}
if [ ! -x "$kind" ]; then
  mkdir -p "$(dirname "$kind")"
  curl --proto '=https' --tlsv1.2 -fLsS https://kind.sigs.k8s.io/dl/v0.33.0/kind-linux-amd64 -o "$kind"
  echo "aee6151561422756b764a4ae28e7f44cda5af5a9eead3cc9985112b1de8d8e0d  $kind" | sha256sum -c - >/dev/null
  chmod +x "$kind"
fi

cleanup() {
  status=$?
  if [ "${KEEP:-}" != 1 ]; then "$kind" delete cluster --name "$cluster" >/dev/null 2>&1 || true; fi
  rm -rf "$work"
  exit "$status"
}
trap cleanup EXIT INT TERM
say() { printf '\n== %s\n' "$*"; }
k() { kubectl -n "$ns" "$@"; }
replicas() { k get deployment durable-db -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo 0; }

say "creating a three-node kind cluster"
"$kind" delete cluster --name "$cluster" >/dev/null 2>&1 || true
"$kind" create cluster --name "$cluster" --wait 180s --config - <<YAML
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
  - role: worker
  - role: worker
YAML
"$kind" load docker-image "$image" --name "$cluster"

say "installing metrics-server for the autoscaler"
kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/download/v0.7.2/components.yaml >/dev/null
kubectl -n kube-system patch deployment metrics-server --type=json \
  -p='[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--kubelet-insecure-tls"}]' >/dev/null

say "deploying durable-db"
kubectl create namespace "$ns" >/dev/null
openssl rand -hex 32 > "$work/admin.key"
k create secret generic durable-db-keys --from-file=admin.key="$work/admin.key" >/dev/null
k create secret generic durable-db-storage \
  --from-literal=DURABLE_DB_BUCKET="$DDB_S3_BUCKET" \
  --from-literal=DURABLE_DB_ENDPOINT="$DDB_S3_ENDPOINT" \
  --from-literal=DURABLE_DB_PREFIX="kubernetes-test-$(date +%s)" \
  --from-literal=AWS_ACCESS_KEY_ID="$DDB_S3_ACCESS_KEY_ID" \
  --from-literal=AWS_SECRET_ACCESS_KEY="$DDB_S3_SECRET_ACCESS_KEY" >/dev/null
mkdir "$work/deploy"
cp "$repo"/deploy/kubernetes/*.yaml "$work/deploy/"
# The shipped manifests, with this image and an autoscaler that reacts in
# seconds rather than minutes so the test stays short.
cat >> "$work/deploy/kustomization.yaml" <<YAML
images:
  - name: durable-db
    newName: ${image%:*}
    newTag: ${image##*:}
patches:
  - target: {kind: HorizontalPodAutoscaler, name: durable-db}
    patch: |
      - {op: replace, path: /spec/behavior/scaleDown/stabilizationWindowSeconds, value: 20}
      - {op: replace, path: /spec/behavior/scaleDown/policies/0/periodSeconds, value: 15}
      - {op: replace, path: /spec/maxReplicas, value: 5}
YAML
k apply -k "$work/deploy" >/dev/null
k rollout status deployment/durable-db --timeout=240s

job() { # name, then the bench arguments
  name=$1; shift
  args=$(printf '"%s",' "$@")
  k apply -f - >/dev/null <<YAML
apiVersion: batch/v1
kind: Job
metadata: {name: $name}
spec:
  backoffLimit: 0
  template:
    spec:
      restartPolicy: Never
      securityContext: {runAsNonRoot: true, runAsUser: 65532, runAsGroup: 65532, fsGroup: 65532}
      containers:
        - name: bench
          image: $image
          args: ["bench", "--url", "http://durable-db:7654", "--key-file", "/run/keys/admin.key", ${args%,}]
          volumeMounts: [{name: keys, mountPath: /run/keys, readOnly: true}]
      volumes: [{name: keys, secret: {secretName: durable-db-keys, defaultMode: 0440}}]
YAML
}

say "starting the ledger: ${ledger_seconds}s of transfers through the Service"
job ledger --workload ledger --clients 16 --databases 8 --seconds "$ledger_seconds"
sleep 20

say "adding CPU load so the autoscaler adds pods"
job load --workload session-read --clients 192 --databases 4 --keys 500 --seconds 80
peak=2
deadline=$(($(date +%s) + 150))
while [ "$(date +%s)" -lt "$deadline" ]; do
  now=$(replicas)
  [ "${now:-0}" -gt "$peak" ] && peak=$now
  [ "$peak" -ge 3 ] && break
  sleep 5
done
[ "$peak" -ge 3 ] || { k describe hpa durable-db | tail -15; echo 'the autoscaler did not add a pod' >&2; exit 1; }
echo "the autoscaler added pods: $peak are ready"

say "killing one pod outright"
victim=$(k get pods -l app=durable-db -o jsonpath='{.items[0].metadata.name}')
k delete pod "$victim" --grace-period=0 --force >/dev/null 2>&1
echo "killed $victim"

say "waiting for the load to end and the autoscaler to remove pods"
deadline=$(($(date +%s) + 240))
while [ "$(date +%s)" -lt "$deadline" ]; do
  now=$(replicas)
  [ "${now:-0}" -gt "$peak" ] && peak=$now
  want=$(k get deployment durable-db -o jsonpath='{.spec.replicas}')
  [ "$want" -le 2 ] && break
  sleep 5
done
echo "the autoscaler scaled in to $(k get deployment durable-db -o jsonpath='{.spec.replicas}') pods; $peak were ready at most"

say "waiting for the ledger to finish and verify"
k wait --for=condition=complete job/ledger --timeout=$((ledger_seconds + 180))s >/dev/null 2>&1 || true
k logs job/load | tail -1
k logs job/ledger | tail -2
k logs job/ledger | grep -q '^ledger verified' || { echo 'THE LEDGER DID NOT VERIFY' >&2; exit 1; }
echo
echo "passed: the ledger verified through scale-out to $peak pods, a killed pod, and scale-in"
