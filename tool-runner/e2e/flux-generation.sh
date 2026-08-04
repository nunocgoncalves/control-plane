#!/usr/bin/env bash
# HOR-397 real-cluster contract: exact Flux artifact -> registration -> new
# generation -> pinned old drain -> release. Destructive only to its kind cluster.
set -euo pipefail
for bin in kind kubectl helm flux docker node; do command -v "$bin" >/dev/null || { echo "missing $bin" >&2; exit 1; }; done
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
CHART_DIR=${ITERABASE_CHART_DIR:-"$ROOT/../iterabase-charts/charts/control-plane"}
CLUSTER=${HOR397_KIND_CLUSTER:-hor397-tool-runner}
CP_IMAGE=hor397-control-plane:dev
RUNNER_IMAGE=hor397-tool-runner:dev
GIT_IMAGE=hor397-git-server:dev
cleanup() { kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true; rm -rf "${TMP:-}"; }
trap cleanup EXIT
TMP=$(mktemp -d)

make -C "$ROOT" tool-runner-build
make_tool() {
  local version=$1 result=$2
  local dir="$TMP/revision-$version/tools/product/echo"
  mkdir -p "$dir"
  cat >"$dir/index.mjs" <<EOF
export const identity={name:"platform.echo",version:"$version"};
export async function invoke(_context,args){return {result:{generation:"$result",args}};}
EOF
  cat >"$dir/manifest.json" <<EOF
{"apiVersion":"iterabase.io/tool/v1","name":"platform.echo","version":"$version","digest":"sha256:$(printf '0%.0s' {1..64})","description":"Cluster contract echo","bundle":"index.mjs","inputSchema":{"type":"object"},"effectClass":"read_only","timeoutMs":5000}
EOF
  local digest
  digest=$(node "$ROOT/tool-runner/dist/main.js" digest "$dir")
  python3 - "$dir/manifest.json" "$digest" <<'PY'
import json,sys
p,d=sys.argv[1:]; value=json.load(open(p)); value['digest']=d
open(p,'w').write(json.dumps(value,separators=(',',':'))+'\n')
PY
  printf '%s' "$digest"
}
D1=$(make_tool 1.0.0 old)
D2=$(make_tool 2.0.0 new)

kind create cluster --name "$CLUSTER" --wait 90s
docker build -t "$CP_IMAGE" "$ROOT"
docker build -t "$RUNNER_IMAGE" -f "$ROOT/tool-runner/Dockerfile" "$ROOT/tool-runner"
cat >"$TMP/GitDockerfile" <<'EOF'
FROM alpine:3.20
RUN apk add --no-cache git openssh
EOF
docker build -t "$GIT_IMAGE" -f "$TMP/GitDockerfile" "$TMP"
kind load docker-image --name "$CLUSTER" "$CP_IMAGE" "$RUNNER_IMAGE" "$GIT_IMAGE"
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.21.0/cert-manager.yaml
kubectl wait -n cert-manager --for=condition=Available deployment/cert-manager deployment/cert-manager-webhook deployment/cert-manager-cainjector --timeout=180s
flux install

ssh-keygen -q -t ed25519 -N '' -f "$TMP/client-key"
ssh-keygen -q -t ed25519 -N '' -f "$TMP/host-key"
printf 'tool-git.default.svc ssh-ed25519 %s\n' "$(awk '{print $2}' "$TMP/host-key.pub")" >"$TMP/known_hosts"
kubectl create configmap tool-repo-seed --from-file="$TMP/revision-1.0.0/tools/product/echo/index.mjs" --from-file="$TMP/revision-1.0.0/tools/product/echo/manifest.json"
kubectl create configmap tool-git-authorized --from-file=authorized_keys="$TMP/client-key.pub"
kubectl create secret generic tool-git-host --from-file=ssh_host_ed25519_key="$TMP/host-key"
kubectl -n flux-system create secret generic overlay-git-auth --from-file=identity="$TMP/client-key" --from-file=known_hosts="$TMP/known_hosts"
cat <<'YAML' | kubectl apply -f -
apiVersion: apps/v1
kind: Deployment
metadata: {name: tool-git}
spec:
  replicas: 1
  selector: {matchLabels: {app: tool-git}}
  template:
    metadata: {labels: {app: tool-git}}
    spec:
      containers:
        - name: git
          image: hor397-git-server:dev
          imagePullPolicy: Never
          command: ["/bin/sh","-ceu"]
          args:
            - |
              mkdir -p /git/repo/tools/product/echo
              cp /seed/* /git/repo/tools/product/echo/
              cd /git/repo
              git init -b master
              git config user.name test
              git config user.email test@example.com
              git add . && git commit -m v1
              adduser -D -h /home/git -s /usr/bin/git-shell git
              passwd -d git
              chown -R git:git /git/repo
              mkdir -p /run/sshd
              exec /usr/sbin/sshd -D -e -o HostKey=/host/ssh_host_ed25519_key -o AuthorizedKeysFile=/auth/authorized_keys -o PasswordAuthentication=no -o PermitRootLogin=no -o StrictModes=no -o AllowUsers=git
          ports: [{containerPort: 22}]
          volumeMounts:
            - {name: seed, mountPath: /seed, readOnly: true}
            - {name: repo, mountPath: /git}
            - {name: host, mountPath: /host, readOnly: true}
            - {name: auth, mountPath: /auth, readOnly: true}
      volumes:
        - {name: seed, configMap: {name: tool-repo-seed}}
        - {name: repo, emptyDir: {}}
        - {name: host, secret: {secretName: tool-git-host, defaultMode: 256}}
        - {name: auth, configMap: {name: tool-git-authorized, defaultMode: 292}}
---
apiVersion: v1
kind: Service
metadata: {name: tool-git}
spec: {selector: {app: tool-git}, ports: [{port: 22, targetPort: 22}]}
YAML
kubectl rollout status deployment/tool-git --timeout=120s
cat <<'YAML' | kubectl apply -f -
apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: overlay, namespace: flux-system}
spec:
  interval: 2s
  url: "ssh://git@tool-git.default.svc/git/repo"
  ref: {branch: master}
  secretRef: {name: overlay-git-auth}
YAML
if ! kubectl wait -n flux-system --for=condition=Ready gitrepository/overlay --timeout=180s; then
  kubectl -n flux-system describe gitrepository/overlay >&2 || true
  kubectl logs deployment/tool-git >&2 || true
  exit 1
fi
cat <<'YAML' | kubectl apply -f -
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata: {name: allow-tool-materializer, namespace: flux-system}
spec:
  podSelector: {matchLabels: {app.kubernetes.io/component: source-controller}}
  policyTypes: [Ingress]
  ingress:
    - from:
        - namespaceSelector: {matchLabels: {kubernetes.io/metadata.name: iterabase-system}}
          podSelector: {matchLabels: {app.kubernetes.io/component: tool-runner}}
      # source-controller Service port 80 targets the artifact server's pod port 9090.
      ports: [{protocol: TCP, port: 9090}]
YAML

helm install hor397 "$CHART_DIR" -n iterabase-system --create-namespace \
  --set postgresql.enabled=true \
  --set image.repository=hor397-control-plane --set image.tag=dev --set image.pullPolicy=Never \
  --set gateway.enabled=true --set artifact.enabled=false \
  --set toolRunner.enabled=true --set toolRunner.image.repository=hor397-tool-runner \
  --set toolRunner.image.tag=dev --set toolRunner.image.pullPolicy=Never \
  --set 'toolRunner.allowedToolNamespaces={platform}' --timeout=8m
kubectl -n iterabase-system wait --for=condition=Ready certificate/hor397-tool-runner --timeout=180s
kubectl -n iterabase-system rollout status statefulset/hor397-postgresql --timeout=300s
kubectl -n iterabase-system rollout status deployment/hor397-control-plane-api --timeout=300s
kubectl -n iterabase-system rollout status deployment/hor397-control-plane-gateway --timeout=300s
if ! kubectl -n iterabase-system rollout status deployment/hor397-tool-runner --timeout=300s; then
  kubectl -n iterabase-system describe pod -l app.kubernetes.io/component=tool-runner >&2 || true
  kubectl -n iterabase-system logs deployment/hor397-tool-runner --all-containers --tail=200 >&2 || true
  exit 1
fi

PGPOD=$(kubectl -n iterabase-system get pod -l app.kubernetes.io/name=postgresql -o jsonpath='{.items[0].metadata.name}')
PGPASS=$(kubectl -n iterabase-system get secret hor397-postgresql -o jsonpath='{.data.password}' | base64 -d)
psqlq() { kubectl -n iterabase-system exec "$PGPOD" -- env PGPASSWORD="$PGPASS" psql -U controlplane -d controlplane -Atqc "$1"; }
wait_sql() {
  local query=$1 expected=$2
  for _ in $(seq 1 90); do [[ "$(psqlq "$query" 2>/dev/null || true)" == "$expected" ]] && return 0; sleep 2; done
  echo "timed out: $query != $expected" >&2; kubectl -n iterabase-system logs deployment/hor397-tool-runner --all-containers --tail=200 >&2 || true; exit 1
}
wait_sql "SELECT count(*) FROM toolgateway.runner_registrations WHERE tool_name='platform.echo' AND tool_digest='$D1' AND active AND accepting_new" 1

# Pin v1 before publishing v2. A missing work.attempt row is conservatively
# unfinished, exactly the restart/legacy-safe drain behavior.
psqlq "INSERT INTO toolgateway.attempt_tool_pins(attempt_id,tool_name,tool_version_digest) VALUES('cluster-pin','platform.echo','$D1')"
GITPOD=$(kubectl get pod -l app=tool-git -o jsonpath='{.items[0].metadata.name}')
kubectl cp "$TMP/revision-2.0.0/tools/product/echo/index.mjs" "$GITPOD:/git/repo/tools/product/echo/index.mjs"
kubectl cp "$TMP/revision-2.0.0/tools/product/echo/manifest.json" "$GITPOD:/git/repo/tools/product/echo/manifest.json"
kubectl exec "$GITPOD" -- sh -ceu 'git config --global --add safe.directory /git/repo; cd /git/repo; git add .; git commit -m v2; chown -R git:git /git/repo'
flux reconcile source git overlay -n flux-system
wait_sql "SELECT count(*) FROM toolgateway.runner_registrations WHERE tool_name='platform.echo' AND tool_digest='$D2' AND active AND accepting_new" 1
wait_sql "SELECT count(*) FROM toolgateway.runner_registrations WHERE tool_name='platform.echo' AND tool_digest='$D1' AND active AND NOT accepting_new" 1

psqlq "DELETE FROM toolgateway.attempt_tool_pins WHERE attempt_id='cluster-pin'"
wait_sql "SELECT count(*) FROM toolgateway.runner_registrations WHERE tool_name='platform.echo' AND tool_digest='$D1' AND active" 0

echo "PASS: exact Flux revisions registered immutable v1/v2; v1 stayed routable while pinned and retired after release"
