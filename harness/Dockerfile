# syntax=docker/dockerfile:1
# The pi harness worker image (HOR-381). Bakes pi + the warm-worker supervisor
# (dist/main.js) + the per-turn pi child entry (dist/child.js) + the SDK
# runtime. The supervisor is the mTLS gRPC CLIENT (no inbound RPC port). Config,
# the overlay pi/ tree, the PVC sandbox, mTLS certs, and the WAL emptyDir are
# MOUNTED at runtime by the AgentSandbox operator (HOR-245) — never baked in.
# setpriv (util-linux, present in bookworm-slim) is the privilege-dropping
# launcher; CAP_SETUID/CAP_SETGID are granted to the supervisor by the pod
# security context (HOR-245), not by the image. See harness/README.md.

FROM node:24-bookworm-slim AS builder
WORKDIR /src
COPY package.json package-lock.json ./
RUN npm ci
COPY tsconfig.json ./
COPY src ./src
RUN npm run build

FROM node:24-bookworm-slim
RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates \
  && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev
COPY --from=builder /src/dist ./dist
USER 65532:65532
# 8081 = plain-HTTP kubelet probes (/healthz, /readyz) — loopback only.
# No inbound RPC port: the worker is the gRPC client (it dials the control-plane).
EXPOSE 8081
ENTRYPOINT ["node", "dist/main.js"]
