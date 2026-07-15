# syntax=docker/dockerfile:1
# The pi harness image (HOR-351). Generic + agnostic: bakes pi + the harness
# Connect server + the SDK runtime. Persona, tools/skills (the overlay pi/
# tree), config, the PVC session, and mTLS certs are MOUNTED at runtime by the
# AgentSandbox operator (HOR-245) — never baked in. See harness/README.md.

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
# 8443 = mTLS Connect RPC (control-plane -> harness).
# 8081 = plain-HTTP kubelet probes (/readyz, /healthz) — loopback only.
EXPOSE 8443 8081
ENTRYPOINT ["node", "dist/server.js"]
