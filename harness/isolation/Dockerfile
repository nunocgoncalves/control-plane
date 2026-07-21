# syntax=docker/dockerfile:1
# The HOR-381 sandbox isolation test image. Builds the harness (for the real
# launcher at dist/launcher.js), then runs the probe + runner as ROOT — the
# runner needs CAP_SETUID/CAP_SETGID to setpriv-spawn children as per-session
# UIDs and to chown the sandboxes. Production grants those caps via the K8s
# security context (HOR-245), not by running the supervisor as root.
#
# This is the HOR-381 isolation gate (bullets 1-5). Bullets 6-7 (RWX-CSI
# cross-pod ownership + Pod Security admission) are HOR-245's gate.
# Run: make harness-isolation-test

FROM node:24-bookworm-slim AS builder
WORKDIR /src
COPY package.json package-lock.json tsconfig.json ./
RUN npm ci
COPY src ./src
RUN npm run build

FROM node:24-bookworm-slim
WORKDIR /app
COPY --from=builder /src/dist ./dist
COPY isolation/probe.mjs isolation/run.mjs ./
# Runs as root (default): the runner chowns sandboxes + setpriv-spawns as
# per-session UIDs. No node_modules needed — launcher + probe use only Node
# built-ins.
ENTRYPOINT ["node", "/app/run.mjs"]
