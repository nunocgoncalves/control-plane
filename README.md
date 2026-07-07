# control-plane

The Horizonshift/Iterabase control plane: a Go Kubernetes operator + HTTP API
that provides identity, per-caller permissions, credential-safe egress,
sandbox reconciliation, a durable turn runtime, and the model catalog the
inference-gateway consumes.

Per-customer, fully isolated, self-hosted. See the Platform Direction doc
(Obsidian: *Horizonshift Platform Direction*) for the full architecture; this
repo is the source of truth for control-plane infrastructure intent.

## Status

**HOR-241 — walking-skeleton foundation.** This is the skeleton: a runnable
two-binary project, the operator framework, the DB schema + migrations, config,
observability, build/CI, and tests — with **no business logic and no CRD
kinds**. Everything downstream lands in its own ticket.

## Binaries

One image, two entrypoints (see `Dockerfile`):

| Binary | Path | Role |
|---|---|---|
| `manager` | `cmd/manager` | controller-runtime operator: reconcilers, webhooks, probes, metrics |
| `api` | `cmd/api` | HTTP API (chi) + durable runtime (later); subcommands `serve`, `migrate up`, `migrate down` |

`migrate` runs as an RBAC-less init container before `serve`/`manager` start.

## Layout

```
cmd/manager/        K8s operator (controller-runtime)
cmd/api/            HTTP API + migrations subcommand
api/v1alpha1/       CRD types — created by `kubebuilder create api` (none yet; group platform.iterabase.com)
internal/config/    YAML + env config (api)
internal/database/  pgx pool + embedded golang-migrate migrations
internal/server/    chi HTTP routes (/healthz, /readyz)
internal/logging/   shared slog logger + logr bridge
internal/version/   build-time version metadata
config/             kubebuilder Kustomize — DEV/envtest only (prod = forge Helm)
```

## Develop

```bash
make build              # build bin/manager + bin/api
make run-manager        # run the operator locally
make run-api            # run the API (needs DATABASE_URL)
make migrate-up         # apply DB migrations
make test-unit          # fast unit tests (skips Docker/envtest)
make test               # unit + envtest (downloads kube-apiserver assets)
make lint               # golangci-lint
make fmt-check
make install-hooks      # use .githooks/pre-commit
```

The API reads `control-plane.example.yaml` (copy to `control-plane.yaml`) or
env vars (`DATABASE_URL`, `API_ADDR`, `LOG_LEVEL`, `LOG_FORMAT`).

## Database

Postgres is the system of record. The initial migration (`000001_init_schemas`)
creates four schema namespaces — `identity`, `permissions`, `usage`, `ai_data` —
plus the `pgvector` extension. No business tables: identity → HOR-242,
permissions → HOR-243, durable turns/events → HOR-246. `usage`/`ai_data` content
is post-v1. Requires a pgvector-enabled Postgres image (tests use
`pgvector/pgvector:pg16`).

## CRD landscape

All CRDs use group `platform.iterabase.com` / `v1alpha1`, reconciled by this
operator: `AgentSandbox` (HOR-245), `ModelBackend` (HOR-306), `Model`
(HOR-268), `IdentityMapping` (HOR-242), `PermissionPolicy` (HOR-243), `Tool`
(HOR-271). None are defined in this skeleton; they land with their tickets.

## Git workflow

Branches/commits/PRs carry the Linear ticket ID (e.g. `HOR-241`). See
`AGENTS.md`. Only the user approves/merges to `master`.
