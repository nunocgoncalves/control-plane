# control-plane

The Horizonshift/Iterabase control plane: a Go Kubernetes operator + HTTP API
that provides identity, per-caller permissions, credential-safe egress,
sandbox reconciliation, a durable turn runtime, and the model catalog the
inference-gateway consumes.

Per-customer, fully isolated, self-hosted. See the Platform Direction doc
(Obsidian: *Horizonshift Platform Direction*) for the full architecture; this
repo is the source of truth for control-plane infrastructure intent.

## Status

**Walking skeleton.** HOR-241 landed the two-binary foundation (operator +
API, DB schema, config, CI). HOR-242 adds the identity store: the
`IdentityMapping` CRD + reconciler, local users, API keys, delegated JWT/JWKS
issuance, and the admin bootstrap. Per-caller permissions (HOR-243), sandbox
reconciliation (HOR-245), the model catalog (HOR-268/306), and the durable turn
runtime (HOR-246) land in their own tickets.

## Binaries

One image, two entrypoints (see `Dockerfile`):

| Binary | Path | Role |
|---|---|---|
| `manager` | `cmd/manager` | controller-runtime operator: reconcilers, webhooks, probes, metrics |
| `api` | `cmd/api` | HTTP API (chi) + durable runtime (later); subcommands `serve`, `migrate up`, `migrate down`, `bootstrap` |

`migrate` runs as an RBAC-less init container before `serve`/`manager` start.

## Layout

```
cmd/manager/        K8s operator (controller-runtime)
cmd/api/            HTTP API + migrate/bootstrap subcommands
api/v1alpha1/       CRD types (IdentityMapping); group platform.iterabase.com
internal/config/    YAML + env config (api) + DatabaseFromEnv (manager)
internal/database/  pgx pool + embedded golang-migrate migrations
internal/identity/  identity store, API keys, JWT/JWKS issuer, resolver
internal/controller/ IdentityMapping reconciler (Git -> DB bridge)
internal/server/    chi HTTP routes (health, jwks, token, admin CRUD)
internal/logging/   shared slog logger + logr bridge
internal/version/   build-time version metadata
internal/testutil/  shared Postgres test helper (testcontainers)
config/             kubebuilder Kustomize — DEV/envtest only (prod = forge Helm)
```

## Develop

```bash
make build              # build bin/manager + bin/api
make run-manager        # run the operator locally
make run-api            # run the API (needs DATABASE_URL)
make migrate-up         # apply DB migrations
make setup-envtest      # download kube-apiserver assets (for make test)
make test-unit          # fast unit tests (skips Docker/envtest)
make test               # unit + envtest + integration (needs Docker)
make lint               # golangci-lint
make fmt-check
make install-hooks      # use .githooks/pre-commit
```

The API reads `control-plane.example.yaml` (copy to `control-plane.yaml`) or
env vars (`DATABASE_URL`, `API_ADDR`, `LOG_LEVEL`, `LOG_FORMAT`,
`JWT_SIGNING_KEY_PATH`, `JWT_KEY_ID`, `IDENTITY_MODE`). The manager reads only
`DATABASE_URL` from the environment.

## Database

Postgres is the system of record. The initial migration (`000001_init_schemas`)
creates four schema namespaces — `identity`, `permissions`, `usage`, `ai_data` —
plus the `pgvector` extension. `000002_identity` (HOR-242) adds the identity
store: `identities`, `external_mappings`, `local_users`, `api_keys`.
permissions → HOR-243, durable turns/events → HOR-246; `usage`/`ai_data` content
is post-v1. Requires a pgvector-enabled Postgres image (tests use
`pgvector/pgvector:pg16`).

## Identity (HOR-242)

The operator materializes `IdentityMapping` CRs into Postgres (Git → DB bridge);
the API never touches Kubernetes. Two auth paths:

- **Path 1 (user → gateway, API key):** a long-lived `cp-` key (scope `gateway`)
  bound to a local user / service account; the gateway validates it from a
  control-plane-synced snapshot.
- **Path 2 (user → agent → gateway, delegated JWT):** a service account
  (scope `token`) calls `POST /v1/token` with a surface user; control-plane
  resolves it (enrolled mode: linked `IdentityMapping` required) and issues a
  short-lived RS256 JWT (`sub` = identity id) for the gateway to enforce.

API endpoints:

| Method | Path | Auth | Purpose |
|---|---|---|---|
| GET | `/.well-known/jwks.json` | – | JWT verification key set |
| POST | `/v1/token` | `scope=token` | resolve surface user → delegated JWT |
| POST/GET | `/v1/users`, `GET /v1/users/{id}` | `scope=admin` | local-user CRUD |
| POST/GET | `/v1/api-keys`, `DELETE /v1/api-keys/{id}` | `scope=admin` | API-key management |

**Bootstrap** (`control-plane-api bootstrap`, run as an init container after
`migrate up`) creates the admin local user + admin key and, with
`--service-account <name>`, seeds service accounts; keys are printed once.
`--reset` revokes + reissues. The JWT signing key is an RSA private key mounted
from a Kubernetes Secret (forge-provisioned). Open mode + SSO are fast-follows
(HOR-313/314).

## CRD landscape

All CRDs use group `platform.iterabase.com` / `v1alpha1`, reconciled by this
operator: `AgentSandbox` (HOR-245), `ModelBackend` (HOR-306), `Model`
(HOR-268), `IdentityMapping` (HOR-242, **defined here**), `PermissionPolicy`
(HOR-243), `Tool` (HOR-271). The others land with their tickets.

## Git workflow

Branches/commits/PRs carry the Linear ticket ID (e.g. `HOR-241`). See
`AGENTS.md`. Only the user approves/merges to `master`.
