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
issuance, and the admin bootstrap. HOR-243 adds the permission engine: the
`PermissionPolicy` CRD + reconciler and the `effective_capabilities` view
(broad-default). HOR-306/268 add the model catalog (`ModelBackend`/`Model` CRDs
+ the `effective_catalog` view). HOR-246 adds the durable turn runtime: the
`runtime` schema + store (workflow_run/step/turn state machines + append-only
event/audit log) — the data layer HOR-249 (orchestration) and HOR-252 (workflow
definitions) consume. Sandbox reconciliation (HOR-245) lands in its own ticket.

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
internal/runtime/   durable turn runtime store (run/step/turn SM + event log) — HOR-246
internal/server/    chi HTTP routes (health, jwks, token, admin CRUD)
internal/logging/   shared slog logger + logr bridge
internal/version/   build-time version metadata
internal/testutil/  shared Postgres test helper (testcontainers)
config/             kubebuilder Kustomize — DEV/envtest only (prod = forge Helm)
proto/              harness RPC contract (buf) — HOR-351
harness/            Node pi harness (the agent) — HOR-351; see harness/README.md
internal/harnessrpc/ generated Go Connect stubs (HOR-249 consumes) — HOR-351
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

## Permissions (HOR-243)

The operator materializes `PermissionPolicy` CRs into Postgres (`permissions`
schema), mirroring the IdentityMapping Git→DB bridge. The permission engine is
the **`permissions.effective_capabilities` view**: `identity_id → (resource,
action)` rows where presence = allow, absence = deny. Consumers (the
inference-gateway HOR-247, agent-fleet) read the view **directly** — no
request-path calls to control-plane — and own their own Redis cache +
freshness (LISTEN/NOTIFY on the `permissions_changed` channel, emitted by
triggers on `permissions.policies` and `identity.identities`).

**Broad-default (v1):** every linked (active) identity gets a single wildcard
`('*', '*')` capability; unknown/soft-deleted identities get no rows (denied).
`PermissionPolicy` CRs are materialized but **not enforced** in v1 — their
`subject` is stored; fine-grained `scopes` (narrowing) land in deepen-phase,
enriching the view's contents without changing the contract or consumer code.

Admin debug endpoint (reads the same view):

| Method | Path | Auth | Purpose |
|---|---|---|---|
| GET | `/v1/permissions/identities/{id}` | `scope=admin` | effective capabilities for an identity (404 if none) |

## CRD landscape

All CRDs use group `platform.iterabase.com` / `v1alpha1`, reconciled by this
operator: `AgentSandbox` (HOR-245), `ModelBackend` (HOR-306), `Model`
(HOR-268), `IdentityMapping` (HOR-242, **defined here**), `PermissionPolicy`
(HOR-243, **defined here**), `Tool` (HOR-271). The others land with their tickets.

## Git workflow

Branches/commits/PRs carry the Linear ticket ID (e.g. `HOR-241`). See
`AGENTS.md`. Only the user approves/merges to `master`.
