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
definitions) consume. HOR-244 adds the credential source + per-sandbox egress
proxy: the `EgressRoute` CRD + `egress` schema/`effective_routes` view +
`internal/egress.Resolve` + a separate proxy image (a credentialed reverse
proxy — the single egress point for sandbox model + tool traffic; agents never
hold creds). Sandbox reconciliation (HOR-245) lands in its own ticket.

## Binaries

Two Go images + one Node image:

| Binary | Path | Image | Role |
|---|---|---|---|
| `manager` | `cmd/manager` | `control-plane` | controller-runtime operator: reconcilers, webhooks, probes, metrics |
| `api` | `cmd/api` | `control-plane` | HTTP API (chi) + durable runtime (later); subcommands `serve`, `migrate up`, `migrate down`, `bootstrap` |
| `proxy` | `cmd/proxy` | `control-plane-proxy` | per-sandbox egress proxy (HOR-244): credentialed reverse proxy, single egress point for sandbox model + tool traffic |

The `control-plane` image (manager + api) runs in the platform namespace;
`control-plane-proxy` runs as a sidecar in AgentSandbox pods (less-trusted) and
is self-contained (no control-plane packages). The harness image is Node
(`harness/Dockerfile`). See `Dockerfile` (manager/api), `Dockerfile.proxy`
(proxy), `harness/Dockerfile` (harness).

`migrate` runs as an RBAC-less init container before `serve`/`manager` start.

## Layout

```
cmd/manager/        K8s operator (controller-runtime)
cmd/api/            HTTP API + migrate/bootstrap subcommands
cmd/proxy/          per-sandbox egress proxy (HOR-244) — the credentialed reverse proxy
api/v1alpha1/       CRD types (IdentityMapping, PermissionPolicy, ModelBackend, Model, EgressRoute); group platform.iterabase.com
internal/config/    YAML + env config (api) + DatabaseFromEnv (manager)
internal/database/  pgx pool + embedded golang-migrate migrations
internal/identity/  identity store, API keys, JWT/JWKS issuer, resolver
internal/permissions/ permission store + effective_capabilities view (HOR-243)
internal/catalog/   model catalog store (backends + models) + effective_catalog view (HOR-306/268)
internal/egress/    egress credential store + effective_routes view + Resolve + BuildProxyConfig (HOR-244)
internal/controller/ CRD reconcilers (Git -> DB bridge): identitymapping, permissionpolicy, modelbackend, model, egressroute
internal/runtime/   durable turn runtime store (run/step/turn SM + event log) — HOR-246
internal/proxy/     the per-sandbox egress proxy (config, routing, auth inject, OAuth, hot-reload) — HOR-244
internal/server/    chi HTTP routes (health, jwks, token, admin CRUD)
internal/logging/   shared slog logger + logr bridge
internal/version/   build-time version metadata
internal/testutil/  shared Postgres test helper (testcontainers)
config/             kubebuilder Kustomize — DEV/envtest only (prod = forge Helm)
proto/              harness RPC contract (buf) — HOR-351
harness/            Node pi harness (the agent) — HOR-351; see harness/README.md
internal/harnessrpc/ generated Go Connect stubs (HOR-249 consumes) — HOR-351
Dockerfile          manager + api image (one image, two entrypoints)
Dockerfile.proxy    the egress proxy image (HOR-244)
```

## Develop

```bash
make build              # build bin/manager + bin/api
make build-proxy        # build bin/proxy (egress proxy, HOR-244)
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
permissions → HOR-243, durable turns/events → HOR-246, egress credential
source → HOR-244 (`egress` schema: `credentials` + `effective_routes` view);
`usage`/`ai_data` content is post-v1. Requires a pgvector-enabled Postgres
image (tests use `pgvector/pgvector:pg16`).

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

## Egress credential source (HOR-244)

The per-sandbox egress proxy is the **data-scope enforcement boundary**
(credentials-as-scope): the agent never holds credentials; the proxy injects
the real credential per route. It is the single egress point for an
AgentSandbox pod's model + tool traffic.

- **`EgressRoute` CRD → `egress` schema → `effective_routes` view** (broad-default:
  every active identity gets every route; `subject` stored, not enforced in v1).
  The reconciler materializes CRs into Postgres (Git→DB bridge), mirroring
  identity/permissions/catalog.
- **`internal/egress.Resolve(scopeIdentityID)`** returns the effective tool routes
  + the K8s Secret refs to mount; `egress.BuildProxyConfig` assembles the
  proxy's `ProxyConfig` from a resolve result + the platform model route. The
  AgentSandbox operator (HOR-245) calls these at provisioning.
- **The proxy** (`cmd/proxy` / `internal/proxy`, separate `control-plane-proxy`
  image) is DB-less and identity-agnostic at runtime: reverse-proxy path-based
  routes (model `/v1` built-in → inference-gateway; tools `/upstreams/<id>/…`),
  TLS on the localhost leg, strips inbound auth + injects the route's real cred
  (static `bearer` from a mounted Secret, or `oauthClientCredentials` it acquires
  + refreshes), opaque bodies, hot-reload of the mounted ConfigMap.
- **Model route is internal-trusted** (shared `agent-egress` SA gateway key, Path
  1); the gateway's per-identity enforcement (delegated JWT / Path 2) is for
  **direct inference-product callers only** — agent model egress is owned by the
  control-plane (`scope_identity_id` + `runtime.events`), not the gateway.
- **Enforcement (v1):** credential-based (primary) + the tool set as action-scope
  (built-in coding tools off; only overlay tools → no arbitrary-HTTP tool → the
  network-egress gap is moot). Pod security + in-cluster NetworkPolicy (gateway
  + kube-dns) are hygiene (HOR-245). Prompt-injection detection + action-scoping
  are a separate follow-up (HOR-379); external-egress FQDN pinning (Cilium/
  egress-gateway) is deferred until coding agents / untrusted tools.

The credential **value** never lives in Postgres — only the K8s Secret
reference; the value stays in a Secret mounted into the proxy.

## CRD landscape

All CRDs use group `platform.iterabase.com` / `v1alpha1`, reconciled by this
operator: `AgentSandbox` (HOR-245), `ModelBackend` (HOR-306), `Model`
(HOR-268), `IdentityMapping` (HOR-242, **defined here**), `PermissionPolicy`
(HOR-243, **defined here**), `EgressRoute` (HOR-244, **defined here**), `Tool`
(HOR-271). The others land with their tickets.

## Git workflow

Branches/commits/PRs carry the Linear ticket ID (e.g. `HOR-241`). See
`AGENTS.md`. Only the user approves/merges to `master`.
