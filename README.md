# control-plane

The Horizonshift/Iterabase control plane: a Go Kubernetes operator + HTTP API
that provides identity, per-caller permissions, sandbox reconciliation,
a durable turn runtime, and the model catalog the inference-gateway consumes.

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
definitions) consume. HOR-245 adds the AgentPool CRD/operator: isolated
warm-worker pods (SPIFFE certs via the cert-manager CSI driver, shared RWX
sandbox PVC, deny-by-default NetworkPolicy, deny-by-default workspace-tool
switch, maximum gateway grants + credential-slot bindings) and removes the
superseded per-sandbox egress proxy (ARCH-009). Sandbox reconciliation (HOR-245)
lands in its own ticket. HOR-249 adds the durable-dispatch Work server
(`cmd/dispatch`): the warm-worker mTLS bidi stream, worker fencing, one-credit
dispatch, durable TurnEvent ACK/dedup, cancellation/worker-loss, the
active-assignment context (`runtime.turn_assignments`) that binds a running turn
to the verified worker + fencing generation, and the dispatch reconciler.
HOR-254 evolves that foundation into a single-active-node graph runtime and adds
the durable `work` domain: stable work items, first-class attempts, append-only
node visits/transitions, blockers, feedback, artifact references, business
SSE events, and an estimated-value ledger.

## Binaries

Two Go images + one Node image:

| Binary | Path | Image | Role |
|---|---|---|---|
| `manager` | `cmd/manager` | `control-plane` | controller-runtime operator: reconcilers, webhooks, probes, metrics |
| `api` | `cmd/api` | `control-plane` | HTTP API (chi) + durable runtime (later); subcommands `serve`, `migrate up`, `migrate down`, `bootstrap` |
| `gateway` | `cmd/gateway` | `control-plane` | tool gateway gRPC server (HOR-392): mTLS `RunnerService` + `GatewayService` |
| `dispatch` | `cmd/dispatch` | `control-plane` | durable-dispatch Work gRPC server (HOR-249): mTLS `Harness.Work` bidi stream, worker fencing, one-credit dispatch, TurnEvent ACK/dedup, cancellation/worker-loss |

The `control-plane` image (manager + api + gateway + dispatch) runs in the platform
namespace. The harness image is Node (`harness/Dockerfile`). See `Dockerfile`
(manager/api/gateway/dispatch) and `harness/Dockerfile` (harness).

`migrate` runs as an RBAC-less init container before `serve`/`manager` start.

## Layout

```
cmd/manager/        K8s operator (controller-runtime)
cmd/api/            HTTP API + migrate/bootstrap subcommands
api/v1alpha1/       CRD types (IdentityMapping, PermissionPolicy, ModelBackend, Model); group platform.iterabase.com
internal/config/    YAML + env config (api) + DatabaseFromEnv (manager)
internal/database/  pgx pool + embedded golang-migrate migrations
internal/identity/  identity store, API keys, JWT/JWKS issuer, resolver
internal/permissions/ permission store + effective_capabilities view (HOR-243)
internal/catalog/   model catalog store (backends + models) + effective_catalog view (HOR-306/268)
internal/gateway/   tool gateway: registry, authorization, credential resolution, durable invocation ledger (HOR-392)
internal/dispatch/  durable dispatch Work server: warm-worker bidi stream, one-credit dispatch, worker fencing, active-assignment context, TurnEvent ACK/dedup, cancellation/worker-loss (HOR-249)
internal/spiffe/    shared SPIFFE/mTLS identity verifier (HOR-392; reused by HOR-249)
internal/controller/ CRD reconcilers (Git -> DB bridge): identitymapping, permissionpolicy, modelbackend, model
internal/runtime/   durable turn + graph runtime state and technical event log — HOR-246/254
internal/work/      work items, graph orchestration, blockers, feedback, value, business events — HOR-254
internal/server/    chi HTTP routes (health, identity/admin, work REST + SSE)
internal/logging/   shared slog logger + logr bridge
internal/version/   build-time version metadata
internal/testutil/  shared Postgres test helper (testcontainers)
config/             kubebuilder Kustomize — DEV/envtest only (prod = forge Helm)
proto/              harness RPC contract (buf) — HOR-351
harness/            Node pi harness (the agent) — HOR-351; see harness/README.md
internal/harnessrpc/ generated Go Connect stubs (HOR-249 consumes) — HOR-351
Dockerfile          manager + api + gateway image (one image, three entrypoints)
```

## Develop

```bash
make build              # build bin/manager + bin/api + bin/gateway
make run-manager        # run the operator locally
make run-gateway        # run the tool gateway (serve); needs DATABASE_URL + mTLS certs
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
permissions → HOR-243, durable turns/events → HOR-246; `usage`/`ai_data`
content is post-v1. The `egress` schema (HOR-244) is dropped by 000012 under
ARCH-009 (HOR-245). Requires a pgvector-enabled Postgres
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

## Tool gateway (HOR-392)

The tool gateway is the authorized boundary for customer-system and externally
side-effecting tool execution (ARCH-001..ARCH-018). It runs as `cmd/gateway`, a
third entrypoint in the `control-plane` image, serving the `iterabase.gateway.v1`
gRPC contract over native HTTP/2 + mTLS (a SPIFFE URI SAN binds caller identity):

- **`RunnerService.RegisterRunner`** — a long-lived bidi stream. Trusted tool
  runners connect outbound, self-register immutable `ToolDescriptor`s (name /
  version / digest / JSON input schema / effect class / credential slots /
  artifact capabilities / timeout / write-consequence summary template),
  heart-beat availability, and receive
  invocations to execute. Runners expose no inbound endpoint (ARCH-015).
- **`GatewayService`** — the caller-facing surface. `DiscoverEffectiveTools`
  returns only the descriptors permitted for the caller's active attempt/turn or
  workflow-step context (registered healthy versions ∩ AgentPool grants ∩
  workflow-requested tools ∩ pool-level identity scope). `InvokeTool` is the
  ledger-gated path: the gateway commits an invocation row **before** the
  side-effect boundary, then dispatches over the runner stream.

The durable at-most-once ledger (`toolgateway.invocations`) is uniquely keyed by
attempt + caller scope + tool-call id + exact version digest + idempotency key.
Duplicates of a completed invocation return the committed result; duplicates in
progress report the existing invocation; a possible effect with no committed
result becomes `outcome_unknown` and is never automatically repeated. Retry is
classified by effect class: `read_only` may bounded-retry; `idempotent_write`
only when the descriptor proves a stable strategy; `non_idempotent_write` never
auto-retries after execution begins (ARCH-014).

Every write descriptor must also provide immutable English and Portuguese
consequence templates. Placeholders bind only to explicitly allow-listed RFC
6901 argument paths. After schema/resource validation, the gateway renders and
persists the localized customer-safe summary with the invocation **before**
dispatch; missing, structured, or unrenderable fields fail closed. Cycle and
revised-attempt confirmation return that exact persisted summary and never raw
arguments/results, including when the write becomes `outcome_unknown`
(ARCH-022 / REQ-026).

The gateway resolves logical credential slots to K8s Secret values via the
in-cluster API (Secret-read RBAC scoped to `gateway.kube_namespace`) and hands a
short-lived `CredentialContext` to the trusted runner over mTLS. Raw values
never enter Postgres, logs, tool results, or the sandbox (ARCH-008). Large
inputs/outputs are `ArtifactRef`s (the MinIO-backed artifact service is
HOR-399).

Several `toolgateway` tables (`pools`, `pool_grants`, `credential_bindings`,
`workflow_pool_bindings`, `approved_runners`) are operator-seeded now and
populated by later tickets via the Git→DB bridge: AgentPool CRD (HOR-245),
Workflow definitions (HOR-252), the Node runner materializer (HOR-397). The
real Node 24 runner is HOR-397; the supervisor IPC client is HOR-395; concrete
Graph tools are HOR-358. AgentPool CRD/operator + EgressRoute/proxy removal
(ARCH-009) is HOR-245.

## Model backends (HOR-306/388)

A `ModelBackend` (`kind: vLLM`) deploys an internal GPU workload (Deployment +
Service) and materializes into `catalog.backends`; a `Model` references it and
carries the client-facing alias/config. For **plug-n-play** models the
controller assembles the serving command (`--model <id> --port --host
<extraArgs>`) and manages the HF cache (`hf-cache` hostPath + `HF_HOME`), a
sized `/dev/shm` (`devShmSize`, default 2Gi), the `nvidia` runtimeClass, the GPU
request, the GPU node selector, and a 10-min startup probe.

**Custom vLLM builds** (HOR-388) — e.g. a quantized/SM120-specific build with a
non-stock entrypoint, build-tuning env vars, or runtime patch overlays — use the
generic corev1-passthrough escape hatches. None of these affect the plug-n-play
path when unset:

| Field | Effect |
|---|---|
| `spec.command` / `spec.args` | Override the container ENTRYPOINT/CMD. When `args` is set, the controller **skips** its `--model/--port/--host` assembly and `extraArgs`; the deployer owns the whole command shape (e.g. positional `vllm … serve <model>`). `healthProbe.port` stays the sole port source for the Service + probes — the deployer must bind `--port <healthProbe.port>` in `args`. |
| `spec.env` | Extra env vars (`corev1.EnvVar`, supports `valueFrom` for `HF_TOKEN` from a Secret). The controller injects `HF_HOME=/data/hf-cache` only if `env` doesn't already set it (user wins). |
| `spec.volumes` / `spec.volumeMounts` | Extra volumes/mounts, appended after the managed `hf-cache` + `dshm`. Reserved names `hf-cache`/`dshm` are rejected. Use for runtime file-artifact overlays (a ConfigMap of patch `.py` files subPath-mounted over venv paths) or a PV. |
| `spec.hostIPC` | Opt-in `hostIPC` (default false). Sized `/dev/shm` (`devShmSize`) is the normal TP shm mechanism; flip on only if a build's NCCL/CUDA IPC proves to need it. |
| `spec.securityContext` | Container-level `corev1.SecurityContext` passthrough. Set `capabilities.add: [IPC_LOCK]` for NCCL TP — `CAP_IPC_LOCK` bypasses `RLIMIT_MEMLOCK` (the K8s equivalent of `--ulimit memlock=-1`; a bash `ulimit` wrapper is a no-op without `CAP_SYS_RESOURCE`). Also covers `runAsUser`, `readOnlyRootFilesystem`, etc. |
| `spec.healthProbe.startupTimeoutSeconds` | Scales the startupProbe window (default 600s; controller renders `period=10, failureThreshold=ceil(n/10)`). Raise for large/long-context models whose warmup is slow. |

The platform does **not** carry forked serving images: a custom build consumes
its upstream image verbatim and overlays any patches as file artifacts
(ConfigMaps). Weights are downloaded from HuggingFace into the `hf-cache` hostPath
(no pre-mount needed); set `HF_TOKEN` in `env` for gated models. See
`config/samples/platform_v1alpha1_modelbackend_dsv4flash_b12x.yaml` for a full
B12X/SM120 example (DeepSeek-V4-Flash NVFP4, TP=2, MTP, 256k context, patch
overlays). Multi-replica TP/PP and SGLang are deferred (HOR-323 / deepen).

## Agent pools (HOR-245)

An `AgentPool` is the deployable workflow security/integration boundary
(ARCH-018): it provisions a bounded set of isolated warm-worker pods and
declares the maximum gateway capability grants + logical credential-slot
bindings for work dispatched to the pool. No separate Tool/EgressRoute/
IntegrationBinding CRD exists in v1.

The operator reconciles, per pool: a shared **RWX sandbox PVC** (per-session
`0700` subdirs owned by the session UID/GID), a **deny-by-default
NetworkPolicy** (`denied` = kube-dns + the three gateways; `internet` =
per-pool opt-in for non-cluster egress — customer-system credentialed access
still routes through the gateway), a **per-pod config ConfigMap** (rendered
harness boot config), and the **warm-worker pods**.

**Per-pod SPIFFE certs** are issued by the **cert-manager CSI driver**
(`csi.cert-manager.io`) annotated on each pod with URI SAN
`spiffe://<td>/pools/<pool-uid>/workers/<pod-name>`; the operator never holds
the CA key (cert-manager owns the CA in `platform-ca`, backed by a
`ClusterIssuer`). Forge prerequisites: cert-manager + cert-manager-csi-driver +
the CA `Certificate`/`ClusterIssuer`. The supervisor runs as root (UID 0, PSS
`baseline`) to read the CSI driver's root-owned `0600` key and launch the
per-turn child as the session UID via `setpriv` (CAP_SETUID/SETGID, which PSS
`restricted` forbids — hence `baseline`); the child (dropped groups,
`no_new_privs`) cannot read the key.

`spec.workspaceTools` is the deny-by-default local-tool switch (ARCH-016):
`false` exposes none; `true` exposes exactly `read`/`write`/`edit`/`bash`.
`spec.gatewayGrants`/`credentialBindings`/`resourceConstraints` declare the
maximum gateway permissions and slot→Secret bindings (values never in the CRD);
semantic tool-registry validation is the gateway's job (HOR-392/397). Dispatch,
warm-pool scaling, and the Work server's worker-identity/generation fencing are
HOR-249; `worker_id` = pod name (stable slot), recorded as an amendment to the
HOR-381/249 identity contract. Real-cluster PSS/CSI/RWX/isolation validation is
the ticket's stated real-cluster gate (envtest covers assembly + structural
validation).

## Dispatch (HOR-249)

The dispatch Work server (`cmd/dispatch`) is the native-gRPC mTLS endpoint warm
workers connect to (`iterabase.harness.v1.Harness.Work` bidi stream). It owns
worker fencing, one-credit dispatch, durable `TurnEvent` ACK/dedup, cancellation
and worker-loss semantics, and the dispatch reconciler that drives pending
runs/steps/turns to eligible idle workers.

A worker connects (Hello), the server verifies the cert SAN (pool UID + pod name)
against Hello and sends a `Welcome` with a fresh fencing generation. A reconnect
for the same (pool, worker) fences the prior generation and terminalizes its
active turn as worker loss (aborted). `Ready` advertises one dispatch credit; the
reconciler assigns a pending turn (whose run is durably assigned to the worker's
pool via `runtime.run_pool_assignments`) to an idle worker and sends
`AssignTurn`. `TurnEvent`s are committed to the append-only `runtime.events` log
with per-turn sequence dedup and cumulatively ACKed; a terminal `WorkerOutcome`
terminalizes the turn/step/run (the CP is first-terminal-writer; a late outcome
is after-terminal audit only). Cancellation sends `AbortTurn` and terminalizes
the turn aborted; an ambiguous non-idempotent turn is never auto-redelivered.

The active-assignment context (`runtime.turn_assignments`) is the durable record
binding a running turn to the specific verified worker (cert SAN pod name) +
current fencing generation under one AgentPool, with the immutable
attempt/model/capability/tool-version snapshot. The tool gateway (this repo,
`ResolveTurnScope`) and the inference gateway (cross-repo, HOR-398) read it
fail-closed to deny a still-valid supervisor cert from a different same-pool
worker or a fenced/old-generation caller for a running turn — closing the
HOR-398 / DEC-041 interim residual.

Trigger adapters remain separate slices: email (HOR-356) and operator-artifact
(HOR-393) call the idempotent work-start API. HOR-254's graph service prepares
the active node; `cmd/dispatch` assigns only `agent_task` nodes to workers while
`human_gate` and consequence-confirmation nodes remain durable control-plane
state. Worker fencing/ACK semantics remain the HOR-249 boundary.

## CRD landscape

All CRDs use group `platform.iterabase.com` / `v1alpha1`, reconciled by this
operator: `AgentPool` (HOR-245), `ModelBackend` (HOR-306), `Model`
(HOR-268), `IdentityMapping` (HOR-242, **defined here**), `PermissionPolicy`
(HOR-243, **defined here**), `Workflow` (HOR-252, **defined here**), `Tool`
(HOR-271). The others land with their tickets.

## Workflow definitions (HOR-252)

A `Workflow` is an operator-defined, versioned customer operational workflow
(REQ-001). The operator deploys one from product/client overlay artifacts:
its source adapter/trigger, exact immutable skill references, default/per-node
model selection, requested gateway capabilities, customer-facing presentation,
value-model reference, and a single-active-node directed graph.

- Definitions are **immutable per version** (ARCH-007): `(key, version)` is the
  unique version identity. A content change must be published under a new
  `spec.version`; the reconciler rejects a content change under an
  already-registered version. Re-registering the same `(key, version, digest)`
  is idempotent; distinct versions may share a content digest. Every version of
  one logical `key` retains the same durable workflow scope identity, so latest-
  version resolution cannot cross workflow/customer ownership boundaries.
- **Validation runs before execution**: unknown source/model/node kind,
  capability/skill narrowing, malformed JSON Schema, missing/duplicate outcome
  route, unreachable node, non-terminating component, or binding fails with an
  inspectable `validationStatus: invalid`. v1 source types are `graph_email` (Walter) and
  `operator_artifact` (XBS) — no customer-specific schema — and a recognized
  type becomes Ready only when its adapter is installed in the deployment.
- `spec.graph` declares one entry node, `agent_task`/`human_gate` nodes,
  outcome-routed edges, explicit terminal outcomes, and a bounded transition
  count. Cycles are supported; parallel branches/joins are not. Every visit is
  append-only. Gateway tools are agent-callable capabilities, never graph nodes.
- Every agent node finishes through the reserved `complete_step` control
  function. Its declared outcome and JSON output are validated before the
  engine atomically follows an edge. A clean worker outcome is also required.
- A workflow **cannot request capabilities beyond its `poolRef` AgentPool**:
  each requested tool must be granted and its effect/action narrowing must not
  exceed the pool grant; every node exposes an explicit subset, enforced from
  the durable turn assignment through gateway discovery/invocation.
- **Trigger bindings are source-specific typed, non-secret routes** (`graphEmail.mailboxAddress`
  or `operatorArtifact.sourceID`). There is no opaque trigger config/secret
  persistence path; credentials resolve through AgentPool `credentialBindings`
  via the gateway (ARCH-008).
- The reconciler materializes: an immutable `workflow.definitions` row,
  non-secret `workflow.trigger_bindings`, a `kind=workflow` scope identity
  (HOR-242), and a `toolgateway.workflow_pool_binding` (permitted tools). The
  `definition_key` wire format `<key>:<version>` is the stable cross-schema
  reference stored in `runtime.workflow_runs.definition_key` (HOR-246) and
  `toolgateway.workflow_pool_bindings.workflow_definition_key` (HOR-392).
- `ResolveForAttempt(key, version)` returns the exact versioned canonical
  workflow/config, immutable skill identities, typed trigger bindings, scope
  identity, and complete capability narrowing for attempt creation (HOR-254
  atomically composes it with deterministic latest-healthy tool pinning).
  Skill digests are `sha256:<hex>` over the complete materialized skill tree:
  for each sorted slash-separated relative file, hash
  `file\\0<path>\\0<byte-length>\\0<exact-bytes>`; symlinks and non-regular
  entries are rejected. The harness loads a named
  product/client skill only when those bytes match the attempt's pinned digest.
- Customer-facing metadata (workflow title, persona name/avatar, locale)
  supports the single-workflow Dashboard **without a separate Persona CRD**
  (REQ-021; HOR-363 canceled).

See `config/samples/platform_v1alpha1_workflow.yaml` for Walter `graph_email`
and XBS `operator_artifact` examples.

## Durable customer work (HOR-254)

One customer work item owns one or more attempts; each attempt shares its UUID
with exactly one `runtime.workflow_run`. Current To do / In progress / Blocked /
Done / Failed presentation is projected from the current attempt plus its open
actionable blocker, avoiding a duplicate mutable state machine. Failed remains
outside the four normal columns.

Work APIs require a `work`-scope bearer key and operation capability
(`work:read`, `work:start`, `work:respond`, `work:feedback`, or `work:retry`):

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/v1/work-items` | Idempotent workflow start (`Idempotency-Key` required) |
| `GET` | `/v1/work-items` / `/v1/work-items/{id}` | Search/list and customer-safe detail |
| `GET` | `/v1/work-items/{id}/attempts` | Preserved attempt history |
| `GET` | `/v1/work-attempts/{id}/nodes` | Append-only graph-node visits and structured outputs |
| `GET` | `/v1/work-items/{id}/timeline` | Customer-safe business timeline |
| `GET` | `/v1/work-items/{id}/consequences` | Customer-safe external-action tokens required by a revision |
| `GET` | `/v1/work-items/{id}/blocker` | Current actionable request |
| `POST` | `/v1/work-blockers/{id}/responses` | Resolve human/consequence gate |
| `GET` | `/v1/work-items/{id}/feedback` / `/v1/work-items/{id}/feedback/{feedbackID}` | Original-attempt feedback and revised-attempt association |
| `POST` | `/v1/work-items/{id}/feedback` | Save feedback without starting work |
| `POST` | `/v1/work-items/{id}/revisions` | Explicit revised attempt with exact consequence confirmation |
| `GET` | `/v1/work-dashboard` | State counts plus explainable estimated value/trend |
| `GET` | `/v1/work-events` | Resumable SSE (`Last-Event-ID`) of business-safe events |
| `POST` | `/v1/value-models` | Admin-only immutable value-model version creation |

`runtime.events` and gateway ledgers remain technical/operator records and are
never returned by customer work APIs. `work.timeline_events` is append-only and
contains only semantic business codes/parameters. HOR-399 owns artifact bytes
and metadata; HOR-254 stores immutable references and trace relationships.

## Git workflow

Branches/commits/PRs carry the Linear ticket ID (e.g. `HOR-241`). See
`AGENTS.md`. Only the user approves/merges to `master`.
