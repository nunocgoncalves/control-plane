# harness

The Node pi harness that runs in the AgentSandbox — **the agent** (HOR-351).
The harness embeds pi via the SDK (`createAgentSession`) in-process and exposes
a Connect RPC server (mTLS) that the Go control-plane (HOR-249) drives to feed
messages and receive streamed agent events. Per-customer, fully isolated,
self-hosted. See the Platform Direction doc (Obsidian: *Horizonshift Platform
Direction*) §4/§7 for the architecture; this directory is the source of truth
for the harness implementation intent.

## Status

**Scaffold (HOR-351).** The `.proto` contract, buf config, project skeleton,
and config loader are in place. Wiring (Connect server, event mapping, model
provider registration, probes, SIGTERM) lands as the ticket progresses.
Prerequisite for HOR-245 (the pod needs the image) and HOR-249 (orchestration
drives the RPC). Part of the 3-day demo.

## Contract (`proto/iterabase/harness/v1/harness.proto`)

Connect + protobuf; the harness is the server (Node `@connectrpc/connect-node`),
the control-plane is the client (Go `connect-go`). **mTLS from day one** — no
shared token; certs provisioned by HOR-245.

| RPC | Type | Purpose |
|---|---|---|
| `Prompt` | server-streaming | send a message; stream `Event`s until `Settled`, then close |
| `Abort` | unary | cancel the active turn; the open `Prompt` stream emits `Settled{aborted}` |

`Event` is a curated oneof: `TurnStarted` / `AssistantMessage` / `ToolResult` /
`Error` / `Settled(reason)` (terminal). Plus plain-HTTP `GET /readyz` +
`/healthz` for the kubelet (not RPC, loopback only).

**Not in v1:** no session-management RPC, no `Health`/`GetState` RPC (the
control-plane owns the `session.id`). `Steer`/`FollowUp` + live token streaming
(`message_update` deltas) + `queue_update` are deferred — the "interactive
surfaces" extension required by Teams (HOR-248) and Slack (HOR-261/262);
non-breaking oneof/RPC additions.

## Session model

One pod = one pi session = one workflow-run/thread. The **control-plane owns
the workflow/chat → `session.id` mapping** (Postgres, HOR-249) and generates the
id. The harness **never auto-detects "most recent"** — it loads the specific
session directed by `config.session.id` (scoped to a per-id dir under
`config.session.dir`): resume (`SessionManager.open`) if it exists, create
(`SessionManager.create`) if new. On crash, the control-plane restarts the pod
with the same `session.id` and continues from its own recorded state.

## Config (`/etc/harness/config.yaml`, ConfigMap-mounted by HOR-245)

```yaml
persona: |              # v1 inline (systemPromptOverride); future = Persona CRD (HOR-363)
  You are an email-triage agent…
model: { id, endpoint, api: openai-completions, contextWindow }  # placeholder key; proxy injects real
toolAllowList: "*"      # broad-default v1; HOR-243 resolves per caller, narrows via HOR-283
session: { id, dir: /data/sessions }   # id is control-plane-generated; harness scopes by id
piDirs: [/pi/product, /pi/client]      # client overrides product (last-wins)
egressProxyUrl: https://localhost:<port>   # sidecar; routes model + tool traffic
tls: { cert, key, ca }                 # mTLS; provisioned by HOR-245
server: { port: 8443, healthPort: 8081 }
```

**The harness holds zero real credentials.** Caller identity + real
gateway/tool credentials live in the per-sandbox egress proxy (HOR-244); this
config carries only a placeholder model endpoint + the resolved tool allow-list.
Model traffic routes through the proxy (forwards to the internal inference-
gateway now, external providers later, injecting the real key).

## Internals (`src/`)

- `config.ts` — load + validate `config.yaml` (self-contained; the config schema made concrete).
- `session.ts` — `createAgentSession` wiring: persona via `systemPromptOverride`, `SessionManager` scoped by `session.id` (resume-or-create), built-in coding tools off (`tools: []`), tools/skills from `piDirs` (product then client), model via an inline `pi.registerProvider` at the proxy endpoint with a placeholder key.
- `enforcement.ts` — load-time tool allow-list filtering (in-process; broad-default `*` passes all; runtime `tool_call` interception deferred to HOR-283).
- `events.ts` — pi `AgentSessionEvent` → the 5-kind `Event` oneof.
- `server.ts` — the mTLS Connect server (`Prompt`/`Abort`) + plain-HTTP `/readyz`/`/healthz` + SIGTERM handler.

## Tools & skills

Loaded from the mounted overlay `pi/` tree (HOR-341): `/pi/product` then
`/pi/client` (client overrides product by same-name `registerTool`, last-wins —
the §5/§7 "supersede, not edit" precedence). Built-in coding tools
(`read`/`bash`/`edit`/`write`) are **off** for v1; the agent's capabilities are
the overlay tools (Graph email/Excel). Skills (SKILL.md, e.g. the client
classification Skill) are discovered the same way and invoked by the
control-plane's prompt (`/skill:…`).

## Image (`Dockerfile`)

`node:24-bookworm-slim`, multi-stage (`tsc` build → runtime `dist/`), non-root
(`65532`). **Bakes** pi + the harness server + the Connect runtime. **Mounts at
runtime** (never baked): `/etc/harness/config.yaml`, `/pi` (overlay tree),
`/data/sessions` (PVC), TLS certs. One generic, agnostic image serves any
bot/persona/tools. Second GHCR image: `ghcr.io/nunocgoncalves/control-plane-harness`.

## Layout

```
proto/iterabase/harness/v1/harness.proto   the contract (Prompt/Abort/Event)
proto/buf.{yaml,gen.yaml}        buf module + codegen (Go -> internal/harnessrpc, TS -> src/gen)
harness/package.json             pi + connect + buf/protobuf; vitest; typescript
harness/tsconfig.json            ESM, NodeNext, strict
harness/Dockerfile              the harness image
harness/src/config.ts           config schema + loader (self-contained)
harness/src/session.ts          pi createAgentSession wiring (skeleton)
harness/src/enforcement.ts      load-time allow-list filtering
harness/src/events.ts           pi event -> proto Event mapping (skeleton)
harness/src/server.ts           mTLS Connect server + probes + SIGTERM (skeleton)
harness/src/gen/                generated TS (committed; `make proto`)
internal/harnessrpc/            generated Go (committed; HOR-249 consumes; `make proto`)
```

## Develop

```bash
make proto-tools        # install buf + protoc plugins (brew install buf works too)
make proto              # generate Go + TS stubs (buf lint + generate)
cd harness && npm install   # install Node deps (first time; commit package-lock.json)
make harness-build      # tsc -> dist
make harness-test       # vitest
make harness-lint       # tsc --noEmit
make harness-image      # build the harness container image
```

The generated `src/gen/` and `internal/harnessrpc/` are **committed**; `make
proto-check` (CI) guards that they're fresh. Go consumers (HOR-249) import
`internal/harnessrpc` directly — they do not need Node or buf installed.

## Testing

- **Unit (vitest):** config loading/validation, event mapping, allow-list
  filtering, session resume-or-create logic, model provider registration.
- **Contract/integration (vitest):** the real harness server (mTLS) + a Connect
  client + a **mock model** (OpenAI-compatible stub) — `Prompt`→`Settled{completed}`,
  `Abort`→`Settled{aborted}`, `/readyz`. Hermetic; no real LLM, no egress proxy, no pod.
- **Cross-component e2e:** `forge/test/e2e/` `TestAgentFlowContract` (HOR-365,
  after HOR-249) — Kind + umbrella chart, drives a real turn end-to-end.
- **Demo rehearsal:** HOR-353 (real Graph + real LLM email-workflow).

## Sequencing

HOR-351 (Wave 1) is the prerequisite for HOR-245 (needs the image) and HOR-249
(drives the RPC). Documented follow-ups: HOR-363 (Persona CRD), HOR-283
(fine-grained per-action permissions), HOR-365 (forge e2e), and the
`Steer`/`FollowUp` + live-streaming "interactive surfaces" extension (Teams
HOR-248 / Slack HOR-261/262).

## Git workflow

Branches/commits/PRs carry the Linear ticket ID (e.g. `HOR-351`). See
`AGENTS.md`. Only the user approves/merges to `master`.
