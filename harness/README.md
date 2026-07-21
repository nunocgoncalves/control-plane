# harness

The Node pi harness worker that runs in the AgentSandbox — **the agent**
(HOR-381). A **warm trusted supervisor** process maintains one long-lived mTLS
gRPC `Work` bidi-stream to the control-plane (HOR-249) and, per turn, spawns a
**disposable pi child** under a per-session UID/GID (via a `setpriv` launcher)
that runs one turn, streams durable lifecycle events, and exits. Per-customer,
fully isolated, self-hosted. See the Platform Direction doc (Obsidian:
*Horizonshift Platform Direction*) §4/§7/§9 + the HOR-381 plan note for the
architecture; this directory is the source of truth for the harness
implementation intent.

Supersedes the HOR-351 "boot-bound-to-one-session Connect server" (Prompt/Abort)
— the worker is now the mTLS gRPC **client** (no inbound RPC); per-turn
stateless (the process is warm; only the per-turn child + its in-memory session
are per-turn).

## Architecture

```
worker pod
├── supervisor (long-lived, trusted; dist/main.js)         UID 65532 + CAP_SETUID/SETGID (HOR-245)
│   ├── mTLS Work bidi-stream client (worker=client, CP=server)
│   ├── protocol state machine (single-credit invariant) + heartbeat
│   ├── event outbox + WAL (durable audit; no tail loss on crash)
│   ├── probes (/healthz, /readyz) + SIGTERM drain
│   └── child-process.ts: spawn the child via the setpriv launcher + IPC
└── pi child (one assignment; dist/child.js)               session UID/GID, no caps, no_new_privs
    ├── fresh AgentSessionRuntime (resume-or-create from the PVC)
    └── pi AgentSessionEvent -> durable TurnEvent payloads (stdout IPC)
```

The supervisor never imports pi/extensions/tools — it talks to the `Child`
abstraction. The child is the only place model-directed code runs, under the
session UID with kernel-enforced filesystem isolation.

## Contract (`proto/iterabase/harness/v1/harness.proto`)

Native gRPC over HTTP/2 + mTLS (SPIFFE URI SAN binds Pod/pool identity). One
bidi method: `rpc Work(stream WorkerMessage) returns (stream ControlMessage)`.

- **Worker→CP:** `Hello` (cert-SAN-bound), `Ready` (one dispatch credit),
  `Heartbeat`, `TurnEvent` (durable, sequenced, cumulatively ACKed),
  `TokenDelta` (ephemeral, non-sequenced — live token streaming).
- **CP→worker:** `Welcome` (fencing generation + lease intervals),
  `AssignTurn` (per-turn session/persona/model/tool-allowlist/sandbox/message),
  `AbortTurn` (idempotent), `EventAck` (cumulative, post-Postgres-commit).

12 durable `TurnEvent` payloads (`ExecutionStarted`, `ModelCallStarted`,
`AssistantMessage`, `ModelCallFailed`, `ModelRetryScheduled/Finished`,
`ToolCallStarted`, `ToolResult`, `CompactionStarted/Finished`, `HarnessError`,
`WorkerOutcome{COMPLETED|ABORTED|FAILED}`). `Steer`/`FollowUp` + CP-side
token-delta UI forwarding are deferred (interactive surfaces).

## Sandbox

`/data/sandboxes/<sandbox-id>/{home,tmp,session,workspace}`, root `0700` owned
by a stable CP-assigned session UID/GID; mount-root `0711` (traversable, not
listable). The **provisioner (HOR-245) creates+chowns** sandboxes (+ repo CoW
checkouts for agentic coding); the **supervisor validates-only** (never chowns,
no v1 auto-create — missing/mismatched → typed `FAILED`). The child runs under
the session UID with `no_new_privs` + all caps dropped (kernel `EACCES` for
sibling session roots). Extension code is pool-bound read-only; the per-turn
`toolAllowList` picks exposed tools.

## Config (`/etc/harness/config.yaml`, ConfigMap-mounted by HOR-245)

**Infra-only at boot** — no persona/model/session/tools (those are per-turn via
`AssignTurn`): control-plane gRPC URL + expected server name, worker Pod UID +
pool UID, optional pool scope identity, mTLS cert/key/CA paths, sandbox mount
root, read-only `piDirs`, egress-proxy URL, WAL spool dir (emptyDir), probe
port, + HTTP/2 ping / reconnect / child-liveness / abort-grace / outbox-bound /
model-retry / token-delta-buffer tunables. Certs are re-read each reconnect
(rotation). The harness holds zero real credentials — the per-sandbox egress
proxy (HOR-244) injects them.

## Failure semantics

- **Durable audit, no tail loss on crash:** durable `TurnEvent`s are fsync'd to
  a local WAL before send; a supervisor *crash* loses no audit tail (on restart
  the WAL is replayed as `after_terminal` — the CP already terminalized the turn
  as worker-loss via fencing). Pod-death/per-worker-PVC survival is deferred.
- **Stream loss (supervisor survives):** fail-closed — abort the child, never
  resume; the unacked tail is replayed as `after_terminal` on reconnect.
- **Non-idempotent turns:** no auto-retry; a failed turn is a workflow decision
  (HOR-246/HOR-249). Tool calls carry `turn_id + tool_call_id` as audit/correlation.
- **Cancellation:** CP `RUNNING→CANCELED` CAS fences immediately; `AbortTurn` is
  best-effort; the worker's late outcome is `after_terminal` (first-terminal-writer).
- **Outcomes:** `COMPLETED` only after `agent_settled` + flush + `session_shutdown`
  + dispose + clean exit + ACK; a successful message + failed cleanup = `FAILED`.

## Internals (`src/`)

- `main.ts` — entry point: boot config + probes + supervisor + SIGTERM/SIGINT drain.
- `work-client.ts` — mTLS gRPC HTTP/2 transport + bidi `Work` stream + `Hello`/`Welcome`.
- `worker-state.ts` — protocol state machine + single-credit invariant.
- `supervisor.ts` — connect/turn loop, reconnect+backoff, heartbeat, outbox/replay.
- `event-outbox.ts` — per-turn outbox + WAL (fsync per event/ack; crash recovery).
- `child-process.ts` — spawn the child via the launcher + IPC + exit classification.
- `child.ts` — the pi child: `AgentSession` + pi-event → `TurnEvent` mapping.
- `launcher.ts` — the `setpriv` privilege-dropping launcher (full cap-drop + `no_new_privs`).
- `sandbox.ts` — canonical paths + ownership/mode/cwd validation.
- `config.ts` — infra-only boot config loader/validator.
- `probes.ts` — `/healthz` + `/readyz`.

## Image (`Dockerfile`)

`node:24-bookworm-slim`, multi-stage (`tsc` → `dist/`), non-root (`65532`).
Bakes pi + the supervisor + the child entry + the SDK runtime. `setpriv`
(util-linux) is present. Mounts at runtime (HOR-245): config, `/pi` overlay,
`/data/sandboxes` (PVC), TLS certs, the WAL emptyDir. No inbound RPC port — the
worker dials the control-plane. `CAP_SETUID`/`CAP_SETGID` are granted to the
supervisor by the pod security context (HOR-245), not the image.

## Develop + test

```bash
make proto-tools          # buf + protoc plugins
make proto                # generate Go (internal/harnessrpc) + TS (src/gen)
make harness-build        # tsc -> dist
make harness-test         # vitest (unit + router-transport integration)
make harness-lint         # tsc --noEmit
make harness-image        # build the worker image
make harness-isolation-test   # Linux container: setpriv launcher + per-UID isolation (bullets 1-5)
```

Generated `src/gen/` + `internal/harnessrpc/` are committed; `make proto-check`
(CI) guards freshness. Tests: TS unit + router-transport integration (mock CP)
for the protocol/supervisor/outbox/child-process; the Linux-container isolation
gate (setpriv + per-session UID `EACCES`). The Go mTLS test server (real
TS-client ↔ Go-server wire) + the sequential pi-extension-state isolation test
land as follow-ups. E2E (real turn) is HOR-249/HOR-245 integration.

## Sequencing

HOR-381 is the prerequisite for HOR-245 (pool/operator/pod assembly + sandbox
provisioning + security context) and HOR-249 (dispatch + the `Work` server).
Both consume the `Work` contract this ticket defines. See the HOR-381 plan note
(Obsidian) + Linear HOR-381/245/249.

## Git workflow

Branches/commits/PRs carry the Linear ticket ID (e.g. `HOR-381`). See
`AGENTS.md`. Only the user approves/merges to `master`.
