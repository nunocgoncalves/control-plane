# HOR-392 — Tool Gateway implementation shaping

Status: **proposed** (awaiting founder approval) · 2026-07-31
Author: engineering
Governing spec: `Areas/ho/Tool Gateway and Lightweight Sandbox — Architecture Direction.md` (`ARCH-001`–`ARCH-018`, approved 2026-07-30)
Ticket: HOR-392 (In Review). Triggered by PR #32 re-review round `5144036991`.

## Why this note exists

The PR #32 re-review returned 13 Spec findings (5 blocker, 7 major, 1 minor), all
confirmed against the approved architecture and the ticket scope. The reviewer is
correct. Most findings flow from one root architectural gap: **the gateway trusts
caller-supplied `attempt_id` / `caller_scope` / `caller_scope_id` / `run_step_id`
instead of resolving them from durable control-plane state** (`ARCH-004`).

AGENTS.md requires explicit founder approval before implementing cross-service
contracts, failure/isolation models, and patterns other tickets follow — even when
the *behavior* is already approved in `ARCH-*`. The architecture doc itself lists
several of these details under "Implementation details still to shape… must be
specified in traceable engineering tickets." This note shapes those four open
details so implementation can proceed against an approved contract rather than
making the choices unilaterally.

After approval, the work happens on the existing `HOR-392-tool-gateway-registry-ledger`
branch and the 13 findings are replied to via the review-workflow tools (fixed /
partial where implemented; none `disagreed` — they are all spec-backed).

## What does NOT need shaping (mechanical, in-scope fixes)

These findings are clear defects against already-specified behavior and need no
new architectural decision; they will be fixed directly once the shaping below is
approved (listed here so the shaping section can stay focused):

- **#12** migration `tool_versions_updated` trigger fires `NEW.updated_at = now()`
  on a table with no `updated_at` column → re-registration crashes; and
  `RegisterToolVersion`'s `ON CONFLICT DO UPDATE SET description` mutates an
  immutable descriptor (`ARCH-007`). Fix: drop the trigger on `tool_versions`;
  make same-`(name,digest)` publication a validated no-op (no field mutation).
- **#10** `effectClassFromProto` defaults unspecified → `read_only`; a non-nil but
  empty `IdempotencyProof` is accepted. Fix: reject unspecified effect class at
  registration; require a concrete strategy + upstream key for `idempotent_write`
  before enabling retry (`ARCH-014` fail-closed).
- **#4** post-send `streamLost` is ignored; writes are committed `failed`. Fix:
  classify post-send cancellation/transport loss by effect class — writes →
  `outcome_unknown`, reads → `failed` (`ARCH-014`).
- **#5** `FinishInvocation` errors are discarded and a terminal response is
  returned uncommitted; runner output is unbounded/unvalidated. Fix: bound +
  validate runner JSON before commit; do not emit a terminal response unless the
  ledger transition commits (`REQ-009`/`ARCH-014`).
- **#7** arguments checked for size + JSON syntax only. Fix: validate arguments
  against the pinned descriptor's `input_schema` before the effect boundary
  (`REQ-010`). *Dependency choice flagged below.*
- **#11** credential resolution loads every active binding without comparing to
  the pinned descriptor's `credential_slots`. Fix: pass `tv` into resolution;
  require exact name/scheme/required match; reject extras and omissions
  (`ARCH-008`).
- **#13** no retry / recovery / cancellation tests. Fix: add hermetic coverage for
  read-only retry, proven-idempotent retry, empty/invalid proof, post-dispatch
  cancellation, restart recovery, action denial, version-pin enforcement
  (acceptance criteria).

## Shaping decisions requiring approval

### SD-1 — Durable caller-scope resolution (gateway ↔ runtime contract)

**Resolves:** #1 (blocker, caller-supplied IDs unvalidated), #9 (major, cancel
has no ownership check). Sharpens #3 (fail-open permitted set).

**Problem.** `ARCH-004`/`ARCH-010` require the gateway to resolve customer,
workflow, run, version, and permissions from durable state, not agent-supplied
scope. Today `resolveCallerScope` trusts `req.AttemptId` / `CallerScopeId`
(`run_step_id`) and, for the workflow-step path, looks up the binding by the
caller-supplied `CallerScopeId` as if it were a workflow-definition key. A valid
supervisor/runtime certificate can therefore invent context.

**Durable state available today.** `runtime.workflow_runs` (kind, `definition_key`,
`scope_identity_id`, state), `runtime.run_steps` (run_id, seq, state),
`runtime.turns` (id, run_id, step_id, session_id, state). The supervisor SPIFFE id
encodes the pool: `spiffe://iterabase.local/pools/<pool-uid>/workers/<pod-uid>`
(harness proto). The harness `AssignTurn` carries `pool_id`/`worker_id`.

**Missing link (the cross-service decision).** There is **no durable run→pool
assignment record** in the schema. The turn→run→scope_identity chain exists, but
nothing durably binds a run/turn to the pool/supervisor that owns it — assignment
today lives only in the live `Work` dispatch stream (HOR-249). The gateway cannot
prove a turn belongs to the calling supervisor's pool without it.

**Proposed contract.**

1. **Turn path (supervisor caller):**
   - Gateway resolves pool from the verified SPIFFE id (existing
     `ResolvePoolBySpiffePrefix`) — unchanged.
   - Gateway validates the supplied `turn_id` (= `caller_scope_id`) against
     `runtime.turns`: row exists, `state IN ('pending','running')`, and its run is
     assigned to **this pool**.
   - The run→pool binding is read from a new minimal durable assignment pointer
     (see *SD-1 contract table* below). Fail closed if absent.
   - `attempt_id`: for v1 the attempt is the run (chat) or the run's active
     attempt (workflow). The gateway treats `attempt_id` as the `runtime.turns.run_id`
     and validates it matches the turn's run — it is *not* caller-selected freely.
     (HOR-254 may later introduce a first-class `attempts` table; the gateway
     reads `attempt_id` as a run id until then, documented in code.)

2. **Workflow-step path (control-plane runtime caller):**
   - The caller's SPIFFE id is a control-plane workflow identity
     (`spiffe.KindControlPlaneWorkflow`).
   - Gateway validates the supplied `run_step_id` against `runtime.run_steps`:
     row exists, `state = 'running'`, and its run's `definition_key` resolves a
     `workflow_pool_binding` (the binding is looked up by the run's
     `definition_key`, **not** by a caller-supplied key).
   - `permitted_tools` comes from that binding. The `caller_scope_id` is the
     validated `run_step_id`; the workflow key is derived from the run, never
     trusted from the caller.

3. **Cancellation (#9):** `CancelInvocation` reads the authenticated identity,
   resolves the caller's durable scope (same as above), and rejects unless the
   invocation's ledger row belongs to the same pool/attempt. Runner identities
   (`spiffe.KindRunner`) are rejected from the caller-facing APIs entirely (the
   shared mTLS middleware admits them on the runner stream only).

**SD-1 contract table (new):**

```sql
-- Durable run -> pool assignment. Written by HOR-249 (dispatch) when a turn is
-- assigned to a pool/worker; read fail-closed by the gateway. One row per run
-- (a run executes under one pool's scope). ARCH-004.
CREATE TABLE runtime.run_pool_assignments (
    run_id      uuid PRIMARY KEY REFERENCES runtime.workflow_runs(id) ON DELETE CASCADE,
    pool_id     uuid NOT NULL,        -- toolgateway.pools.id (no cross-schema FK; validated in Go)
    assigned_at timestamptz NOT NULL DEFAULT now()
);
```

Cross-schema read (gateway queries `runtime.run_pool_assignments` /
`runtime.turns` / `runtime.run_steps` read-only). No cross-schema FK (mirrors the
existing `definition_key`/`attempt_id` no-FK-until-later pattern).

**Dependencies / cross-ticket contracts:**
- HOR-249 (dispatch) writes `runtime.run_pool_assignments` on `AssignTurn`.
- HOR-254 (attempts) may introduce a first-class attempts table later; the
  gateway's `attempt_id` contract is "the run id" until then.

**For the hermetic test:** seed `runtime.workflow_runs` + `runtime.turns` +
`runtime.run_steps` + `runtime.run_pool_assignments` + `toolgateway.*` directly;
assert denial when any binding is absent or cross-scope.

**Alternatives considered:**
- *Snapshot-on-first-discovery / trust supervisor attestation:* rejected —
  violates `ARCH-004` ("rather than trusting agent-supplied scope").
- *Add a `pool_id` column to `runtime.workflow_runs`:* rejected — assignment is a
  dispatch concern (HOR-249), not a run-creation concern; a separate table keeps
  the writer clean.

**Approval ask:** approve the `runtime.run_pool_assignments` contract and the
turn/workflow-step resolution rules above (including treating `attempt_id` as the
run id for v1).

---

### SD-2 — Attempt-scoped tool-version pinning

**Resolves:** #2 (major, no attempt→tool pin; any registered digest selectable).

**Problem.** `ARCH-007` requires each attempt to snapshot one exact immutable
version per permitted tool at attempt creation; every turn/invocation in that
attempt uses that snapshot. Today `InvokeTool` looks up `(name, digest)` globally
and accepts any registered digest, so a caller can select a newly published digest
mid-attempt. Discovery returns all healthy versions.

**Proposed contract.**

```sql
-- The attempt's immutable tool-version snapshot (ARCH-007). One row per
-- (attempt, tool) resolved at attempt creation. The gateway resolves the digest
-- from here and IGNORES any caller-supplied digest. Absence = fail closed.
CREATE TABLE toolgateway.attempt_tool_pins (
    attempt_id          text NOT NULL,            -- runtime run id (v1; HOR-254 attempts later)
    tool_name           text NOT NULL,
    tool_version_digest text NOT NULL,            -- the pinned immutable digest
    pinned_at           timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (attempt_id, tool_name)
);
```

**Resolution changes:**
- `DiscoverEffectiveTools`: intersect `available_tool_versions` ∩ `pool_grants` ∩
  workflow-requested set ∩ **`attempt_tool_pins` for the caller's attempt**.
  Returns only pinned versions. A pinned version with no live runner is still
  *discoverable* (the descriptor is returned) but invocation fails
  `tool_unavailable` rather than substituting (ARCH-007 no-substitution).
- `InvokeTool`: resolve `tool_version_digest` from the attempt's pin for
  `msg.ToolName`; ignore `msg.ToolVersionDigest` (or require it to equal the pin
  and reject otherwise). No pin → `FAILED_PRECONDITION` (fail closed).

**Who writes the pins?** Attempt creation. For v1, attempt creation is not in
this repo's built code path yet (HOR-254). HOR-392 ships the table + a
`SnapshotAttemptTools(attempt_id, pool_id, permitted_tools)` store method that
resolves current `available_tool_versions` ∩ grants ∩ permitted and inserts the
pins; the attempt-creation caller (HOR-254, or the workflow runtime for
workflow-step attempts) invokes it. The hermetic test calls it directly.

**Alternatives considered:**
- *Snapshot-on-first-Discover:* rejected — `ARCH-007` says "at attempt creation,"
  and first-discovery races across concurrent turns.
- *No pin table, trust caller digest but verify it's the "current" version:*
  rejected — explicitly allows mid-attempt substitution, the exact bug flagged.

**Approval ask:** approve the `attempt_tool_pins` table and the
`SnapshotAttemptTools` contract (with HOR-254 as the production caller).

---

### SD-3 — Action / resource authorization policy

**Resolves:** #8 (blocker, only `max_effect_class` checked; no action/resource
policy). Completes #3 (fail-open).

**Problem.** `ARCH-008`/`ARCH-016`/`ARCH-018` and the security invariants require
authorization to be **action-specific, not just tool-name-specific**, and resource
constraints enforced before the effect boundary. Today `authorized` reads only
`max_effect_class`; `pool_grants.allowed_actions` and `credential_bindings.resource_constraints`
never participate. Granting one action on a tool authorizes every
argument/resource that tool can address.

**Proposed contract.**

1. **Action allow-list.** `pool_grants.allowed_actions` is a JSONB array of
   action keys (e.g. `["graph.read_mail","graph.send_mail"]`). The descriptor
   optionally declares the action its arguments target (a field in
   `input_schema` / a declared `action` argument, or a tool-level default). The
   gateway evaluates the effective action from the validated arguments and
   requires it to be in the grant's `allowed_actions`. If `allowed_actions` is
   empty/`[]`, only the effect-class ceiling applies (back-compat for tools with
   no action decomposition). *Open:* whether v1 requires the descriptor to
   declare actions, or treats the whole tool as one action when undeclared. **My
   recommendation:** v1 treats an undeclared action as the single action
   `"<tool_name>"` (so a grant must list it or set `["*"]`), keeping the model
   extensible without forcing action decomposition on every tool now.

2. **Resource constraints.** `credential_bindings.resource_constraints` (non-secret
   tenant/site/mailbox scope) is resolved alongside credentials and asserted
   against the resource target derived from validated arguments. A mismatch →
   denial before dispatch. The resource target is derived from arguments per the
   descriptor's declared resource pointer (v1: a simple `resource` argument or
   the binding's fixed scope). Mismatch → `permission_denied`.

3. **Fail-open fix (#3).** Distinguish:
   - **absent narrowing** (supervisor/turn path has no workflow-requested set) →
     all pool-granted tools are in scope (narrowed only by grants + pins), and
   - **explicitly empty workflow-requested set** (workflow-step binding with
     `permitted_tools = []`) → deny all tools.
   Implementation: `permitted_tools == nil` ⇒ no narrowing; `len == 0` ⇒ deny all.

**Alternatives considered:**
- *Defer action/resource policy to a follow-up ticket:* rejected — `ARCH-008`/`018`
  and the security invariants make it part of the v1 boundary; the reviewer is
  right that effect-class-only authz is a blocker.

**Approval ask:** approve the action allow-list semantics (undeclared action =
`"<tool_name>"`, `[]` = effect-class-only) and the resource-constraint assertion
model.

---

### SD-4 — Crash recovery / lease model for orphaned invocations

**Resolves:** #6 (blocker, no recovery; orphaned `dispatching`/`running` rows
report `in_progress` forever).

**Problem.** `SCN-008`/`REQ-009`/`ARCH-014` require restart recovery: if the
process dies after dispatch, a possible effect without a committed result must
become `outcome_unknown`, never automatically repeated. Today there is no lease
metadata and no startup reconciliation; a restart leaves rows `dispatching`/`running`
indefinitely, so duplicates return `in_progress` despite a possible completed
write.

**Proposed contract.**

1. **Lease metadata on `invocations`:**
   ```sql
   ALTER TABLE toolgateway.invocations
       ADD COLUMN dispatch_lease_expires_at timestamptz,
       ADD COLUMN gateway_instance_id text;       -- the process that owns the dispatch
   ```
   Set `dispatch_lease_expires_at = now() + lease` (default 2× heartbeat, configurable)
   and `gateway_instance_id` at `BeginInvocation`/`MarkRunning`.

2. **Startup reconciliation (`RecoverOrphanedInvocations`):** on gateway start,
   sweep `state IN ('dispatching','running') AND dispatch_lease_expires_at < now()`:
   - `effect_class = read_only` → `failed` (no effect possible; safe).
   - `effect_class IN (idempotent_write, non_idempotent_write)` →
     `outcome_unknown` (a possible effect with no committed result; never
     auto-repeated — `ARCH-014`).
   The sweep runs once at boot before the service accepts traffic, in one
   transaction per row with the `state IN (...)` guard.

3. **In-process lease expiry:** a background ticker re-runs the same sweep for
   rows whose lease has expired (covers a stuck dispatch without full process
   death). Same classification.

4. **No automatic retry/resume of writes.** Only `read_only` is safe to classify
   `failed` (caller may retry). Writes are never resumed by the gateway; they
   require explicit operator/customer compensation (`ARCH-014`).

**Alternatives considered:**
- *Resume in-flight writes on restart:* rejected — `ARCH-014` forbids automatic
  repetition of uncertain effects.
- *Lease via a separate heartbeat table:* rejected — a column on `invocations` is
  simpler and co-located with the row the sweep touches.

**Approval ask:** approve the lease columns, the boot + ticker reconciliation, and
the read_only→failed / write→outcome_unknown classification.

---

## Open questions for the founder

1. **SD-1:** OK to add `runtime.run_pool_assignments` (written by HOR-249) and
   treat `attempt_id` as the run id until HOR-254?
2. **SD-2:** OK to add `toolgateway.attempt_tool_pins` + `SnapshotAttemptTools`,
   with HOR-254 as the production caller?
3. **SD-3:** OK with "undeclared action = `<tool_name>`, `[]` = effect-class-only"
   for v1?
4. **SD-4:** OK with lease columns + boot/ticker sweep + the
   read_only→failed / write→outcome_unknown classification?
5. **#7 dependency:** OK to add a JSON-Schema validator dependency? Candidate:
   `github.com/sanity-laying/jsonschema` is unmaintained; I'd propose
   `github.com/twpayne/go-jsonschema` (kept current) or a vendored minimal
   validator if you'd rather avoid a new dep. Which do you prefer?

## After approval

- Implement on `HOR-392-tool-gateway-registry-ledger`.
- Migration `000012_toolgateway_pins_recovery.up.sql` adds the SD-1/SD-2/SD-4
  tables/columns; migration `000012_toolgateway_fixes.up.sql` (or amending 000011
  if not yet applied to master) fixes the SD-trigger bug (#12). *Open: 000011 is
  already on the branch but not on master — I'll amend 000011 in place rather
  than add 000012, since master has not received it. Confirm preferred.*
- Reply to all 13 findings via `review_post_reply` (fixed/partial as implemented),
  then `review_post_response_summary`.

## Implementation addendum (executed)

Implemented as approved with the following realized choices:

- **Migration:** amended `000011_toolgateway.up.sql` in place (not yet on master):
  removed the broken `tool_versions_updated` trigger; added
  `runtime.run_pool_assignments`, `toolgateway.attempt_tool_pins`, and the
  `dispatch_lease_expires_at` / `gateway_instance_id` columns + recovery index on
  `invocations`. Down migration updated.
- **#7 dependency:** `github.com/twpayne/go-jsonschema` does not exist at that
  path; used `github.com/santhosh-tekuri/jsonschema/v5@v5.3.1` (actively
  maintained, drafts 4–2020) instead. Validator lives in `internal/gateway/argschema.go`.
- **Terminal commits after caller disconnect:** the finish/classify helpers commit
  via a detached context (`detachedCtx`) so a caller deadline/disconnect after a
  possible effect still durably records `outcome_unknown`/`failed` (strengthens
  #4/#5/#6 — the ledger is authoritative regardless of caller state).
- **SD-1 `attempt_id`:** treated as the runtime run id (validated against
  `runtime.turns.run_id` / `runtime.run_steps.run_id`); HOR-254 may introduce a
  first-class attempts table later.
- **Tests:** added coverage for read-only retry, proven-idempotent retry,
  idempotent-without-proof rejection, unspecified-effect rejection, schema
  validation, action denial, version-pin enforcement, credential-slot mismatch,
  post-dispatch context cancellation, restart recovery, caller-scope validation,
  and cancel ownership (#13).
