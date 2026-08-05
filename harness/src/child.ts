// The per-turn pi child entry (HOR-381; HOR-395 gateway bridges). Run via the
// setpriv launcher under the session UID/GID. Reads a framed `assignment` from
// fd 0, awaits the non-secret gateway-tool descriptors on fd 5, creates a
// fresh pi AgentSession (resume-or-create by the EXACT assignment session.id
// from the PVC session dir — never auto-detect), runs one turn, maps pi
// lifecycle events → durable TurnEvent payloads + ephemeral TokenDeltas over
// the framed fd-3 channel, emits a heartbeat for liveness, and writes a final
// `result`. A framed `abort` on fd 0 aborts pi.
//
// The child holds NO gateway/inference credential and has NO direct network
// route (ARCH-003/010). Model calls cross the custom `streamSimple` provider →
// fd 4/fd 5 → supervisor → inference gateway (mTLS). Gateway tool calls cross
// fd 4/fd 5 → supervisor → tool gateway (mTLS). The supervisor validates every
// request against the active assignment (ARCH-004).
//
// Provider-SDK retries are disabled (settingsManager retry.provider.maxRetries
// = 0) so there is exactly one observable retry layer — pi's own bounded
// auto-retry (retry.maxRetries = HARNESS_MODEL_MAX_ATTEMPTS).

import { createHash } from "node:crypto";
import { createReadStream, existsSync, lstatSync, mkdirSync, readFileSync, readdirSync, writeSync } from "node:fs";
import { join, normalize, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { toJson, create } from "@bufbuild/protobuf";
import { Type } from "typebox";
import {
  AuthStorage,
  ModelRegistry,
  SessionManager,
  SettingsManager,
  createAgentSessionFromServices,
  createAgentSessionServices,
  createAgentSessionRuntime,
  AgentSessionRuntime,
  defineTool,
  type AgentSession,
  type AgentSessionEvent,
  type AgentSessionServices,
  type CreateAgentSessionRuntimeResult,
  type ExtensionFactory,
  type ProviderConfig,
  type ToolDefinition,
} from "@earendil-works/pi-coding-agent";
import type { Context, SimpleStreamOptions, Model, Api } from "@earendil-works/pi-ai";
import {
  AssistantMessageSchema,
  CompactionFinishedSchema,
  CompactionStartedSchema,
  HarnessErrorSchema,
  ModelCallFailedSchema,
  ModelCallStartedSchema,
  ModelRetryFinishedSchema,
  ModelRetryScheduledSchema,
  ToolCallStartedSchema,
  ToolResultSchema,
  TurnEventSchema,
  UsageSchema,
  ToolCallSchema,
  ErrorDetailSchema,
  Retryability,
  Outcome,
  type TurnEvent,
} from "./gen/iterabase/harness/v1/harness_pb.js";
import { FrameReader, encodeFrame, parseSupervisorFrame, type ArtifactInputRefFrame, type GatewayToolDescriptor } from "./ipc.js";
import { ChildRpc } from "./child-rpc.js";
import { buildOpenAIRequestBody } from "./openai-stream.js";

const PROVIDER = "iterabase-inference";
const SESSION_ID_RE = /^[A-Za-z0-9](?:[A-Za-z0-9._-]*[A-Za-z0-9])?$/;
const ALLOWED_IMAGE_MIME = new Set(["image/png", "image/jpeg", "image/gif", "image/webp"]);
const MAX_IMAGE_BYTES = 20 * 1024 * 1024;

interface AssignmentImage {
  data: string; // base64
  mimeType: string;
}
interface Assignment {
  turnId: string;
  sessionId: string;
  runId?: string;
  workItemId?: string;
  nodeExecutionId?: string;
  nodeKey?: string;
  contextJson?: string;
  completionOutcomes?: string[];
  completionOutputSchemaJson?: string;
  skills?: { name: string; version: string; digest: string }[];
  artifactInputs: ArtifactInputRefFrame[];
  persona: string;
  model: { id: string; api: string; contextWindow: number; maxOutputTokens?: number; thinkingLevel?: string };
  workspaceTools: boolean;
  message: string;
  images?: AssignmentImage[];
}

/** Per-turn state for the reserved complete_step platform control function. */
export class StepCompletionState {
  readonly required: boolean;
  reported = false;
  private reporting = false;

  constructor(private readonly assignment: Assignment, private readonly rpc: Pick<ChildRpc, "reportStepCompletion">) {
    this.required = Boolean(assignment.nodeExecutionId);
  }

  tool(): ToolDefinition {
    const outcomes = this.assignment.completionOutcomes ?? [];
    let outputSchema: unknown = {};
    try {
      outputSchema = JSON.parse(this.assignment.completionOutputSchemaJson || "{}");
    } catch {
      throw new Error("invalid completion output schema JSON");
    }
    const parameters = {
      type: "object",
      additionalProperties: false,
      required: ["outcome", "summary", "output"],
      properties: {
        outcome: { type: "string", enum: outcomes },
        summary: { type: "string", minLength: 1 },
        output: outputSchema,
        artifact_refs: {
          type: "array",
          items: {
            type: "object",
            additionalProperties: false,
            required: ["artifact_id"],
            properties: { artifact_id: { type: "string", minLength: 1 }, role: { type: "string", enum: ["output", "evidence"] }, metadata: {} },
          },
        },
      },
    };
    return defineTool({
      name: "complete_step",
      label: "Complete workflow step",
      description: "Report the declared workflow outcome and structured output after the task is fully complete.",
      parameters: Type.Unsafe(parameters as never),
      execute: async (_toolCallId, raw) => {
        if (this.reported || this.reporting) throw new Error("complete_step may be called exactly once");
        const p = raw as { outcome?: unknown; summary?: unknown; output?: unknown; artifact_refs?: unknown };
        if (typeof p.outcome !== "string" || !outcomes.includes(p.outcome)) throw new Error("complete_step outcome is not declared by this node");
        if (typeof p.summary !== "string" || p.summary.length === 0) throw new Error("complete_step summary is required");
        const refs = Array.isArray(p.artifact_refs) ? p.artifact_refs : [];
        const artifactRefs = refs.map((rawRef) => {
          if (!rawRef || typeof rawRef !== "object") throw new Error("complete_step artifact ref must be an object");
          const ref = rawRef as { artifact_id?: unknown; role?: unknown; metadata?: unknown };
          if (typeof ref.artifact_id !== "string" || ref.artifact_id.length === 0) throw new Error("complete_step artifact_id is required");
          return {
            artifactId: ref.artifact_id,
            role: typeof ref.role === "string" ? ref.role : "output",
            metadataJson: JSON.stringify(ref.metadata ?? {}),
          };
        });
        this.reporting = true;
        await this.rpc.reportStepCompletion({ outcome: p.outcome, summary: p.summary, outputJson: JSON.stringify(p.output ?? {}), artifactRefs });
        this.reported = true;
        return { content: [{ type: "text", text: "Workflow step completion recorded. Do not call customer-system tools again." }], details: {} };
      },
    });
  }
}

class ArtifactPublicationState {
  constructor(private readonly rpc: Pick<ChildRpc, "publishArtifact">) {}

  tool(): ToolDefinition {
    return defineTool({
      name: "publish_artifact",
      label: "Publish workspace artifact",
      description: "Publish one regular file from the active workspace as a new immutable artifact reference.",
      parameters: Type.Object({
        relative_path: Type.String({ minLength: 1 }),
        mime_type: Type.String({ minLength: 1 }),
      }, { additionalProperties: false }),
      execute: async (_toolCallId, raw) => {
        const p = raw as { relative_path?: unknown; mime_type?: unknown };
        if (typeof p.relative_path !== "string" || typeof p.mime_type !== "string") throw new Error("relative_path and mime_type are required");
        const ref = await this.rpc.publishArtifact(p.relative_path, p.mime_type);
        return {
          content: [{ type: "text", text: JSON.stringify({ artifact_id: ref.artifactId, mime_type: ref.mimeType, size_bytes: ref.sizeBytes, digest: ref.digest }) }],
          details: ref,
        };
      },
    });
  }
}

function assignmentMessage(a: Assignment): string {
  if (!a.nodeExecutionId) return a.message;
  const outcomes = (a.completionOutcomes ?? []).join(", ");
  return [
    "<workflow_node_task>",
    a.message,
    "</workflow_node_task>",
    "<workflow_context_json untrusted_customer_data=\"true\">",
    a.contextJson || "{}",
    "</workflow_context_json>",
    `When the task is fully complete, call complete_step exactly once with one of these outcomes: ${outcomes}.`,
  ].join("\n");
}

/** Write a framed ChildFrame to fd 3 (the child→supervisor IPC channel). */
function writeFrame(frame: unknown): void {
  try {
    writeSync(3, encodeFrame(frame));
  } catch {
    /* fd closed (supervisor gone) — exit promptly via the main loop */
  }
}

function emit(payload: TurnEvent["kind"]): void {
  const te = create(TurnEventSchema, { turnId: "", sequence: 0n, timestampMs: 0n, kind: payload });
  writeFrame({ type: "event", event: toJson(TurnEventSchema, te) });
}

function emitTokenDelta(contentIndex: number, deltaType: "TEXT" | "THINKING", delta: string): void {
  writeFrame({ type: "tokenDelta", contentIndex, deltaType, delta });
}

function emitHeartbeat(): void {
  writeFrame({ type: "heartbeat" });
}

function emitResult(outcome: Outcome, message?: string): void {
  writeFrame({ type: "result", outcome, message });
}

/**
 * Minimal view of a pi `ExtensionRunner` for shutdown-error capture: anything
 * with an `onError(listener)` unsubscribe pattern.
 */
export interface ExtensionErrorEmitter {
  onError(listener: (error: { event: string; extensionPath: string; error: string }) => void): () => void;
}

/**
 * Register an error listener that records `session_shutdown` handler failures.
 *
 * pi's `ExtensionRunner.emit()` catches `session_shutdown` handler exceptions
 * and routes them to error listeners instead of rethrowing, so
 * `AgentSessionRuntime.dispose()` resolves cleanly even when a shutdown
 * handler failed. Without this capture, teardown failures are silently
 * swallowed and a successful prompt still produces `COMPLETED`, contrary to
 * HOR-381 ("successful assistant message + failed cleanup = FAILED"). The
 * caller appends any `dispose()` rejection to `errors` and, if non-empty,
 * classifies the outcome FAILED.
 *
 * @returns `{ unsubscribe, errors }` — `errors` is the live mutable list so
 * the caller can also record a dispose() rejection.
 */
export function captureShutdownErrors(runner: ExtensionErrorEmitter): {
  unsubscribe: () => void;
  errors: string[];
} {
  const errors: string[] = [];
  const unsubscribe = runner.onError((e) => {
    if (e.event === "session_shutdown") errors.push(`${e.extensionPath}: ${e.error}`);
  });
  return { unsubscribe, errors };
}

async function main(): Promise<void> {
  const assignment = await readAssignment();
  if (!assignment) {
    emitResult(Outcome.FAILED, "no valid assignment on stdin");
    return;
  }

  const sessionDir = process.env.HARNESS_SESSION_DIR ?? "/data/session";
  const cwd = process.env.HARNESS_WORKING_DIR ?? sessionDir;
  const piDirs = (process.env.HARNESS_PI_DIRS ?? "").split(":").filter(Boolean);
  const maxAttempts = Number(process.env.HARNESS_MODEL_MAX_ATTEMPTS ?? "3") || 3;
  const livenessMs = Number(process.env.HARNESS_LIVENESS_INTERVAL_MS ?? "5000") || 5000;
  mkdirSync(sessionDir, { recursive: true });

  // The supervisor→child RPC channel (fd 5): non-secret gateway descriptors +
  // model/tool responses. fd 4 (child→supervisor requests) is written via rpc.
  // fd 5 is always present in production; guard so a missing fd (tests that
  // only exercise the heartbeat path) does not crash the child.
  const rpc = new ChildRpc({ write: (buf) => writeSync(4, buf) });
  try {
    const rpcStream = createReadStream("", { fd: 5 });
    rpcStream.on("data", (chunk: Buffer | string) => rpc.feed(chunk));
    rpcStream.on("end", () => rpc.close());
    rpcStream.on("error", () => rpc.close());
  } catch {
    /* fd 5 unavailable — rpc will never resolve; the supervisor kills the child */
  }

  // Emit an immediate heartbeat so the supervisor's watchdog sees liveness
  // without waiting one interval, then keep it warm for the turn.
  emitHeartbeat();
  const hb = setInterval(emitHeartbeat, Math.max(50, Math.floor(livenessMs / 2)));
  hb.unref?.();

  // Listen for a framed `abort` from the supervisor (fd 0).
  let aborted = false;
  const onAbortFrame = () => {
    aborted = true;
    void currentRuntime?.session.abort().catch(() => {});
  };
  const abortReader = new FrameReader((raw) => {
    const f = parseSupervisorFrame(raw);
    if (f?.type === "abort") onAbortFrame();
  });
  process.stdin.on("data", (chunk: Buffer) => abortReader.feed(chunk));

  const completion = new StepCompletionState(assignment, rpc);
  let currentRuntime: AgentSessionRuntime | undefined;
  try {
    // Await the supervisor's gateway-tool discovery before building the session
    // (ARCH-006): the child registers pi tool stubs from the non-secret
    // descriptors. An empty list is valid (workspace-only turn).
    const descriptors = await rpc.awaitGatewayTools();
    currentRuntime = await createSession(assignment, sessionDir, cwd, piDirs, maxAttempts, rpc, descriptors, completion);
  } catch (err) {
    clearInterval(hb);
    emit({
      case: "harnessError",
      value: create(HarnessErrorSchema, {
        error: create(ErrorDetailSchema, {
          message: `session setup failed: ${err instanceof Error ? err.message : String(err)}`,
          retryability: Retryability.NON_RETRYABLE,
        }),
      }),
    });
    emitResult(Outcome.FAILED);
    return;
  }

  // Map pi events → durable TurnEvent payloads + ephemeral token deltas.
  // Track the last assistant stopReason so an exhausted/non-retryable model
  // failure (stopReason "error") is classified FAILED, not COMPLETED.
  let lastAssistantStopReason: string | undefined;
  const session = currentRuntime.session;

  // pi's `ExtensionRunner.emit()` catches `session_shutdown` handler exceptions
  // and routes them to an error listener instead of rethrowing, so
  // `AgentSessionRuntime.dispose()` resolves cleanly even when a shutdown
  // handler failed. Capture those errors so a failed cleanup turns the outcome
  // into FAILED per HOR-381 ("successful assistant message + failed cleanup =
  // FAILED"), rather than being silently swallowed into COMPLETED.
  const shutdown = captureShutdownErrors(session.extensionRunner);

  const unsub = session.subscribe((ev: AgentSessionEvent) => {
    // Forward live token deltas (ephemeral, non-sequenced, non-ACKed).
    if (ev.type === "message_update") {
      const d = ev.assistantMessageEvent;
      if (d.type === "text_delta") emitTokenDelta(d.contentIndex, "TEXT", d.delta);
      else if (d.type === "thinking_delta") emitTokenDelta(d.contentIndex, "THINKING", d.delta);
      return;
    }
    if (ev.type === "message_end" && ev.message.role === "assistant") {
      lastAssistantStopReason = ev.message.stopReason;
    }
    const payload = mapEvent(ev, session);
    if (payload) emit(payload);
  });

  // SIGTERM -> abort pi (the supervisor's bounded-escalation abort path).
  const onTerm = () => {
    aborted = true;
    void currentRuntime?.session.abort().catch(() => {});
  };
  process.on("SIGTERM", onTerm);
  process.on("SIGINT", onTerm);

  try {
    const images = buildImages(assignment);
    await session.prompt(assignmentMessage(assignment), images ? { images } : undefined);
    // agent_settled fires during prompt; flush + run the async shutdown
    // lifecycle (extension `session_shutdown` handlers) before reporting.
    // AgentSessionRuntime.dispose() awaits those handlers. Handler failures
    // are captured by the error listener above (pi swallows them inside
    // emit()); a dispose() rejection or any captured shutdown error must turn
    // the outcome into FAILED, not COMPLETED.
    try {
      await currentRuntime.dispose();
    } catch (disposeErr) {
      shutdown.errors.push(`dispose: ${disposeErr instanceof Error ? disposeErr.message : String(disposeErr)}`);
    } finally {
      shutdown.unsubscribe();
    }
    if (shutdown.errors.length > 0) {
      unsub();
      clearInterval(hb);
      emit({
        case: "harnessError",
        value: create(HarnessErrorSchema, {
          error: create(ErrorDetailSchema, {
            message: `session shutdown failed: ${shutdown.errors.join("; ")}`,
            retryability: Retryability.NON_RETRYABLE,
          }),
        }),
      });
      emitResult(Outcome.FAILED);
      return;
    }
    unsub();
    clearInterval(hb);
    emitHeartbeat();
    // Outcome classification: abort wins; else a terminal model failure
    // (last assistant stopReason "error") is FAILED; otherwise COMPLETED.
    if (aborted) emitResult(Outcome.ABORTED);
    else if (lastAssistantStopReason === "error") emitResult(Outcome.FAILED, "model call failed");
    else if (completion.required && !completion.reported) emitResult(Outcome.FAILED, "agent settled without complete_step");
    else emitResult(Outcome.COMPLETED);
  } catch (err) {
    unsub();
    clearInterval(hb);
    shutdown.unsubscribe();
    // Best-effort shutdown even on the failure path; ignore teardown errors
    // here since we are already reporting FAILED.
    await currentRuntime.dispose().catch(() => {});
    emit({
      case: "harnessError",
      value: create(HarnessErrorSchema, {
        error: create(ErrorDetailSchema, {
          message: err instanceof Error ? err.message : String(err),
          retryability: Retryability.UNKNOWN,
        }),
      }),
    });
    emitResult(Outcome.FAILED);
  }
}

/**
 * Read the first framed `assignment` from fd 0 (one-shot). The supervisor's
 * `AssignmentFrame` is `{type:"assignment", assignment:<Assignment json>}`, so
 * `parseSupervisorFrame(raw).assignment` IS the assignment object — it is NOT
 * nested again. The reader is detached once the assignment is decoded (or stdin
 * ends) so it cannot keep the event loop alive.
 */
function readAssignment(): Promise<Assignment | undefined> {
  return new Promise((resolve) => {
    let resolved = false;
    const stdin = process.stdin;
    const finish = (v: Assignment | undefined): void => {
      if (resolved) return;
      resolved = true;
      stdin.removeListener("data", onData);
      stdin.removeListener("end", onEnd);
      resolve(v);
    };
    const reader = new FrameReader((raw) => {
      const f = parseSupervisorFrame(raw);
      if (f?.type === "assignment") finish(parseAssignment(f.assignment));
    });
    const onData = (chunk: Buffer): void => reader.feed(chunk);
    const onEnd = (): void => finish(undefined);
    stdin.on("data", onData);
    stdin.on("end", onEnd);
  });
}

/**
 * Runtime-validate a decoded assignment object (the IPC trust boundary). A
 * malformed frame must not reach pi/session setup. Returns the typed
 * assignment, or undefined if the shape is invalid.
 */
export function parseAssignment(raw: unknown): Assignment | undefined {
  if (!raw || typeof raw !== "object") return undefined;
  const r = raw as Record<string, unknown>;
  if (typeof r.turnId !== "string" || typeof r.sessionId !== "string" || typeof r.persona !== "string" || typeof r.message !== "string") return undefined;
  if (!r.model || typeof r.model !== "object") return undefined;
  const m = r.model as Record<string, unknown>;
  if (typeof m.id !== "string" || typeof m.api !== "string" || typeof m.contextWindow !== "number") return undefined;
  if (typeof r.workspaceTools !== "boolean") return undefined;
  const a: Assignment = {
    turnId: r.turnId,
    sessionId: r.sessionId,
    persona: r.persona,
    model: { id: m.id, api: m.api, contextWindow: m.contextWindow },
    workspaceTools: r.workspaceTools,
    artifactInputs: [],
    message: r.message,
  };
  if (Array.isArray(r.artifactInputs)) {
    for (const rawRef of r.artifactInputs) {
      if (!rawRef || typeof rawRef !== "object") return undefined;
      const ref = rawRef as Record<string, unknown>;
      if (
        typeof ref.artifactId !== "string" || typeof ref.mimeType !== "string" ||
        typeof ref.sizeBytes !== "string" || !/^(0|[1-9][0-9]*)$/.test(ref.sizeBytes) ||
        typeof ref.digest !== "string"
      ) return undefined;
      a.artifactInputs.push({ artifactId: ref.artifactId, mimeType: ref.mimeType, sizeBytes: ref.sizeBytes, digest: ref.digest });
    }
  }
  if (typeof r.runId === "string") a.runId = r.runId;
  if (typeof r.workItemId === "string") a.workItemId = r.workItemId;
  if (typeof r.nodeExecutionId === "string") a.nodeExecutionId = r.nodeExecutionId;
  if (typeof r.nodeKey === "string") a.nodeKey = r.nodeKey;
  if (typeof r.contextJson === "string") a.contextJson = r.contextJson;
  if (typeof r.completionOutputSchemaJson === "string") a.completionOutputSchemaJson = r.completionOutputSchemaJson;
  if (Array.isArray(r.completionOutcomes) && r.completionOutcomes.every((v) => typeof v === "string")) a.completionOutcomes = r.completionOutcomes as string[];
  if (a.nodeExecutionId) {
    if (!a.nodeKey || !a.contextJson || !a.completionOutcomes?.length || !a.completionOutputSchemaJson) return undefined;
    try {
      JSON.parse(a.contextJson);
      JSON.parse(a.completionOutputSchemaJson);
    } catch {
      return undefined;
    }
  }
  if (Array.isArray(r.skills)) {
    const skills: { name: string; version: string; digest: string }[] = [];
    for (const skill of r.skills) {
      if (!skill || typeof skill !== "object") return undefined;
      const s = skill as Record<string, unknown>;
      if (typeof s.name !== "string" || typeof s.version !== "string" || typeof s.digest !== "string") return undefined;
      skills.push({ name: s.name, version: s.version, digest: s.digest });
    }
    a.skills = skills;
  }
  if (typeof m.maxOutputTokens === "number") a.model.maxOutputTokens = m.maxOutputTokens;
  if (typeof m.thinkingLevel === "string") a.model.thinkingLevel = m.thinkingLevel;
  if (Array.isArray(r.images)) {
    const imgs: AssignmentImage[] = [];
    for (const img of r.images) {
      if (!img || typeof img !== "object") return undefined;
      const im = img as Record<string, unknown>;
      if (typeof im.data !== "string" || typeof im.mimeType !== "string") return undefined;
      imgs.push({ data: im.data, mimeType: im.mimeType });
    }
    a.images = imgs;
  }
  return a;
}

/**
 * Build a pi ToolDefinition stub for one gateway descriptor (ARCH-006). The
 * stub forwards execute() over fd 4/fd 5 to the supervisor, which stamps
 * durable caller context + idempotency and calls InvokeTool over mTLS
 * (ARCH-004/014). The parameter schema is the descriptor's non-secret input
 * schema so the model sees the real tool contract; the gateway re-validates
 * arguments before the effect boundary (#7, ARCH-008).
 */
function gatewayToolStub(rpc: ChildRpc, completion: StepCompletionState, artifactInputs: ArtifactInputRefFrame[]): (d: GatewayToolDescriptor) => ToolDefinition {
  return (d) =>
    defineTool({
      name: d.name,
      label: d.name,
      description: d.description,
      parameters: Type.Unsafe(d.inputSchema as never),
      execute: async (toolCallId, params, signal) => {
        if (completion.reported) throw new Error("customer-system tools are disabled after complete_step");
        const res = await rpc.invokeTool(
          {
            toolCallId,
            toolName: d.name,
            toolVersionDigest: d.digest,
            argumentsJson: JSON.stringify(params),
            artifactInputRefs: d.readsArtifacts
              ? artifactInputs.filter((ref) => !d.acceptedArtifactMimeTypes?.length || d.acceptedArtifactMimeTypes.includes(ref.mimeType))
              : [],
            idempotencyKey: toolCallId,
          },
          signal,
        );
        if (res.isError) {
          // Gateway denial/error must be an attributable failure, not a
          // successful tool result. pi's executor sets isError=false whenever
          // execute() resolves, so the supported error path is to throw: the
          // agent loop marks the call isError=true and the durable
          // tool_execution_end / model history carry it (REQ-010/SCN-009).
          throw new Error(res.errorMessage ?? `gateway tool ${d.name} failed`);
        }
        const artifactRefs = res.artifactRefs ?? [];
        let text = res.resultJson ?? "";
        if (artifactRefs.length > 0) {
          let result: unknown = text;
          try { result = text ? JSON.parse(text) : {}; } catch { /* retain text */ }
          text = JSON.stringify({
            result,
            artifact_refs: artifactRefs.map((ref) => ({ artifact_id: ref.artifactId, mime_type: ref.mimeType, size_bytes: ref.sizeBytes, digest: ref.digest })),
          });
        }
        return {
          content: [{ type: "text", text }],
          details: { artifactRefs },
        };
      },
    });
}

/** Validate + build pi image content from the assignment images (MIME + size checked). */
function buildImages(a: Assignment): { type: "image"; data: string; mimeType: string }[] | undefined {
  if (!a.images || a.images.length === 0) return undefined;
  const out: { type: "image"; data: string; mimeType: string }[] = [];
  for (const img of a.images) {
    if (!ALLOWED_IMAGE_MIME.has(img.mimeType)) throw new Error(`unsupported image mime: ${img.mimeType}`);
    // base64 decoded byte length (account for padding).
    const decodedLen = Math.floor((img.data.replace(/[^A-Za-z0-9+/=]/g, "").length * 3) / 4);
    if (decodedLen > MAX_IMAGE_BYTES) throw new Error(`image too large (${decodedLen} bytes)`);
    out.push({ type: "image", data: img.data, mimeType: img.mimeType });
  }
  return out;
}

// Hash one materialized skill tree by sorted relative path + exact file bytes.
// Symlinks and non-regular entries are rejected so the digest cannot depend on
// mutable content outside the pinned overlay artifact. The published workflow
// skill digest uses this canonical `sha256:<hex>` representation.
export function skillContentDigest(root: string): string {
  if (!lstatSync(root).isDirectory()) throw new Error(`assigned skill path is not a directory: ${root}`);
  const hash = createHash("sha256");
  const visit = (dir: string, prefix: string): void => {
    const entries = readdirSync(dir, { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name));
    for (const entry of entries) {
      const rel = prefix ? `${prefix}/${entry.name}` : entry.name;
      const path = join(dir, entry.name);
      if (entry.isSymbolicLink()) throw new Error(`assigned skill contains a symlink: ${rel}`);
      if (entry.isDirectory()) {
        visit(path, rel);
        continue;
      }
      if (!entry.isFile()) throw new Error(`assigned skill contains a non-regular entry: ${rel}`);
      const content = readFileSync(path);
      hash.update("file\0");
      hash.update(rel);
      hash.update("\0");
      hash.update(String(content.length));
      hash.update("\0");
      hash.update(content);
    }
  };
  visit(root, "");
  return `sha256:${hash.digest("hex")}`;
}

export function resolveSkillPaths(skills: { name: string; version: string; digest: string }[], piDirs: string[]): string[] {
  const out: string[] = [];
  for (const skill of skills) {
    const rel = normalize(skill.name);
    if (!rel || rel === "." || rel.startsWith(".." + sep) || rel === ".." || rel.startsWith(sep)) throw new Error(`invalid assigned skill name: ${skill.name}`);
    if (!/^sha256:[0-9a-f]{64}$/i.test(skill.digest)) throw new Error(`invalid assigned skill digest for ${skill.name}@${skill.version}`);
    let selected: string | undefined;
    // Search in reverse overlay precedence, but select only bytes matching the
    // durable pin. An updated same-name client skill cannot silently replace an
    // active attempt's exact product/client skill version.
    for (const root of [...piDirs].reverse()) {
      const candidate = join(root, "skills", rel);
      if (!existsSync(candidate)) continue;
      if (skillContentDigest(candidate).toLowerCase() === skill.digest.toLowerCase()) {
        selected = candidate;
        break;
      }
    }
    if (!selected) throw new Error(`assigned immutable skill is unavailable: ${skill.name}@${skill.version} (${skill.digest})`);
    out.push(selected);
  }
  return out;
}

/**
 * Create the pi session: resume-or-create by the EXACT session.id; pi cwd =
 * validated working dir. The custom `streamSimple` provider routes model
 * calls over fd 4/fd 5 to the supervisor's inference-gateway bridge (no local
 * endpoint, no credential — ARCH-010/011). Gateway tool stubs are registered
 * from the non-secret descriptors (ARCH-006); workspace tools (read/write/
 * edit/bash) are exposed only when workspaceTools=true (ARCH-016). Returns an
 * owned `AgentSessionRuntime` so the caller can run the async
 * `session_shutdown` lifecycle via `dispose()` before exit.
 */
export async function createSession(
  a: Assignment,
  sessionDir: string,
  cwd: string,
  piDirs: string[],
  maxAttempts: number,
  rpc: ChildRpc,
  descriptors: GatewayToolDescriptor[],
  completion = new StepCompletionState(a, rpc),
): Promise<AgentSessionRuntime> {
  if (!SESSION_ID_RE.test(a.sessionId)) throw new Error(`invalid session id: ${JSON.stringify(a.sessionId)}`);

  const authStorage = AuthStorage.create(join(sessionDir, "auth.json"));
  const skillPaths = resolveSkillPaths(a.skills ?? [], piDirs);
  const modelRegistry = ModelRegistry.create(authStorage);

  const providerFactory: ExtensionFactory = (pi) => {
    const provider: ProviderConfig = {
      // No baseUrl/apiKey/models — the child has no endpoint or real credential.
      // Model traffic crosses the streamSimple bridge → supervisor → inference
      // gateway (mTLS). ARCH-010/011.
      //
      // A placeholder apiKey is REQUIRED: pi gates every real send on
      // hasConfiguredAuth(provider) (authStorage OR the provider apiKey) — see
      // pi model-registry hasConfiguredAuth + agent-session send path. A
      // streamSimple-only registration populates neither, so a real turn throws
      // "No API key found for <provider>" before streaming (HOR-431). streamSimple
      // is the ONLY stream handler registered, so this placeholder is never
      // sent — the model call still crosses the mTLS bridge. Mirrors pi's
      // documented keyless-provider pattern (e.g. apiKey: "ollama").
      api: a.model.api as unknown as ProviderConfig["api"],
      apiKey: "iterabase-placeholder",
      streamSimple: (model, context: Context, options?: SimpleStreamOptions) => {
        const body = buildOpenAIRequestBody(
          model.id,
          { systemPrompt: context.systemPrompt, messages: context.messages, tools: context.tools as { name: string; description: string; parameters: unknown }[] | undefined },
          { reasoning: options?.reasoning, maxTokens: a.model.maxOutputTokens },
        );
        return rpc.streamModel(body, options?.signal, model.id);
      },
    };
    pi.registerProvider(PROVIDER, provider);
  };

  // The assigned model, constructed directly (no registry `models` block → no
  // baseUrl/apiKey required). streamSimple is registered for `a.model.api`, so
  // pi resolves this model's stream through the IPC bridge, never the (empty)
  // baseUrl. `input` includes `image` to preserve per-turn image behavior.
  const assignedModel: Model<Api> = {
    id: a.model.id,
    name: a.model.id,
    api: a.model.api as Api,
    provider: PROVIDER,
    baseUrl: "", // unused — streamSimple intercepts (ARCH-010)
    reasoning: false,
    input: ["text", "image"],
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
    contextWindow: a.model.contextWindow,
    maxTokens: a.model.maxOutputTokens ?? 4096,
  };

  // Deterministic runtime settings: provider-SDK retries disabled (one retry
  // layer — pi's own bounded auto-retry at maxAttempts).
  const settingsManager = SettingsManager.inMemory({
    retry: { enabled: true, maxRetries: maxAttempts, baseDelayMs: 1000, provider: { maxRetries: 0 } },
  });

  // ARCH-016: workspaceTools=true exposes exactly the four built-in tools
  // under session UID/GID; false exposes none. Gateway stubs are always added
  // via customTools. The allow-set MUST include the gateway descriptor names —
  // pi's `_refreshToolRegistry` filters both built-ins AND customTools by
  // `allowedToolNames`, so passing only the four built-ins (or `noTools:"all"`)
  // would strip every gateway stub from the agent (HOR-395/ARCH-006).
  const gatewayToolNames = descriptors.map((d) => d.name);
  // Reserved platform controls are independent of ARCH-016's AgentPool
  // workspace-tool switch. publish_artifact crosses supervisor IPC and is not
  // one of the local read/write/edit/bash tools.
  const controlToolNames = ["publish_artifact", ...(completion.required ? ["complete_step"] : [])];
  const toolOpts = a.workspaceTools
    ? { tools: [...gatewayToolNames, ...controlToolNames, "read", "write", "edit", "bash"] as string[] }
    : { tools: [...gatewayToolNames, ...controlToolNames] };
  const customTools = descriptors.map(gatewayToolStub(rpc, completion, a.artifactInputs));
  customTools.push(new ArtifactPublicationState(rpc).tool());
  if (completion.required) customTools.push(completion.tool());

  // The runtime factory closes over the assignment-specific inputs (provider,
  // settings, tools, persona) and creates cwd-bound services + session. It is
  // stored on the AgentSessionRuntime and reused for /new,/resume,/fork — which
  // we never invoke here (single turn), but the factory is required for
  // initial construction. `session_shutdown` runs through `runtime.dispose()`.
  const createRuntime = async ({
    cwd: rtCwd,
    sessionManager,
  }: {
    cwd: string;
    agentDir: string;
    sessionManager: SessionManager;
  }): Promise<CreateAgentSessionRuntimeResult> => {
    const services: AgentSessionServices = await createAgentSessionServices({
      cwd: rtCwd,
      agentDir: sessionDir, // scope discovery to the session (no global ~/.pi)
      authStorage,
      settingsManager,
      modelRegistry,
      resourceLoaderOptions: {
        additionalExtensionPaths: piDirs,
        additionalSkillPaths: skillPaths,
        extensionFactories: [providerFactory],
        systemPromptOverride: () => a.persona,
      },
    });

    const result = await createAgentSessionFromServices({
      services,
      sessionManager,
      model: assignedModel,
      ...toolOpts,
      ...(customTools.length ? { customTools } : {}),
    });
    return {
      ...result,
      services,
      diagnostics: services.diagnostics,
    };
  };

  const sessionManager = resolveSessionManager(a.sessionId, sessionDir, cwd);
  return createAgentSessionRuntime(createRuntime, { cwd, agentDir: sessionDir, sessionManager });
}

/** Resume-or-create by the EXACT session.id: open `<ts>_<sessionId>.jsonl` if present, else create with that id. */
function resolveSessionManager(sessionId: string, sessionDir: string, cwd: string): SessionManager {
  const existing = findSessionFile(sessionDir, sessionId);
  if (existing) return SessionManager.open(existing, sessionDir, cwd);
  return SessionManager.create(cwd, sessionDir, { id: sessionId });
}

/** Find the most recent session file ending in `_<sessionId>.jsonl` (exact id match). */
function findSessionFile(sessionDir: string, sessionId: string): string | undefined {
  if (!existsSync(sessionDir)) return undefined;
  const suffix = `_${sessionId}.jsonl`;
  const files = readdirSync(sessionDir).filter((f) => f.endsWith(suffix));
  if (files.length === 0) return undefined;
  files.sort(); // timestamp-prefixed names sort chronologically
  return join(sessionDir, files.at(-1)!);
}

/** Map a pi AgentSessionEvent -> a durable TurnEvent payload (or undefined to skip). */
function mapEvent(ev: AgentSessionEvent, s: AgentSession): TurnEvent["kind"] | undefined {
  switch (ev.type) {
    case "turn_start":
      return {
        case: "modelCallStarted",
        value: create(ModelCallStartedSchema, { model: s.model?.id ?? "", thinkingLevel: s.thinkingLevel ?? "" }),
      };
    case "message_end": {
      const m = ev.message;
      if (m.role !== "assistant") return undefined;
      // Model failure: stopReason "error" (or "aborted" without an abort) must
      // NOT be misclassified as a completed assistant message.
      if (m.stopReason === "error") {
        return {
          case: "modelCallFailed",
          value: create(ModelCallFailedSchema, {
            error: create(ErrorDetailSchema, {
              message: m.errorMessage ?? "model call failed",
              retryability: Retryability.UNKNOWN,
            }),
          }),
        };
      }
      const blocks = m.content as Array<{ type: string; text?: string; id?: string; name?: string; arguments?: unknown }>;
      const text = blocks.filter((c) => c.type === "text").map((c) => c.text ?? "").join("");
      const toolCalls = blocks
        .filter((c) => c.type === "toolCall")
        .map((c) => create(ToolCallSchema, { id: c.id ?? "", name: c.name ?? "", argumentsJson: JSON.stringify(c.arguments ?? {}) }));
      const u = m.usage;
      return {
        case: "assistantMessage",
        value: create(AssistantMessageSchema, {
          text,
          toolCalls,
          usage: create(UsageSchema, {
            inputTokens: BigInt(u.input),
            outputTokens: BigInt(u.output),
            cacheReadTokens: BigInt(u.cacheRead),
            cacheWriteTokens: BigInt(u.cacheWrite),
            costUsd: u.cost.total,
          }),
          stopReason: m.stopReason,
          timestampMs: BigInt(m.timestamp),
        }),
      };
    }
    case "tool_execution_start":
      return {
        case: "toolCallStarted",
        value: create(ToolCallStartedSchema, {
          toolCallId: ev.toolCallId,
          toolName: ev.toolName,
          argumentsJson: JSON.stringify(ev.args ?? {}),
        }),
      };
    case "tool_execution_end": {
      const content = (ev.result?.content as Array<{ type: string; text?: string }> | undefined) ?? [];
      const resultText = content.filter((c) => c.type === "text").map((c) => c.text ?? "").join("");
      return {
        case: "toolResult",
        value: create(ToolResultSchema, {
          toolCallId: ev.toolCallId,
          toolName: ev.toolName,
          argumentsJson: "",
          resultText,
          isError: ev.isError,
          timestampMs: BigInt(Date.now()),
        }),
      };
    }
    case "auto_retry_start":
      return {
        case: "modelRetryScheduled",
        value: create(ModelRetryScheduledSchema, {
          attempt: ev.attempt,
          maxAttempts: ev.maxAttempts,
          delayMs: BigInt(ev.delayMs),
          errorMessage: ev.errorMessage,
        }),
      };
    case "auto_retry_end":
      return {
        case: "modelRetryFinished",
        value: create(ModelRetryFinishedSchema, { success: ev.success, attempt: ev.attempt, finalError: ev.finalError ?? "" }),
      };
    case "compaction_start":
      return { case: "compactionStarted", value: create(CompactionStartedSchema, { reason: ev.reason }) };
    case "compaction_end":
      return {
        case: "compactionFinished",
        value: create(CompactionFinishedSchema, {
          reason: ev.reason,
          aborted: ev.aborted,
          willRetry: ev.willRetry,
          errorMessage: ev.errorMessage ?? "",
        }),
      };
    default:
      return undefined;
  }
}

// Run only when this module is the entry point (not when imported by tests).
if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  main().catch((err) => {
    console.error(`child fatal: ${err instanceof Error ? err.message : err}`);
    emitResult(Outcome.FAILED);
    process.exit(1);
  });
}
