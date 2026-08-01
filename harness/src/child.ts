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

import { writeSync } from "node:fs";
import { existsSync, readdirSync, mkdirSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { createReadStream } from "node:fs";
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
import { FrameReader, encodeFrame, parseSupervisorFrame, type GatewayToolDescriptor } from "./ipc.js";
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
  persona: string;
  model: { id: string; api: string; contextWindow: number; maxOutputTokens?: number; thinkingLevel?: string };
  workspaceTools: boolean;
  message: string;
  images?: AssignmentImage[];
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

  let currentRuntime: AgentSessionRuntime | undefined;
  try {
    // Await the supervisor's gateway-tool discovery before building the session
    // (ARCH-006): the child registers pi tool stubs from the non-secret
    // descriptors. An empty list is valid (workspace-only turn).
    const descriptors = await rpc.awaitGatewayTools();
    currentRuntime = await createSession(assignment, sessionDir, cwd, piDirs, maxAttempts, rpc, descriptors);
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
    await session.prompt(assignment.message, images ? { images } : undefined);
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
    message: r.message,
  };
  if (typeof r.runId === "string") a.runId = r.runId;
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
function gatewayToolStub(rpc: ChildRpc): (d: GatewayToolDescriptor) => ToolDefinition {
  return (d) =>
    defineTool({
      name: d.name,
      label: d.name,
      description: d.description,
      parameters: Type.Unsafe(d.inputSchema as never),
      execute: async (toolCallId, params, signal) => {
        const res = await rpc.invokeTool(
          {
            toolCallId,
            toolName: d.name,
            toolVersionDigest: d.digest,
            argumentsJson: JSON.stringify(params),
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
        return {
          content: [{ type: "text", text: res.resultJson ?? "" }],
          details: {},
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
): Promise<AgentSessionRuntime> {
  if (!SESSION_ID_RE.test(a.sessionId)) throw new Error(`invalid session id: ${JSON.stringify(a.sessionId)}`);

  const authStorage = AuthStorage.create(join(sessionDir, "auth.json"));
  const modelRegistry = ModelRegistry.create(authStorage);

  const providerFactory: ExtensionFactory = (pi) => {
    const provider: ProviderConfig = {
      // No baseUrl/apiKey/models — the child has no endpoint or credential.
      // Model traffic crosses the streamSimple bridge → supervisor → inference
      // gateway (mTLS). ARCH-010/011. pi's validateProviderConfig requires
      // baseUrl+apiKey whenever `models` is supplied, so the model metadata is
      // supplied directly to createAgentSessionFromServices instead (a
      // credentialless registration: streamSimple intercepts every call for
      // this `api`, so no endpoint/auth is ever consulted).
      api: a.model.api as unknown as ProviderConfig["api"],
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
  const toolOpts = a.workspaceTools
    ? { tools: [...gatewayToolNames, "read", "write", "edit", "bash"] as string[] }
    : { tools: gatewayToolNames.length ? gatewayToolNames : [] };
  const customTools = descriptors.map(gatewayToolStub(rpc));

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
