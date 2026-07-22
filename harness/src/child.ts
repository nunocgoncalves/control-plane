// The per-turn pi child entry (HOR-381). Run via the setpriv launcher under the
// session UID/GID. Reads a framed `assignment` from fd 0, creates a fresh pi
// AgentSession (resume-or-create by the EXACT assignment session.id from the
// PVC session dir — never auto-detect), runs one turn, maps pi lifecycle
// events → durable TurnEvent payloads + ephemeral TokenDeltas over the framed
// fd-3 channel, emits a heartbeat for liveness, and writes a final `result`.
// A framed `abort` on fd 0 aborts pi. The supervisor sequences + WAL's the
// durable payloads; this process holds no state between turns.
//
// Provider-SDK retries are disabled (settingsManager retry.provider.maxRetries
// = 0) so there is exactly one observable retry layer — pi's own bounded
// auto-retry (retry.maxRetries = HARNESS_MODEL_MAX_ATTEMPTS).

import { writeSync } from "node:fs";
import { existsSync, readdirSync, mkdirSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { toJson, create } from "@bufbuild/protobuf";
import {
  AuthStorage,
  ModelRegistry,
  SessionManager,
  SettingsManager,
  createAgentSessionFromServices,
  createAgentSessionServices,
  createAgentSessionRuntime,
  AgentSessionRuntime,
  type AgentSession,
  type AgentSessionEvent,
  type AgentSessionServices,
  type CreateAgentSessionRuntimeResult,
  type ExtensionFactory,
  type ProviderConfig,
} from "@earendil-works/pi-coding-agent";
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
import { FrameReader, encodeFrame, parseSupervisorFrame } from "./ipc.js";

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
  persona: string;
  model: { id: string; api: string; contextWindow: number; maxOutputTokens?: number; thinkingLevel?: string };
  toolAllowList: { all: boolean; tools: string[] };
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

async function main(): Promise<void> {
  const assignment = await readAssignment();
  if (!assignment) {
    emitResult(Outcome.FAILED, "no valid assignment on stdin");
    return;
  }

  const sessionDir = process.env.HARNESS_SESSION_DIR ?? "/data/session";
  const cwd = process.env.HARNESS_WORKING_DIR ?? sessionDir;
  const egressProxyUrl = process.env.HARNESS_EGRESS_PROXY_URL ?? "";
  const piDirs = (process.env.HARNESS_PI_DIRS ?? "").split(":").filter(Boolean);
  const maxAttempts = Number(process.env.HARNESS_MODEL_MAX_ATTEMPTS ?? "3") || 3;
  const livenessMs = Number(process.env.HARNESS_LIVENESS_INTERVAL_MS ?? "5000") || 5000;
  mkdirSync(sessionDir, { recursive: true });

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
    currentRuntime = await createSession(assignment, sessionDir, cwd, egressProxyUrl, piDirs, maxAttempts);
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
    // AgentSessionRuntime.dispose() awaits those handlers; a teardown
    // failure must turn the outcome into FAILED, not COMPLETED.
    try {
      await currentRuntime.dispose();
    } catch (disposeErr) {
      unsub();
      clearInterval(hb);
      emit({
        case: "harnessError",
        value: create(HarnessErrorSchema, {
          error: create(ErrorDetailSchema, {
            message: `session shutdown failed: ${disposeErr instanceof Error ? disposeErr.message : String(disposeErr)}`,
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
  if (!r.toolAllowList || typeof r.toolAllowList !== "object") return undefined;
  const t = r.toolAllowList as Record<string, unknown>;
  if (typeof t.all !== "boolean" || !Array.isArray(t.tools) || t.tools.some((x) => typeof x !== "string")) return undefined;
  const a: Assignment = {
    turnId: r.turnId,
    sessionId: r.sessionId,
    persona: r.persona,
    model: { id: m.id, api: m.api, contextWindow: m.contextWindow },
    toolAllowList: { all: t.all, tools: t.tools as string[] },
    message: r.message,
  };
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
 * validated working dir. Returns an owned `AgentSessionRuntime` so the caller
 * can run the async `session_shutdown` lifecycle via `dispose()` before exit.
 */
async function createSession(
  a: Assignment,
  sessionDir: string,
  cwd: string,
  egressProxyUrl: string,
  piDirs: string[],
  maxAttempts: number,
): Promise<AgentSessionRuntime> {
  if (!SESSION_ID_RE.test(a.sessionId)) throw new Error(`invalid session id: ${JSON.stringify(a.sessionId)}`);

  const authStorage = AuthStorage.create(join(sessionDir, "auth.json"));
  const modelRegistry = ModelRegistry.create(authStorage);

  const providerFactory: ExtensionFactory = (pi) => {
    const provider: ProviderConfig = {
      baseUrl: egressProxyUrl,
      apiKey: "placeholder",
      api: a.model.api as unknown as ProviderConfig["api"],
      models: [
        {
          id: a.model.id,
          name: a.model.id,
          reasoning: false,
          input: ["text"],
          cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
          contextWindow: a.model.contextWindow,
          maxTokens: a.model.maxOutputTokens ?? 4096,
        },
      ],
    };
    pi.registerProvider(PROVIDER, provider);
  };

  // Deterministic runtime settings: provider-SDK retries disabled (one retry
  // layer — pi's own bounded auto-retry at maxAttempts).
  const settingsManager = SettingsManager.inMemory({
    retry: { enabled: true, maxRetries: maxAttempts, baseDelayMs: 1000, provider: { maxRetries: 0 } },
  });

  const toolOpts = a.toolAllowList.all ? { noTools: "builtin" as const } : { tools: a.toolAllowList.tools };

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

    const model = services.modelRegistry.find(PROVIDER, a.model.id);
    if (!model) throw new Error(`model not found after provider registration: ${a.model.id}`);

    const result = await createAgentSessionFromServices({
      services,
      sessionManager,
      model,
      ...toolOpts,
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
