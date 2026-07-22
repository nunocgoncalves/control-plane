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

import { createInterface } from "node:readline";
import { writeSync } from "node:fs";
import { existsSync, readdirSync, mkdirSync } from "node:fs";
import { join } from "node:path";
import { toJson, create } from "@bufbuild/protobuf";
import {
  AuthStorage,
  DefaultResourceLoader,
  ModelRegistry,
  SessionManager,
  SettingsManager,
  createAgentSession,
  type AgentSession,
  type AgentSessionEvent,
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
    emitResult(Outcome.FAILED, "no assignment on stdin");
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
    void currentSession?.abort().catch(() => {});
  };
  const abortReader = new FrameReader((raw) => {
    const f = parseSupervisorFrame(raw);
    if (f?.type === "abort") onAbortFrame();
  });
  process.stdin.on("data", (chunk: Buffer) => abortReader.feed(chunk));

  let currentSession: AgentSession | undefined;
  try {
    currentSession = await createSession(assignment, sessionDir, cwd, egressProxyUrl, piDirs, maxAttempts);
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
  const unsub = currentSession.subscribe((ev: AgentSessionEvent) => {
    // Forward live token deltas (ephemeral, non-sequenced, non-ACKed).
    if (ev.type === "message_update") {
      const d = ev.assistantMessageEvent;
      if (d.type === "text_delta") emitTokenDelta(d.contentIndex, "TEXT", d.delta);
      else if (d.type === "thinking_delta") emitTokenDelta(d.contentIndex, "THINKING", d.delta);
      return;
    }
    const payload = mapEvent(ev, currentSession!);
    if (payload) emit(payload);
  });

  // SIGTERM -> abort pi (the supervisor's bounded-escalation abort path).
  const onTerm = () => {
    aborted = true;
    void currentSession?.abort().catch(() => {});
  };
  process.on("SIGTERM", onTerm);
  process.on("SIGINT", onTerm);

  try {
    const images = buildImages(assignment);
    await currentSession.prompt(assignment.message, images ? { images } : undefined);
    // agent_settled fires during prompt; flush + dispose before reporting.
    await currentSession.dispose();
    unsub();
    clearInterval(hb);
    emitHeartbeat();
    emitResult(aborted ? Outcome.ABORTED : Outcome.COMPLETED);
  } catch (err) {
    unsub();
    clearInterval(hb);
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

/** Read the first framed `assignment` from fd 0 (one-shot). */
function readAssignment(): Promise<Assignment | undefined> {
  return new Promise((resolve) => {
    const reader = new FrameReader((raw) => {
      const f = parseSupervisorFrame(raw);
      if (f?.type === "assignment") {
        resolve((f.assignment as { assignment: Assignment }).assignment);
      }
    });
    const stdin = process.stdin;
    const onData = (chunk: Buffer) => reader.feed(chunk);
    stdin.on("data", onData);
    stdin.on("end", () => resolve(undefined));
    // readline keeps the event loop alive; close it once we have the assignment.
    const rl = createInterface({ input: stdin, crlfDelay: Infinity });
    rl.on("line", () => {}); // ensure stdin flows
    setTimeout(() => {}, 0); // noop
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

/** Create the pi session: resume-or-create by the EXACT session.id; pi cwd = validated working dir. */
async function createSession(
  a: Assignment,
  sessionDir: string,
  cwd: string,
  egressProxyUrl: string,
  piDirs: string[],
  maxAttempts: number,
): Promise<AgentSession> {
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

  const resourceLoader = new DefaultResourceLoader({
    cwd,
    agentDir: sessionDir, // scope discovery to the session (no global ~/.pi)
    settingsManager,
    additionalExtensionPaths: piDirs,
    extensionFactories: [providerFactory],
    systemPromptOverride: () => a.persona,
  });
  await resourceLoader.reload();

  const sessionManager = resolveSessionManager(a.sessionId, sessionDir, cwd);

  const toolOpts = a.toolAllowList.all ? { noTools: "builtin" as const } : { tools: a.toolAllowList.tools };
  const { session } = await createAgentSession({
    cwd,
    authStorage,
    modelRegistry,
    resourceLoader,
    settingsManager,
    sessionManager,
    ...toolOpts,
  });

  const model = modelRegistry.find(PROVIDER, a.model.id);
  if (!model) throw new Error(`model not found after provider registration: ${a.model.id}`);
  await session.setModel(model);
  return session;
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

main().catch((err) => {
  console.error(`child fatal: ${err instanceof Error ? err.message : err}`);
  emitResult(Outcome.FAILED);
  process.exit(1);
});
