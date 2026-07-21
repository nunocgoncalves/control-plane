// The per-turn pi child entry (HOR-381). Run via the setpriv launcher under the
// session UID/GID. Reads the assignment from stdin (one JSON line), creates a
// fresh pi AgentSession (resume-or-create by session.id from the PVC), runs one
// turn, maps pi lifecycle events -> durable TurnEvent payloads on stdout (JSON
// lines), and writes a final result. SIGTERM aborts pi. The supervisor
// sequences + WAL's the payloads; this process holds no state between turns.
//
// Token deltas (pi message_update) are deferred — they need a separate ephemeral
// IPC channel; this maps the durable event set first.

import { createInterface } from "node:readline";
import { toJson, create } from "@bufbuild/protobuf";
import {
  AuthStorage,
  DefaultResourceLoader,
  ModelRegistry,
  SessionManager,
  createAgentSession,
  type AgentSession,
  type AgentSessionEvent,
  type ExtensionFactory,
  type ProviderConfig,
} from "@earendil-works/pi-coding-agent";
import { existsSync, readdirSync, mkdirSync } from "node:fs";
import { join } from "node:path";
import {
  AssistantMessageSchema,
  CompactionFinishedSchema,
  CompactionStartedSchema,
  HarnessErrorSchema,
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

const PROVIDER = "iterabase-inference";

interface Assignment {
  turnId: string;
  sessionId: string;
  persona: string;
  model: { id: string; api: string; contextWindow: number; maxOutputTokens?: number; thinkingLevel?: string };
  toolAllowList: { all: boolean; tools: string[] };
  message: string;
}

function emit(payload: TurnEvent["kind"]): void {
  const te = create(TurnEventSchema, { turnId: "", sequence: 0n, timestampMs: 0n, kind: payload });
  process.stdout.write(`${JSON.stringify({ type: "event", event: toJson(TurnEventSchema, te) })}\n`);
}

function emitResult(outcome: Outcome, message?: string): void {
  process.stdout.write(`${JSON.stringify({ type: "result", outcome, message })}\n`);
}

async function main(): Promise<void> {
  const rl = createInterface({ input: process.stdin });
  const assignment: Assignment | undefined = await new Promise((resolve) => {
    rl.once("line", (line) => {
      try {
        resolve((JSON.parse(line) as { assignment: Assignment }).assignment);
      } catch {
        resolve(undefined);
      }
      rl.close();
    });
  });
  if (!assignment) {
    emitResult(Outcome.FAILED, "no assignment on stdin");
    return;
  }

  const sessionDir = process.env.HARNESS_SESSION_DIR ?? "/data/session";
  const egressProxyUrl = process.env.HARNESS_EGRESS_PROXY_URL ?? "";
  const piDirs = (process.env.HARNESS_PI_DIRS ?? "").split(":").filter(Boolean);
  mkdirSync(sessionDir, { recursive: true });

  let session: AgentSession | undefined;
  try {
    session = await createSession(assignment, sessionDir, egressProxyUrl, piDirs);
  } catch (err) {
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

  // Map pi events -> durable TurnEvent payloads.
  let settled = false;
  const unsub = session.subscribe((ev: AgentSessionEvent) => {
    const payload = mapEvent(ev, session!);
    if (payload) emit(payload);
    if (ev.type === "agent_settled") settled = true;
  });

  // SIGTERM -> abort pi (the supervisor's abort path).
  let aborted = false;
  const onTerm = () => {
    aborted = true;
    void session?.abort().catch(() => {});
  };
  process.on("SIGTERM", onTerm);
  process.on("SIGINT", onTerm);

  try {
    await session.prompt(assignment.message);
    // agent_settled fires during prompt; flush + dispose before reporting.
    await session.dispose();
    unsub();
    emitResult(aborted ? Outcome.ABORTED : Outcome.COMPLETED);
  } catch (err) {
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

/** Create the pi session: resume-or-create by session.id, model via the egress proxy, persona override, tool allowlist. */
async function createSession(
  a: Assignment,
  sessionDir: string,
  egressProxyUrl: string,
  piDirs: string[],
): Promise<AgentSession> {
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

  const resourceLoader = new DefaultResourceLoader({
    cwd: sessionDir,
    agentDir: sessionDir, // scope discovery to the session (no global ~/.pi)
    additionalExtensionPaths: piDirs,
    extensionFactories: [providerFactory],
    systemPromptOverride: () => a.persona,
  });
  await resourceLoader.reload();

  const existing = findSessionFile(sessionDir);
  const sessionManager = existing
    ? SessionManager.open(existing, sessionDir, sessionDir)
    : SessionManager.create(sessionDir, sessionDir);

  const toolOpts = a.toolAllowList.all ? { noTools: "builtin" as const } : { tools: a.toolAllowList.tools };
  const { session } = await createAgentSession({
    cwd: sessionDir,
    authStorage,
    modelRegistry,
    resourceLoader,
    sessionManager,
    ...toolOpts,
  });

  const model = modelRegistry.find(PROVIDER, a.model.id);
  if (!model) throw new Error(`model not found after provider registration: ${a.model.id}`);
  await session.setModel(model);
  return session;
}

function findSessionFile(sessionDir: string): string | undefined {
  if (!existsSync(sessionDir)) return undefined;
  const files = readdirSync(sessionDir).filter((f) => f.endsWith(".jsonl"));
  if (files.length === 0) return undefined;
  return join(sessionDir, files.sort().at(-1)!);
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
