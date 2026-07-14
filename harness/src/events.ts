// pi event -> proto Event mapping (HOR-351). Translates pi's
// AgentSessionEvent stream into the curated 5-kind Event oneof the
// control-plane records (HOR-249). v1 forwards only the semantic events;
// message_update deltas (live token streaming), queue_update, and compaction
// detail are deferred. agent_settled is NOT mapped here — the server builds
// the terminal Settled event (it owns the aborted/failed reason).

import { create } from "@bufbuild/protobuf";
import type { AgentSession, AgentSessionEvent } from "@earendil-works/pi-coding-agent";
import {
  AssistantMessageSchema,
  ErrorSchema,
  EventSchema,
  ToolCallSchema,
  ToolResultSchema,
  TurnStartedSchema,
  UsageSchema,
  type Event,
} from "./gen/iterabase/harness/v1/harness_pb.js";

interface TextLike {
  type: "text";
  text: string;
}
interface ToolCallLike {
  type: "toolCall";
  id: string;
  name: string;
  arguments: unknown;
}
interface ContentBlock {
  type: string;
  text?: string;
}

// Stateful: tracks tool-call args from tool_execution_start so the
// tool_execution_end Event can carry them (pi puts args on start, result on end).
export class EventMapper {
  private readonly args = new Map<string, unknown>();
  constructor(private readonly session: AgentSession) {}

  map(ev: AgentSessionEvent): Event | undefined {
    switch (ev.type) {
      case "turn_start":
        return create(EventSchema, {
          kind: {
            case: "turnStarted",
            value: create(TurnStartedSchema, {
              model: this.session.model?.id ?? "",
              thinkingLevel: this.session.thinkingLevel ?? "",
            }),
          },
        });

      case "message_end": {
        const m = ev.message;
        if (m.role !== "assistant") return undefined;
        const blocks = m.content as ContentBlock[];
        const text = blocks.filter((c): c is TextLike => c.type === "text").map((c) => c.text).join("");
        const toolCalls = blocks
          .filter((c): c is ToolCallLike => c.type === "toolCall")
          .map((c) =>
            create(ToolCallSchema, { id: c.id, name: c.name, argumentsJson: JSON.stringify(c.arguments) }),
          );
        const u = m.usage;
        return create(EventSchema, {
          kind: {
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
          },
        });
      }

      case "tool_execution_start":
        this.args.set(ev.toolCallId, ev.args);
        return undefined;

      case "tool_execution_end": {
        const content = (ev.result?.content as ContentBlock[] | undefined) ?? [];
        const resultText = content.filter((c): c is TextLike => c.type === "text").map((c) => c.text).join("");
        return create(EventSchema, {
          kind: {
            case: "toolResult",
            value: create(ToolResultSchema, {
              toolCallId: ev.toolCallId,
              toolName: ev.toolName,
              argumentsJson: JSON.stringify(this.args.get(ev.toolCallId) ?? {}),
              resultText,
              isError: ev.isError,
              timestampMs: BigInt(Date.now()),
            }),
          },
        });
      }

      // extension_error is not a subscribe event (extension errors surface via
      // the session's error listener, not the event stream) — not mapped in v1.

      case "auto_retry_end":
        return ev.success
          ? undefined
          : create(EventSchema, {
              kind: {
                case: "error",
                value: create(ErrorSchema, { source: "retry", message: ev.finalError ?? "retry failed" }),
              },
            });

      default:
        return undefined;
    }
  }
}
