import { describe, it, expect } from "vitest";
import { create } from "@bufbuild/protobuf";
import { EventMapper } from "./events.js";
import { EventSchema, SettledSchema, Settled_Reason } from "./gen/iterabase/harness/v1/harness_pb.js";
import type { AgentSession, AgentSessionEvent } from "@earendil-works/pi-coding-agent";

function mockSession(over: Partial<AgentSession> = {}): AgentSession {
  return {
    model: { id: "llama-3.1-70b" },
    thinkingLevel: "medium",
    messages: [],
    ...over,
  } as unknown as AgentSession;
}

describe("EventMapper", () => {
  it("maps turn_start to TurnStarted (model + thinkingLevel from the session)", () => {
    const m = new EventMapper(mockSession());
    const e = m.map({ type: "turn_start" } as AgentSessionEvent);
    expect(e?.kind.case).toBe("turnStarted");
    if (e?.kind.case === "turnStarted") {
      expect(e.kind.value.model).toBe("llama-3.1-70b");
      expect(e.kind.value.thinkingLevel).toBe("medium");
    }
  });

  it("maps an assistant message_end to AssistantMessage with text + usage", () => {
    const m = new EventMapper(mockSession());
    const e = m.map({
      type: "message_end",
      message: {
        role: "assistant",
        content: [
          { type: "text", text: "Hello " },
          { type: "text", text: "world" },
          { type: "toolCall", id: "c1", name: "graph_read", arguments: { q: "x" } },
        ],
        usage: { input: 10, output: 5, cacheRead: 1, cacheWrite: 0, cost: { total: 0.002 } },
        stopReason: "toolUse",
        timestamp: 1700000000,
      },
    } as AgentSessionEvent);
    expect(e?.kind.case).toBe("assistantMessage");
    if (e?.kind.case === "assistantMessage") {
      expect(e.kind.value.text).toBe("Hello world");
      expect(e.kind.value.toolCalls).toHaveLength(1);
      expect(e.kind.value.toolCalls[0]?.name).toBe("graph_read");
      expect(e.kind.value.toolCalls[0]?.argumentsJson).toBe(JSON.stringify({ q: "x" }));
      expect(e.kind.value.usage?.inputTokens).toBe(10n);
      expect(e.kind.value.usage?.costUsd).toBe(0.002);
      expect(e.kind.value.stopReason).toBe("toolUse");
      expect(e.kind.value.timestampMs).toBe(1700000000n);
    }
  });

  it("ignores non-assistant message_end", () => {
    const m = new EventMapper(mockSession());
    const e = m.map({
      type: "message_end",
      message: { role: "user", content: "hi", timestamp: 1 },
    } as AgentSessionEvent);
    expect(e).toBeUndefined();
  });

  it("pairs tool_execution_start args with tool_execution_end result", () => {
    const m = new EventMapper(mockSession());
    m.map({ type: "tool_execution_start", toolCallId: "c1", toolName: "graph_read", args: { q: "x" } } as AgentSessionEvent);
    const e = m.map({
      type: "tool_execution_end",
      toolCallId: "c1",
      toolName: "graph_read",
      result: { content: [{ type: "text", text: "row 1" }] },
      isError: false,
    } as AgentSessionEvent);
    expect(e?.kind.case).toBe("toolResult");
    if (e?.kind.case === "toolResult") {
      expect(e.kind.value.toolCallId).toBe("c1");
      expect(e.kind.value.argumentsJson).toBe(JSON.stringify({ q: "x" }));
      expect(e.kind.value.resultText).toBe("row 1");
      expect(e.kind.value.isError).toBe(false);
    }
  });

  it("maps a failed auto_retry_end to an Error", () => {
    const m = new EventMapper(mockSession());
    const e = m.map({ type: "auto_retry_end", success: false, attempt: 3, finalError: "overloaded" } as AgentSessionEvent);
    expect(e?.kind.case).toBe("error");
    if (e?.kind.case === "error") {
      expect(e.kind.value.source).toBe("retry");
      expect(e.kind.value.message).toBe("overloaded");
    }
  });

  it("drops a successful auto_retry_end", () => {
    const m = new EventMapper(mockSession());
    expect(m.map({ type: "auto_retry_end", success: true, attempt: 2 } as AgentSessionEvent)).toBeUndefined();
  });

  it("agent_settled is not mapped by the mapper (the server builds Settled)", () => {
    const m = new EventMapper(mockSession({ messages: [{}, {}] } as unknown as AgentSession));
    expect(m.map({ type: "agent_settled" } as AgentSessionEvent)).toBeUndefined();
    // sanity: the server's buildSettled shape
    const settled = create(EventSchema, {
      kind: { case: "settled", value: create(SettledSchema, { reason: Settled_Reason.COMPLETED, messageCount: 2 }) },
    });
    expect(settled.kind.case).toBe("settled");
  });
});
