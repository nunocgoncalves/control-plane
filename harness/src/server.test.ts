import { describe, it, expect } from "vitest";
import { createClient, createRouterTransport } from "@connectrpc/connect";
import type { AgentSession, AgentSessionEvent } from "@earendil-works/pi-coding-agent";
import { Harness } from "./gen/iterabase/harness/v1/harness_pb.js";
import { Settled_Reason } from "./gen/iterabase/harness/v1/harness_pb.js";
import { createHarnessService } from "./server.js";

// A fake AgentSession that emits a scripted event stream. Hermetic — no pi
// model, no HTTP, no mTLS. Tests the Connect contract (streaming + Settled
// reason) of createHarnessService, not the model wiring.
function fakeSession(opts: { waitForAbort?: boolean } = {}): AgentSession {
  const listeners = new Set<(ev: AgentSessionEvent) => void>();
  let abortFn: (() => void) | null = null;
  const session = {
    model: { id: "mock-model" },
    thinkingLevel: "medium",
    messages: [] as unknown[],
    subscribe(fn: (ev: AgentSessionEvent) => void) {
      listeners.add(fn);
      return () => listeners.delete(fn);
    },
    async prompt(_text: string) {
      const emit = (ev: AgentSessionEvent) => {
        for (const l of listeners) l(ev);
      };
      emit({ type: "turn_start" } as AgentSessionEvent);
      emit({
        type: "message_end",
        message: {
          role: "assistant",
          content: [{ type: "text", text: "hi" }],
          usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, cost: { total: 0 } },
          stopReason: "stop",
          timestamp: 1,
        },
      } as AgentSessionEvent);
      if (opts.waitForAbort) {
        await new Promise<void>((r) => {
          abortFn = r;
        });
      }
      emit({ type: "agent_settled" } as AgentSessionEvent);
      session.messages.push({});
    },
    async abort() {
      abortFn?.();
    },
  };
  return session as unknown as AgentSession;
}

describe("Harness service (Connect contract)", () => {
  it("streams turnStarted -> assistantMessage -> settled{completed}", async () => {
    const transport = createRouterTransport((r) => r.service(Harness, createHarnessService(fakeSession())));
    const client = createClient(Harness, transport);
    const events = [];
    for await (const e of await client.prompt({ message: "hi" })) events.push(e);

    expect(events.map((e) => e.kind.case)).toEqual(["turnStarted", "assistantMessage", "settled"]);
    const last = events.at(-1);
    expect(last?.kind.case).toBe("settled");
    if (last?.kind.case === "settled") expect(last.kind.value.reason).toBe(Settled_Reason.COMPLETED);
  });

  it("Abort mid-turn -> settled{aborted}", async () => {
    const transport = createRouterTransport((r) =>
      r.service(Harness, createHarnessService(fakeSession({ waitForAbort: true }))),
    );
    const client = createClient(Harness, transport);
    const events = [];

    const done = (async () => {
      for await (const e of await client.prompt({ message: "hi" })) events.push(e);
    })();
    // Let the stream start + drain pre-abort events, then abort.
    await new Promise((r) => setImmediate(r));
    await client.abort({});
    await done;

    expect(events.map((e) => e.kind.case)).toEqual(["turnStarted", "assistantMessage", "settled"]);
    const last = events.at(-1);
    expect(last?.kind.case).toBe("settled");
    if (last?.kind.case === "settled") expect(last.kind.value.reason).toBe(Settled_Reason.ABORTED);
  });
});
