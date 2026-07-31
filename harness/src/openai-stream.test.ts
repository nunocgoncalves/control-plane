import { describe, it, expect } from "vitest";
import { OpenAIStreamAccumulator, buildOpenAIRequestBody } from "./openai-stream.js";

/** Feed a sequence of SSE `data:` payloads; collect all emitted events. */
function feedAll(acc: OpenAIStreamAccumulator, payloads: string[]): unknown[] {
  const out: unknown[] = [];
  for (const p of payloads) out.push(...acc.feed(p));
  return out;
}

describe("OpenAIStreamAccumulator", () => {
  it("accumulates text deltas and emits done on finish_reason stop", () => {
    const acc = new OpenAIStreamAccumulator();
    const events = feedAll(acc, [
      JSON.stringify({ choices: [{ delta: { content: "Hello" } }] }),
      JSON.stringify({ choices: [{ delta: { content: " world" } }] }),
      JSON.stringify({ choices: [{ delta: {}, finish_reason: "stop" }], usage: { prompt_tokens: 3, completion_tokens: 2 } }),
    ]);
    const types = events.map((e) => (e as { type: string }).type);
    expect(types).toContain("text_start");
    expect(types.filter((t) => t === "text_delta")).toHaveLength(2);
    expect(types).toContain("text_end");
    const done = acc.finish("ok");
    expect(done.type).toBe("done");
    expect(done.type === "done" && done.reason).toBe("stop");
    expect(done.type === "done" && done.message.usage.input).toBe(3);
    expect(done.type === "done" && done.message.usage.output).toBe(2);
  });

  it("accumulates tool_calls and maps stopReason to toolUse", () => {
    const acc = new OpenAIStreamAccumulator();
    const events = feedAll(acc, [
      JSON.stringify({ choices: [{ delta: { tool_calls: [{ index: 0, id: "call_1", type: "function", function: { name: "graph.read_mail", arguments: "" } }] } }] }),
      JSON.stringify({ choices: [{ delta: { tool_calls: [{ index: 0, function: { arguments: '{"limit":5}' } }] } }] }),
      JSON.stringify({ choices: [{ delta: {}, finish_reason: "tool_calls" }] }),
    ]);
    expect(events.some((e) => (e as { type: string }).type === "toolcall_start")).toBe(true);
    expect(events.some((e) => (e as { type: string }).type === "toolcall_end")).toBe(true);
    const done = acc.finish("ok");
    expect(done.type === "done" && done.reason).toBe("toolUse");
  });

  it("maps a modelEnd error status to an error terminal", () => {
    const acc = new OpenAIStreamAccumulator();
    feedAll(acc, [JSON.stringify({ choices: [{ delta: { content: "x" } }] })]);
    const ev = acc.finish("error", "upstream 500");
    expect(ev.type).toBe("error");
    expect(ev.type === "error" && ev.error.errorMessage).toBe("upstream 500");
  });

  it("maps an aborted status to an aborted error terminal", () => {
    const acc = new OpenAIStreamAccumulator();
    const ev = acc.finish("aborted", "aborted");
    expect(ev.type === "error" && ev.reason).toBe("aborted");
  });

  it("drops [DONE] and malformed payloads", () => {
    const acc = new OpenAIStreamAccumulator();
    expect(acc.feed("[DONE]")).toEqual([]);
    expect(acc.feed("not json")).toEqual([]);
  });
});

describe("buildOpenAIRequestBody", () => {
  it("builds a streaming OpenAI chat-completions body with system + user + tools", () => {
    const body = buildOpenAIRequestBody(
      "m1",
      {
        systemPrompt: "you are an agent",
        messages: [{ role: "user", content: "hi" }],
        tools: [{ name: "t", description: "d", parameters: { type: "object" } }],
      },
      { reasoning: "medium", maxTokens: 512 },
    ) as Record<string, unknown>;
    expect(body.model).toBe("m1");
    expect(body.stream).toBe(true);
    expect(body.stream_options).toEqual({ include_usage: true });
    expect(body.max_tokens).toBe(512);
    expect(body.reasoning_effort).toBe("medium");
    const messages = body.messages as { role: string; content?: string }[];
    expect(messages[0]).toEqual({ role: "system", content: "you are an agent" });
    expect(messages[1]).toEqual({ role: "user", content: "hi" });
    expect(body.tools).toEqual([{ type: "function", function: { name: "t", description: "d", parameters: { type: "object" } } }]);
  });
});
