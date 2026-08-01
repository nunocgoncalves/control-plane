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

  it("emits a `start` event before the first content event (pi agent-loop contract)", () => {
    const acc = new OpenAIStreamAccumulator("m1", "iterabase-inference");
    const events = feedAll(acc, [JSON.stringify({ choices: [{ delta: { content: "hi" } }] })]);
    const start = events.find((e) => (e as { type: string }).type === "start");
    expect(start).toBeDefined();
    expect((start as { partial: { model: string; provider: string } }).partial.model).toBe("m1");
    expect((start as { partial: { provider: string } }).partial.provider).toBe("iterabase-inference");
    // start must precede text_start.
    expect(events.map((e) => (e as { type: string }).type).indexOf("start")).toBeLessThan(
      events.map((e) => (e as { type: string }).type).indexOf("text_start"),
    );
  });

  it("preserves assigned model metadata on the terminal done/error message", () => {
    const acc = new OpenAIStreamAccumulator("m1", "iterabase-inference");
    feedAll(acc, [JSON.stringify({ choices: [{ delta: { content: "hi" } }] }), JSON.stringify({ choices: [{ delta: {}, finish_reason: "stop" }] })]);
    const done = acc.finish("ok");
    expect(done.type).toBe("done");
    if (done.type === "done") {
      expect(done.message.model).toBe("m1");
      expect(done.message.provider).toBe("iterabase-inference");
    }
    // Error terminal must also carry the assigned model (it replaces the
    // partial in pi's agent loop).
    const acc2 = new OpenAIStreamAccumulator("m2", "iterabase-inference");
    feedAll(acc2, [JSON.stringify({ choices: [{ delta: { content: "x" } }] })]);
    const err = acc2.finish("error", "upstream 500");
    expect(err.type).toBe("error");
    if (err.type === "error") {
      expect(err.error.model).toBe("m2");
      expect(err.error.provider).toBe("iterabase-inference");
    }
  });

  it("finishes as an error when the stream ends without a terminal signal (truncation)", () => {
    const acc = new OpenAIStreamAccumulator();
    feedAll(acc, [JSON.stringify({ choices: [{ delta: { content: "partial" } }] })]);
    // No finish_reason and no [DONE] — a truncated HTTP-200 stream.
    const ev = acc.finish("ok");
    expect(ev.type).toBe("error");
    expect(ev.type === "error" && ev.error.errorMessage).toMatch(/terminal/);
  });

  it("latches a malformed payload as a protocol error even if a later terminal arrives", () => {
    const acc = new OpenAIStreamAccumulator();
    feedAll(acc, [
      JSON.stringify({ choices: [{ delta: { content: "hi" } }] }),
      "not-json",
      JSON.stringify({ choices: [{ delta: {}, finish_reason: "stop" }] }),
    ]);
    const ev = acc.finish("ok");
    expect(ev.type).toBe("error");
    expect(ev.type === "error" && ev.error.errorMessage).toMatch(/malformed/);
  });

  it("latches a wrong-shaped chunk (non-string content) as a protocol error even if a later terminal arrives", () => {
    const acc = new OpenAIStreamAccumulator();
    feedAll(acc, [
      JSON.stringify({ choices: [{ delta: { content: "hi" } }] }),
      // Syntactically valid JSON, but `content` is a number, not a string.
      JSON.stringify({ choices: [{ delta: { content: 42 } }] }),
      JSON.stringify({ choices: [{ delta: {}, finish_reason: "stop" }] }),
    ]);
    const ev = acc.finish("ok");
    expect(ev.type).toBe("error");
    expect(ev.type === "error" && ev.error.errorMessage).toMatch(/content is not a string/i);
  });

  it("latches a wrong-shaped tool_call (missing/non-numeric index) as a protocol error", () => {
    const acc = new OpenAIStreamAccumulator();
    feedAll(acc, [
      JSON.stringify({ choices: [{ delta: { tool_calls: [{ id: "call_1", type: "function", function: { name: "t", arguments: "" } }] } }] }),
      JSON.stringify({ choices: [{ delta: {}, finish_reason: "tool_calls" }] }),
    ]);
    const ev = acc.finish("ok");
    expect(ev.type).toBe("error");
    expect(ev.type === "error" && ev.error.errorMessage).toMatch(/index/i);
  });

  it("accepts an empty/usage-only chunk without latching (legitimate keepalive)", () => {
    const acc = new OpenAIStreamAccumulator();
    const events = feedAll(acc, [
      JSON.stringify({ choices: [{ delta: { content: "hi" } }] }),
      JSON.stringify({}), // empty keepalive — valid, no events, no latch
      JSON.stringify({ usage: { prompt_tokens: 1, completion_tokens: 1 } }), // usage-only — valid
      JSON.stringify({ choices: [{ delta: {}, finish_reason: "stop" }] }),
    ]);
    // No protocol error; finish succeeds.
    const ev = acc.finish("ok");
    expect(ev.type).toBe("done");
    // The empty/usage-only chunks produced no content events beyond the one text delta.
    expect(events.filter((e) => (e as { type: string }).type === "text_delta")).toHaveLength(1);
  });

  it("accepts realistic include_usage chunks (usage:null on non-final, content:null, nullable delta fields)", () => {
    // Mirrors a real Chat Completions stream with stream_options.include_usage
    // = true: every non-final chunk carries `usage: null`, the first (role)
    // chunk and tool-call/usage frames carry `delta.content: null`, and only
    // the terminal chunk carries real usage. The validator must not latch a
    // protocol error on any of these (regression for the over-strict validator).
    const acc = new OpenAIStreamAccumulator("m1", "iterabase-inference");
    const events = feedAll(acc, [
      JSON.stringify({ choices: [{ delta: { role: "assistant", content: null }, finish_reason: null }], usage: null }),
      JSON.stringify({ choices: [{ delta: { content: "Hello" }, finish_reason: null }], usage: null }),
      JSON.stringify({ choices: [{ delta: { content: " world" }, finish_reason: null }], usage: null }),
      JSON.stringify({ choices: [{ delta: { content: null, reasoning_content: null }, finish_reason: null }], usage: null }),
      JSON.stringify({ choices: [{ delta: {}, finish_reason: "stop" }], usage: { prompt_tokens: 5, completion_tokens: 2, prompt_tokens_details: { cached_tokens: 1 } } }),
    ]);
    const ev = acc.finish("ok");
    expect(ev.type).toBe("done");
    // Two real text deltas accumulated; the null-content frames produced none.
    expect(events.filter((e) => (e as { type: string }).type === "text_delta")).toHaveLength(2);
    if (ev.type === "done") {
      expect(ev.message.usage.input).toBe(5);
      expect(ev.message.usage.output).toBe(2);
      expect(ev.message.usage.cacheRead).toBe(1);
    }
  });

  it("still rejects a wrong-typed (non-null) content as a protocol error", () => {
    // `null` is valid; a number is still a shape violation.
    const acc = new OpenAIStreamAccumulator();
    feedAll(acc, [
      JSON.stringify({ choices: [{ delta: { content: "hi" } }] }),
      JSON.stringify({ choices: [{ delta: { content: 42 } }] }),
      JSON.stringify({ choices: [{ delta: {}, finish_reason: "stop" }] }),
    ]);
    const ev = acc.finish("ok");
    expect(ev.type).toBe("error");
    expect(ev.type === "error" && ev.error.errorMessage).toMatch(/content is not a string/i);
  });

  it("still rejects a wrong-typed (non-null) usage as a protocol error", () => {
    // `usage: null` is valid; `usage: "x"` is still a shape violation.
    const acc = new OpenAIStreamAccumulator();
    feedAll(acc, [
      JSON.stringify({ choices: [{ delta: { content: "hi" } }] }),
      JSON.stringify({ choices: [{ delta: {}, finish_reason: "stop" }], usage: "not-an-object" }),
    ]);
    const ev = acc.finish("ok");
    expect(ev.type).toBe("error");
    expect(ev.type === "error" && ev.error.errorMessage).toMatch(/usage is not an object/i);
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

  it("preserves user image blocks as image_url data URIs (HOR-395 per-turn images)", () => {
    const body = buildOpenAIRequestBody(
      "m1",
      {
        messages: [
          {
            role: "user",
            content: [
              { type: "text", text: "what is this?" },
              { type: "image", data: "AAABBB==", mimeType: "image/png" },
            ],
          },
        ],
      },
      undefined,
    ) as Record<string, unknown>;
    const messages = body.messages as { role: string; content: unknown }[];
    const content = messages[0].content as { type: string; text?: string; image_url?: { url: string } }[];
    expect(Array.isArray(content)).toBe(true);
    expect(content[0]).toEqual({ type: "text", text: "what is this?" });
    expect(content[1]).toEqual({ type: "image_url", image_url: { url: "data:image/png;base64,AAABBB==" } });
  });

  it("preserves tool-result image blocks as a follow-up user message (pinned-provider semantics)", () => {
    const body = buildOpenAIRequestBody(
      "m1",
      {
        messages: [
          {
            role: "toolResult",
            toolCallId: "tc-1",
            toolName: "screenshot",
            content: [
              { type: "text", text: "captured" },
              { type: "image", data: "QkFTRTY0==", mimeType: "image/png" },
            ],
          },
        ],
      },
      undefined,
    ) as Record<string, unknown>;
    const messages = body.messages as { role: string; content: unknown; tool_call_id?: string }[];
    // The tool message carries the text.
    const toolMsg = messages.find((m) => m.role === "tool");
    expect(toolMsg).toBeDefined();
    expect(toolMsg?.tool_call_id).toBe("tc-1");
    expect(toolMsg?.content).toBe("captured");
    // A follow-up user message carries the image as an image_url data URI.
    const userMsg = messages.find((m) => m.role === "user");
    expect(userMsg).toBeDefined();
    const parts = userMsg?.content as { type: string; text?: string; image_url?: { url: string } }[];
    expect(parts[0]).toEqual({ type: "text", text: "Attached image(s) from tool result:" });
    expect(parts[1]).toEqual({ type: "image_url", image_url: { url: "data:image/png;base64,QkFTRTY0==" } });
  });

  it("uses an image placeholder when a tool result has only images", () => {
    const body = buildOpenAIRequestBody(
      "m1",
      {
        messages: [
          { role: "toolResult", toolCallId: "tc-1", toolName: "snap", content: [{ type: "image", data: "AA==", mimeType: "image/png" }] },
        ],
      },
      undefined,
    ) as Record<string, unknown>;
    const messages = body.messages as { role: string; content: unknown }[];
    const toolMsg = messages.find((m) => m.role === "tool");
    expect(toolMsg?.content).toBe("(see attached image)");
  });
});
