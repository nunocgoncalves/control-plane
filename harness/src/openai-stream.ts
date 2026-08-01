// OpenAI chat-completions SSE → pi AssistantMessageEvent conversion (HOR-395,
// ARCH-010/011). The supervisor stays transport-oriented: it forwards raw
// OpenAI SSE `data:` payloads (one JSON object string per `modelChunk` frame)
// and an authoritative `modelEnd` terminal. The child's custom `streamSimple`
// provider feeds those payloads here to emit the pi stream events.
//
// This owns model semantics on the child side (delta accumulation, stopReason
// mapping, usage) so the supervisor does not. It mirrors the event ordering
// pi-ai's own providers use: `start` → `*_start`/`*_delta`/`*_end` per content
// block → terminal `done`/`error`, with a running `partial` AssistantMessage.
// Bodies are never logged (ARCH-011).

import type {
  AssistantMessage,
  AssistantMessageEvent,
  Message,
  StopReason,
  ToolCall,
  Usage,
} from "@earendil-works/pi-ai";

interface OpenAIUsage {
  prompt_tokens?: number;
  completion_tokens?: number;
  prompt_tokens_details?: { cached_tokens?: number };
}
interface OpenAIToolCallDelta {
  index: number;
  id?: string;
  type?: string;
  function?: { name?: string; arguments?: string };
}
interface OpenAIChoice {
  delta?: {
    content?: string;
    reasoning_content?: string;
    tool_calls?: OpenAIToolCallDelta[];
  };
  finish_reason?: string | null;
}
interface OpenAIChunk {
  choices?: OpenAIChoice[];
  usage?: OpenAIUsage;
}

interface ToolCallSlot {
  contentIndex: number;
  id: string;
  name: string;
  args: string;
  started: boolean;
  ended: boolean;
}

function emptyUsage(): Usage {
  return { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } };
}
function usageFrom(u: OpenAIUsage | undefined): Usage {
  if (!u) return emptyUsage();
  return {
    input: u.prompt_tokens ?? 0,
    output: u.completion_tokens ?? 0,
    cacheRead: u.prompt_tokens_details?.cached_tokens ?? 0,
    cacheWrite: 0,
    totalTokens: (u.prompt_tokens ?? 0) + (u.completion_tokens ?? 0),
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
  };
}

/**
 * Accumulator state for one streamed assistant message. Feed SSE `data:`
 * payloads; collect AssistantMessageEvents; call `finish()` for the terminal
 * event. The running `partial` mirrors pi-ai's provider pattern.
 */
export class OpenAIStreamAccumulator {
  private readonly modelId: string;
  private readonly provider: string;
  private content: AssistantMessage["content"] = [];
  private textIndex = -1;
  private textBuf = "";
  private thinkingIndex = -1;
  private thinkingBuf = "";
  private toolCalls: Map<number, ToolCallSlot> = new Map();
  private nextContentIndex = 0;
  private usage: Usage = emptyUsage();
  private stopReason: StopReason = "stop";
  /** Emitted the required `start` event yet? pi's agent loop keeps
   * partialMessage=null until `start` and otherwise drops every
   * text_delta/thinking_delta/tool-call update (see AssistantMessageEvent docs). */
  private started = false;
  /** Saw a valid OpenAI terminal (finish_reason or `[DONE]`)? A truncated or
   * malformed HTTP-200 stream never reaches a terminal; without this guard it
   * would be reported as a successful `done` (HOR-395: upstream behavior must
   * not corrupt protocol channels). */
  private sawTerminal = false;
  /** Latched on a malformed/invalid SSE payload. HOR-395 requires upstream
   * corruption not be silently swallowed: once latched, `finish("ok")` fails
   * even if a later valid terminal arrives (a corrupt stream is a model
   * failure, not success). */
  private protocolError: string | null = null;

  constructor(modelId = "", provider = "iterabase-inference") {
    this.modelId = modelId;
    this.provider = provider;
  }

  /** The running partial message (passed with each streamed event). */
  private partial(): AssistantMessage {
    return {
      role: "assistant",
      content: this.content,
      api: "openai-completions" as never,
      provider: this.provider as never,
      model: this.modelId,
      usage: this.usage,
      stopReason: this.stopReason,
      timestamp: Date.now(),
    };
  }

  /** Emit the `start` event once, before any content event. */
  private begin(): AssistantMessageEvent[] {
    if (this.started) return [];
    this.started = true;
    return [{ type: "start", partial: this.partial() }];
  }

  /**
   * Feed one SSE `data:` payload (a JSON object string, or "[DONE]"). Returns
   * the pi events to push for this payload (possibly empty). "[DONE]" produces
   * no event — the terminal `done`/`error` is emitted by `finish()`.
   */
  feed(data: string): AssistantMessageEvent[] {
    if (data === "[DONE]") {
      this.sawTerminal = true;
      return [];
    }
    let chunk: OpenAIChunk;
    try {
      chunk = JSON.parse(data) as OpenAIChunk;
    } catch {
      // Malformed SSE payload — latch a protocol error. A later valid terminal
      // must NOT turn a corrupt stream into a successful `done` (HOR-395).
      this.protocolError = "malformed OpenAI SSE payload";
      return [];
    }
    const events: AssistantMessageEvent[] = [];
    const choice = chunk.choices?.[0];
    const delta = choice?.delta;
    // Any processable payload means the stream has started — emit `start`
    // before the first content event (required by pi's agent loop).
    if (delta?.content || delta?.reasoning_content || delta?.tool_calls || choice?.finish_reason) {
      events.push(...this.begin());
    }

    if (delta?.content) {
      if (this.textIndex < 0) {
        this.textIndex = this.nextContentIndex++;
        this.content.push({ type: "text", text: "" });
        events.push({ type: "text_start", contentIndex: this.textIndex, partial: this.partial() });
      }
      this.textBuf += delta.content;
      (this.content[this.textIndex] as { text: string }).text = this.textBuf;
      events.push({ type: "text_delta", contentIndex: this.textIndex, delta: delta.content, partial: this.partial() });
    }
    if (delta?.reasoning_content) {
      if (this.thinkingIndex < 0) {
        this.thinkingIndex = this.nextContentIndex++;
        this.content.push({ type: "thinking", thinking: "" });
        events.push({ type: "thinking_start", contentIndex: this.thinkingIndex, partial: this.partial() });
      }
      this.thinkingBuf += delta.reasoning_content;
      (this.content[this.thinkingIndex] as { thinking: string }).thinking = this.thinkingBuf;
      events.push({ type: "thinking_delta", contentIndex: this.thinkingIndex, delta: delta.reasoning_content, partial: this.partial() });
    }
    if (delta?.tool_calls) {
      for (const tc of delta.tool_calls) {
        let slot = this.toolCalls.get(tc.index);
        if (!slot) {
          slot = { contentIndex: this.nextContentIndex++, id: "", name: "", args: "", started: false, ended: false };
          this.toolCalls.set(tc.index, slot);
          this.content.push({ type: "toolCall", id: "", name: "", arguments: {} });
        }
        if (tc.id || tc.function?.name) {
          slot.id = tc.id ?? slot.id;
          slot.name = tc.function?.name ?? slot.name;
        }
        if (!slot.started) {
          slot.started = true;
          events.push({ type: "toolcall_start", contentIndex: slot.contentIndex, partial: this.partial() });
        }
        if (tc.function?.arguments) {
          slot.args += tc.function.arguments;
          events.push({ type: "toolcall_delta", contentIndex: slot.contentIndex, delta: tc.function.arguments, partial: this.partial() });
        }
      }
    }
    if (choice?.finish_reason) {
      this.sawTerminal = true;
      this.stopReason = mapStop(choice.finish_reason);
      // Close open content blocks.
      if (this.textIndex >= 0) events.push({ type: "text_end", contentIndex: this.textIndex, content: this.textBuf, partial: this.partial() });
      if (this.thinkingIndex >= 0) events.push({ type: "thinking_end", contentIndex: this.thinkingIndex, content: this.thinkingBuf, partial: this.partial() });
      for (const slot of this.toolCalls.values()) {
        if (slot.started && !slot.ended) {
          slot.ended = true;
          const toolCall: ToolCall = { type: "toolCall", id: slot.id, name: slot.name, arguments: safeParseArgs(slot.args) };
          (this.content[slot.contentIndex] as ToolCall).id = toolCall.id;
          (this.content[slot.contentIndex] as ToolCall).name = toolCall.name;
          (this.content[slot.contentIndex] as ToolCall).arguments = toolCall.arguments;
          events.push({ type: "toolcall_end", contentIndex: slot.contentIndex, toolCall, partial: this.partial() });
        }
      }
    }
    if (chunk.usage) this.usage = usageFrom(chunk.usage);
    return events;
  }

  /** Build the terminal `done`/`error` event from accumulated state + modelEnd status. */
  finish(status: "ok" | "error" | "aborted", errorMessage?: string): Extract<AssistantMessageEvent, { type: "done" } | { type: "error" }> {
    if (status === "aborted") {
      const msg = this.message("aborted", errorMessage);
      return { type: "error", reason: "aborted", error: msg };
    }
    if (status === "error") {
      const msg = this.message("error", errorMessage);
      return { type: "error", reason: "error", error: msg };
    }
    // A latched protocol error (malformed payload) is a model failure even if a
    // later valid terminal arrived — upstream corruption must not be reported
    // as success (HOR-395).
    if (this.protocolError) {
      const msg = this.message("error", errorMessage ?? this.protocolError);
      return { type: "error", reason: "error", error: msg };
    }
    // A successful terminal requires a valid OpenAI finish_reason/[DONE]. A
    // truncated or malformed HTTP-200 stream (no terminal) is a model failure.
    if (!this.sawTerminal) {
      const msg = this.message("error", errorMessage ?? "model stream ended without a terminal signal");
      return { type: "error", reason: "error", error: msg };
    }
    const reason = this.stopReason;
    if (reason === "error") {
      const msg = this.message("error", errorMessage ?? "model call failed");
      return { type: "error", reason: "error", error: msg };
    }
    const doneReason = (reason === "length" || reason === "toolUse" ? reason : "stop") as Extract<StopReason, "stop" | "length" | "toolUse">;
    return { type: "done", reason: doneReason, message: this.message(doneReason) };
  }

  private message(stopReason: StopReason, errorMessage?: string): AssistantMessage {
    // Preserve the assigned model metadata on the terminal message too — pi's
    // agent loop replaces `partialMessage` with this final message, so a
    // hard-coded `model: ""` would clobber the correctly initialized `start`
    // partial (HOR-395 / pi stream contract).
    return {
      role: "assistant",
      content: this.content,
      api: "openai-completions" as never,
      provider: this.provider as never,
      model: this.modelId,
      usage: this.usage,
      stopReason,
      errorMessage,
      timestamp: Date.now(),
    };
  }
}

function mapStop(fr: string): StopReason {
  switch (fr) {
    case "stop":
      return "stop";
    case "length":
      return "length";
    case "tool_calls":
    case "toolUse":
      return "toolUse";
    case "error":
      return "error";
    default:
      return "stop";
  }
}

function safeParseArgs(args: string): Record<string, unknown> {
  if (!args) return {};
  try {
    return JSON.parse(args) as Record<string, unknown>;
  } catch {
    return { _raw: args };
  }
}

/** Build the OpenAI chat-completions request body from a pi streamSimple call. */
export function buildOpenAIRequestBody(
  modelId: string,
  context: { systemPrompt?: string; messages: Message[]; tools?: { name: string; description: string; parameters: unknown }[] },
  options: { reasoning?: string; maxTokens?: number } | undefined,
): unknown {
  const messages: unknown[] = [];
  if (context.systemPrompt) messages.push({ role: "system", content: context.systemPrompt });
  // Tool-result image blocks are emitted as a follow-up `user` message with
  // `image_url` data URIs, mirroring pi's pinned OpenAI-completions provider
  // (which groups consecutive tool results and attaches their images as a
  // user turn). HOR-395 keeps existing per-turn image behavior intact.
  const modelSupportsImages = true; // assignedModel.input includes "image"
  for (let i = 0; i < context.messages.length; i++) {
    const m = context.messages[i];
    if (m.role === "user") {
      messages.push({ role: "user", content: piUserContentToOpenAI(m.content) });
      continue;
    }
    if (m.role === "assistant") {
      // Emit assistant text content + any tool_calls.
      const textParts = m.content.filter((c) => c.type === "text").map((c) => (c as { text: string }).text).join("");
      const toolCalls = m.content.filter((c) => c.type === "toolCall").map((c) => ({
        id: (c as ToolCall).id,
        type: "function",
        function: { name: (c as ToolCall).name, arguments: JSON.stringify((c as ToolCall).arguments) },
      }));
      const entry: Record<string, unknown> = { role: "assistant" };
      if (textParts) entry.content = textParts;
      if (toolCalls.length) entry.tool_calls = toolCalls;
      messages.push(entry);
      continue;
    }
    // toolResult — group consecutive tool results so their images are attached
    // as one follow-up user message (pinned-provider semantics).
    const imageBlocks: { type: "image_url"; image_url: { url: string } }[] = [];
    let j = i;
    for (; j < context.messages.length && context.messages[j].role === "toolResult"; j++) {
      const tr = context.messages[j] as Extract<Message, { role: "toolResult" }>;
      const textResult = tr.content.filter((c) => c.type === "text").map((c) => (c as { text: string }).text).join("");
      const hasImages = tr.content.some((c) => c.type === "image");
      const hasText = textResult.length > 0;
      // Always send a tool message with text (placeholder if only images),
      // matching the pinned provider.
      const content = hasText ? textResult : hasImages ? "(see attached image)" : "(no tool output)";
      messages.push({ role: "tool", tool_call_id: tr.toolCallId, content });
      if (modelSupportsImages) {
        for (const c of tr.content) {
          if (c.type === "image" && typeof c.data === "string" && typeof c.mimeType === "string") {
            imageBlocks.push({ type: "image_url", image_url: { url: `data:${c.mimeType};base64,${c.data}` } });
          }
        }
      }
    }
    i = j - 1;
    if (imageBlocks.length > 0) {
      messages.push({
        role: "user",
        content: [{ type: "text", text: "Attached image(s) from tool result:" }, ...imageBlocks],
      });
    }
  }
  const body: Record<string, unknown> = {
    model: modelId,
    messages,
    stream: true,
    stream_options: { include_usage: true },
  };
  if (options?.maxTokens) body.max_tokens = options.maxTokens;
  if (options?.reasoning && options.reasoning !== "off") body.reasoning_effort = options.reasoning;
  if (context.tools?.length) {
    body.tools = context.tools.map((t) => ({
      type: "function",
      function: { name: t.name, description: t.description, parameters: t.parameters },
    }));
  }
  return body;
}

/** Serialize a pi user message's content to OpenAI content parts. Text blocks
 * become `{type:"text"}` parts; image blocks become `{type:"image_url"}` data
 * URIs (the former OpenAI provider serialized them this way). HOR-395 keeps
 * existing Work/per-turn image behavior intact. */
function piUserContentToOpenAI(
  content: string | { type: string; text?: string; data?: string; mimeType?: string }[],
): string | unknown[] {
  if (typeof content === "string") return content;
  const parts: unknown[] = [];
  let textOnly = true;
  for (const c of content) {
    if (c.type === "text") {
      parts.push({ type: "text", text: c.text ?? "" });
    } else if (c.type === "image" && typeof c.data === "string" && typeof c.mimeType === "string") {
      textOnly = false;
      parts.push({ type: "image_url", image_url: { url: `data:${c.mimeType};base64,${c.data}` } });
    }
  }
  // OpenAI accepts a plain string for text-only user messages.
  return textOnly ? parts.map((p) => (p as { text: string }).text).join("") : parts;
}
