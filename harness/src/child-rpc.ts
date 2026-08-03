// The child→supervisor RPC client (HOR-395, ARCH-004/010/011). The disposable
// child holds no gateway credential and no network route; it sends model/tool
// requests over fd 4 and receives responses + non-secret descriptors over fd 5.
// This module demuxes fd 5 by `requestId`, exposes:
//   - awaitGatewayTools(): resolves the one-shot descriptor list (ARCH-006)
//   - streamModel(body, signal): an AssistantMessageEventStream backed by the
//     supervisor's inference-gateway bridge (ARCH-011)
//   - invokeTool(call, signal): a Promise<tool result> backed by the
//     supervisor's tool-gateway InvokeTool (ARCH-004)
// Cancellation propagates: a child abort sends `cancel` on fd 4; a
// supervisor-initiated cancel (AbortTurn) arrives as `cancel` on fd 5.
//
// The supervisor validates every request against the active assignment before
// calling a gateway — this client only carries the requestId + business payload.

import { createAssistantMessageEventStream } from "@earendil-works/pi-ai";
import type { ProviderConfig } from "@earendil-works/pi-coding-agent";
import {
  FrameReader,
  parseSupervisorRpcFrame,
  encodeFrame,
  type GatewayToolDescriptor,
  type ToolResultFrame,
} from "./ipc.js";
import { OpenAIStreamAccumulator } from "./openai-stream.js";

/** The AssistantMessageEventStream type pi's ProviderConfig.streamSimple expects
 * (pi-coding-agent's nested pi-ai copy). The runtime factory comes from the
 * hoisted pi-ai; both ship identical 0.80.6 code so the instance is
 * structurally compatible (duck-typed push/end/asyncIterator). */
type EventStream = NonNullable<ProviderConfig["streamSimple"]> extends (...args: never[]) => infer R ? R : never;

export interface ToolCallRequest {
  toolCallId: string;
  toolName: string;
  toolVersionDigest: string;
  argumentsJson: string;
  idempotencyKey?: string;
}
export interface ToolCallResult {
  resultJson?: string;
  isError: boolean;
  errorMessage?: string;
}

export interface ChildRpcDeps {
  /** Write framed bytes to fd 4 (child → supervisor requests). */
  write: (buf: Buffer) => void;
}

interface PendingModel {
  stream: ReturnType<typeof createAssistantMessageEventStream>;
  acc: OpenAIStreamAccumulator;
  cancelled: boolean;
}
interface PendingTool {
  resolve: (r: ToolCallResult) => void;
  cancelled: boolean;
}

/** A unique request id generator (child-local; correlated on fd 5). */
let requestCounter = 0;
function nextRequestId(): string {
  requestCounter += 1;
  return `r${Date.now().toString(36)}-${requestCounter}`;
}

export class ChildRpc {
  private readonly reader: FrameReader;
  private gatewayToolsResolve: ((d: GatewayToolDescriptor[]) => void) | null = null;
  private readonly gatewayTools: Promise<GatewayToolDescriptor[]>;
  private readonly pendingModels = new Map<string, PendingModel>();
  private readonly pendingTools = new Map<string, PendingTool>();
  private readonly pendingCompletions = new Map<string, () => void>();
  private closed = false;

  constructor(private readonly deps: ChildRpcDeps) {
    this.gatewayTools = new Promise<GatewayToolDescriptor[]>((resolve) => {
      this.gatewayToolsResolve = resolve;
    });
    this.reader = new FrameReader((raw) => this.onFrame(raw));
  }

  /** Feed raw bytes from fd 5 (the supervisor→child RPC channel). */
  feed(chunk: Buffer | string): void {
    this.reader.feed(chunk);
  }

  /** Resolve the one-shot gateway-tool descriptor list (ARCH-006). */
  awaitGatewayTools(): Promise<GatewayToolDescriptor[]> {
    return this.gatewayTools;
  }

  /**
   * Open a model stream: write a `modelRequest` on fd 4 and return a pi
   * AssistantMessageEventStream fed by `modelChunk`/`modelEnd` on fd 5. The
   * supervisor validates the model against the active assignment and opens the
   * authenticated inference-gateway stream (ARCH-010/011).
   */
  streamModel(body: unknown, signal: AbortSignal | undefined, modelId = ""): EventStream {
    const requestId = nextRequestId();
    const stream = createAssistantMessageEventStream();
    const acc = new OpenAIStreamAccumulator(modelId);
    const pending: PendingModel = { stream, acc, cancelled: false };
    this.pendingModels.set(requestId, pending);

    const cancel = (reason: string): void => {
      if (pending.cancelled) return;
      pending.cancelled = true;
      this.send({ type: "cancel", requestId });
      const ev = acc.finish("aborted", reason);
      stream.push(ev);
      stream.end(ev.type === "error" ? ev.error : ev.message);
    };

    if (signal) {
      if (signal.aborted) {
        // Pre-aborted: terminate locally without sending a request the
        // supervisor has no controller for yet (cancellation must propagate
        // upstream — sending modelRequest + cancel races the controller).
        const ev = acc.finish("aborted", "aborted before request");
        stream.push(ev);
        stream.end(ev.type === "error" ? ev.error : ev.message);
        return stream as unknown as EventStream;
      }
      signal.addEventListener("abort", () => cancel("aborted"), { once: true });
    }

    this.send({ type: "modelRequest", requestId, body });
    return stream as unknown as EventStream;
  }

  /**
   * Invoke a gateway tool: write a `toolCall` on fd 4 and await `toolResult`
   * on fd 5. The supervisor stamps durable caller context + idempotency and
   * calls InvokeTool over mTLS (ARCH-004/014).
   */
  invokeTool(call: ToolCallRequest, signal: AbortSignal | undefined): Promise<ToolCallResult> {
    const requestId = nextRequestId();
    return new Promise<ToolCallResult>((resolve) => {
      const pending: PendingTool = { resolve, cancelled: false };
      this.pendingTools.set(requestId, pending);

      const cancel = (): void => {
        if (pending.cancelled) return;
        pending.cancelled = true;
        this.send({ type: "cancel", requestId });
        // Do not resolve here — wait for the supervisor's toolResult/cancel
        // frame so the caller sees the gateway's best-known terminal state
        // (ARCH-014: cancel cannot undo an effect already started). If the
        // supervisor never replies (stream lost), the child is killed and the
        // promise never resolves — that is acceptable (the turn aborts).
      };
      if (signal) {
        if (signal.aborted) {
          // Pre-aborted: do not send a toolCall the supervisor has no
          // controller for yet; resolve as an aborted error terminal.
          pending.cancelled = true;
          resolve({ isError: true, errorMessage: "aborted before request" });
          return;
        }
        signal.addEventListener("abort", () => cancel(), { once: true });
      }

      this.send({
        type: "toolCall",
        requestId,
        toolCallId: call.toolCallId,
        toolName: call.toolName,
        toolVersionDigest: call.toolVersionDigest,
        argumentsJson: call.argumentsJson,
        ...(call.idempotencyKey !== undefined ? { idempotencyKey: call.idempotencyKey } : {}),
      });
    });
  }

  /** Report complete_step through the trusted supervisor. Requests on fd 4 are
   * ordered, so the supervisor disables subsequent gateway calls before ACK. */
  reportStepCompletion(report: { outcome: string; summary: string; outputJson: string; artifactRefs: Array<{artifactId:string;role:string;metadataJson:string}> }): Promise<void> {
    const requestId = nextRequestId();
    return new Promise<void>((resolve) => {
      this.pendingCompletions.set(requestId, resolve);
      this.send({ type: "stepCompletion", requestId, ...report });
    });
  }

  /** Release the reader and reject pending callers (child exit / fatal). */
  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.reader.end();
    for (const p of this.pendingModels.values()) p.stream.end();
    this.pendingModels.clear();
    for (const p of this.pendingTools.values()) p.resolve({ isError: true, errorMessage: "child rpc closed" });
    this.pendingTools.clear();
    for (const resolve of this.pendingCompletions.values()) resolve();
    this.pendingCompletions.clear();
  }

  private send(frame: unknown): void {
    if (this.closed) return;
    try {
      this.deps.write(encodeFrame(frame));
    } catch {
      /* fd closed (supervisor gone) — exit via the main loop */
    }
  }

  private onFrame(raw: unknown): void {
    const frame = parseSupervisorRpcFrame(raw);
    if (!frame) return;
    switch (frame.type) {
      case "gatewayTools":
        if (this.gatewayToolsResolve) {
          this.gatewayToolsResolve(frame.descriptors);
          this.gatewayToolsResolve = null;
        }
        return;
      case "modelChunk": {
        const p = this.pendingModels.get(frame.requestId);
        if (!p || p.cancelled) return;
        for (const ev of p.acc.feed(frame.data)) p.stream.push(ev);
        return;
      }
      case "modelEnd": {
        const p = this.pendingModels.get(frame.requestId);
        if (!p) return;
        this.pendingModels.delete(frame.requestId);
        if (!p.cancelled) {
          const ev = p.acc.finish(frame.status, frame.errorMessage);
          p.stream.push(ev);
          p.stream.end(ev.type === "done" ? ev.message : ev.error);
        }
        return;
      }
      case "toolResult": {
        const p = this.pendingTools.get(frame.requestId);
        if (!p) return;
        this.pendingTools.delete(frame.requestId);
        p.resolve(toolResultFrom(frame));
        return;
      }
      case "stepCompletionAck": {
        const resolve = this.pendingCompletions.get(frame.requestId);
        if (!resolve) return;
        this.pendingCompletions.delete(frame.requestId);
        resolve();
        return;
      }
      case "cancel": {
        // Supervisor-initiated cancel (e.g. AbortTurn reached the supervisor).
        const m = this.pendingModels.get(frame.requestId);
        if (m && !m.cancelled) {
          m.cancelled = true;
          const ev = m.acc.finish("aborted", "supervisor cancel");
          m.stream.push(ev);
          m.stream.end(ev.type === "error" ? ev.error : ev.message);
        }
        const t = this.pendingTools.get(frame.requestId);
        if (t && !t.cancelled) {
          t.cancelled = true;
          t.resolve({ isError: true, errorMessage: "supervisor cancel" });
        }
        return;
      }
    }
  }
}

function toolResultFrom(f: ToolResultFrame): ToolCallResult {
  const r: ToolCallResult = { isError: f.isError };
  if (f.resultJson !== undefined) r.resultJson = f.resultJson;
  if (f.errorMessage !== undefined) r.errorMessage = f.errorMessage;
  return r;
}
