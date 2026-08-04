// The supervisor↔child IPC boundary (HOR-381; HOR-395 adds the dedicated
// model + gateway-tool RPC channels).
//
// Trust boundary contract (approved spec): length-prefixed JSON over a TS
// discriminated union with runtime validation, on dedicated duplex channels
// of inherited fds:
//   - fd 0  : supervisor → child  (control: assignment, abort)
//   - fd 3  : child → supervisor (audit: event, tokenDelta, heartbeat, result)
//   - fd 4  : child → supervisor (RPC: modelRequest, toolCall,
//                                  stepCompletion, cancel)
//   - fd 5  : supervisor → child  (RPC: modelChunk, modelEnd, toolResult,
//                                  stepCompletionAck, gatewayTools, cancel)
// stdout and stderr are NOT a protocol channel — they are piped separately and
// drained as tagged logs (child-process.ts). A stray/spoofed byte sequence
// cannot masquerade as a frame: the 4-byte length prefix must describe a full,
// valid JSON document matching the discriminated union, or the frame is
// dropped (and, for the durable `event` frame, re-validated against the
// TurnEvent proto schema before it can touch the audit outbox).
//
// The supervisor never imports pi/extensions/tools; the child receives only its
// assignment + non-secret descriptors + RPC responses over these channels.
// Model/tool bodies cross fd 4/fd 5 ONLY — they never touch the `Work` audit
// stream and are never logged (ARCH-011).

/** Max single-frame body size (16 MiB). A length prefix beyond this is treated as stream corruption. */
export const MAX_FRAME_BYTES = 16 * 1024 * 1024;

// ---- Child → Supervisor ----

export interface EventFrame {
  type: "event";
  /** A full TurnEvent JSON object (turnId/sequence/timestamp placeholders; the supervisor re-sequences). */
  event: unknown;
}
export interface TokenDeltaFrame {
  type: "tokenDelta";
  contentIndex: number;
  deltaType: "TEXT" | "THINKING";
  delta: string;
}
export interface HeartbeatFrame {
  type: "heartbeat";
  /** Optional pi phase hint from the child (maps to PiPhase). */
  piPhase?: "SESSION_SETUP" | "MODEL_CALL" | "TOOL_CALL" | "COMPACTION" | "RETRY_BACKOFF" | "SHUTDOWN";
}
export interface ResultFrame {
  type: "result";
  /** Outcome enum numeric value (OUTCOME_COMPLETED=1 | ABORTED=2 | FAILED=3). */
  outcome: number;
  message?: string;
}
export type ChildFrame = EventFrame | TokenDeltaFrame | HeartbeatFrame | ResultFrame;

// ---- Supervisor → Child (control: fd 0) ----

export interface AssignmentFrame {
  type: "assignment";
  assignment: unknown;
}
export interface AbortFrame {
  type: "abort";
}
export type SupervisorFrame = AssignmentFrame | AbortFrame;

// ---- Child → Supervisor RPC (fd 4) — HOR-395 ----
//
// The disposable child sends model/tool requests it cannot satisfy itself; the
// trusted supervisor validates each against the active assignment (ARCH-004),
// stamps durable caller context, and calls the authorized gateway over mTLS.
// `requestId` is a child-generated opaque string that correlates responses on
// fd 5. Model/tool bodies cross this channel ONLY — never the Work stream.

export interface ModelRequestFrame {
  type: "modelRequest";
  requestId: string;
  /** The OpenAI chat-completions request body (model/messages/tools/reasoning/max_tokens/stream:true). */
  body: unknown;
}
export interface ToolCallFrame {
  type: "toolCall";
  requestId: string;
  toolCallId: string;
  toolName: string;
  toolVersionDigest: string;
  argumentsJson: string;
  idempotencyKey?: string;
}
export interface PublishArtifactRpcFrame {
  type: "publishArtifact";
  requestId: string;
  relativePath: string;
  mimeType: string;
}
export interface StepCompletionRpcFrame {
  type: "stepCompletion";
  requestId: string;
  outcome: string;
  summary: string;
  outputJson: string;
  artifactRefs: Array<{ artifactId: string; role: string; metadataJson: string }>;
}
export interface CancelRpcFrame {
  type: "cancel";
  requestId: string;
}
export type ChildRpcFrame = ModelRequestFrame | ToolCallFrame | PublishArtifactRpcFrame | StepCompletionRpcFrame | CancelRpcFrame;

// ---- Supervisor → Child RPC (fd 5) — HOR-395 ----
//
// `gatewayTools` is sent once after DiscoverEffectiveTools, before the child's
// first model call; the child registers pi tool stubs from it. Model chunks are
// raw OpenAI SSE `data:` payloads (the JSON object string, or "[DONE]"); the
// child parses them into pi stream events (ARCH-010/011 — the supervisor stays
// transport-oriented and does not own model semantics). `modelEnd` is the
// authoritative terminal signal (ok/error/aborted).

/** Non-secret gateway tool descriptor passed to the child (ARCH-006). */
export interface GatewayToolDescriptor {
  name: string;
  version: string;
  digest: string;
  description: string;
  /** JSON Schema for arguments (parsed JSON object). */
  inputSchema: unknown;
  effectClass: "read_only" | "idempotent_write" | "non_idempotent_write";
  timeoutMs?: number;
}
export interface GatewayToolsFrame {
  type: "gatewayTools";
  descriptors: GatewayToolDescriptor[];
}
export interface ModelChunkFrame {
  type: "modelChunk";
  requestId: string;
  /** One OpenAI SSE `data:` payload string (a JSON object, or "[DONE]"). */
  data: string;
}
export interface ModelEndFrame {
  type: "modelEnd";
  requestId: string;
  status: "ok" | "error" | "aborted";
  httpStatus?: number;
  errorMessage?: string;
}
export interface ToolResultFrame {
  type: "toolResult";
  requestId: string;
  /** Committed result JSON string (when succeeded). */
  resultJson?: string;
  artifactRefs?: Array<{ artifactId: string; mimeType: string; sizeBytes: number; digest: string }>;
  isError: boolean;
  errorMessage?: string;
}
export interface ArtifactPublishedFrame {
  type: "artifactPublished";
  requestId: string;
  artifactId?: string;
  mimeType?: string;
  sizeBytes?: number;
  digest?: string;
  errorMessage?: string;
}
export interface StepCompletionAckFrame {
  type: "stepCompletionAck";
  requestId: string;
}
export interface SupervisorCancelFrame {
  type: "cancel";
  requestId: string;
}
export type SupervisorRpcFrame =
  | GatewayToolsFrame
  | ModelChunkFrame
  | ModelEndFrame
  | ToolResultFrame
  | ArtifactPublishedFrame
  | StepCompletionAckFrame
  | SupervisorCancelFrame;

/**
 * Encode a frame as a 4-byte big-endian length prefix + UTF-8 JSON body.
 * Throws if the body exceeds MAX_FRAME_BYTES (caller should treat as fatal).
 */
export function encodeFrame(body: unknown): Buffer {
  const json = Buffer.from(JSON.stringify(body), "utf8");
  if (json.length > MAX_FRAME_BYTES) throw new Error(`IPC frame too large (${json.length} bytes)`);
  const header = Buffer.allocUnsafe(4);
  header.writeUInt32BE(json.length, 0);
  return Buffer.concat([header, json]);
}

/** Write a single framed message to a writable byte stream. Returns the
 * value of `stream.write()` (false when the kernel pipe buffer is full and the
 * caller should apply backpressure instead of queuing more writes). */
export function writeFrame(stream: { write(buf: Buffer): boolean }, body: unknown): boolean {
  return stream.write(encodeFrame(body));
}

/**
 * Parse + validate a decoded JSON object into a ChildFrame, or return null if
 * it is malformed/unknown (the caller drops it and logs). `event` frames are
 * NOT proto-validated here — the supervisor validates them against
 * TurnEventSchema before they enter the outbox (single validation site that
 * also re-sequences).
 */
export function parseChildFrame(raw: unknown): ChildFrame | null {
  if (!raw || typeof raw !== "object") return null;
  const r = raw as Record<string, unknown>;
  switch (r.type) {
    case "event":
      if (r.event === undefined) return null;
      return { type: "event", event: r.event };
    case "tokenDelta": {
      if (typeof r.contentIndex !== "number" || (r.deltaType !== "TEXT" && r.deltaType !== "THINKING") || typeof r.delta !== "string") return null;
      return { type: "tokenDelta", contentIndex: r.contentIndex, deltaType: r.deltaType, delta: r.delta };
    }
    case "heartbeat": {
      const f: HeartbeatFrame = { type: "heartbeat" };
      if (typeof r.piPhase === "string") f.piPhase = r.piPhase as HeartbeatFrame["piPhase"];
      return f;
    }
    case "result": {
      if (typeof r.outcome !== "number") return null;
      const f: ResultFrame = { type: "result", outcome: r.outcome };
      if (typeof r.message === "string") f.message = r.message;
      return f;
    }
    default:
      return null;
  }
}

export function parseSupervisorFrame(raw: unknown): SupervisorFrame | null {
  if (!raw || typeof raw !== "object") return null;
  const r = raw as Record<string, unknown>;
  switch (r.type) {
    case "assignment":
      if (r.assignment === undefined) return null;
      return { type: "assignment", assignment: r.assignment };
    case "abort":
      return { type: "abort" };
    default:
      return null;
  }
}

/** Parse + validate a ChildRpcFrame (fd 4). Returns null for malformed/unknown. */
export function parseChildRpcFrame(raw: unknown): ChildRpcFrame | null {
  if (!raw || typeof raw !== "object") return null;
  const r = raw as Record<string, unknown>;
  if (typeof r.requestId !== "string") return null;
  switch (r.type) {
    case "modelRequest":
      if (r.body === undefined) return null;
      return { type: "modelRequest", requestId: r.requestId, body: r.body };
    case "toolCall": {
      if (
        typeof r.toolCallId !== "string" ||
        typeof r.toolName !== "string" ||
        typeof r.toolVersionDigest !== "string" ||
        typeof r.argumentsJson !== "string"
      )
        return null;
      const f: ToolCallFrame = {
        type: "toolCall",
        requestId: r.requestId,
        toolCallId: r.toolCallId,
        toolName: r.toolName,
        toolVersionDigest: r.toolVersionDigest,
        argumentsJson: r.argumentsJson,
      };
      if (typeof r.idempotencyKey === "string") f.idempotencyKey = r.idempotencyKey;
      return f;
    }
    case "publishArtifact":
      if (typeof r.relativePath !== "string" || typeof r.mimeType !== "string") return null;
      return { type: "publishArtifact", requestId: r.requestId, relativePath: r.relativePath, mimeType: r.mimeType };
    case "stepCompletion": {
      if (typeof r.outcome !== "string" || typeof r.summary !== "string" || typeof r.outputJson !== "string" || !Array.isArray(r.artifactRefs)) return null;
      const artifactRefs: StepCompletionRpcFrame["artifactRefs"] = [];
      for (const rawRef of r.artifactRefs) {
        if (!rawRef || typeof rawRef !== "object") return null;
        const ref = rawRef as Record<string, unknown>;
        if (typeof ref.artifactId !== "string" || typeof ref.role !== "string" || typeof ref.metadataJson !== "string") return null;
        artifactRefs.push({ artifactId: ref.artifactId, role: ref.role, metadataJson: ref.metadataJson });
      }
      return { type: "stepCompletion", requestId: r.requestId, outcome: r.outcome, summary: r.summary, outputJson: r.outputJson, artifactRefs };
    }
    case "cancel":
      return { type: "cancel", requestId: r.requestId };
    default:
      return null;
  }
}

/** Parse + validate a SupervisorRpcFrame (fd 5). Returns null for malformed/unknown. */
export function parseSupervisorRpcFrame(raw: unknown): SupervisorRpcFrame | null {
  if (!raw || typeof raw !== "object") return null;
  const r = raw as Record<string, unknown>;
  switch (r.type) {
    case "gatewayTools": {
      if (!Array.isArray(r.descriptors)) return null;
      const descriptors: GatewayToolDescriptor[] = [];
      for (const d of r.descriptors) {
        if (!d || typeof d !== "object") return null;
        const dd = d as Record<string, unknown>;
        if (
          typeof dd.name !== "string" ||
          typeof dd.version !== "string" ||
          typeof dd.digest !== "string" ||
          typeof dd.description !== "string" ||
          dd.inputSchema === undefined ||
          typeof dd.effectClass !== "string"
        )
          return null;
        if (dd.effectClass !== "read_only" && dd.effectClass !== "idempotent_write" && dd.effectClass !== "non_idempotent_write")
          return null;
        const desc: GatewayToolDescriptor = {
          name: dd.name,
          version: dd.version,
          digest: dd.digest,
          description: dd.description,
          inputSchema: dd.inputSchema,
          effectClass: dd.effectClass,
        };
        if (typeof dd.timeoutMs === "number") desc.timeoutMs = dd.timeoutMs;
        descriptors.push(desc);
      }
      return { type: "gatewayTools", descriptors };
    }
    case "modelChunk":
      if (typeof r.requestId !== "string" || typeof r.data !== "string") return null;
      return { type: "modelChunk", requestId: r.requestId, data: r.data };
    case "modelEnd": {
      if (typeof r.requestId !== "string") return null;
      if (r.status !== "ok" && r.status !== "error" && r.status !== "aborted") return null;
      const f: ModelEndFrame = { type: "modelEnd", requestId: r.requestId, status: r.status };
      if (typeof r.httpStatus === "number") f.httpStatus = r.httpStatus;
      if (typeof r.errorMessage === "string") f.errorMessage = r.errorMessage;
      return f;
    }
    case "toolResult": {
      if (typeof r.requestId !== "string" || typeof r.isError !== "boolean") return null;
      const f: ToolResultFrame = { type: "toolResult", requestId: r.requestId, isError: r.isError };
      if (typeof r.resultJson === "string") f.resultJson = r.resultJson;
      if (Array.isArray(r.artifactRefs)) {
        const refs: NonNullable<ToolResultFrame["artifactRefs"]> = [];
        for (const rawRef of r.artifactRefs) {
          if (!rawRef || typeof rawRef !== "object") return null;
          const ref = rawRef as Record<string, unknown>;
          if (typeof ref.artifactId !== "string" || typeof ref.mimeType !== "string" || typeof ref.sizeBytes !== "number" || typeof ref.digest !== "string") return null;
          refs.push({ artifactId: ref.artifactId, mimeType: ref.mimeType, sizeBytes: ref.sizeBytes, digest: ref.digest });
        }
        f.artifactRefs = refs;
      }
      if (typeof r.errorMessage === "string") f.errorMessage = r.errorMessage;
      return f;
    }
    case "artifactPublished": {
      if (typeof r.requestId !== "string") return null;
      const f: ArtifactPublishedFrame = { type: "artifactPublished", requestId: r.requestId };
      if (typeof r.artifactId === "string") f.artifactId = r.artifactId;
      if (typeof r.mimeType === "string") f.mimeType = r.mimeType;
      if (typeof r.sizeBytes === "number") f.sizeBytes = r.sizeBytes;
      if (typeof r.digest === "string") f.digest = r.digest;
      if (typeof r.errorMessage === "string") f.errorMessage = r.errorMessage;
      if (!f.errorMessage && (!f.artifactId || !f.mimeType || f.sizeBytes === undefined || !f.digest)) return null;
      return f;
    }
    case "stepCompletionAck":
      if (typeof r.requestId !== "string") return null;
      return { type: "stepCompletionAck", requestId: r.requestId };
    case "cancel":
      if (typeof r.requestId !== "string") return null;
      return { type: "cancel", requestId: r.requestId };
    default:
      return null;
  }
}

/**
 * A buffered length-prefixed frame reader. Feed raw bytes via `feed()`; complete
 * frames are pushed to `onFrame`. By default it is lenient: a too-large/corrupt
 * length prefix is dropped (resync), malformed JSON is dropped, and a trailing
 * partial frame at `end()` is discarded. For strict channels (e.g. the fd-4
 * RPC channel, where a dropped request frame would leave the child waiting
 * forever), pass `onError`: every framing/JSON/truncation error is reported to
 * it and the reader stops, so the caller can fail closed (HOR-395).
 */
export class FrameReader {
  private buf: Buffer = Buffer.alloc(0);
  private stopped = false;
  constructor(
    private readonly onFrame: (json: unknown) => void,
    private readonly onError?: (reason: string) => void,
  ) {}

  feed(chunk: Buffer | string): void {
    if (this.stopped) return;
    this.buf = Buffer.concat([this.buf, typeof chunk === "string" ? Buffer.from(chunk, "utf8") : chunk]);
    // Decode as many complete frames as are available.
    while (this.buf.length >= 4) {
      const len = this.buf.readUInt32BE(0);
      if (len > MAX_FRAME_BYTES) {
        // Corrupt length prefix.
        if (this.onError) {
          this.fail(`frame length prefix ${len} exceeds MAX_FRAME_BYTES`);
          return;
        }
        // Lenient: drop the prefix and resync.
        this.buf = this.buf.subarray(4);
        continue;
      }
      if (this.buf.length < 4 + len) break; // wait for the rest of the body
      const body = this.buf.subarray(4, 4 + len);
      this.buf = this.buf.subarray(4 + len);
      let parsed: unknown;
      try {
        parsed = JSON.parse(body.toString("utf8"));
      } catch {
        // Malformed JSON body.
        if (this.onError) {
          this.fail("frame body is not valid JSON");
          return;
        }
        continue; // lenient: drop the frame
      }
      this.onFrame(parsed);
    }
  }

  end(): void {
    if (this.stopped) return;
    if (this.buf.length > 0 && this.onError) {
      // A trailing partial frame is truncation on a strict channel.
      this.fail("truncated frame at end of stream");
      return;
    }
    this.buf = Buffer.alloc(0);
  }

  /** Report a framing/JSON/truncation error on a strict channel and stop. */
  private fail(reason: string): void {
    this.stopped = true;
    this.buf = Buffer.alloc(0);
    this.onError?.(reason);
  }
}
