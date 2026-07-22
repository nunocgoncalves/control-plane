// The supervisor↔child IPC boundary (HOR-381).
//
// Trust boundary contract (approved spec): length-prefixed JSON over a TS
// discriminated union with runtime validation, on a dedicated duplex channel
// of inherited fds (fd 3 = supervisor→child, fd 4 = child→supervisor). stdout
// and stderr are NOT the protocol channel — they are piped separately and
// drained as tagged logs (child-process.ts). A stray/spoofed byte sequence
// cannot masquerade as a frame: the 4-byte length prefix must describe a full,
// valid JSON document matching the discriminated union, or the frame is
// dropped (and, for the durable `event` frame, re-validated against the
// TurnEvent proto schema before it can touch the audit outbox).
//
// The supervisor never imports pi/extensions/tools; the child receives only its
// assignment + non-secret constants over this channel.

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

// ---- Supervisor → Child ----

export interface AssignmentFrame {
  type: "assignment";
  assignment: unknown;
}
export interface AbortFrame {
  type: "abort";
}
export type SupervisorFrame = AssignmentFrame | AbortFrame;

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

/** Write a single framed message to a writable byte stream. */
export function writeFrame(stream: { write(buf: Buffer): boolean }, body: unknown): void {
  stream.write(encodeFrame(body));
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

/**
 * A buffered length-prefixed frame reader. Feed raw bytes via `feed()`; complete
 * frames are pushed to `onFrame`. Resyncs past a too-large/corrupt length
 * prefix by dropping it (the next 4 bytes are re-read as a fresh prefix).
 */
export class FrameReader {
  private buf: Buffer = Buffer.alloc(0);
  constructor(private readonly onFrame: (json: unknown) => void) {}

  feed(chunk: Buffer | string): void {
    this.buf = Buffer.concat([this.buf, typeof chunk === "string" ? Buffer.from(chunk, "utf8") : chunk]);
    // Decode as many complete frames as are available.
    while (this.buf.length >= 4) {
      const len = this.buf.readUInt32BE(0);
      if (len > MAX_FRAME_BYTES) {
        // Corrupt length prefix — drop it and continue (resync).
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
        continue; // malformed JSON — drop the frame
      }
      this.onFrame(parsed);
    }
  }

  end(): void {
    this.buf = Buffer.alloc(0);
  }
}
