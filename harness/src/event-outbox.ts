// The per-turn event outbox + WAL (HOR-381). The supervisor sequences durable
// TurnEvents here before sending: each event is appended to a local WAL
// (emptyDir, supervisor-UID-owned, child-inaccessible) with fsync BEFORE send,
// so a supervisor *crash* loses no audit tail. A cumulative EventAck advances
// the durable ACK cursor (fsync); when the final outcome is ACKed the WAL is
// deleted. On reconnect (stream loss, supervisor survives) the in-memory buffer
// is replayed; on *crash* (restart) the WAL is scanned and unACKed events are
// replayed as after_terminal audit (the CP has already terminalized the turn
// as worker-loss via fencing).
//
// Token deltas bypass this entirely (ephemeral, non-sequenced, non-ACKed).
// Overflow (bound exceeded) fails the turn rather than dropping audit silently.

import { openSync, writeSync, fsyncSync, closeSync, unlinkSync, existsSync, readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { toBinary, fromBinary, create } from "@bufbuild/protobuf";
import { TurnEventSchema, type TurnEvent } from "./gen/iterabase/harness/v1/harness_pb.js";
import type { TurnEventPayload } from "./supervisor.js";

export class OutboxOverflow extends Error {
  constructor(message: string) {
    super(message);
    this.name = "OutboxOverflow";
  }
}

/** Raised when an EventAck is out-of-range or regressing (a protocol violation). */
export class AckError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "AckError";
  }
}

/** Validate a turn id before it is interpolated into a supervisor-owned path. */
const TURN_ID_RE = /^[A-Za-z0-9](?:[A-Za-z0-9._-]*[A-Za-z0-9])?$/;
export function validateTurnId(turnId: string): void {
  if (!TURN_ID_RE.test(turnId)) throw new Error(`invalid turn id: ${JSON.stringify(turnId)}`);
}

export interface RecoveredTurn {
  turnId: string;
  events: TurnEvent[]; // unACKed durable events (replay as after_terminal audit)
}

/**
 * A per-turn outbox backed by a WAL file `<walDir>/<turnId>.wal`.
 *
 * Records: `E\t<seq>\t<base64(toBinary(TurnEvent))>\n` (event) and
 * `A\t<throughSeq>\n` (cumulative ack). Each write is fsync'd.
 */
export class EventOutbox {
  private sequence = 0;
  private ackedSeq = 0;
  private finalSequence: number | null = null;
  private readonly buffer: TurnEvent[] = [];
  private readonly walPathValue: string;
  private readonly fd: number;

  constructor(
    private readonly walDir: string,
    private readonly turnId: string,
    private readonly bound: number,
  ) {
    validateTurnId(turnId); // refuse path-traversal in the WAL filename
    this.walPathValue = join(walDir, `${turnId}.wal`);
    this.fd = openSync(this.walPathValue, "a"); // append; supervisor-UID-owned dir
  }

  /**
   * Append a durable event: assign sequence + timestamp, fsync to WAL, buffer.
   * Throws OutboxOverflow at capacity (the event is NOT appended). Terminal
   * events (the overflow HarnessError + WorkerOutcome) use appendTerminal(),
   * which cannot re-enter this bound check — so overflow always leaves a path
   * to emit a FAILED outcome instead of hanging the turn.
   */
  append(payload: TurnEventPayload): TurnEvent {
    if (this.buffer.length >= this.bound) throw new OutboxOverflow(`outbox bound (${this.bound}) exceeded for turn ${this.turnId}`);
    return this.appendTerminal(payload);
  }

  /** Append a terminal event bypassing the bound (overflow HarnessError + WorkerOutcome). */
  appendTerminal(payload: TurnEventPayload): TurnEvent {
    this.sequence += 1;
    const ev = create(TurnEventSchema, {
      turnId: this.turnId,
      sequence: BigInt(this.sequence),
      timestampMs: BigInt(Date.now()),
      kind: payload,
    });
    const bytes = toBinary(TurnEventSchema, ev);
    this.write(`E\t${this.sequence}\t${Buffer.from(bytes).toString("base64")}\n`);
    this.buffer.push(ev);
    return ev;
  }

  /** Mark the final outcome's sequence (the last append before the turn ends). */
  markFinal(sequence: number): void {
    this.finalSequence = sequence;
  }

  /**
   * Advance the cumulative ACK cursor. Drops ACKed events from the buffer.
   * Returns true if the final outcome is ACKed (caller deletes the WAL / re-credits).
   */
  /**
   * Advance the cumulative ACK cursor. Rejects out-of-range (beyond the highest
   * emitted sequence) and regressing (below the current cursor) ACKs as protocol
   * errors; a duplicate at the current cursor is an idempotent no-op. Drops
   * ACKed events from the buffer. Returns true if the final outcome is ACKed
   * (caller deletes the WAL / re-credits).
   */
  ack(throughSequence: number): boolean {
    if (throughSequence > this.sequence) throw new AckError(`ack through ${throughSequence} > highest sequence ${this.sequence}`);
    if (throughSequence < this.ackedSeq) throw new AckError(`regressing ack ${throughSequence} < ${this.ackedSeq}`);
    if (throughSequence === this.ackedSeq) return false;
    this.ackedSeq = throughSequence;
    this.write(`A\t${throughSequence}\n`);
    while (this.buffer.length > 0 && Number(this.buffer[0]!.sequence) <= throughSequence) this.buffer.shift();
    if (this.finalSequence !== null && throughSequence >= this.finalSequence) {
      this.destroy();
      return true;
    }
    return false;
  }

  /** UnACKed events (for stream-loss replay — the supervisor survived). */
  unacked(): TurnEvent[] {
    return this.buffer.filter((e) => Number(e.sequence) > this.ackedSeq);
  }

  get highestSequence(): number {
    return this.sequence;
  }

  get walPath(): string {
    return this.walPathValue;
  }
  /** Close the WAL fd without deleting (e.g., on drain). */
  close(): void {
    try {
      closeSync(this.fd);
    } catch {
      /* already closed */
    }
  }

  /** Delete the WAL (final outcome ACKed — the turn is durably complete). */
  private destroy(): void {
    this.close();
    try {
      unlinkSync(this.walPath);
    } catch {
      /* already gone */
    }
  }

  private write(line: string): void {
    writeSync(this.fd, line);
    fsyncSync(this.fd);
  }

  /**
   * Crash recovery: scan `walDir` for unfinished turn WALs and return their
   * unACKed events. Finished turns (final outcome ACKed) have no WAL (deleted).
   * The supervisor replays these as after_terminal audit after reconnecting.
   */
  static recover(walDir: string): RecoveredTurn[] {
    if (!existsSync(walDir)) return [];
    const recovered: RecoveredTurn[] = [];
    for (const f of readdirSafe(walDir)) {
      if (!f.endsWith(".wal")) continue;
      const turnId = f.slice(0, -".wal".length);
      const events = replayWal(join(walDir, f));
      if (events.length > 0) recovered.push({ turnId, events });
      else {
        // No unACKed events — a leftover empty/fully-acked WAL; clean it up.
        try {
          unlinkSync(join(walDir, f));
        } catch {
          /* ignore */
        }
      }
    }
    return recovered;
  }
}

function readdirSafe(dir: string): string[] {
  try {
    return readdirSync(dir);
  } catch {
    return [];
  }
}

function replayWal(path: string): TurnEvent[] {
  let content: string;
  try {
    content = readFileSync(path, "utf8");
  } catch {
    return [];
  }
  let ackCursor = 0;
  const events = new Map<number, TurnEvent>();
  for (const line of content.split("\n")) {
    if (!line) continue;
    const idx = line.indexOf("\t");
    if (idx < 0) continue;
    const tag = line.slice(0, idx);
    const rest = line.slice(idx + 1);
    if (tag === "A") {
      ackCursor = Number(rest);
    } else if (tag === "E") {
      const tab = rest.indexOf("\t");
      if (tab < 0) continue;
      const seq = Number(rest.slice(0, tab));
      const b64 = rest.slice(tab + 1);
      try {
        events.set(seq, fromBinary(TurnEventSchema, Buffer.from(b64, "base64")));
      } catch {
        /* skip corrupt record */
      }
    }
  }
  return [...events.entries()].filter(([seq]) => seq > ackCursor).sort((a, b) => a[0] - b[0]).map(([, ev]) => ev);
}
