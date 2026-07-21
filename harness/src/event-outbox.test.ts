import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { create } from "@bufbuild/protobuf";
import { mkdtempSync, rmSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { AssistantMessageSchema } from "./gen/iterabase/harness/v1/harness_pb.js";
import { EventOutbox, OutboxOverflow } from "./event-outbox.js";
import type { TurnEventPayload } from "./supervisor.js";

function assistantPayload(text: string): TurnEventPayload {
  return { case: "assistantMessage", value: create(AssistantMessageSchema, { text }) };
}

let dir: string;
beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), "harness-outbox-"));
});
afterEach(() => {
  rmSync(dir, { recursive: true, force: true });
});

describe("EventOutbox append + ack", () => {
  it("assigns monotonic 1-based sequences and buffers events", () => {
    const ob = new EventOutbox(dir, "turn-1", 100);
    const a = ob.append(assistantPayload("a"));
    const b = ob.append(assistantPayload("b"));
    expect(Number(a.sequence)).toBe(1);
    expect(Number(b.sequence)).toBe(2);
    expect(ob.unacked().map((e) => Number(e.sequence))).toEqual([1, 2]);
    expect(ob.highestSequence).toBe(2);
    ob.close();
  });

  it("cumulative ack drops acked events and keeps the tail", () => {
    const ob = new EventOutbox(dir, "turn-1", 100);
    ob.append(assistantPayload("a"));
    ob.append(assistantPayload("b"));
    ob.append(assistantPayload("c"));
    expect(ob.ack(2)).toBe(false); // not final
    expect(ob.unacked().map((e) => Number(e.sequence))).toEqual([3]);
    ob.close();
  });

  it("deletes the WAL when the final outcome is acked", () => {
    const ob = new EventOutbox(dir, "turn-1", 100);
    ob.append(assistantPayload("a"));
    const outcome = ob.append(assistantPayload("done"));
    ob.markFinal(Number(outcome.sequence));
    expect(existsSync(join(dir, "turn-1.wal"))).toBe(true);
    expect(ob.ack(Number(outcome.sequence))).toBe(true); // final acked
    expect(existsSync(join(dir, "turn-1.wal"))).toBe(false); // WAL deleted
  });

  it("rejects a regressing ack (no-op)", () => {
    const ob = new EventOutbox(dir, "turn-1", 100);
    ob.append(assistantPayload("a"));
    ob.ack(1);
    expect(ob.ack(0)).toBe(false); // regress — no-op
    ob.close();
  });

  it("throws OutboxOverflow when the bound is exceeded", () => {
    const ob = new EventOutbox(dir, "turn-1", 2);
    ob.append(assistantPayload("a"));
    ob.append(assistantPayload("b"));
    expect(() => ob.append(assistantPayload("c"))).toThrow(OutboxOverflow);
    ob.close();
  });
});

describe("EventOutbox crash recovery", () => {
  it("recovers unACKed events after a crash (no final ack)", () => {
    // Simulate a crash: append events, ack some, then "die" (never ack the final).
    const live = new EventOutbox(dir, "turn-1", 100);
    live.append(assistantPayload("a")); // seq 1
    live.append(assistantPayload("b")); // seq 2
    live.append(assistantPayload("c")); // seq 3 (unacked)
    live.ack(1); // ack seq 1; 2 + 3 remain
    // simulate crash: abandon the outbox (don't close cleanly — the WAL persists)
    // (in production the supervisor process dies; the WAL file remains on disk)

    const recovered = EventOutbox.recover(dir);
    expect(recovered).toHaveLength(1);
    expect(recovered[0]!.turnId).toBe("turn-1");
    expect(recovered[0]!.events.map((e) => Number(e.sequence))).toEqual([2, 3]);
    expect(recovered[0]!.events[0]!.kind.case).toBe("assistantMessage");
  });

  it("returns nothing when all turns finished (WALs deleted on final ack)", () => {
    const live = new EventOutbox(dir, "turn-1", 100);
    live.append(assistantPayload("a"));
    const fin = live.append(assistantPayload("done"));
    live.markFinal(Number(fin.sequence));
    live.ack(Number(fin.sequence)); // deletes the WAL

    expect(EventOutbox.recover(dir)).toEqual([]);
  });

  it("recovers multiple turns independently", () => {
    const a = new EventOutbox(dir, "turn-a", 100);
    a.append(assistantPayload("a1"));
    a.close();
    const b = new EventOutbox(dir, "turn-b", 100);
    b.append(assistantPayload("b1"));
    b.close();

    const recovered = EventOutbox.recover(dir).sort((x, y) => x.turnId.localeCompare(y.turnId));
    expect(recovered.map((r) => r.turnId)).toEqual(["turn-a", "turn-b"]);
  });
});
