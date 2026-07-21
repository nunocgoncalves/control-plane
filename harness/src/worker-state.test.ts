import { describe, it, expect } from "vitest";
import { WorkerState, ProtocolError } from "./worker-state.js";

describe("WorkerState handshake", () => {
  it("transitions disconnected -> connecting -> idle on Hello/Welcome", () => {
    const s = new WorkerState();
    expect(s.phase).toBe("disconnected");
    expect(s.canAdvertiseCredit).toBe(false);
    s.onConnecting();
    expect(s.phase).toBe("connecting");
    s.onWelcome();
    expect(s.phase).toBe("idle");
    expect(s.canAdvertiseCredit).toBe(true);
  });

  it("forbids advertising credit before Welcome", () => {
    const s = new WorkerState();
    s.onConnecting();
    expect(() => s.advertiseCredit()).toThrow(ProtocolError);
  });

  it("forbids Welcome from a non-connecting phase", () => {
    const s = new WorkerState();
    expect(() => s.onWelcome()).toThrow(ProtocolError); // disconnected
    s.onConnecting();
    s.onWelcome();
    expect(() => s.onWelcome()).toThrow(ProtocolError); // idle
  });
});

describe("WorkerState single-credit invariant", () => {
  it("runs the happy path: Ready -> AssignTurn -> child exit -> ACK -> idle", () => {
    const s = new WorkerState();
    s.onConnecting();
    s.onWelcome();
    s.advertiseCredit();
    expect(s.phase).toBe("armed");
    s.onAssignTurn("turn-1");
    expect(s.phase).toBe("running");
    expect(s.activeTurnId).toBe("turn-1");
    expect(s.hasActiveTurn).toBe(true);
    s.onChildExited();
    expect(s.phase).toBe("cleaning");
    s.onOutcomeAcked();
    expect(s.phase).toBe("idle");
    expect(s.activeTurnId).toBeNull();
    expect(s.canAdvertiseCredit).toBe(true); // ready for the next credit
  });

  it("forbids AssignTurn without a Ready credit (the invariant)", () => {
    const s = new WorkerState();
    s.onConnecting();
    s.onWelcome();
    expect(() => s.onAssignTurn("turn-1")).toThrow(ProtocolError); // idle, not armed
    s.advertiseCredit();
    s.onAssignTurn("turn-1");
    // a second AssignTurn before the next Ready is a violation
    expect(() => s.onAssignTurn("turn-2")).toThrow(ProtocolError);
  });

  it("forbids advertising a second credit while a turn is active", () => {
    const s = new WorkerState();
    s.onConnecting();
    s.onWelcome();
    s.advertiseCredit();
    s.onAssignTurn("turn-1");
    expect(() => s.advertiseCredit()).toThrow(ProtocolError); // running
    s.onChildExited();
    expect(() => s.advertiseCredit()).toThrow(ProtocolError); // cleaning (no ACK yet)
  });

  it("forbids outcome ACK except in cleaning", () => {
    const s = new WorkerState();
    s.onConnecting();
    s.onWelcome();
    expect(() => s.onOutcomeAcked()).toThrow(ProtocolError); // idle
  });

  it("forbids AssignTurn with an empty turn_id", () => {
    const s = new WorkerState();
    s.onConnecting();
    s.onWelcome();
    s.advertiseCredit();
    expect(() => s.onAssignTurn("")).toThrow(ProtocolError);
  });
});

describe("WorkerState drain / stream-loss / fatal", () => {
  it("draining forbids new credits", () => {
    const s = new WorkerState();
    s.onConnecting();
    s.onWelcome();
    s.beginDrain();
    expect(s.phase).toBe("draining");
    expect(() => s.advertiseCredit()).toThrow(ProtocolError);
  });

  it("stream loss resets to disconnected and clears the active turn", () => {
    const s = new WorkerState();
    s.onConnecting();
    s.onWelcome();
    s.advertiseCredit();
    s.onAssignTurn("turn-1");
    s.onStreamLost();
    expect(s.phase).toBe("disconnected");
    expect(s.activeTurnId).toBeNull();
  });

  it("fatal is terminal and drain becomes a no-op", () => {
    const s = new WorkerState();
    s.fatal();
    expect(s.phase).toBe("fatal");
    s.beginDrain(); // no-op from fatal
    expect(s.phase).toBe("fatal");
  });
});
