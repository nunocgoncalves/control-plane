import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { createRouterTransport } from "@connectrpc/connect";
import { create } from "@bufbuild/protobuf";
import { mkdtempSync, rmSync, chmodSync, mkdirSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, basename } from "node:path";
import {
  Harness,
  WorkerMessageSchema,
  HelloSchema,
  ControlMessageSchema,
  WelcomeSchema,
  AssignTurnSchema,
  SandboxRefSchema,
  EventAckSchema,
  AssistantMessageSchema,
  Outcome,
  type AssignTurn,
  type WorkerMessage,
} from "./gen/iterabase/harness/v1/harness_pb.js";
import { Supervisor, type Child, type ChildEvent, type ChildResult, type TurnEventPayload } from "./supervisor.js";
import { Probes } from "./probes.js";
import { EventOutbox } from "./event-outbox.js";
import type { HarnessConfig } from "./config.js";

// Minimal async queue for the FakeChild's event stream.
class Q<T> implements AsyncIterable<T> {
  private buf: T[] = [];
  private closed = false;
  private waiters: Array<(r: IteratorResult<T>) => void> = [];
  push(v: T): void {
    if (this.closed) return;
    const w = this.waiters.shift();
    if (w) w({ value: v, done: false });
    else this.buf.push(v);
  }
  close(): void {
    this.closed = true;
    for (const w of this.waiters) w({ value: undefined as never, done: true });
    this.waiters.length = 0;
  }
  [Symbol.asyncIterator](): AsyncIterator<T> {
    return {
      next: () => {
        if (this.buf.length) return Promise.resolve({ value: this.buf.shift() as T, done: false });
        if (this.closed) return Promise.resolve({ value: undefined as never, done: true });
        return new Promise((r) => this.waiters.push(r));
      },
    };
  }
}

/** A FakeChild: emits the given events, then resolves `result`. */
function fakeChild(events: ChildEvent[], outcome: Outcome, message?: string): Child {
  const q = new Q<ChildEvent>();
  for (const e of events) q.push(e);
  q.close();
  const result = Promise.resolve<ChildResult>({ outcome, message });
  return { abort: () => {}, events: q, result };
}

const UID = process.getuid();
const GID = process.getgid();

function makeCfg(sandboxRoot: string, walDir: string): HarnessConfig {
  return {
    controlPlane: { url: "https://cp", serverName: "cp" },
    worker: { workerId: "pod-1", poolId: "pool-1" },
    tls: { cert: "", key: "", ca: "" },
    sandboxRoot,
    piDirs: [],
    egressProxyUrl: "",
    walDir,
    probe: { port: 0 },
    transport: { http2PingIntervalMs: 30000, http2PingTimeoutMs: 10000 },
    reconnect: { initialBackoffMs: 1, maxBackoffMs: 2, resetAfterMs: 1000 },
    child: { livenessIntervalMs: 1000, abortGraceMs: 1000 },
    outbox: { bound: 4096 },
    modelRetry: { maxAttempts: 3 },
    tokenDelta: { sendBufferBytes: 1048576 },
  } as HarnessConfig;
}

describe("Supervisor turn loop", () => {
  let sandboxParent: string;
  let walDir: string;
  let sandboxId: string;
  let probes: Probes;

  beforeEach(() => {
    sandboxParent = mkdtempSync(join(tmpdir(), "harness-sup-"));
    walDir = mkdtempSync(join(tmpdir(), "harness-wal-"));
    sandboxId = "sess-a";
    const root = join(sandboxParent, sandboxId);
    mkdirSync(root, { recursive: true }); // the sandbox dir (owned by UID/GID)
    chmodSync(root, 0o700);
    probes = new Probes();
  });
  afterEach(() => {
    rmSync(sandboxParent, { recursive: true, force: true });
    rmSync(walDir, { recursive: true, force: true });
  });

  it("connects, runs a turn (child events + COMPLETED), ACKs, re-advertises a credit", async () => {
    const received: WorkerMessage[] = [];
    let assignTurnSent = false;

    const transport = createRouterTransport((router) => {
      router.service(Harness, {
        async *work(req) {
          yield create(ControlMessageSchema, {
            kind: {
              case: "welcome",
              value: create(WelcomeSchema, {
                protocolVersion: "1",
                fencingGeneration: 1n,
                heartbeatIntervalMs: 60000,
                leaseTimeoutMs: 120000,
              }),
            },
          });
          for await (const m of req) {
            received.push(m);
            if (m.kind.case === "ready" && !assignTurnSent) {
              assignTurnSent = true;
              yield assignTurn(sandboxId);
            } else if (m.kind.case === "turnEvent") {
              const te = m.kind.value;
              if (te.kind.case === "workerOutcome") {
                yield create(ControlMessageSchema, {
                  kind: {
                    case: "eventAck",
                    value: create(EventAckSchema, { turnId: te.turnId, throughSequence: te.sequence }),
                  },
                });
              }
            }
          }
        },
      });
    });

    let credits = 0;
    const credit2 = new Promise<void>((r) => {
      const orig = () => {
        credits += 1;
        if (credits === 2) r();
      };
      // patch: stash orig for the dep callback
      (globalThis as { __creditCb?: () => void }).__creditCb = orig;
    });
    const onCreditAdvertised = () => (globalThis as { __creditCb?: () => void }).__creditCb?.();

    const sup = new Supervisor({
      cfg: makeCfg(sandboxParent, walDir),
      hello: create(WorkerMessageSchema, {
        kind: { case: "hello", value: create(HelloSchema, { workerId: "pod-1", poolId: "pool-1" }) },
      }),
      childFactory: () =>
        fakeChild(
          [
            {
              payload: {
                case: "assistantMessage",
                value: create(AssistantMessageSchema, { text: "classified: pricing" }),
              },
            },
          ],
          Outcome.COMPLETED,
        ),
      probes,
      transport: () => transport,
      onCreditAdvertised,
    });

    const runP = sup.run();
    await credit2; // initial credit + post-turn credit
    await sup.drain();
    await runP;

    // The CP received: Hello(initial, via openWorkStream), Ready x2, the assistant message, the COMPLETED outcome, + heartbeats.
    const kinds = received.map((m) => m.kind.case);
    expect(kinds.filter((k) => k === "ready").length).toBe(2);
    expect(kinds).toContain("turnEvent");
    const outcomes = received
      .filter((m) => m.kind.case === "turnEvent" && m.kind.value.kind.case === "workerOutcome")
      .map((m) => m.kind!.value!.kind!.value as { outcome: Outcome });
    expect(outcomes[0]?.outcome).toBe(Outcome.COMPLETED);
  });

  it("emits FAILED when the sandbox is missing (not provisioned)", async () => {
    const received: WorkerMessage[] = [];
    let assignTurnSent = false;
    const transport = createRouterTransport((router) => {
      router.service(Harness, {
        async *work(req) {
          yield create(ControlMessageSchema, {
            kind: { case: "welcome", value: create(WelcomeSchema, { fencingGeneration: 1n }) } as never,
          });
          for await (const m of req) {
            received.push(m);
            if (m.kind.case === "ready" && !assignTurnSent) {
              assignTurnSent = true;
              yield assignTurn("no-such-sandbox");
            } else if (m.kind.case === "turnEvent" && m.kind.value.kind.case === "workerOutcome") {
              yield create(ControlMessageSchema, {
                kind: {
                  case: "eventAck",
                  value: create(EventAckSchema, {
                    turnId: m.kind.value.turnId,
                    throughSequence: m.kind.value.sequence,
                  }),
                },
              });
            }
          }
        },
      });
    });

    let credits = 0;
    const credit2 = new Promise<void>((r) => {
      (globalThis as { __creditCb?: () => void }).__creditCb = () => {
        credits += 1;
        if (credits === 2) r();
      };
    });
    const sup = new Supervisor({
      cfg: makeCfg(sandboxParent, walDir),
      hello: create(WorkerMessageSchema, {
        kind: { case: "hello", value: create(HelloSchema, { workerId: "pod-1", poolId: "pool-1" }) },
      }),
      childFactory: () => fakeChild([], Outcome.COMPLETED), // never reached (sandbox invalid)
      probes,
      transport: () => transport,
      onCreditAdvertised: () => (globalThis as { __creditCb?: () => void }).__creditCb?.(),
    });

    const runP = sup.run();
    await credit2;
    await sup.drain();
    await runP;

    const outcomes = received
      .filter((m) => m.kind.case === "turnEvent" && m.kind.value.kind.case === "workerOutcome")
      .map((m) => m.kind!.value!.kind!.value as { outcome: Outcome });
    expect(outcomes[0]?.outcome).toBe(Outcome.FAILED);
    // A harness error preceded the outcome.
    const hasHarnessError = received.some(
      (m) => m.kind.case === "turnEvent" && m.kind.value.kind.case === "harnessError",
    );
    expect(hasHarnessError).toBe(true);
  });
});

describe("Supervisor crash recovery", () => {
  let sandboxParent: string;
  let walDir: string;
  let probes: Probes;

  beforeEach(() => {
    sandboxParent = mkdtempSync(join(tmpdir(), "harness-sup-"));
    walDir = mkdtempSync(join(tmpdir(), "harness-wal-"));
    probes = new Probes();
  });
  afterEach(() => {
    rmSync(sandboxParent, { recursive: true, force: true });
    rmSync(walDir, { recursive: true, force: true });
  });

  it("replays a crashed turn's unacked WAL events as after_terminal after Welcome", async () => {
    // Simulate a crashed supervisor: a prior turn wrote an event to the WAL but
    // never acked it (the supervisor died mid-turn). The WAL file persists.
    const payload: TurnEventPayload = {
      case: "assistantMessage",
      value: create(AssistantMessageSchema, { text: "crashed-mid-turn" }),
    };
    const dead = new EventOutbox(walDir, "turn-crash", 100);
    dead.append(payload); // seq 1, WAL'd, never acked
    dead.close(); // release the fd; the WAL file remains

    const received: WorkerMessage[] = [];
    let replayDone!: () => void;
    const replayed = new Promise<void>((r) => (replayDone = r));
    const transport = createRouterTransport((router) => {
      router.service(Harness, {
        async *work(req) {
          yield create(ControlMessageSchema, {
            kind: { case: "welcome", value: create(WelcomeSchema, { fencingGeneration: 2n }) },
          });
          for await (const m of req) {
            received.push(m);
            if (m.kind.case === "turnEvent") replayDone();
          }
        },
      });
    });

    // Constructing the supervisor loads the unfinished WAL (EventOutbox.recover).
    const sup = new Supervisor({
      cfg: makeCfg(sandboxParent, walDir),
      hello: create(WorkerMessageSchema, {
        kind: { case: "hello", value: create(HelloSchema, { workerId: "pod-1", poolId: "pool-1" }) },
      }),
      childFactory: () => fakeChild([], Outcome.COMPLETED), // not used (no AssignTurn)
      probes,
      transport: () => transport,
    });

    const runP = sup.run();
    await replayed; // the crashed turn's event was replayed after Welcome
    await sup.drain();
    await runP;

    const replayedEvent = received.find(
      (m) => m.kind.case === "turnEvent" && m.kind.value.kind.case === "assistantMessage",
    );
    expect(replayedEvent).toBeDefined();
    expect(replayedEvent!.kind!.value!.turnId).toBe("turn-crash");
    expect(Number(replayedEvent!.kind!.value!.sequence)).toBe(1);
    // The WAL was deleted after replay (the turn is durably done as after_terminal).
    expect(existsSync(join(walDir, "turn-crash.wal"))).toBe(false);
  });
});

function assignTurn(sandboxId: string): ControlMessageLike {
  return create(ControlMessageSchema, {
    kind: {
      case: "assignTurn",
      value: create(AssignTurnSchema, {
        turnId: "turn-1",
        sessionId: "sess-1",
        sandbox: create(SandboxRefSchema, { sandboxId, uid: UID, gid: GID, workingDir: "home" }),
        persona: "you are an agent",
        message: "classify this email",
      }) as AssignTurn,
    },
  }) as ControlMessageLike;
}

type ControlMessageLike = ReturnType<typeof create<never>> extends never ? never : import("./gen/iterabase/harness/v1/harness_pb.js").ControlMessage;
