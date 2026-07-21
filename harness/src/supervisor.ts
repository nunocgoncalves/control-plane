// The warm-worker supervisor (HOR-381). Orchestrates the long-lived Work bidi
// stream: connect -> Hello -> Welcome -> advertise a Ready credit -> on
// AssignTurn, validate the sandbox + spawn the child -> sequence + stream the
// child's durable TurnEvents -> emit the final WorkerOutcome -> await the
// cumulative EventAck -> re-advertise a credit. Reconnects with bounded
// backoff + jitter on stream loss (fail-closed: abort/kill the child, never
// resume the execution); resets backoff after a stable Welcome. Heartbeats at
// the Welcome lease interval while a turn is active.
//
// The supervisor never imports pi/extensions/tools — it talks to the Child
// abstraction (spawned per turn). The real Child (pi via the setpriv launcher +
// IPC) lands in a later step; a FakeChild is used for the step-5 tests. The
// WAL-backed outbox + token-delta forwarding + the full event mapper land
// later; this is the protocol/turn-loop backbone.

import { create } from "@bufbuild/protobuf";
import type { Transport } from "@connectrpc/connect";
import {
  ErrorDetailSchema,
  HarnessErrorSchema,
  HeartbeatSchema,
  Outcome,
  PiPhase,
  ReadySchema,
  Retryability,
  TurnEventSchema,
  WorkerMessageSchema,
  WorkerOutcomeSchema,
  WorkerState,
  type AssignTurn,
  type ControlMessage,
  type TurnEvent,
  type WorkerMessage,
} from "./gen/iterabase/harness/v1/harness_pb.js";
import type { HarnessConfig } from "./config.js";
import { createWorkTransport, openWorkStream, type WorkStream, type Welcome } from "./work-client.js";
import { WorkerState as WorkerStateMachine, ProtocolError } from "./worker-state.js";
import { resolveSandboxRoot, validateSandbox, resolveWorkingDir, SandboxError, sandboxSubpaths, type SandboxPaths } from "./sandbox.js";
import type { Probes } from "./probes.js";

/** A durable TurnEvent payload (the oneof) the supervisor sequences + sends. */
export type TurnEventPayload = TurnEvent["kind"];

/** One child lifecycle event: a durable TurnEvent payload. */
export interface ChildEvent {
  payload: TurnEventPayload;
}
/** The child's self-assessed result after shutdown (the supervisor emits WorkerOutcome). */
export interface ChildResult {
  outcome: Outcome;
  message?: string;
}
/** A per-turn child. The supervisor sequences its events + classifies its result. */
export interface Child {
  abort(): void;
  events: AsyncIterable<ChildEvent>;
  result: Promise<ChildResult>;
}
export type ChildFactory = (assignment: AssignTurn, sandbox: SandboxPaths, cwd: string) => Child;

export class SupervisorError extends Error {}

interface TurnCtx {
  turnId: string;
  sequence: number;
  finalSequence: number | null; // set when WorkerOutcome is emitted
  aborted: boolean;
  acked: Promise<void>;
  resolveAck: () => void;
}

export interface SupervisorDeps {
  cfg: HarnessConfig;
  hello: WorkerMessage;
  childFactory: ChildFactory;
  probes: Probes;
  /** Transport factory for (re)connect. Defaults to createWorkTransport (mTLS). */
  transport?: () => Transport;
  /** Test hook: invoked each time the supervisor advertises a Ready credit. */
  onCreditAdvertised?: () => void;
}

export class Supervisor {
  private readonly state = new WorkerStateMachine();
  private stream: WorkStream | null = null;
  private welcome: Welcome | null = null;
  private turn: TurnCtx | null = null;
  private currentChild: Child | null = null;
  private heartbeat: ReturnType<typeof setInterval> | null = null;
  private running = false;

  constructor(private readonly d: SupervisorDeps) {}

  /** Run the connect/turn loop until drained or fatal. Reconnects on stream loss. */
  async run(): Promise<void> {
    this.running = true;
    let backoff = this.d.cfg.reconnect.initialBackoffMs;
    const min = this.d.cfg.reconnect.initialBackoffMs;
    const max = this.d.cfg.reconnect.maxBackoffMs;
    while (this.running && this.state.phase !== "fatal" && this.state.phase !== "draining") {
      const connectedAt = Date.now();
      try {
        await this.connectAndServe();
        return; // clean exit (drained)
      } catch (err) {
        // this.state.phase is a getter that mutates across the await (failClosed/fatal);
        // cast to string so TS doesn't carry the while-loop's stale narrowing.
        if ((this.state.phase as string) === "fatal") return;
        this.failClosed(err);
        const after = this.state.phase as string;
        if (!this.running || after === "fatal" || after === "draining") return;
        // Reset backoff after a stable connection; otherwise exponential growth.
        if (Date.now() - connectedAt >= this.d.cfg.reconnect.resetAfterMs) backoff = min;
        await sleep(jitter(backoff, min));
        backoff = Math.min(backoff * 2, max);
      }
    }
  }

  /** SIGTERM: no new credits; abort the active turn, close the stream, exit. */
  async drain(): Promise<void> {
    this.running = false;
    this.state.beginDrain();
    this.d.probes.setReady(false);
    this.abortActiveTurn();
    this.stream?.close();
  }

  private async connectAndServe(): Promise<void> {
    const transport = (this.d.transport ?? (() => createWorkTransport(this.d.cfg)))();
    this.state.onConnecting();
    const conn = await openWorkStream(this.d.hello, transport);
    this.stream = conn.stream;
    this.welcome = conn.welcome;
    this.state.onWelcome();
    this.d.probes.setReady(true);
    this.advertiseCredit();
    for await (const msg of conn.stream.control) {
      this.onControl(msg);
      const phase = this.state.phase;
      if (phase === "draining" || phase === "fatal") break;
    }
    if (this.state.phase !== "draining" && this.state.phase !== "fatal") {
      throw new SupervisorError("control stream ended");
    }
  }

  private onControl(msg: ControlMessage): void {
    switch (msg.kind.case) {
      case "assignTurn":
        void this.handleAssignTurn(msg.kind.value);
        return;
      case "abortTurn":
        if (this.turn && this.turn.turnId === msg.kind.value.turnId) this.abortActiveTurn();
        return;
      case "eventAck": {
        const ack = msg.kind.value;
        const t = this.turn;
        if (t && t.finalSequence !== null && Number(ack.throughSequence) >= t.finalSequence) t.resolveAck();
        return;
      }
      case "welcome":
        return; // consumed in openWorkStream; ignore a stray
    }
  }

  private advertiseCredit(): void {
    if (!this.state.canAdvertiseCredit) return;
    this.state.advertiseCredit();
    this.stream?.send(create(WorkerMessageSchema, { kind: { case: "ready", value: create(ReadySchema, {}) } }));
    this.d.onCreditAdvertised?.();
  }

  private async handleAssignTurn(at: AssignTurn): Promise<void> {
    if (this.turn) return; // single credit — shouldn't happen
    let resolveAck!: () => void;
    const acked = new Promise<void>((r) => (resolveAck = r));
    this.turn = { turnId: at.turnId, sequence: 0, finalSequence: null, aborted: false, acked, resolveAck };
    try {
      this.state.onAssignTurn(at.turnId);
    } catch (err) {
      if (err instanceof ProtocolError) return this.fatal(err);
      throw err;
    }
    this.startHeartbeat(at.turnId);
    try {
      const sandbox = this.resolveSandbox(at);
      const cwd = resolveWorkingDir(sandbox.root, at.sandbox?.workingDir || "home");
      const child = this.d.childFactory(at, sandbox, cwd);
      this.currentChild = child;
      for await (const ev of child.events) {
        if (this.turn?.aborted) break;
        this.sendChildEvent(ev.payload);
      }
      const result = await child.result;
      this.emitOutcome(result.outcome, result.message);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      if (err instanceof SandboxError) this.sendHarnessError(`sandbox invalid: ${message}`);
      this.emitOutcome(Outcome.FAILED, message);
    } finally {
      this.stopHeartbeat();
      this.currentChild = null;
    }
    // Wait for the final outcome to be ACKed before re-advertising a credit.
    // On stream loss, failClosed resolves acked early and phase != cleaning;
    // tolerate that so this turn unblocks and run() reconnects.
    await this.turn.acked;
    if (this.state.phase === "cleaning") {
      this.state.onOutcomeAcked();
      this.turn = null;
      this.advertiseCredit();
    } else {
      this.turn = null; // disrupted (stream loss / drain) — run() handles reconnect/exit
    }
  }

  private resolveSandbox(at: AssignTurn): SandboxPaths {
    const sb = at.sandbox;
    if (!sb) throw new SandboxError("AssignTurn missing sandbox");
    const root = resolveSandboxRoot(this.d.cfg.sandboxRoot, sb.sandboxId);
    return validateSandbox(root, sb.uid, sb.gid);
  }

  private sendChildEvent(payload: TurnEventPayload): void {
    const t = this.turn;
    if (!t) return;
    t.sequence += 1;
    this.stream?.send(
      create(WorkerMessageSchema, {
        kind: {
          case: "turnEvent",
          value: create(TurnEventSchema, {
            turnId: t.turnId,
            sequence: BigInt(t.sequence),
            timestampMs: BigInt(Date.now()),
            kind: payload,
          }),
        },
      }),
    );
  }

  private sendHarnessError(message: string): void {
    this.sendChildEvent({
      case: "harnessError",
      value: create(HarnessErrorSchema, {
        error: create(ErrorDetailSchema, { message, retryability: Retryability.NON_RETRYABLE }),
      }),
    });
  }

  private emitOutcome(outcome: Outcome, message?: string): void {
    const t = this.turn;
    if (!t || t.finalSequence !== null) return;
    this.sendChildEvent({
      case: "workerOutcome",
      value: create(WorkerOutcomeSchema, { outcome, message: message ?? "" }),
    });
    t.finalSequence = t.sequence;
    try {
      this.state.onChildExited();
    } catch {
      /* already cleaning/draining — fine */
    }
  }

  private abortActiveTurn(): void {
    if (this.turn) this.turn.aborted = true;
    this.currentChild?.abort();
  }

  private startHeartbeat(turnId: string): void {
    this.stopHeartbeat();
    const interval = this.welcome?.heartbeatIntervalMs ?? this.d.cfg.child.livenessIntervalMs;
    this.heartbeat = setInterval(() => {
      const t = this.turn;
      if (!t) return;
      this.stream?.send(
        create(WorkerMessageSchema, {
          kind: {
            case: "heartbeat",
            value: create(HeartbeatSchema, {
              state: WorkerState.RUNNING,
              turnId,
              piPhase: PiPhase.MODEL_CALL, // placeholder until the child reports phases (IPC, later step)
              highestSequence: BigInt(t.sequence),
            }),
          },
        }),
      );
    }, interval);
  }
  private stopHeartbeat(): void {
    if (this.heartbeat) clearInterval(this.heartbeat);
    this.heartbeat = null;
  }

  private failClosed(err: unknown): void {
    // Stream loss / connect failure mid-turn: abort the child, never resume.
    // The CP terminalizes the turn as worker-loss via fencing (HOR-249); the
    // supervisor does NOT emit a spurious outcome here (it'd be after_terminal).
    this.abortActiveTurn();
    this.stopHeartbeat();
    const t = this.turn;
    if (t) {
      t.resolveAck(); // unblock handleAssignTurn so run() can reconnect
      this.turn = null;
    }
    this.d.probes.setReady(false);
    this.state.onStreamLost();
    if (err instanceof Error) console.error(`harness: stream lost: ${err.message}`);
  }

  private fatal(err: unknown): void {
    this.state.fatal();
    this.d.probes.setHealthy(false);
    this.d.probes.setReady(false);
    console.error(`harness: fatal: ${err instanceof Error ? err.message : err}`);
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}
/** Full-jitter backoff in [min, backoff]. */
function jitter(backoff: number, min: number): number {
  return Math.floor(min + Math.random() * Math.max(0, backoff - min + 1));
}

// Re-export for callers/tests that build sandbox subpaths.
export { sandboxSubpaths };
