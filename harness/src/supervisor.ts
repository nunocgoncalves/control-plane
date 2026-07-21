// The warm-worker supervisor (HOR-381). Orchestrates the long-lived Work bidi
// stream: connect -> Hello -> Welcome -> (replay any pending audit tail from a
// prior crash/stream-loss) -> advertise a Ready credit -> on AssignTurn,
// validate the sandbox + spawn the child -> sequence + stream the child's
// durable TurnEvents (WAL'd before send) -> emit the final WorkerOutcome ->
// await the cumulative EventAck -> re-advertise a credit. Reconnects with
// bounded backoff + jitter on stream loss (fail-closed: abort/kill the child,
// never resume the execution — the unacked audit tail is replayed as
// after_terminal on reconnect); resets backoff after a stable Welcome.
// Heartbeats at the Welcome lease interval while a turn is active.
//
// The supervisor never imports pi/extensions/tools — it talks to the Child
// abstraction (spawned per turn). The real Child (pi via the setpriv launcher +
// IPC) lands in a later step; a FakeChild is used for the step-5 tests. The
// token-delta forwarding + the full event mapper land later.

import { create } from "@bufbuild/protobuf";
import { unlinkSync } from "node:fs";
import { join } from "node:path";
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
import { resolveSandboxRoot, validateSandbox, resolveWorkingDir, SandboxError, type SandboxPaths } from "./sandbox.js";
import type { Probes } from "./probes.js";
import { EventOutbox, OutboxOverflow } from "./event-outbox.js";

/** A durable TurnEvent payload (the oneof) the supervisor sequences + sends. */
export type TurnEventPayload = TurnEvent["kind"];
export interface ChildEvent {
  payload: TurnEventPayload;
}
export interface ChildResult {
  outcome: Outcome;
  message?: string;
}
export interface Child {
  abort(): void;
  events: AsyncIterable<ChildEvent>;
  result: Promise<ChildResult>;
}
export type ChildFactory = (assignment: AssignTurn, sandbox: SandboxPaths, cwd: string) => Child;

export class SupervisorError extends Error {}

interface TurnCtx {
  turnId: string;
  aborted: boolean;
  acked: Promise<void>;
  resolveAck: () => void;
}

interface PendingReplay {
  turnId: string;
  events: TurnEvent[]; // unacked durable events to re-send as after_terminal audit
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
  private outbox: EventOutbox | null = null;
  private currentChild: Child | null = null;
  private heartbeat: ReturnType<typeof setInterval> | null = null;
  private running = false;
  /** Unacked audit tail from a prior crash (WAL recover) or stream-loss, replayed after the next Welcome. */
  private pendingReplay: PendingReplay[] = [];

  constructor(private readonly d: SupervisorDeps) {
    // Crash recovery: load unfinished turn WALs at startup (replayed as after_terminal
    // after the first Welcome — the CP has already terminalized them as worker-loss).
    this.pendingReplay = EventOutbox.recover(d.cfg.walDir);
  }

  async run(): Promise<void> {
    this.running = true;
    let backoff = this.d.cfg.reconnect.initialBackoffMs;
    const min = this.d.cfg.reconnect.initialBackoffMs;
    const max = this.d.cfg.reconnect.maxBackoffMs;
    while (this.running && (this.state.phase as string) !== "fatal" && (this.state.phase as string) !== "draining") {
      const connectedAt = Date.now();
      try {
        await this.connectAndServe();
        return; // clean exit (drained)
      } catch (err) {
        if ((this.state.phase as string) === "fatal") return;
        this.failClosed(err);
        const after = this.state.phase as string;
        if (!this.running || after === "fatal" || after === "draining") return;
        if (Date.now() - connectedAt >= this.d.cfg.reconnect.resetAfterMs) backoff = min;
        await sleep(jitter(backoff, min));
        backoff = Math.min(backoff * 2, max);
      }
    }
  }

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
    this.replayPending(); // crash-recovery / stream-loss tail (after_terminal audit)
    this.advertiseCredit();
    for await (const msg of conn.stream.control) {
      this.onControl(msg);
      const phase = this.state.phase as string;
      if (phase === "draining" || phase === "fatal") break;
    }
    if ((this.state.phase as string) !== "draining" && (this.state.phase as string) !== "fatal") {
      throw new SupervisorError("control stream ended");
    }
  }

  /** Re-send staged unacked events (after_terminal) + delete their WALs. */
  private replayPending(): void {
    if (this.pendingReplay.length === 0) return;
    for (const r of this.pendingReplay) {
      for (const ev of r.events) this.stream?.send(turnEventMessage(ev));
      try {
        unlinkSync(join(this.d.cfg.walDir, `${r.turnId}.wal`));
      } catch {
        /* already gone */
      }
    }
    this.pendingReplay = [];
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
        // Final-outcome ACK resolves the turn; intermediate acks just advance the outbox cursor.
        if (this.outbox && this.outbox.ack(Number(ack.throughSequence)) && this.turn) this.turn.resolveAck();
        return;
      }
      case "welcome":
        return;
    }
  }

  private advertiseCredit(): void {
    if (!this.state.canAdvertiseCredit) return;
    this.state.advertiseCredit();
    this.stream?.send(create(WorkerMessageSchema, { kind: { case: "ready", value: create(ReadySchema, {}) } }));
    this.d.onCreditAdvertised?.();
  }

  private async handleAssignTurn(at: AssignTurn): Promise<void> {
    if (this.turn) return; // single credit
    let resolveAck!: () => void;
    const acked = new Promise<void>((r) => (resolveAck = r));
    this.turn = { turnId: at.turnId, aborted: false, acked, resolveAck };
    this.outbox = new EventOutbox(this.d.cfg.walDir, at.turnId, this.d.cfg.outbox.bound);
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
      if (err instanceof OutboxOverflow) this.sendHarnessError(`outbox overflow: ${message}`);
      this.emitOutcome(Outcome.FAILED, message);
    } finally {
      this.stopHeartbeat();
      this.currentChild = null;
    }
    await this.turn.acked;
    if ((this.state.phase as string) === "cleaning") {
      this.state.onOutcomeAcked();
      this.turn = null;
      this.outbox = null; // WAL already deleted by outbox.ack on final ACK
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

  /** Append the durable event to the WAL+outbox, then send. Marks the final outcome. */
  private sendChildEvent(payload: TurnEventPayload): void {
    const ob = this.outbox;
    if (!ob) return;
    const te = ob.append(payload);
    if (payload.case === "workerOutcome") ob.markFinal(Number(te.sequence));
    this.stream?.send(turnEventMessage(te));
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
    if (!this.outbox) return;
    this.sendChildEvent({
      case: "workerOutcome",
      value: create(WorkerOutcomeSchema, { outcome, message: message ?? "" }),
    });
    try {
      this.state.onChildExited();
    } catch {
      /* already cleaning/draining */
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
      const ob = this.outbox;
      this.stream?.send(
        create(WorkerMessageSchema, {
          kind: {
            case: "heartbeat",
            value: create(HeartbeatSchema, {
              state: WorkerState.RUNNING,
              turnId,
              piPhase: PiPhase.MODEL_CALL, // placeholder until the child reports phases (IPC, later step)
              highestSequence: BigInt(ob?.highestSequence ?? 0),
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
    // Stage the unacked audit tail for replay (after_terminal) on reconnect;
    // the CP terminalizes the turn as worker-loss via fencing (HOR-249).
    this.abortActiveTurn();
    this.stopHeartbeat();
    if (this.outbox && this.turn) {
      this.pendingReplay.push({ turnId: this.turn.turnId, events: this.outbox.unacked() });
      this.outbox.close(); // WAL persists on disk for crash recovery; fd released
      this.outbox = null;
    }
    if (this.turn) {
      this.turn.resolveAck(); // unblock handleAssignTurn so run() can reconnect
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

/** Wrap a TurnEvent in a WorkerMessage (no re-sequencing — preserves the WAL'd sequence for dedup). */
function turnEventMessage(te: TurnEvent): WorkerMessage {
  return create(WorkerMessageSchema, { kind: { case: "turnEvent", value: te } });
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}
function jitter(backoff: number, min: number): number {
  return Math.floor(min + Math.random() * Math.max(0, backoff - min + 1));
}
