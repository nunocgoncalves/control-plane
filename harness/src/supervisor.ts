// The warm-worker supervisor (HOR-381). Orchestrates the long-lived Work bidi
// stream: connect -> Hello -> Welcome -> (replay any pending audit tail from a
// prior crash/stream-loss, gated on its cumulative ACK) -> advertise a Ready
// credit only once no replay is outstanding -> on AssignTurn, validate the
// sandbox + pool scope -> emit ExecutionStarted -> spawn the child -> sequence
// + stream the child's durable TurnEvents (WAL'd before send) + forward
// ephemeral TokenDeltas -> emit the final WorkerOutcome -> await the cumulative
// EventAck -> re-advertise a credit. Reconnects with bounded backoff + jitter
// on stream loss (fail-closed: abort/kill the child with bounded escalation,
// append ABORTED, retain the unacked audit tail + WAL, replay as after_terminal
// on reconnect, gate Ready on the replay ACK — never resume the execution).
// Heartbeats at the Welcome lease interval while a turn is active.
//
// The supervisor never imports pi/extensions/tools — it talks to the Child
// abstraction (spawned per turn).

import { create } from "@bufbuild/protobuf";
import { unlinkSync } from "node:fs";
import { join } from "node:path";
import type { Transport } from "@connectrpc/connect";
import {
  ErrorDetailSchema,
  ExecutionStartedSchema,
  HarnessErrorSchema,
  HeartbeatSchema,
  Outcome,
  PiPhase,
  ReadySchema,
  Retryability,
  StepArtifactRefSchema,
  StepCompletionSchema,
  TokenDeltaSchema,
  DeltaType,
  TurnEventSchema,
  WorkerMessageSchema,
  WorkerOutcomeSchema,
  WorkerState,
  type AssignTurn,
  type ControlMessage,
  type SessionEnd,
  type TurnEvent,
  type WorkerMessage,
} from "./gen/iterabase/harness/v1/harness_pb.js";
import type { HarnessConfig } from "./config.js";
import { createWorkTransport, openWorkStream, type WorkStream, type Welcome } from "./work-client.js";
import { WorkerState as WorkerStateMachine, ProtocolError } from "./worker-state.js";
import { resolveSandboxRoot, validateSandbox, provisionSandbox, reapSandbox, resolveWorkingDir, SandboxError, type SandboxPaths } from "./sandbox.js";
import type { Probes } from "./probes.js";
import { EventOutbox, OutboxOverflow, AckError } from "./event-outbox.js";
import { createGatewayClient, type GatewayClient, type AssignmentScope } from "./gateway-client.js";
import { streamModel } from "./model-bridge.js";
import { InvokeState } from "./gen/iterabase/gateway/v1/gateway_pb.js";
import type { ArtifactInputRefFrame, GatewayToolDescriptor } from "./ipc.js";
import { materializeArtifacts, publishWorkspaceArtifact } from "./artifact-files.js";

/** A durable TurnEvent payload (the oneof) the supervisor sequences + sends. */
export type TurnEventPayload = TurnEvent["kind"];

export type ChildEvent =
  | { kind: "event"; payload: TurnEventPayload }
  | { kind: "tokenDelta"; contentIndex: number; deltaType: "TEXT" | "THINKING"; delta: string };

export interface ChildResult {
  outcome: Outcome;
  message?: string;
}
/** A child→supervisor RPC request (fd 4) — model/tool/cancel (HOR-395). */
export type ChildRpcRequest =
  | { type: "modelRequest"; requestId: string; body: unknown }
  | { type: "toolCall"; requestId: string; toolCallId: string; toolName: string; toolVersionDigest: string; argumentsJson: string; artifactInputRefs?: ArtifactInputRefFrame[]; idempotencyKey?: string }
  | { type: "publishArtifact"; requestId: string; relativePath: string; mimeType: string }
  | { type: "stepCompletion"; requestId: string; outcome: string; summary: string; outputJson: string; artifactRefs: Array<{artifactId:string;role:string;metadataJson:string}> }
  | { type: "cancel"; requestId: string };
export interface Child {
  abort(): void;
  events: AsyncIterable<ChildEvent>;
  /** child→supervisor RPC requests (fd 4) — model/tool calls (HOR-395). */
  rpcRequests: AsyncIterable<ChildRpcRequest>;
  /** Write a supervisor→child RPC frame (fd 5): modelChunk/modelEnd/toolResult/gatewayTools/cancel.
   * Returns false when the fd-5 pipe buffer is full (apply backpressure). */
  rpcSend: (frame: unknown) => boolean;
  /** Subscribe to fd-5 drain (resume a paused upstream producer). Returns an unsubscribe. */
  rpcOnDrain: (listener: () => void) => () => void;
  result: Promise<ChildResult>;
}
export type ChildFactory = (assignment: AssignTurn, sandbox: SandboxPaths, cwd: string) => Child;

export class SupervisorError extends Error {}

/** Bounded deadline for gateway-tool discovery (HOR-395: slow upstream behavior
 * must remain bounded). Aborts the discovery request and fails the turn. */
const DISCOVERY_DEADLINE_MS = 10_000;
const MATERIALIZATION_DEADLINE_MS = 5 * 60_000;

interface TurnCtx {
  turnId: string;
  aborted: boolean;
  completionReported: boolean;
  acked: Promise<void>;
  resolveAck: () => void;
  /** Bounded/cancellable discovery (HOR-395): aborted on AbortTurn or deadline. */
  discoveryAc: AbortController | null;
}

interface PendingReplay {
  turnId: string;
  events: TurnEvent[]; // unacked durable events re-sent as after_terminal audit
  lastSeq: number; // highest sequence in `events` (gate Ready on its ACK)
  walPath: string; // retained until the cumulative ACK; deleted on commit
}

export interface SupervisorDeps {
  cfg: HarnessConfig;
  hello: WorkerMessage;
  childFactory: ChildFactory;
  probes: Probes;
  /** Transport factory for (re)connect. Defaults to createWorkTransport (mTLS). */
  transport?: () => Transport;
  /** Tool-gateway client (ARCH-004). Defaults to createGatewayClient(cfg). */
  gatewayClient?: GatewayClient;
  /** Inference-gateway model stream (ARCH-010/011). Defaults to streamModel. */
  modelStream?: typeof streamModel;
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
  /** Unacked audit tail from a prior crash (WAL recover) or stream-loss, replayed + ACK-gated after Welcome. */
  private pendingReplay: PendingReplay[] = [];
  private readonly tokens: TokenDeltaForwarder;
  private readonly gatewayClient: GatewayClient;
  private readonly modelStream: typeof streamModel;

  constructor(private readonly d: SupervisorDeps) {
    this.tokens = new TokenDeltaForwarder(() => this.stream, d.cfg.tokenDelta.sendBufferBytes);
    this.gatewayClient = d.gatewayClient ?? createGatewayClient(d.cfg);
    this.modelStream = d.modelStream ?? streamModel;
    // Crash recovery: load unfinished turn WALs at startup (replayed as
    // after_terminal after the first Welcome — the CP already terminalized them
    // as worker-loss). The WAL is retained until the cumulative ACK.
    this.pendingReplay = EventOutbox.recover(d.cfg.walDir).map((r) => ({
      turnId: r.turnId,
      events: r.events,
      lastSeq: r.events.length ? Number(r.events.at(-1)!.sequence) : 0,
      walPath: join(d.cfg.walDir, `${r.turnId}.wal`),
    }));
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
        await this.failClosed(err);
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
    await this.awaitChildTermination();
    this.stream?.close();
  }

  private async connectAndServe(): Promise<void> {
    // Re-read mTLS credentials on each reconnect (certificate rotation) —
    // matches the documented guarantee for the tool-gateway transport.
    this.gatewayClient.resetTransport();
    const transport = (this.d.transport ?? (() => createWorkTransport(this.d.cfg)))();
    this.state.onConnecting();
    const conn = await openWorkStream(this.d.hello, transport);
    this.stream = conn.stream;
    this.welcome = conn.welcome;
    this.state.onWelcome();
    this.d.probes.setReady(true);
    this.replayPending(); // re-send staged unacked events (after_terminal); WAL retained until ACK
    this.tokens.flush(); // flush deltas buffered during the outage (ephemeral; best-effort)
    this.maybeAdvertiseCredit(); // Ready only when no replay is outstanding
    for await (const msg of conn.stream.control) {
      this.onControl(msg);
      const phase = this.state.phase as string;
      if (phase === "draining" || phase === "fatal") break;
    }
    if ((this.state.phase as string) !== "draining" && (this.state.phase as string) !== "fatal") {
      throw new SupervisorError("control stream ended");
    }
  }

  /** Re-send staged unacked events (after_terminal). WALs are retained until the cumulative ACK. */
  private replayPending(): void {
    for (const r of this.pendingReplay) {
      for (const ev of r.events) this.stream?.send(turnEventMessage(ev));
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
        this.onEventAck(msg.kind.value.turnId, Number(msg.kind.value.throughSequence));
        return;
      }
      case "sessionEnd":
        this.handleSessionEnd(msg.kind.value);
        return;
      case "welcome":
        return;
    }
  }

  /** Apply a cumulative ACK to the active turn or a pending replay; fail-close on mismatch. */
  private onEventAck(turnId: string, through: number): void {
    // Replay tail ACK?
    const replayIdx = this.pendingReplay.findIndex((r) => r.turnId === turnId);
    if (replayIdx >= 0) {
      const r = this.pendingReplay[replayIdx]!;
      if (through > r.lastSeq) {
        this.fatal(new ProtocolError(`replay ack ${through} > last seq ${r.lastSeq} for turn ${turnId}`));
        return;
      }
      if (through >= r.lastSeq) {
        // Replay committed — delete the retained WAL, clear the replay, then Ready.
        this.pendingReplay.splice(replayIdx, 1);
        try {
          unlinkSync(r.walPath);
        } catch {
          /* already gone */
        }
        this.maybeAdvertiseCredit();
      }
      // A partial cumulative ACK (through < lastSeq) is valid; keep waiting.
      return;
    }
    // Active-turn ACK?
    if (this.turn && this.turn.turnId === turnId && this.outbox) {
      try {
        const done = this.outbox.ack(through);
        if (done) this.turn.resolveAck();
      } catch (err) {
        this.fatal(err instanceof Error ? err : new ProtocolError(String(err)));
      }
      return;
    }
    // Mismatched / unknown ACK — protocol violation (fail-closed).
    this.fatal(new ProtocolError(`EventAck for unknown turn ${turnId}`));
  }

  /** Advertise a Ready credit only when idle, not draining/fatal, and no replay outstanding. */
  private maybeAdvertiseCredit(): void {
    if (this.pendingReplay.length > 0) return; // gate Ready on the replay ACK
    if (!this.state.canAdvertiseCredit) return;
    this.state.advertiseCredit();
    this.stream?.send(create(WorkerMessageSchema, { kind: { case: "ready", value: create(ReadySchema, {}) } }));
    this.d.onCreditAdvertised?.();
  }

  private async handleAssignTurn(at: AssignTurn): Promise<void> {
    // Route through the state machine first: a second AssignTurn before the
    // next Ready is a protocol violation → fail-close (never silently ignored).
    try {
      this.state.onAssignTurn(at.turnId);
    } catch (err) {
      if (err instanceof ProtocolError) return this.fatal(err);
      throw err;
    }
    let resolveAck!: () => void;
    const acked = new Promise<void>((r) => (resolveAck = r));
    this.turn = { turnId: at.turnId, aborted: false, completionReported: false, acked, resolveAck, discoveryAc: null };
    this.outbox = new EventOutbox(this.d.cfg.walDir, at.turnId, this.d.cfg.outbox.bound);
    this.startHeartbeat(at.turnId);
    try {
      const sandbox = this.resolveSandbox(at);
      this.validatePoolScope(at);
      this.intersectPoolWorkspaceTools(at);
      // Durable execution boundary — observed before child lifecycle events.
      this.sendChildEvent({
        case: "executionStarted",
        value: create(ExecutionStartedSchema, { sessionId: at.sessionId, sandbox: at.sandbox ?? undefined }),
      });
      const cwd = resolveWorkingDir(sandbox.root, at.sandbox?.workingDir || "home");
      const scope = { turnId: at.turnId, runId: at.runId, fencingGeneration: this.welcome?.fencingGeneration ?? 0n };
      const materializeAc = new AbortController();
      if (this.turn) this.turn.discoveryAc = materializeAc;
      const materializeDeadline = setTimeout(() => materializeAc.abort(), MATERIALIZATION_DEADLINE_MS);
      materializeDeadline.unref?.();
      try {
        await materializeArtifacts(this.gatewayClient, scope, at.materializations, sandbox.workspace, at.sandbox?.uid ?? 0, at.sandbox?.gid ?? 0, materializeAc.signal);
      } catch (err) {
        throw new SandboxError(`artifact materialization failed: ${err instanceof Error ? err.message : String(err)}`);
      } finally {
        clearTimeout(materializeDeadline);
        if (this.turn) this.turn.discoveryAc = null;
      }
      if (this.turn?.aborted) throw new SandboxError("aborted during artifact materialization");
      // Discover the effective gateway tools BEFORE spawning the child and pass
      // the non-secret descriptors to the child (ARCH-006). Discovery is
      // bounded (deadline) and cancellable (aborted on AbortTurn): a hanging
      // discovery no longer leaves the turn stuck, and a failed discovery
      // fails the turn fail-closed with NO live child left to terminate
      // (HOR-395: slow upstream behavior remains bounded and every setup
      // failure terminates the per-turn child lifecycle).
      const discoveryAc = new AbortController();
      if (this.turn) this.turn.discoveryAc = discoveryAc;
      const deadline = setTimeout(() => discoveryAc.abort(), DISCOVERY_DEADLINE_MS);
      deadline.unref?.();
      let descriptors: GatewayToolDescriptor[];
      try {
        descriptors = await this.gatewayClient.discover(scope, discoveryAc.signal);
      } catch (err) {
        throw new SandboxError(
          `gateway discovery failed: ${err instanceof Error ? err.message : String(err)}`,
        );
      } finally {
        clearTimeout(deadline);
        if (this.turn) this.turn.discoveryAc = null;
      }
      if (this.turn?.aborted) throw new SandboxError("aborted during discovery");
      const child = this.d.childFactory(at, sandbox, cwd);
      this.currentChild = child;
      child.rpcSend({ type: "gatewayTools", descriptors });
      // Drain the child's model/tool RPC requests (fd 4) concurrently with the
      // audit event stream (fd 3). Both close when the child exits.
      const rpcDone = this.dispatchChildRpc(child, at, descriptors, sandbox).catch(() => {
        /* errors surface as tool/model error frames; logged only as outcome */
      });
      for await (const ev of child.events) {
        if (this.turn?.aborted) break;
        if (ev.kind === "event") this.sendChildEvent(ev.payload);
        else this.tokens.push(at.turnId, ev.contentIndex, ev.deltaType, ev.delta);
      }
      const result = await child.result;
      await rpcDone;
      this.emitOutcome(result.outcome, result.message);
    } catch (err) {
      this.failTurn(err);
    } finally {
      this.stopHeartbeat();
      this.currentChild = null;
    }
    await this.turn.acked;
    if ((this.state.phase as string) === "cleaning") {
      this.state.onOutcomeAcked();
      this.turn = null;
      this.outbox = null; // WAL already deleted by outbox.ack on final ACK
      this.maybeAdvertiseCredit();
    } else {
      this.turn = null; // disrupted (stream loss / drain) — run() handles reconnect/exit
    }
  }

  /**
   * Dispatch the child's model/tool RPC requests (fd 4) to the authenticated
   * gateway bridges (ARCH-004/010/011). Each request is validated against the
   * active assignment: the model must equal the assigned model; the tool must
   * be in the discovered effective set. The supervisor stamps durable caller
   * context (run/turn); the child supplies only business arguments. Bodies are
   * never logged or WAL'd. Cancellation propagates: a child `cancel` aborts
   * the in-flight upstream call (ARCH-014 — cannot undo an effect already
   * started; the gateway classifies per effect class on caller disconnect).
   */
  private async dispatchChildRpc(child: Child, at: AssignTurn, descriptors: GatewayToolDescriptor[], sandbox: SandboxPaths): Promise<void> {
    const scope = { turnId: at.turnId, runId: at.runId, fencingGeneration: this.welcome?.fencingGeneration ?? 0n };
    const assignedModel = at.model?.id ?? "";
    // The effective gateway tools discovered for this turn (ARCH-006). The
    // supervisor rejects a toolCall whose name is not in this set BEFORE
    // calling the gateway — defense-in-depth fail-closed (acceptance: an
    // unassigned tool request fails closed in the supervisor). The gateway
    // re-validates scope + version pin upstream.
    const discovered = new Map<string, GatewayToolDescriptor>();
    for (const d of descriptors) discovered.set(d.name, d);
    // Bounded in-flight RPC work (HOR-395): a runaway/compromised child cannot
    // open an unbounded number of concurrent upstream model/tool calls. The cap
    // counts every launched operation independently of the child-controlled
    // `requestId` (a duplicate id cannot shrink the count to bypass the cap).
    // Duplicate active ids are rejected fail-closed so cancellation correlation
    // stays unambiguous (`controllers.set` never overwrites a live controller).
    // Overflow is fail-closed — the request gets an error terminal.
    const controllers = new Map<string, AbortController>();
    let inflight = 0;
    const MAX_INFLIGHT_RPC = 8;

    for await (const req of child.rpcRequests) {
      if (this.turn?.aborted) break;
      if (req.type === "cancel") {
        const ac = controllers.get(req.requestId);
        if (ac) ac.abort();
        continue;
      }
      if (req.type === "stepCompletion") {
        if (!at.nodeExecutionId || !at.completionOutcomes.includes(req.outcome) || !this.turn || this.turn.completionReported) {
          this.failTurn(new ProtocolError("invalid or duplicate complete_step report"));
          break;
        }
        this.turn.completionReported = true;
        this.sendChildEvent({
          case: "stepCompletion",
          value: create(StepCompletionSchema, {
            outcome: req.outcome,
            summary: req.summary,
            outputJson: req.outputJson,
            artifactRefs: req.artifactRefs.map((ref) => create(StepArtifactRefSchema, ref)),
          }),
        });
        child.rpcSend({ type: "stepCompletionAck", requestId: req.requestId });
        continue;
      }
      // Duplicate active requestId = protocol violation (the child reused an id
      // that is still in flight). Reject fail-closed without launching, so the
      // in-flight cap counts real work and cancellation stays unambiguous.
      if (controllers.has(req.requestId)) {
        if (req.type === "modelRequest") {
          child.rpcSend({ type: "modelEnd", requestId: req.requestId, status: "error", errorMessage: "duplicate request id" });
        } else if (req.type === "publishArtifact") {
          child.rpcSend({ type: "artifactPublished", requestId: req.requestId, errorMessage: "duplicate request id" });
        } else {
          child.rpcSend({ type: "toolResult", requestId: req.requestId, isError: true, errorMessage: "duplicate request id" });
        }
        continue;
      }
      if (inflight >= MAX_INFLIGHT_RPC) {
        // Fail-closed overflow: bound queued/in-flight work.
        if (req.type === "modelRequest") {
          child.rpcSend({ type: "modelEnd", requestId: req.requestId, status: "error", errorMessage: "too many in-flight model/tool requests" });
        } else if (req.type === "publishArtifact") {
          child.rpcSend({ type: "artifactPublished", requestId: req.requestId, errorMessage: "too many in-flight requests" });
        } else {
          child.rpcSend({ type: "toolResult", requestId: req.requestId, isError: true, errorMessage: "too many in-flight model/tool requests" });
        }
        continue;
      }
      inflight += 1;
      const done = (): void => { inflight -= 1; };
      if (req.type === "modelRequest") {
        void this.handleModelRequest(child, req, assignedModel, at, controllers, done);
        continue;
      }
      if (req.type === "publishArtifact") {
        // Reserved platform control: unlike read/write/edit/bash, publication
        // is not gated by the AgentPool workspace-tools switch.
        if (this.turn?.completionReported) {
          done();
          child.rpcSend({ type: "artifactPublished", requestId: req.requestId, errorMessage: "artifact publication is not permitted after complete_step" });
          continue;
        }
        void this.handlePublishArtifact(child, req, scope, at, sandbox, controllers, done);
        continue;
      }
      if (req.type === "toolCall") {
        if (this.turn?.completionReported) {
          done();
          child.rpcSend({ type: "toolResult", requestId: req.requestId, isError: true, errorMessage: "customer-system tools are disabled after complete_step" });
          continue;
        }
        void this.handleToolCall(child, req, scope, at, discovered, controllers, done);
        continue;
      }
    }
    // Abort any still-in-flight upstream calls when the child exits/aborts.
    for (const ac of controllers.values()) ac.abort();
  }

  /** Forward one model request to the inference gateway (ARCH-010/011). */
  private async handleModelRequest(
    child: Child,
    req: { requestId: string; body: unknown },
    assignedModel: string,
    at: AssignTurn,
    controllers: Map<string, AbortController>,
    onDone: () => void,
  ): Promise<void> {
    const ac = new AbortController();
    controllers.set(req.requestId, ac);
    try {
      await this.modelStream(
        { cfg: this.d.cfg },
        { body: req.body },
        assignedModel,
        { runId: at.runId, turnId: at.turnId, fencingGeneration: this.welcome?.fencingGeneration ?? 0n },
        ac.signal,
        {
          onChunk: (data) => child.rpcSend({ type: "modelChunk", requestId: req.requestId, data }),
          onDrain: (listener) => child.rpcOnDrain(listener),
          onEnd: (status, httpStatus, errorMessage) =>
            child.rpcSend({ type: "modelEnd", requestId: req.requestId, status, ...(httpStatus !== undefined ? { httpStatus } : {}), ...(errorMessage !== undefined ? { errorMessage } : {}) }),
        },
      );
    } catch (err) {
      child.rpcSend({ type: "modelEnd", requestId: req.requestId, status: "error", errorMessage: err instanceof Error ? err.message : String(err) });
    } finally {
      controllers.delete(req.requestId);
      onDone();
    }
  }

  /** Forward one gateway tool call (ARCH-004/014). Rejects unassigned tools
   * fail-closed before calling the gateway (acceptance criterion). */
  private async handleToolCall(
    child: Child,
    req: { requestId: string; toolCallId: string; toolName: string; toolVersionDigest: string; argumentsJson: string; artifactInputRefs?: ArtifactInputRefFrame[]; idempotencyKey?: string },
    scope: AssignmentScope,
    at: AssignTurn,
    discovered: Map<string, GatewayToolDescriptor>,
    controllers: Map<string, AbortController>,
    onDone: () => void,
  ): Promise<void> {
    const pinned = discovered.get(req.toolName);
    if (!pinned) {
      child.rpcSend({ type: "toolResult", requestId: req.requestId, isError: true, errorMessage: `tool not permitted: ${req.toolName}` });
      onDone();
      return;
    }
    const ac = new AbortController();
    controllers.set(req.requestId, ac);
    try {
      const canonicalRefs = canonicalToolArtifactInputs(at, req.artifactInputRefs ?? []);
      const resp = await this.gatewayClient.invokeTool(
        scope,
        {
          toolCallId: req.toolCallId,
          toolName: req.toolName,
          // The pinned immutable digest (ARCH-007); the gateway ignores/rejects
          // a caller-supplied digest that differs.
          toolVersionDigest: pinned.digest,
          argumentsJson: req.argumentsJson,
          artifactInputRefs: canonicalRefs,
          ...(req.idempotencyKey !== undefined ? { idempotencyKey: req.idempotencyKey } : {}),
        },
        ac.signal,
      );
      const resultJson = resp.resultJson.length ? new TextDecoder().decode(resp.resultJson) : undefined;
      const isError = resp.state !== InvokeState.SUCCEEDED;
      const errorMessage = resp.error ? resp.error.message : undefined;
      child.rpcSend({
        type: "toolResult",
        requestId: req.requestId,
        isError,
        ...(resultJson !== undefined ? { resultJson } : {}),
        artifactRefs: resp.artifactOutputRefs.map((ref) => ({ artifactId: ref.artifactId, mimeType: ref.mimeType, sizeBytes: Number(ref.sizeBytes), digest: ref.digest })),
        ...(errorMessage !== undefined ? { errorMessage } : {}),
      });
    } catch (err) {
      child.rpcSend({ type: "toolResult", requestId: req.requestId, isError: true, errorMessage: err instanceof Error ? err.message : String(err) });
    } finally {
      controllers.delete(req.requestId);
      onDone();
    }
  }

  private async handlePublishArtifact(
    child: Child,
    req: { requestId: string; relativePath: string; mimeType: string },
    scope: AssignmentScope,
    at: AssignTurn,
    sandbox: SandboxPaths,
    controllers: Map<string, AbortController>,
    onDone: () => void,
  ): Promise<void> {
    const ac = new AbortController();
    controllers.set(req.requestId, ac);
    try {
      const metadata = await publishWorkspaceArtifact(
        this.gatewayClient,
        scope,
        sandbox.workspace,
        req.relativePath,
        req.mimeType,
        at.sandbox?.uid ?? 0,
        at.sandbox?.gid ?? 0,
        ac.signal,
      );
      const ref = metadata.ref;
      if (!ref) throw new SandboxError("artifact service returned no reference");
      child.rpcSend({
        type: "artifactPublished",
        requestId: req.requestId,
        artifactId: ref.artifactId,
        mimeType: ref.mimeType,
        sizeBytes: Number(ref.sizeBytes),
        digest: ref.digest,
      });
    } catch (err) {
      child.rpcSend({ type: "artifactPublished", requestId: req.requestId, errorMessage: err instanceof Error ? err.message : String(err) });
    } finally {
      controllers.delete(req.requestId);
      onDone();
    }
  }

  /** Provision (if missing) then validate the sandbox beneath the mount root. */
  private resolveSandbox(at: AssignTurn): SandboxPaths {
    const sb = at.sandbox;
    if (!sb) throw new SandboxError("AssignTurn missing sandbox");
    const root = resolveSandboxRoot(this.d.cfg.sandboxRoot, sb.sandboxId);
    // HOR-245: the supervisor provisions the per-session sandbox (create+chown
    // 0700, session UID/GID) under the 0711 root-owned mount root, then validates
    // it as the integrity gate before spawning the child. The provisioner never
    // chowns an existing path; a mismatched/partial root is a typed FAILED.
    provisionSandbox(root, sb.uid, sb.gid);
    return validateSandbox(root, sb.uid, sb.gid);
  }

  /**
   * Reap a terminated session's sandbox (HOR-245 cleanup owner). Legal only
   * when no turn is active: the CP sequences SessionEnd after the final outcome
   * ACK (idle/armed). A SessionEnd during an active turn is a dispatch bug →
   * fatal (fail-closed), because reaping a live turn's sandbox would corrupt
   * the running child. Reaping errors (symlink/foreign-owner/IO) are fatal:
   * a SandboxError here signals a security invariant or a volume that silently
   * ignores removal, neither of which v1 tolerates.
   */
  private handleSessionEnd(msg: SessionEnd): void {
    if (this.state.hasActiveTurn) {
      this.fatal(new ProtocolError(`SessionEnd for ${msg.sandboxId} during active turn ${this.state.activeTurnId}`));
      return;
    }
    if (!msg.sandboxId) {
      this.fatal(new ProtocolError("SessionEnd with empty sandbox_id"));
      return;
    }
    const root = resolveSandboxRoot(this.d.cfg.sandboxRoot, msg.sandboxId);
    try {
      reapSandbox(root, msg.uid, msg.gid);
    } catch (err) {
      this.fatal(err instanceof Error ? err : new SandboxError(String(err)));
    }
  }

  /** Defense-in-depth: reject assignments whose scope identity != the configured pool scope. */
  private validatePoolScope(at: AssignTurn): void {
    const pool = this.d.cfg.poolScopeIdentityId;
    if (pool && at.scopeIdentityId !== pool) {
      throw new SandboxError(`scope identity ${at.scopeIdentityId ?? "(none)"} != pool scope ${pool}`);
    }
  }

  /** Interim residual (HOR-395, pending HOR-245): intersect the per-turn
   * workspace-tool request with the deploy-time AgentPool maximum so a turn
   * cannot widen pool permissions (ARCH-016/DEC-036/DEC-038). Deny-by-default —
   * an unset/false pool maximum exposes no local tools regardless of the
   * assignment. The authoritative AgentPool-owned maximum is tracked as a
   * HOR-245 acceptance criterion; until then the supervisor config mirrors it. */
  private intersectPoolWorkspaceTools(at: AssignTurn): void {
    const poolMax = this.d.cfg.poolWorkspaceTools ?? false;
    if (at.workspaceTools && !poolMax) at.workspaceTools = false;
  }

  /** Append the durable event to the WAL+outbox, then send. Marks the final outcome. */
  private sendChildEvent(payload: TurnEventPayload): void {
    const ob = this.outbox;
    if (!ob) return;
    const te = ob.append(payload);
    if (payload.case === "workerOutcome") ob.markFinal(Number(te.sequence));
    this.stream?.send(turnEventMessage(te));
  }

  /** Terminal append path (bypasses the bound) — used for overflow + outcome, never re-enters the bound check. */
  private sendTerminal(payload: TurnEventPayload): void {
    const ob = this.outbox;
    if (!ob) return;
    const te = ob.appendTerminal(payload);
    if (payload.case === "workerOutcome") ob.markFinal(Number(te.sequence));
    this.stream?.send(turnEventMessage(te));
  }

  private sendHarnessError(message: string): void {
    this.sendTerminal({
      case: "harnessError",
      value: create(HarnessErrorSchema, {
        error: create(ErrorDetailSchema, { message, retryability: Retryability.NON_RETRYABLE }),
      }),
    });
  }

  private emitOutcome(outcome: Outcome, message?: string): void {
    if (!this.outbox) return;
    this.sendTerminal({
      case: "workerOutcome",
      value: create(WorkerOutcomeSchema, { outcome, message: message ?? "" }),
    });
    try {
      this.state.onChildExited();
    } catch {
      /* already cleaning/draining */
    }
  }

  /** Fail the turn on a setup/runtime error: terminal HarnessError + FAILED outcome (no re-entry). */
  private failTurn(err: unknown): void {
    const message = err instanceof Error ? err.message : String(err);
    if (err instanceof SandboxError) this.sendHarnessError(`sandbox invalid: ${message}`);
    if (err instanceof OutboxOverflow) this.sendHarnessError(`outbox overflow: ${message}`);
    this.emitOutcome(Outcome.FAILED, message);
  }

  private abortActiveTurn(): void {
    if (this.turn) this.turn.aborted = true;
    this.turn?.discoveryAc?.abort();
    this.currentChild?.abort();
  }

  /** Await bounded child termination after abort (SIGTERM → SIGKILL within abortGraceMs). */
  private async awaitChildTermination(): Promise<void> {
    const child = this.currentChild;
    if (!child) return;
    const grace = this.d.cfg.child.abortGraceMs + 500;
    await Promise.race([child.result, sleep(grace)]);
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
              piPhase: PiPhase.MODEL_CALL, // placeholder until the child reports phases
              highestSequence: BigInt(ob?.highestSequence ?? 0),
            }),
          },
        }),
      );
    }, interval);
    this.heartbeat.unref?.();
  }
  private stopHeartbeat(): void {
    if (this.heartbeat) clearInterval(this.heartbeat);
    this.heartbeat = null;
  }

  /**
   * Stream loss / connect failure mid-turn: abort the child with bounded
   * escalation, append ABORTED, retain the unacked audit tail + WAL for replay
   * (after_terminal) on reconnect, and gate Ready on its cumulative ACK. Never
   * resume the execution — the CP terminalizes the turn as worker-loss via
   * fencing (HOR-249).
   */
  private async failClosed(err: unknown): Promise<void> {
    this.abortActiveTurn();
    this.stopHeartbeat();
    await this.awaitChildTermination(); // bounded: SIGTERM → SIGKILL before reconnecting
    if (this.outbox && this.turn) {
      // Append ABORTED so the replay tail carries the terminal outcome.
      this.outbox.appendTerminal({
        case: "workerOutcome",
        value: create(WorkerOutcomeSchema, { outcome: Outcome.ABORTED, message: "stream loss" }),
      });
      this.pendingReplay.push({
        turnId: this.turn.turnId,
        events: this.outbox.unacked(),
        lastSeq: this.outbox.highestSequence,
        walPath: this.outbox.walPath,
      });
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

/**
 * Bounded drop-oldest forwarder for ephemeral TokenDeltas (non-sequenced,
 * non-ACKed, NOT WAL'd). Drops the oldest pending delta when the send buffer
 * exceeds the byte bound; the durable AssistantMessage carries the full text.
 */
class TokenDeltaForwarder {
  private pending: WorkerMessage[] = [];
  private bytes = 0;
  constructor(
    private readonly getStream: () => WorkStream | null,
    private readonly boundBytes: number,
  ) {}
  push(turnId: string, contentIndex: number, deltaType: "TEXT" | "THINKING", delta: string): void {
    const msg = create(WorkerMessageSchema, {
      kind: {
        case: "tokenDelta",
        value: create(TokenDeltaSchema, {
          turnId,
          contentIndex,
          type: deltaType === "TEXT" ? DeltaType.TEXT : DeltaType.THINKING,
          delta,
        }),
      },
    });
    this.flush();
    const stream = this.getStream();
    if (stream) {
      stream.send(msg);
      return;
    }
    // Stream unavailable (reconnecting) — buffer with drop-oldest on byte bound.
    this.pending.push(msg);
    this.bytes += delta.length;
    while (this.boundBytes > 0 && this.bytes > this.boundBytes && this.pending.length > 1) {
      const dropped = this.pending.shift()!;
      const d = dropped.kind.case === "tokenDelta" ? dropped.kind.value.delta : "";
      this.bytes -= d.length;
    }
  }
  /** Flush buffered deltas to the stream (called on reconnect by the supervisor). */
  flush(): void {
    const stream = this.getStream();
    if (!stream) return;
    for (const m of this.pending) stream.send(m);
    this.pending = [];
    this.bytes = 0;
  }
}

/** Revalidate child-supplied tool refs against the immutable assignment. The
 * child may narrow the set for one invocation but can never add or alter a ref. */
function canonicalToolArtifactInputs(at: AssignTurn, requested: ArtifactInputRefFrame[]): ArtifactInputRefFrame[] {
  const assigned = new Map(at.materializations.flatMap((m) => m.ref ? [[m.ref.artifactId, m.ref] as const] : []));
  const seen = new Set<string>();
  for (const ref of requested) {
    const canonical = assigned.get(ref.artifactId);
    if (
      !canonical || seen.has(ref.artifactId) || canonical.mimeType !== ref.mimeType ||
      canonical.sizeBytes.toString() !== ref.sizeBytes || canonical.digest !== ref.digest
    ) {
      throw new SupervisorError(`tool artifact input is not canonical for this assignment: ${ref.artifactId}`);
    }
    seen.add(ref.artifactId);
  }
  return requested;
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
