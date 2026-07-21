// Worker protocol/lifecycle state machine (HOR-381). Enforces the Work-stream
// invariants the supervisor depends on:
//   - Hello -> Welcome before anything else (no Ready before Welcome).
//   - The single-credit invariant: one Ready advertises exactly one dispatch
//     credit; a second AssignTurn before the next Ready is a protocol violation
//     (the supervisor closes the stream fail-closed).
//   - No Ready while a child, cleanup, or unacknowledged final outcome is
//     outstanding (cleaning -> idle only on outcome ACK + audit replay).
//   - No new credits while draining (SIGTERM) or fatal.
//
// This is the protocol rules layer — pure logic, no I/O. The supervisor holds a
// WorkerState and calls the transition methods; the child lifecycle + outbox
// orchestrate around it. Fencing (a new connection for the same worker_id
// closing the old generation) is CP-side (HOR-249); the worker just honors the
// Welcome it receives.

export type WorkerPhase =
  | "disconnected" // initial / after stream close
  | "connecting" // Hello sent, awaiting Welcome
  | "idle" // Welcome received; may advertise a credit (emit Ready)
  | "armed" // Ready sent; awaiting AssignTurn
  | "running" // AssignTurn received; child running
  | "cleaning" // child exited; outcome emitted; awaiting ACK (+ audit replay)
  | "draining" // SIGTERM; no new credits; finish active turn then exit
  | "fatal"; // unrecoverable; process should exit

export class ProtocolError extends Error {
  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "ProtocolError";
  }
}

export class WorkerState {
  private _phase: WorkerPhase = "disconnected";
  private _activeTurnId: string | null = null;

  get phase(): WorkerPhase {
    return this._phase;
  }
  get activeTurnId(): string | null {
    return this._activeTurnId;
  }
  /** May the supervisor emit Ready now? (only when idle, and not draining/fatal). */
  get canAdvertiseCredit(): boolean {
    return this._phase === "idle";
  }
  /** Is a turn in flight (child running or cleaning up)? */
  get hasActiveTurn(): boolean {
    return this._phase === "running" || this._phase === "cleaning";
  }

  /** DISCONNECTED -> connecting (Hello sent). */
  onConnecting(): void {
    this.require("disconnected", "connect");
    this._phase = "connecting";
  }

  /** connecting -> idle (Welcome received). */
  onWelcome(): void {
    this.require("connecting", "Welcome");
    this._phase = "idle";
  }

  /** idle -> armed (emit Ready — advertise one dispatch credit). */
  advertiseCredit(): void {
    if (this._phase !== "idle") throw new ProtocolError(`cannot advertise credit from ${this._phase}`);
    this._phase = "armed";
  }

  /** armed -> running (AssignTurn received — the single-credit invariant). */
  onAssignTurn(turnId: string): void {
    if (this._phase !== "armed")
      throw new ProtocolError(`AssignTurn without a Ready credit (phase=${this._phase}) — protocol violation`);
    if (!turnId) throw new ProtocolError("AssignTurn with empty turn_id");
    this._activeTurnId = turnId;
    this._phase = "running";
  }

  /** running -> cleaning (child exited; final outcome emitted; awaiting ACK). */
  onChildExited(): void {
    this.require("running", "child exit");
    this._phase = "cleaning";
  }

  /** cleaning -> idle (final outcome ACKed + audit replay complete). */
  onOutcomeAcked(): void {
    this.require("cleaning", "outcome ACK");
    this._activeTurnId = null;
    this._phase = "idle";
  }

  /** Any -> disconnected (stream lost; the supervisor reconnects). */
  onStreamLost(): void {
    this._phase = "disconnected";
    // The active turn, if any, is failed by the CP via fencing/lease (HOR-249).
    this._activeTurnId = null;
  }

  /** Any -> draining (SIGTERM; no new credits). */
  beginDrain(): void {
    if (this._phase === "fatal") return;
    this._phase = "draining";
  }

  /** Any -> fatal (unrecoverable; process exits). */
  fatal(): void {
    this._phase = "fatal";
  }

  private require(expected: WorkerPhase, action: string): void {
    if (this._phase !== expected)
      throw new ProtocolError(`${action} illegal in phase ${this._phase} (expected ${expected})`);
  }
}
