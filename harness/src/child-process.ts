// The per-turn Child (HOR-381): a real pi process spawned via the setpriv
// launcher under the session UID/GID. The supervisor talks to it over a
// dedicated duplex IPC channel of inherited fds:
//   - fd 0 (stdin)  : supervisor → child  (framed SupervisorFrame: assignment, abort)
//   - fd 3          : child → supervisor (framed ChildFrame: event, tokenDelta,
//                     heartbeat, result)
// stdout (fd 1) + stderr (fd 2) are piped separately and drained as tagged
// logs — they are NOT the protocol channel (approved trust boundary: length-
// prefixed JSON over a TS discriminated union with runtime validation, in
// ipc.ts). A stray/spoofed byte sequence cannot form a valid frame.
//
// Liveness: the child emits a heartbeat frame every livenessIntervalMs/2; if
// the supervisor sees no heartbeat within livenessIntervalMs it aborts —
// graceful SIGTERM, then SIGKILL after abortGraceMs (bounded escalation).
//
// Outcome classification: a `result` frame is PROVISIONAL. Success resolves
// only after a clean process exit (code 0) with an explicit valid result; an
// absent result is FAILED. A non-zero exit overrides a provisional COMPLETED
// (successful message + failed cleanup = FAILED). A signal exit during abort is
// ABORTED.
//
// The launch is injectable so the IPC + heartbeat + abort logic is unit-
// testable on macOS (setpriv is Linux-only).

import { type ChildProcess } from "node:child_process";
import type { Readable, Writable } from "node:stream";
import { fromJson, type JsonValue } from "@bufbuild/protobuf";
import { TurnEventSchema, Outcome, type AssignTurn } from "./gen/iterabase/harness/v1/harness_pb.js";
import { launchChild, type LaunchOptions } from "./launcher.js";
import type { Child, ChildEvent, ChildResult, ChildFactory, ChildRpcRequest } from "./supervisor.js";
import type { HarnessConfig } from "./config.js";
import type { SandboxPaths } from "./sandbox.js";
import { AsyncQueue } from "./async-queue.js";
import { FrameReader, encodeFrame, parseChildFrame, parseChildRpcFrame, writeFrame, type ChildRpcFrame } from "./ipc.js";

export type LaunchFn = (opts: LaunchOptions) => ChildProcess;

/** stdio for the child: fd0 control (sup→child), fd1 stdout, fd2 stderr, fd3 audit (child→sup), fd4 rpc requests (child→sup), fd5 rpc responses (sup→child). */
const CHILD_STDIO = ["pipe", "pipe", "pipe", "pipe", "pipe", "pipe"] as const;

/**
 * Build a ChildFactory that spawns the child entry `script` via the launcher.
 * `launch` defaults to launchChild (setpriv); tests inject a plain-node spawn.
 */
export function createChildFactory(cfg: HarnessConfig, script: string, launch: LaunchFn = launchChild): ChildFactory {
  const livenessMs = cfg.child.livenessIntervalMs;
  const graceMs = cfg.child.abortGraceMs;
  return (assignment: AssignTurn, sandbox: SandboxPaths, cwd: string): Child => {
    const sb = assignment.sandbox;
    if (!sb) throw new Error("AssignTurn missing sandbox");
    const proc = launch({
      script,
      uid: sb.uid,
      gid: sb.gid,
      sandboxRoot: sandbox.root,
      workingDir: sb.workingDir || "home",
      stdio: [...CHILD_STDIO],
      env: {
        HARNESS_SANDBOX_ROOT: sandbox.root,
        HARNESS_SESSION_DIR: sandbox.session,
        HARNESS_WORKING_DIR: cwd,
        HARNESS_PI_DIRS: cfg.piDirs.join(":"),
        HARNESS_MODEL_MAX_ATTEMPTS: String(cfg.modelRetry.maxAttempts),
        HARNESS_LIVENESS_INTERVAL_MS: String(livenessMs),
        HOME: sandbox.home,
        TMPDIR: sandbox.tmp,
      },
    });

    const events = new AsyncQueue<ChildEvent>();
    let resultResolved = false;
    let provisional: ChildResult | null = null; // PROVISIONAL until clean exit
    let resolveResult!: (r: ChildResult) => void;
    const result = new Promise<ChildResult>((r) => (resolveResult = r));
    const settle = (r: ChildResult) => {
      if (resultResolved) return;
      resultResolved = true;
      resolveResult(r);
    };

    // Supervisor → child control channel (fd 0). The assignment is the first frame.
    const control = proc.stdin;
    if (control) writeFrame(control, { type: "assignment", assignment: assignmentToJson(assignment) });

    // Child → supervisor RPC channel (fd 4): model/tool/cancel requests (HOR-395).
    const rpcRequests = new AsyncQueue<ChildRpcRequest>();
    const stdio = proc.stdio as unknown as (Readable | Writable | null)[];
    const rpcReqStream = stdio[4];
    if (rpcReqStream) {
      const rpcReader = new FrameReader(
        (raw) => {
          const f = parseChildRpcFrame(raw);
          if (!f) {
            // A malformed RPC request frame is stream corruption, not a
            // droppable audit frame. The child is now waiting on a response
            // that will never come, so terminate the turn fail-closed (HOR-395:
            // runtime validation must be bounded fail-closed, not a silent drop).
            settle({ outcome: Outcome.FAILED, message: "invalid child RPC frame on fd 4" });
            forceAbort();
            return;
          }
          rpcRequests.push(f as ChildRpcRequest);
        },
        // Strict mode for fd 4 (HOR-395 bounded fail-closed): framing/JSON/
        // truncation errors are NOT silently dropped here, because a dropped
        // request frame would leave the child waiting forever on a response
        // that never comes while its heartbeat stays healthy. Report every
        // such error to the turn failure + abort path.
        (reason) => {
          settle({ outcome: Outcome.FAILED, message: `invalid child RPC frame on fd 4: ${reason}` });
          forceAbort();
        },
      );
      (rpcReqStream as Readable).on("data", (chunk: Buffer) => rpcReader.feed(chunk));
      (rpcReqStream as Readable).on("end", () => rpcReader.end());
      (rpcReqStream as Readable).on("error", () => rpcReader.end());
    }
    // Supervisor → child RPC channel (fd 5): framed responses (HOR-395).
    // Backpressure: rpcSend returns the value of `Writable.write()` (false →
    // the pipe buffer is full). Producers (the model bridge) must pause their
    // upstream reader and resume on `drain` via rpcOnDrain, so a slow child
    // cannot grow an unbounded queue in supervisor memory (HOR-395 bounded
    // buffering/backpressure).
    //
    // Hard aggregate response-queue bound (HOR-395): the model-chunk path
    // self-pauses on `false`, but the RPC dispatcher's control/terminal
    // responses (duplicate-id, overflow, toolResult, modelEnd) used to ignore
    // the boolean and `continue`, so a fast/compromised child that stops
    // reading fd 5 could send unlimited requests and make Node queue an
    // unbounded number of response frames despite the in-flight cap. We count
    // every write that returns `false` (a frame buffered in supervisor memory
    // beyond the pipe's high-water mark) and reset the count on `drain`; once
    // the backlog exceeds the cap, the turn fails closed (FAILED + abort) —
    // limiting active upstream calls alone does not bound supervisor memory.
    const rpcRespStream = stdio[5];
    let rpcBacklog = 0; // frames written while fd 5 is backpressured, not yet drained
    let rpcOverflowed = false;
    const MAX_RPC_BACKLOG = 64; // hard aggregate response-queue bound (HOR-395)
    const rpcSend = (frame: unknown): boolean => {
      if (rpcOverflowed) return false; // already failing closed — drop further writes
      try {
        if (rpcRespStream) {
          const ok = writeFrame(rpcRespStream as Writable, frame);
          if (!ok) {
            rpcBacklog += 1;
            if (rpcBacklog >= MAX_RPC_BACKLOG) {
              // fd 5 is not draining — bound supervisor memory by failing the
              // turn closed instead of queueing more response frames.
              rpcOverflowed = true;
              settle({ outcome: Outcome.FAILED, message: "fd 5 response backlog overflow (child not draining)" });
              forceAbort();
            }
          }
          return ok;
        }
      } catch {
        /* fd closed (child gone) — the exit handler classifies the outcome */
      }
      return true;
    };
    // Shared drain listener: when fd 5 drains, the backpressured backlog has
    // been flushed (the buffer dropped below the high-water mark). Swallow
    // async write errors (e.g. EPIPE when the child exits) — the exit handler
    // classifies the outcome; fd 5 is best-effort once the turn is settling.
    if (rpcRespStream) {
      (rpcRespStream as Writable).on("drain", () => { rpcBacklog = 0; });
      (rpcRespStream as Writable).on("error", () => { /* child gone — exit handler classifies */ });
    }

    // HOR-434: once the child has sent its terminal `result` frame, release the
    // supervisor→child channels (fd 5 response pipe + fd 0 control) by closing
    // our write ends. This delivers EOF on the child's fd-5 read — the only way
    // to reliably complete that pending read on every platform (a child-side
    // close of fd 5 only cancels the in-flight libuv read on macOS, not Linux).
    // The child then performs the documented clean exit (HOR-381: COMPLETED
    // requires a clean child exit), so the provisional result still resolves
    // exactly as the contract specifies — this only makes that exit achievable
    // instead of the liveness watchdog SIGKILLing an already-completed turn as
    // ABORTED. Safe: the child reports its result last, so no further
    // RPC/control writes are expected; `.destroy()` gives an immediate EOF even
    // if the pipe is backpressured, and post-result buffered frames are
    // irrelevant to the child.
    const releaseChildChannels = (): void => {
      if (rpcRespStream) {
        (rpcRespStream as Writable).on("error", () => { /* already released */ });
        try {
          (rpcRespStream as Writable).destroy();
        } catch {
          /* already closed */
        }
      }
      if (control) {
        control.on("error", () => { /* already released */ });
        try {
          control.destroy();
        } catch {
          /* already closed */
        }
      }
    };
    const rpcOnDrain = (listener: () => void): (() => void) => {
      const s = rpcRespStream as Writable | null;
      if (!s) return () => {};
      const onDrain = (): void => listener();
      s.on("drain", onDrain);
      return () => s.off("drain", onDrain);
    };

    // Child → supervisor framed channel (fd 3).
    const frameStream = proc.stdio[3];
    let lastHeartbeat = Date.now();
    let watchdog: ReturnType<typeof setInterval> | null = null;
    let killTimer: ReturnType<typeof setTimeout> | null = null;
    let aborting = false;

    const startWatchdog = (): void => {
      if (watchdog) return;
      watchdog = setInterval(() => {
        if (resultResolved) return;
        if (Date.now() - lastHeartbeat > livenessMs) {
          // Stale child — begin bounded escalation.
          forceAbort();
        }
      }, Math.max(50, Math.floor(livenessMs / 2)));
      watchdog.unref?.();
    };
    // Allow a startup grace (one interval) before enforcing, so a slow pi boot
    // is not misclassified as stale.
    lastHeartbeat = Date.now() + livenessMs; // effectively grants one interval of grace
    startWatchdog();

    if (frameStream) {
      const reader = new FrameReader((raw) => {
        const frame = parseChildFrame(raw);
        if (!frame) return; // malformed/unknown — drop (framing prevents spoofed audit)
        if (frame.type === "heartbeat") {
          lastHeartbeat = Date.now();
          return;
        }
        if (frame.type === "tokenDelta") {
          events.push({ kind: "tokenDelta", contentIndex: frame.contentIndex, deltaType: frame.deltaType, delta: frame.delta });
          return;
        }
        if (frame.type === "event") {
          try {
            const te = fromJson(TurnEventSchema, frame.event as JsonValue);
            events.push({ kind: "event", payload: te.kind });
          } catch {
            /* undecodable event payload — drop (validated before outbox) */
          }
          return;
        }
        if (frame.type === "result") {
          provisional = { outcome: frame.outcome as Outcome, message: frame.message };
          // HOR-434: the child is done — release its IPC channels so its pending
          // fd-5 read completes (EOF) and it can clean-exit (see above).
          releaseChildChannels();
          return;
        }
      });
      frameStream.on("data", (chunk: Buffer) => reader.feed(chunk));
      frameStream.on("end", () => reader.end());
      frameStream.on("error", () => reader.end());
    }

    // Drain stdout/stderr as tagged logs (never the protocol channel). A full
    // pipe would block the child; draining keeps it bounded.
    proc.stdout?.on("data", (d: Buffer) => console.error(`[child:stdout] ${d.toString("utf8").trimEnd()}`));
    proc.stderr?.on("data", (d: Buffer) => console.error(`[child:stderr] ${d.toString("utf8").trimEnd()}`));

    const forceAbort = (): void => {
      if (aborting) return;
      aborting = true;
      try {
        proc.kill("SIGTERM");
      } catch {
        /* already dead */
      }
      // Bounded escalation: SIGKILL after the grace if still alive.
      killTimer = setTimeout(() => {
        try {
          proc.kill("SIGKILL");
        } catch {
          /* already dead */
        }
      }, graceMs);
      killTimer.unref?.();
    };

    proc.on("error", (err) => {
      events.close();
      rpcRequests.close();
      settle({ outcome: Outcome.FAILED, message: `spawn error: ${err.message}` });
    });
    proc.on("exit", (code, signal) => {
      events.close();
      rpcRequests.close();
      if (killTimer) clearTimeout(killTimer);
      if (watchdog) clearInterval(watchdog);
      if (resultResolved) return;
      if (signal || aborting) {
        // Aborted (SIGTERM/SIGKILL) or killed by signal.
        settle({ outcome: Outcome.ABORTED, message: signal ? `child killed by ${signal}` : "aborted" });
      } else if (code === 0 && provisional) {
        // Clean exit with an explicit valid result — resolve the provisional.
        settle(provisional);
      } else if (code === 0) {
        // Clean exit WITHOUT a result — spec: an absent result is FAILED.
        settle({ outcome: Outcome.FAILED, message: "child exited without a result" });
      } else {
        // Non-zero exit overrides a provisional COMPLETED (failed cleanup = FAILED).
        settle({ outcome: Outcome.FAILED, message: `child exit ${code}` });
      }
    });

    return {
      abort: forceAbort,
      events,
      rpcRequests,
      rpcSend,
      rpcOnDrain,
      result,
    };
  };
}

/** Serialize an AssignTurn to JSON for the child (the child reconstructs it). */
function assignmentToJson(at: AssignTurn): unknown {
  return {
    turnId: at.turnId,
    sessionId: at.sessionId,
    sandbox: at.sandbox ? { sandboxId: at.sandbox.sandboxId, uid: at.sandbox.uid, gid: at.sandbox.gid, workingDir: at.sandbox.workingDir } : null,
    persona: at.persona,
    model: at.model ? { id: at.model.id, api: at.model.api, contextWindow: at.model.contextWindow, maxOutputTokens: at.model.maxOutputTokens, thinkingLevel: at.model.thinkingLevel } : null,
    workspaceTools: at.workspaceTools,
    scopeIdentityId: at.scopeIdentityId,
    runId: at.runId,
    workItemId: at.workItemId,
    nodeExecutionId: at.nodeExecutionId,
    nodeKey: at.nodeKey,
    contextJson: at.contextJson,
    completionOutcomes: at.completionOutcomes,
    completionOutputSchemaJson: at.completionOutputSchemaJson,
    skills: at.skills.map((skill) => ({ name: skill.name, version: skill.version, digest: skill.digest })),
    artifactInputs: at.materializations.flatMap((materialization) => materialization.ref ? [{
      artifactId: materialization.ref.artifactId,
      mimeType: materialization.ref.mimeType,
      sizeBytes: materialization.ref.sizeBytes.toString(),
      digest: materialization.ref.digest,
    }] : []),
    message: at.message,
    images: at.images.map((img) => ({ data: Buffer.from(img.data).toString("base64"), mimeType: img.mimeType })),
  };
}

// Re-exported so callers that build raw frames (tests) can encode.
export { encodeFrame };
