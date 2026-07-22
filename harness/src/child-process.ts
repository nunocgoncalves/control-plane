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
import { fromJson, type JsonValue } from "@bufbuild/protobuf";
import { TurnEventSchema, Outcome, type AssignTurn } from "./gen/iterabase/harness/v1/harness_pb.js";
import { launchChild, type LaunchOptions } from "./launcher.js";
import type { Child, ChildEvent, ChildResult, ChildFactory } from "./supervisor.js";
import type { HarnessConfig } from "./config.js";
import type { SandboxPaths } from "./sandbox.js";
import { AsyncQueue } from "./async-queue.js";
import { FrameReader, encodeFrame, parseChildFrame, writeFrame } from "./ipc.js";

export type LaunchFn = (opts: LaunchOptions) => ChildProcess;

/** stdio for the child: fd0 control (sup→child), fd1 stdout logs, fd2 stderr logs, fd3 frames (child→sup). */
const CHILD_STDIO = ["pipe", "pipe", "pipe", "pipe"] as const;

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
        HARNESS_EGRESS_PROXY_URL: cfg.egressProxyUrl,
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
      settle({ outcome: Outcome.FAILED, message: `spawn error: ${err.message}` });
    });
    proc.on("exit", (code, signal) => {
      events.close();
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
    toolAllowList: at.toolAllowList ? { all: at.toolAllowList.all, tools: at.toolAllowList.tools } : null,
    scopeIdentityId: at.scopeIdentityId,
    message: at.message,
    images: at.images.map((img) => ({ data: Buffer.from(img.data).toString("base64"), mimeType: img.mimeType })),
  };
}

// Re-exported so callers that build raw frames (tests) can encode.
export { encodeFrame };
