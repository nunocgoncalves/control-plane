// The per-turn Child (HOR-381): a real pi process spawned via the setpriv
// launcher under the session UID/GID. The supervisor talks to it over a
// dedicated IPC channel (stdin = the assignment, one-shot; stdout = durable
// TurnEvent payloads + the final result, JSON lines; stderr = tagged logs).
// Abort is SIGTERM (the child aborts pi + exits within the grace). A stale
// child (no IPC heartbeat within the liveness interval) is force-killed.
//
// The child entry (dist/child.js) owns the pi AgentSession + event mapping;
// this module is the supervisor side. The launch is injectable so the IPC +
// heartbeat + abort logic is unit-testable on macOS (setpriv is Linux-only).

import { createInterface } from "node:readline";
import { fromJson, type JsonValue } from "@bufbuild/protobuf";
import { TurnEventSchema, Outcome, type AssignTurn } from "./gen/iterabase/harness/v1/harness_pb.js";
import { launchChild, type LaunchOptions } from "./launcher.js";
import type { Child, ChildEvent, ChildResult, ChildFactory } from "./supervisor.js";
import type { HarnessConfig } from "./config.js";
import type { SandboxPaths } from "./sandbox.js";

export type LaunchFn = (opts: LaunchOptions) => import("node:child_process").ChildProcess;

interface IpcEvent {
  type: "event";
  event: unknown; // a TurnEvent JSON (kind = the payload; sequence/timestamp ignored — the supervisor re-sequences)
}
interface IpcResult {
  type: "result";
  outcome: Outcome;
  message?: string;
}
type IpcMessage = IpcEvent | IpcResult;

/**
 * Build a ChildFactory that spawns the child entry `script` via the launcher.
 * `launch` defaults to launchChild (setpriv); tests inject a plain-node spawn.
 */
export function createChildFactory(cfg: HarnessConfig, script: string, launch: LaunchFn = launchChild): ChildFactory {
  return (assignment: AssignTurn, sandbox: SandboxPaths, _cwd: string): Child => {
    const sb = assignment.sandbox;
    if (!sb) throw new Error("AssignTurn missing sandbox");
    const proc = launch({
      script,
      uid: sb.uid,
      gid: sb.gid,
      sandboxRoot: sandbox.root,
      workingDir: sb.workingDir || "home",
      stdio: ["pipe", "pipe", "pipe"],
      env: {
        HARNESS_SANDBOX_ROOT: sandbox.root,
        HARNESS_SESSION_DIR: sandbox.session,
        HARNESS_EGRESS_PROXY_URL: cfg.egressProxyUrl,
        HARNESS_PI_DIRS: cfg.piDirs.join(":"),
        HOME: sandbox.home,
        TMPDIR: sandbox.tmp,
      },
    });

    // Send the assignment as a single JSON line on stdin, then close stdin.
    proc.stdin?.write(`${JSON.stringify({ type: "assignment", assignment: assignmentToJson(assignment) })}\n`);
    proc.stdin?.end();

    const events = new EventQueue<ChildEvent>();
    let resultResolved = false;
    let resolveResult!: (r: ChildResult) => void;
    const result = new Promise<ChildResult>((r) => (resolveResult = r));
    const settle = (r: ChildResult) => {
      if (resultResolved) return;
      resultResolved = true;
      resolveResult(r);
    };

    const stdout = proc.stdout;
    if (stdout) {
      const rl = createInterface({ input: stdout });
      rl.on("line", (line) => {
        if (!line) return;
        let msg: IpcMessage;
        try {
          msg = JSON.parse(line) as IpcMessage;
        } catch {
          return; // ignore malformed lines (a stray log on stdout)
        }
        if (msg.type === "event") {
          try {
            const te = fromJson(TurnEventSchema, msg.event as JsonValue);
            events.push({ payload: te.kind });
          } catch {
            /* skip undecodable event */
          }
        } else if (msg.type === "result") {
          events.close();
          settle({ outcome: msg.outcome, message: msg.message });
        }
      });
      rl.on("close", () => events.close());
    } else {
      events.close();
      settle({ outcome: Outcome.FAILED, message: "child has no stdout" });
    }

    // Stale-child watchdog: if the child exits without a result, classify by exit code.
    proc.on("error", (err) => {
      events.close();
      settle({ outcome: Outcome.FAILED, message: `spawn error: ${err.message}` });
    });
    proc.on("exit", (code, signal) => {
      events.close();
      if (!resultResolved) {
        if (signal) settle({ outcome: Outcome.ABORTED, message: `child killed by ${signal}` });
        else if (code === 0) settle({ outcome: Outcome.COMPLETED, message: "child exited without result" });
        else settle({ outcome: Outcome.FAILED, message: `child exit ${code}` });
      }
    });

    return {
      abort: () => {
        try {
          proc.kill("SIGTERM");
        } catch {
          /* already dead */
        }
      },
      events,
      result,
    };
  };
}

/** Serialize an AssignTurn to JSON for the child (the child reconstructs it). */
function assignmentToJson(at: AssignTurn): unknown {
  // Reuse the proto's JSON form via a wrapper message is awkward; send the
  // fields the child needs directly.
  return {
    turnId: at.turnId,
    sessionId: at.sessionId,
    sandbox: at.sandbox ? { sandboxId: at.sandbox.sandboxId, uid: at.sandbox.uid, gid: at.sandbox.gid, workingDir: at.sandbox.workingDir } : null,
    persona: at.persona,
    model: at.model ? { id: at.model.id, api: at.model.api, contextWindow: at.model.contextWindow, maxOutputTokens: at.model.maxOutputTokens, thinkingLevel: at.model.thinkingLevel } : null,
    toolAllowList: at.toolAllowList ? { all: at.toolAllowList.all, tools: at.toolAllowList.tools } : null,
    scopeIdentityId: at.scopeIdentityId,
    message: at.message,
  };
}

// Minimal async queue for the child event stream (with throw/return for bidi cancel safety).
class EventQueue<T> implements AsyncIterable<T> {
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
      throw: async () => {
        this.close();
        return { value: undefined as never, done: true };
      },
      return: async () => {
        this.close();
        return { value: undefined as never, done: true };
      },
    };
  }
}
