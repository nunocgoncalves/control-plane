// HOR-381 child-entrypoint integration test. The supervisor→child assignment
// handoff is a framed `AssignmentFrame` over fd 0; the child decodes it with
// `parseSupervisorFrame` + `parseAssignment` and, on success, emits an
// immediate heartbeat on fd 3 BEFORE session setup. A prior fix commit broke
// this by looking for a nested `.assignment.assignment` that does not exist,
// so every valid frame resolved undefined and the child emitted
// `result{FAILED,"no valid assignment on stdin"}` instead of running the turn.
//
// This test starts the REAL compiled child entrypoint (dist/child.js), writes a
// framed assignment to its stdin, and asserts the first fd-3 frame is a
// heartbeat — proving the real entrypoint parsed the assignment (a stub that
// ignores stdin could not catch this regression). It also covers the malformed
// / EOF paths and the parseAssignment validator directly.

import { describe, it, expect, beforeAll } from "vitest";
import { spawn, execSync } from "node:child_process";
import { mkdtempSync, rmSync, statSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { encodeFrame, parseSupervisorFrame, type ChildFrame } from "./ipc.js";
import { parseAssignment, captureShutdownErrors, createSession, type ExtensionErrorEmitter } from "./child.js";
import { ChildRpc } from "./child-rpc.js";

const HARNESS_ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const CHILD_BIN = join(HARNESS_ROOT, "dist", "child.js");

/** A minimal assignment JSON matching the supervisor's `assignmentToJson` shape. */
function assignmentJson(overrides: Record<string, unknown> = {}): unknown {
  return {
    turnId: "turn-1",
    sessionId: "sess-1",
    sandbox: { sandboxId: "s", uid: 1000, gid: 1000, workingDir: "home" },
    persona: "you are an agent",
    model: { id: "m", api: "openai-completions", contextWindow: 131072, maxOutputTokens: 4096, thinkingLevel: "none" },
    workspaceTools: false,
    scopeIdentityId: "scope-1",
    runId: "run-1",
    message: "hi",
    images: [],
    ...overrides,
  };
}

/** Collect fd-3 frames from a spawned child until `done` returns true or timeout. */
function readFrames(proc: ReturnType<typeof spawn>, done: (frames: ChildFrame[]) => boolean, timeoutMs = 4000): Promise<ChildFrame[]> {
  return new Promise((resolve, reject) => {
    const frames: ChildFrame[] = [];
    let buf = Buffer.alloc(0);
    const fd3 = proc.stdio[3];
    const timer = setTimeout(() => {
      cleanup();
      resolve(frames);
    }, timeoutMs);
    const cleanup = () => {
      clearTimeout(timer);
      fd3?.removeListener("data", onData);
    };
    const onData = (chunk: Buffer): void => {
      buf = Buffer.concat([buf, chunk]);
      while (buf.length >= 4) {
        const len = buf.readUInt32BE(0);
        if (buf.length < 4 + len) break;
        const body = buf.subarray(4, 4 + len);
        buf = buf.subarray(4 + len);
        let parsed: unknown;
        try {
          parsed = JSON.parse(body.toString("utf8"));
        } catch {
          continue;
        }
        // Reuse the child-frame parser for heartbeat/result/tokenDelta/event.
        const f = parseChild(parsed);
        if (f) frames.push(f);
        if (done(frames)) {
          cleanup();
          resolve(frames);
        }
      }
    };
    fd3?.on("data", onData);
  });
}

function parseChild(raw: unknown): ChildFrame | null {
  if (!raw || typeof raw !== "object") return null;
  const r = raw as Record<string, unknown>;
  if (r.type === "heartbeat") return { type: "heartbeat" };
  if (r.type === "result" && typeof r.outcome === "number") return { type: "result", outcome: r.outcome, ...(typeof r.message === "string" ? { message: r.message } : {}) };
  return null;
}

describe("HOR-381 child entrypoint assignment handoff", { timeout: 30_000 }, () => {
  beforeAll(() => {
    // Build the compiled entrypoint (tsc -> dist) only when stale, so a warm
    // checkout (CI built it) doesn't pay the tsc cost. Fast (~1s) when needed.
    const srcMtime = Math.max(
      statSync(join(HARNESS_ROOT, "src", "child.ts")).mtimeMs,
      statSync(join(HARNESS_ROOT, "src", "ipc.ts")).mtimeMs,
    );
    if (!existsSync(CHILD_BIN) || statSync(CHILD_BIN).mtimeMs < srcMtime) {
      execSync("npm run build", { cwd: HARNESS_ROOT, stdio: "ignore" });
    }
  }, 30_000);

  it("parses a framed assignment and emits a heartbeat (not a 'no assignment' failure)", async () => {
    const tmp = mkdtempSync(join(tmpdir(), "harness-child-"));
    const proc = spawn(process.execPath, [CHILD_BIN], {
      stdio: ["pipe", "pipe", "pipe", "pipe", "pipe", "pipe"],
      env: {
        ...process.env,
        HARNESS_SESSION_DIR: join(tmp, "session"),
        HARNESS_WORKING_DIR: tmp,
        HARNESS_PI_DIRS: "",
        HARNESS_MODEL_MAX_ATTEMPTS: "0",
        HARNESS_LIVENESS_INTERVAL_MS: "60000",
        HOME: tmp,
      },
    });
    proc.stderr.on("data", () => {}); // drain
    proc.stdout.on("data", () => {}); // drain
    // Write the framed assignment the way the supervisor does.
    proc.stdin.write(encodeFrame({ type: "assignment", assignment: assignmentJson() }));

    const frames = await readFrames(proc, (fs) => fs.some((f) => f.type === "heartbeat" || f.type === "result"), 4000);
    proc.kill("SIGKILL");
    try {
      proc.kill();
    } catch {
      /* already dead */
    }
    rmSync(tmp, { recursive: true, force: true });

    const first = frames[0];
    expect(first).toBeDefined();
    expect(first!.type).toBe("heartbeat"); // proves the assignment was parsed
    // No 'no valid assignment' failure may precede the heartbeat.
    expect(frames.find((f) => f.type === "result" && f.message?.includes("no valid assignment"))).toBeUndefined();
  });

  it("emits FAILED 'no valid assignment' when stdin closes with no frame", async () => {
    const tmp = mkdtempSync(join(tmpdir(), "harness-child-"));
    const proc = spawn(process.execPath, [CHILD_BIN], {
      stdio: ["pipe", "pipe", "pipe", "pipe", "pipe", "pipe"],
      env: {
        ...process.env,
        HARNESS_SESSION_DIR: join(tmp, "session"),
        HARNESS_WORKING_DIR: tmp,
        HARNESS_PI_DIRS: "",
        HARNESS_MODEL_MAX_ATTEMPTS: "0",
        HARNESS_LIVENESS_INTERVAL_MS: "60000",
        HOME: tmp,
      },
    });
    proc.stderr.on("data", () => {});
    proc.stdout.on("data", () => {});
    proc.stdin.end(); // no assignment frame — EOF

    const frames = await readFrames(proc, (fs) => fs.some((f) => f.type === "result"), 4000);
    try {
      proc.kill("SIGKILL");
    } catch {
      /* already dead */
    }
    rmSync(tmp, { recursive: true, force: true });

    const result = frames.find((f) => f.type === "result");
    expect(result).toBeDefined();
    expect(result!.type === "result" && result!.message).toContain("no valid assignment");
  });
});

describe("captureShutdownErrors", () => {
  /** A minimal fake of pi's ExtensionRunner error surface. */
  function fakeEmitter(): { emitter: ExtensionErrorEmitter; emit: (e: { event: string; extensionPath: string; error: string }) => void; state: { unsubbed: boolean } } {
    let listener: ((e: { event: string; extensionPath: string; error: string }) => void) | undefined;
    const state = { unsubbed: false };
    const emitter: ExtensionErrorEmitter = {
      onError(l) {
        listener = l;
        return () => {
          state.unsubbed = true;
          listener = undefined;
        };
      },
    };
    return { emitter, emit: (e) => listener?.(e), state };
  }

  it("captures session_shutdown handler failures (the pi swallowing path)", () => {
    const { emitter, emit } = fakeEmitter();
    const shutdown = captureShutdownErrors(emitter);
    // pi's ExtensionRunner.emit() catches a throwing session_shutdown handler
    // and routes it here instead of rethrowing to dispose().
    emit({ event: "session_shutdown", extensionPath: "ext-a", error: "flush failed" });
    expect(shutdown.errors).toEqual(["ext-a: flush failed"]);
  });

  it("ignores non-shutdown extension errors", () => {
    const { emitter, emit } = fakeEmitter();
    const shutdown = captureShutdownErrors(emitter);
    emit({ event: "message_end", extensionPath: "ext-a", error: "unrelated" });
    emit({ event: "tool_result", extensionPath: "ext-b", error: "x" });
    expect(shutdown.errors).toEqual([]);
  });

  it("unsubscribes so a failed cleanup can be classified without double-counting", () => {
    const { emitter, emit, state } = fakeEmitter();
    const shutdown = captureShutdownErrors(emitter);
    shutdown.unsubscribe();
    expect(state.unsubbed).toBe(true);
    emit({ event: "session_shutdown", extensionPath: "ext-a", error: "late" });
    expect(shutdown.errors).toEqual([]);
  });

  it("a successful prompt with a shutdown error is non-empty → caller classifies FAILED", () => {
    const { emitter, emit } = fakeEmitter();
    const shutdown = captureShutdownErrors(emitter);
    emit({ event: "session_shutdown", extensionPath: "ext-a", error: "boom" });
    // Caller also records any dispose() rejection into the same list.
    shutdown.errors.push("dispose: timed out");
    expect(shutdown.errors.length).toBe(2);
  });
});

describe("parseAssignment", () => {
  it("round-trips a valid assignment frame (no nested .assignment)", () => {
    const frame = parseSupervisorFrame({ type: "assignment", assignment: assignmentJson() });
    expect(frame?.type).toBe("assignment");
    const a = parseAssignment(frame!.assignment);
    expect(a).toBeDefined();
    expect(a!.turnId).toBe("turn-1");
    expect(a!.sessionId).toBe("sess-1");
    expect(a!.model.id).toBe("m");
    expect(a!.model.contextWindow).toBe(131072);
    expect(a!.model.maxOutputTokens).toBe(4096);
    expect(a!.workspaceTools).toBe(false);
    expect(a!.runId).toBe("run-1");
    expect(a!.message).toBe("hi");
  });

  it("rejects a malformed assignment (nested .assignment, like the bug)", () => {
    // The regression: a frame whose .assignment is itself {assignment: {...}}.
    const a = parseAssignment({ assignment: assignmentJson() });
    expect(a).toBeUndefined();
  });

  it("rejects missing/invalid fields", () => {
    expect(parseAssignment({ turnId: "t" })).toBeUndefined();
    expect(parseAssignment({ ...assignmentJson(), model: { id: "m" } })).toBeUndefined();
    expect(parseAssignment({ ...assignmentJson(), workspaceTools: "yes" })).toBeUndefined();
  });
});

describe("createSession credentialless custom provider (HOR-395)", { timeout: 30_000 }, () => {
  // Regression: the custom provider previously supplied `models` without
  // baseUrl/apiKey, which pi's validateProviderConfig rejects ("baseUrl is
  // required when defining models"), so every real turn failed during session
  // setup. The credentialless path registers streamSimple+api only and supplies
  // the model directly — this test creates the real pi session/provider to
  // prove registration succeeds.
  it("creates a pi session without baseUrl/apiKey by registering streamSimple only", async () => {
    const tmp = mkdtempSync(join(tmpdir(), "harness-session-"));
    try {
      const rpc = new ChildRpc({ write: () => {} }); // fd 4 unused during creation
      const a = parseAssignment(assignmentJson())!;
      const sessionDir = join(tmp, "session");
      const runtime = await createSession(a, sessionDir, tmp, [], 1, rpc, []);
      expect(runtime.session).toBeDefined();
      // The assigned model is the one we constructed directly (no registry lookup).
      expect(runtime.session.model?.id).toBe(a.model.id);
      await runtime.dispose();
    } finally {
      rmSync(tmp, { recursive: true, force: true });
    }
  });

  it("registers gateway tool stubs in the agent allow-set even with workspaceTools=false (HOR-395/ARCH-006)", async () => {
    // Regression: passing `noTools:"all"` (or only the four built-ins) made
    // pi filter every gateway stub out of the registry, so gateway tools never
    // reached the agent. The allow-set now includes gateway descriptor names.
    const tmp = mkdtempSync(join(tmpdir(), "harness-session-"));
    try {
      const rpc = new ChildRpc({ write: () => {} });
      const a = parseAssignment({ ...assignmentJson(), workspaceTools: false })!;
      const descriptors = [
        { name: "graph.read_mail", version: "1.0.0", digest: "sha256:abc", description: "read mail", inputSchema: { type: "object" }, effectClass: "read_only" as const },
      ];
      const sessionDir = join(tmp, "session");
      const runtime = await createSession(a, sessionDir, tmp, [], 1, rpc, descriptors);
      const active = runtime.session.getActiveToolNames();
      expect(active).toContain("graph.read_mail");
      // workspaceTools=false → no local bash/read/write/edit.
      expect(active).not.toContain("bash");
      await runtime.dispose();
    } finally {
      rmSync(tmp, { recursive: true, force: true });
    }
  });
});
