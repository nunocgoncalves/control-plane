import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { spawn } from "node:child_process";
import { writeFileSync, mkdtempSync, rmSync, mkdirSync, chmodSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { create } from "@bufbuild/protobuf";
import {
  AssignTurnSchema,
  AssistantMessageSchema,
  ModelConfigSchema,
  Outcome,
  SandboxRefSchema,
  ToolAllowListSchema,
  TurnEventSchema,
  type AssignTurn,
} from "./gen/iterabase/harness/v1/harness_pb.js";
import { createChildFactory } from "./child-process.js";
import type { HarnessConfig } from "./config.js";

const UID = process.getuid();
const GID = process.getgid();

// A stub child: writes one AssistantMessage event + a COMPLETED result over
// the framed fd-3 IPC channel (4-byte BE length + JSON), then exits.
const STUB_CHILD = `const fs = require('fs');
function frame(obj){const j=Buffer.from(JSON.stringify(obj));const h=Buffer.alloc(4);h.writeUInt32BE(j.length,0);fs.writeSync(3,Buffer.concat([h,j]));}
frame({type:'heartbeat'});
frame({type:'event',event:{turnId:'t',sequence:'0',timestampMs:'0',assistantMessage:{text:'hi'}}});
frame({type:'result',outcome:1});
`;

let dir: string;
let stubScript: string;
beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), "harness-cp-"));
  stubScript = join(dir, "stub-child.cjs");
  writeFileSync(stubScript, STUB_CHILD);
  const root = join(dir, "sess-a");
  mkdirSync(root, { recursive: true });
  for (const sub of ["home", "tmp", "session", "workspace"]) mkdirSync(join(root, sub), { recursive: true });
  chmodSync(root, 0o700);
});
afterEach(() => {
  rmSync(dir, { recursive: true, force: true });
});

function cfg(): HarnessConfig {
  return {
    controlPlane: { url: "", serverName: "" },
    worker: { workerId: "pod-1", poolId: "pool-1" },
    tls: { cert: "", key: "", ca: "" },
    sandboxRoot: dir,
    piDirs: [],
    egressProxyUrl: "https://localhost:8444",
    walDir: dir,
    probe: { port: 0 },
    transport: { http2PingIntervalMs: 30000, http2PingTimeoutMs: 10000 },
    reconnect: { initialBackoffMs: 1, maxBackoffMs: 2, resetAfterMs: 1000 },
    child: { livenessIntervalMs: 1000, abortGraceMs: 1000 },
    outbox: { bound: 4096 },
    modelRetry: { maxAttempts: 3 },
    tokenDelta: { sendBufferBytes: 1048576 },
  } as HarnessConfig;
}

function assignment(): AssignTurn {
  return create(AssignTurnSchema, {
    turnId: "turn-1",
    sessionId: "sess-1",
    sandbox: create(SandboxRefSchema, { sandboxId: "sess-a", uid: UID, gid: GID, workingDir: "home" }),
    persona: "you are an agent",
    model: create(ModelConfigSchema, { id: "m", api: "openai-completions", contextWindow: 131072 }),
    toolAllowList: create(ToolAllowListSchema, { all: true }),
    message: "hi",
  });
}

// Injectable launch that spawns node directly (bypasses setpriv — Linux-only — for macOS tests).
function launchStub(opts: Parameters<Parameters<typeof createChildFactory>[2]>[0]) {
  return spawn(process.execPath, [opts.script], {
    cwd: join(opts.sandboxRoot, opts.workingDir),
    stdio: opts.stdio,
    env: { ...opts.env, PATH: process.env.PATH ?? "" },
  });
}

describe("createChildFactory", () => {
  it("spawns the child, relays its event + result over IPC", async () => {
    const factory = createChildFactory(cfg(), stubScript, launchStub);
    const sandbox = { root: join(dir, "sess-a"), home: join(dir, "sess-a/home"), tmp: join(dir, "sess-a/tmp"), session: join(dir, "sess-a/session"), workspace: join(dir, "sess-a/workspace") };
    const child = factory(assignment(), sandbox, join(dir, "sess-a/home"));

    const events: import("./supervisor.js").ChildEvent[] = [];
    for await (const ev of child.events) events.push(ev);
    const result = await child.result;

    expect(events).toHaveLength(1);
    expect(events[0]!.payload.case).toBe("assistantMessage");
    expect(events[0]!.payload.case === "assistantMessage" && events[0]!.payload.value.text).toBe("hi");
    expect(result.outcome).toBe(Outcome.COMPLETED);
  });

  it("classifies a non-zero exit (no result) as FAILED", async () => {
    const failScript = join(dir, "fail.cjs");
    writeFileSync(failScript, 'process.exit(3);\n');
    const factory = createChildFactory(cfg(), failScript, launchStub);
    const sandbox = { root: join(dir, "sess-a"), home: "", tmp: "", session: "", workspace: "" };
    const child = factory(assignment(), sandbox as never, "");
    const result = await child.result;
    expect(result.outcome).toBe(Outcome.FAILED);
    expect(result.message).toContain("3");
  });

  it("abort() sends SIGTERM (child exits; classified ABORTED if no result)", async () => {
    const hangScript = join(dir, "hang.cjs");
    writeFileSync(hangScript, 'setInterval(()=>{}, 1000);\n');
    const factory = createChildFactory(cfg(), hangScript, launchStub);
    const sandbox = { root: join(dir, "sess-a"), home: "", tmp: "", session: "", workspace: "" };
    const child = factory(assignment(), sandbox as never, "");
    setTimeout(() => child.abort(), 50);
    const result = await child.result;
    expect(result.outcome).toBe(Outcome.ABORTED);
  });
});
