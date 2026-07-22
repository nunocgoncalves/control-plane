// HOR-381 transport integration test (tier 2): drives the REAL TypeScript
// gRPC+mTLS client (createGrpcTransport — native gRPC over HTTP/2) against a
// minimal in-memory Go connect-go server (harness/testserver). Exercises the
// Work wire contract end-to-end: Hello/Welcome/fencing, AssignTurn, EventAck,
// Heartbeat, TokenDelta receipt, AbortTurn, stream-loss + reconnect, unacked-
// tail replay + ACK, Ready-after-replay, and HTTP/2-only (no HTTP/1.1 fallback).
//
// The Go server generates its own CA + server/client certs in memory, so no
// external fixtures are required. Skipped when `go` is unavailable.

import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { spawn, execSync } from "node:child_process";
import { mkdtempSync, rmSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { create } from "@bufbuild/protobuf";
import {
  WorkerMessageSchema,
  HelloSchema,
  ReadySchema,
  TurnEventSchema,
  AssistantMessageSchema,
  WorkerOutcomeSchema,
  TokenDeltaSchema,
  HeartbeatSchema,
  DeltaType,
  Outcome,
  WorkerState,
  PiPhase,
  type TurnEvent,
  type WorkerMessage,
} from "./gen/iterabase/harness/v1/harness_pb.js";
import { createWorkTransport, openWorkStream } from "./work-client.js";
import type { HarnessConfig } from "./config.js";

const REPO_ROOT = join(dirname(fileURLToPath(import.meta.url)), "..", "..");
const hasGo = (() => {
  try {
    execSync("go version", { stdio: "ignore" });
    return true;
  } catch {
    return false;
  }
})();

const itOrSkip = hasGo ? it : it.skip;

describe("HOR-381 gRPC+mTLS transport integration (Go testserver)", { timeout: 60_000 }, () => {
  let bin: string;
  let proc: ReturnType<typeof spawn>;
  let cfg: HarnessConfig;
  let ready: { addr: string; ca: string; cert: string; key: string; serverName: string };
  const stdoutLines: string[] = [];
  let tmpBinDir: string;

  beforeAll(async () => {
    tmpBinDir = mkdtempSync(join(tmpdir(), "harness-testserver-bin-"));
    bin = join(tmpBinDir, "harness-testserver");
    execSync(`go build -o ${bin} ./harness/testserver`, { cwd: REPO_ROOT, stdio: "inherit" });
    proc = spawn(bin, [], { stdio: ["ignore", "pipe", "pipe"] });
    proc.stderr.on("data", (d: Buffer) => console.error(`[testserver:stderr] ${d.toString("utf8").trimEnd()}`));

    ready = await new Promise((resolve, reject) => {
      const onOut = (d: Buffer) => {
        const text = d.toString("utf8");
        for (const line of text.split("\n")) {
          if (!line) continue;
          stdoutLines.push(line);
          const trimmed = line.trim();
          if (trimmed.startsWith("{") && trimmed.includes('"ready"')) {
            try {
              resolve(JSON.parse(trimmed));
            } catch (e) {
              reject(e);
            }
          }
        }
      };
      proc.stdout!.on("data", onOut);
      proc.on("error", reject);
      setTimeout(() => reject(new Error("testserver did not become ready")), 30_000);
    });

    cfg = {
      controlPlane: { url: ready.addr, serverName: ready.serverName },
      worker: { workerId: "pod-1", poolId: "pool-1" },
      tls: { cert: ready.cert, key: ready.key, ca: ready.ca },
      sandboxRoot: "",
      piDirs: [],
      egressProxyUrl: "",
      walDir: "",
      probe: { port: 0 },
      transport: { http2PingIntervalMs: 30000, http2PingTimeoutMs: 10000 },
      reconnect: { initialBackoffMs: 1, maxBackoffMs: 2, resetAfterMs: 1000 },
      child: { livenessIntervalMs: 1000, abortGraceMs: 1000 },
      outbox: { bound: 4096 },
      modelRetry: { maxAttempts: 3 },
      tokenDelta: { sendBufferBytes: 1048576 },
    } as HarnessConfig;
  }, 60_000);

  afterAll(async () => {
    try {
      await new Promise<void>((resolve) => {
        if (proc.exitCode !== null) return resolve();
        proc.once("exit", () => resolve());
        setTimeout(() => {
          try { proc.kill("SIGKILL"); } catch { /* */ }
          resolve();
        }, 5000);
      });
    } finally {
      rmSync(tmpBinDir, { recursive: true, force: true });
    }
  });

  itOrSkip("exercises the full Work wire contract over native gRPC+mTLS", async () => {
    const hello = create(WorkerMessageSchema, {
      kind: { case: "hello", value: create(HelloSchema, { workerId: "pod-1", poolId: "pool-1", protocolVersion: "1" }) },
    });

    // --- Connection 1 (fencing generation 1) ---
    const conn1 = await openWorkStream(hello, createWorkTransport(cfg));
    expect(conn1.welcome.protocolVersion).toBe("1");
    expect(conn1.welcome.fencingGeneration).toBe(1n);
    const it1 = conn1.stream.control[Symbol.asyncIterator]();

    conn1.stream.send(create(WorkerMessageSchema, { kind: { case: "ready", value: create(ReadySchema, {}) } }));
    const assignMsg = await it1.next();
    expect(assignMsg.value?.kind.case).toBe("assignTurn");
    expect(assignMsg.value?.kind.value?.turnId).toBe("turn-1");

    // Durable assistantMessage (seq 1) -> expect ACK through 1.
    conn1.stream.send(turnEvent("turn-1", 1n, { case: "assistantMessage", value: create(AssistantMessageSchema, { text: "hi" }) }));
    const ack1 = await it1.next();
    expect(ack1.value?.kind.case).toBe("eventAck");
    expect(Number(ack1.value!.kind!.value!.throughSequence)).toBe(1);

    // Ephemeral token delta (non-sequenced, non-ACKed).
    conn1.stream.send(create(WorkerMessageSchema, {
      kind: { case: "tokenDelta", value: create(TokenDeltaSchema, { turnId: "turn-1", contentIndex: 0, type: DeltaType.TEXT, delta: "hel" }) },
    }));
    // Heartbeat (ephemeral control data).
    conn1.stream.send(create(WorkerMessageSchema, {
      kind: { case: "heartbeat", value: create(HeartbeatSchema, { state: WorkerState.RUNNING, turnId: "turn-1", piPhase: PiPhase.MODEL_CALL, highestSequence: 1n }) },
    }));

    // Terminal workerOutcome (seq 2) -> ACK through 2 + AbortTurn, then the
    // server closes the stream (simulated stream loss). The control iteration
    // surfaces this as a stream error (Premature close) — the same signal the
    // supervisor's connectAndServe loop catches and routes to failClosed.
    conn1.stream.send(turnEvent("turn-1", 2n, { case: "workerOutcome", value: create(WorkerOutcomeSchema, { outcome: Outcome.COMPLETED }) }));
    const ack2 = await it1.next();
    expect(Number(ack2.value!.kind!.value!.throughSequence)).toBe(2);
    const abortMsg = await it1.next();
    expect(abortMsg.value?.kind.case).toBe("abortTurn");
    await expect(it1.next()).rejects.toThrow(); // stream loss
    conn1.stream.close();

    // --- Connection 2 (reconnect; fencing generation 2; replay the unacked tail) ---
    const conn2 = await openWorkStream(hello, createWorkTransport(cfg));
    expect(conn2.welcome.fencingGeneration).toBe(2n);
    const it2 = conn2.stream.control[Symbol.asyncIterator]();

    // Replay the unacked audit tail (the workerOutcome, seq 2) as after_terminal.
    conn2.stream.send(turnEvent("turn-1", 2n, { case: "workerOutcome", value: create(WorkerOutcomeSchema, { outcome: Outcome.ABORTED, message: "stream loss" }) }));
    const replayAck = await it2.next();
    expect(replayAck.value?.kind.case).toBe("eventAck");
    expect(Number(replayAck.value!.kind!.value!.throughSequence)).toBe(2);

    // Ready is advertised only after the replay is ACKed; then the server ends OK.
    conn2.stream.send(create(WorkerMessageSchema, { kind: { case: "ready", value: create(ReadySchema, {}) } }));
    conn2.stream.close();
    const end2 = await it2.next();
    expect(end2.done).toBe(true); // clean OK end-of-stream (not another stream loss)

    // --- Server report ---
    const report = await readReport();
    expect(report.handshake).toBe(true);
    expect(report.identityVerified).toBe(true);
    expect(report.assignTurnSent).toBe(true);
    expect(report.ackMatched).toBe(true);
    expect(report.tokenDeltaReceived).toBe(true);
    expect(report.heartbeatReceived).toBe(true);
    expect(report.abortTurnSent).toBe(true);
    expect(report.replayAcked).toBe(true);
    expect(report.readyAfterReplay).toBe(true);
    expect(report.http2Only).toBe(true);
    expect(report.fencingGenerations).toEqual([1, 2]);
    expect(report.error).toBeFalsy();
  });

  function turnEvent(turnId: string, seq: bigint, kind: TurnEvent["kind"]): WorkerMessage {
    return create(WorkerMessageSchema, {
      kind: { case: "turnEvent", value: create(TurnEventSchema, { turnId, sequence: seq, timestampMs: 0n, kind }) },
    });
  }

  async function readReport(): Promise<{
    handshake: boolean; identityVerified: boolean; assignTurnSent: boolean; ackMatched: boolean;
    tokenDeltaReceived: boolean; heartbeatReceived: boolean; abortTurnSent: boolean;
    replayAcked: boolean; readyAfterReplay: boolean; http2Only: boolean; fencingGenerations: number[]; error: string;
  }> {
    return new Promise((resolve, reject) => {
      const onOut = (d: Buffer) => {
        for (const line of d.toString("utf8").split("\n")) {
          if (!line) continue;
          stdoutLines.push(line);
          if (line.startsWith("REPORT ")) {
            proc.stdout!.off("data", onOut);
            resolve(JSON.parse(line.slice("REPORT ".length)));
          }
        }
      };
      proc.stdout!.on("data", onOut);
      setTimeout(() => reject(new Error("no REPORT line; lines: " + stdoutLines.join("|"))), 20_000);
    });
  }
});
