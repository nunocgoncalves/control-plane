import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { mkdtempSync, rmSync, writeFileSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import * as http2 from "node:http2";
import * as crypto from "node:crypto";
import { execSync } from "node:child_process";
import { streamModel, assertAssignedModel, ModelBridgeError } from "./model-bridge.js";
import type { HarnessConfig } from "./config.js";

// Real HTTP/2 mTLS + SSE integration test for the model bridge (HOR-395
// validation). A local http2 TLS server requires+verifies the client cert
// (mTLS) and streams OpenAI SSE chunks; the bridge presents the worker cert
// and forwards chunks. Closes the ARCH-010/011 contract: the supervisor holds
// the cert, the child never does, and bodies never touch the Work stream.

let dir: string;
let caPath: string;
let srvCertPath: string;
let srvKeyPath: string;
let cliCertPath: string;
let cliKeyPath: string;

function cfg(baseUrl: string): HarnessConfig {
  return {
    controlPlane: { url: "", serverName: "" },
    worker: { workerId: "pod-1", poolId: "pool-1" },
    tls: { cert: cliCertPath, key: cliKeyPath, ca: caPath },
    sandboxRoot: "",
    piDirs: [],
    toolGateway: { url: "", serverName: "" },
    inferenceGateway: { url: baseUrl, serverName: "localhost" },
    walDir: "",
    probe: { port: 0 },
    transport: { http2PingIntervalMs: 30000, http2PingTimeoutMs: 10000 },
    reconnect: { initialBackoffMs: 1, maxBackoffMs: 2, resetAfterMs: 1000 },
    child: { livenessIntervalMs: 1000, abortGraceMs: 1000 },
    outbox: { bound: 4096 },
    modelRetry: { maxAttempts: 3 },
    tokenDelta: { sendBufferBytes: 1048576 },
  } as HarnessConfig;
}

/** Generate a CA + server (localhost) + client cert, all via openssl. */
function genCerts(d: string): void {
  const run = (args: string[]): void => {
    execSync(`openssl ${args.join(" ")}`, { stdio: "ignore", env: { ...process.env, RANDFILE: join(d, ".rnd") } });
  };
  // CA
  run(["genrsa", "-out", join(d, "ca.key"), "2048"]);
  run(["req", "-x509", "-new", "-nodes", "-key", join(d, "ca.key"), "-sha256", "-days", "1", "-subj", "/CN=test-ca", "-out", join(d, "ca.crt")]);
  // server
  run(["genrsa", "-out", join(d, "srv.key"), "2048"]);
  writeFileSync(join(d, "srv.cnf"), `[req]\ndistinguished_name=req\nreq_extensions=v3\n[v3]\nsubjectAltName=DNS:localhost,IP:127.0.0.1\n[req]\n`);
  run(["req", "-new", "-key", join(d, "srv.key"), "-subj", "/CN=localhost", "-config", join(d, "srv.cnf"), "-out", join(d, "srv.csr")]);
  writeFileSync(join(d, "srv.ext"), "subjectAltName=DNS:localhost,IP:127.0.0.1\n");
  run(["x509", "-req", "-in", join(d, "srv.csr"), "-CA", join(d, "ca.crt"), "-CAkey", join(d, "ca.key"), "-CAcreateserial", "-out", join(d, "srv.crt"), "-days", "1", "-sha256", "-extfile", join(d, "srv.ext")]);
  // client
  run(["genrsa", "-out", join(d, "cli.key"), "2048"]);
  run(["req", "-new", "-key", join(d, "cli.key"), "-subj", "/CN=harness-worker", "-out", join(d, "cli.csr")]);
  run(["x509", "-req", "-in", join(d, "cli.csr"), "-CA", join(d, "ca.crt"), "-CAkey", join(d, "ca.key"), "-CAcreateserial", "-out", join(d, "cli.crt"), "-days", "1", "-sha256"]);
  caPath = join(d, "ca.crt");
  srvCertPath = join(d, "srv.crt");
  srvKeyPath = join(d, "srv.key");
  cliCertPath = join(d, "cli.crt");
  cliKeyPath = join(d, "cli.key");
}

/** Start an mTLS http2 server that streams the given SSE payloads. */
function startServer(sseChunks: string[], onRequest?: (headers: http2.IncomingHttpHeaders) => void): { url: string; close: () => Promise<void> } {
  const ca = readFileSync(caPath);
  const cert = readFileSync(srvCertPath);
  const key = readFileSync(srvKeyPath);
  const server = http2.createSecureServer({ ca, cert, key, requestCert: true, rejectUnauthorized: true });
  server.on("stream", (stream, headers) => {
    onRequest?.(headers);
    stream.respond({ "content-type": "text/event-stream", ":status": 200 });
    for (const chunk of sseChunks) stream.write(chunk);
    stream.end();
  });
  return new Promise((resolve) => {
    server.listen(0, "127.0.0.1", () => {
      const addr = server.address() as { port: number };
      resolve({
        url: `https://localhost:${addr.port}`,
        close: () => new Promise<void>((r) => server.close(() => r())),
      });
    });
  });
}

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), "harness-mb-"));
  genCerts(dir);
});
afterEach(() => {
  rmSync(dir, { recursive: true, force: true });
});

describe("assertAssignedModel (ARCH-004 fail-closed)", () => {
  it("passes when the requested model matches the assignment", () => {
    expect(() => assertAssignedModel("m1", "m1")).not.toThrow();
  });
  it("throws ModelBridgeError on mismatch", () => {
    expect(() => assertAssignedModel("m1", "m2")).toThrow(ModelBridgeError);
  });
});

describe("streamModel (mTLS SSE bridge)", () => {
  it("forwards SSE data payloads and ends ok", async () => {
    const sse = [
      `data: ${JSON.stringify({ choices: [{ delta: { content: "Hello" } }] })}\n\n`,
      `data: ${JSON.stringify({ choices: [{ delta: {}, finish_reason: "stop" }], usage: { prompt_tokens: 1, completion_tokens: 1 } })}\n\n`,
      `data: [DONE]\n\n`,
    ];
    const srv = await startServer(sse, (h) => {
      expect(h["x-iterabase-run-id"]).toBe("run-1");
      expect(h["x-iterabase-turn-id"]).toBe("turn-1");
    });
    try {
      const chunks: string[] = [];
      let endStatus: string | undefined;
      await streamModel(
        { cfg: cfg(srv.url) },
        { body: { model: "m1", messages: [] } },
        "m1",
        { runId: "run-1", turnId: "turn-1", fencingGeneration: 1n },
        undefined,
        { onChunk: (d) => { chunks.push(d); return true; }, onEnd: (status) => (endStatus = status), onDrain: () => () => {} },
      );
      expect(chunks.some((d) => d.includes("Hello"))).toBe(true);
      expect(chunks.some((d) => d === "[DONE]")).toBe(true);
      expect(endStatus).toBe("ok");
    } finally {
      await srv.close();
    }
  }, 10_000);

  it("fails closed when the requested model != the assigned model", async () => {
    const srv = await startServer([]);
    try {
      let endStatus: string | undefined;
      let errMsg: string | undefined;
      await streamModel(
        { cfg: cfg(srv.url) },
        { body: { model: "other", messages: [] } },
        "m1",
        { runId: "run-1", turnId: "turn-1", fencingGeneration: 1n },
        undefined,
        { onChunk: () => true, onEnd: (s, _h, m) => { endStatus = s; errMsg = m; }, onDrain: () => () => {} },
      );
      expect(endStatus).toBe("error");
      expect(errMsg).toContain("model mismatch");
    } finally {
      await srv.close();
    }
  }, 10_000);

  it("propagates cancellation (abort) and emits an aborted terminal", async () => {
    // A server that never ends the stream, so only abort terminates.
    const ca = readFileSync(caPath);
    const cert = readFileSync(srvCertPath);
    const key = readFileSync(srvKeyPath);
    const server = http2.createSecureServer({ ca, cert, key, requestCert: true, rejectUnauthorized: true });
    server.on("stream", (stream) => {
      stream.respond({ "content-type": "text/event-stream", ":status": 200 });
      // Hold the stream open; the client will abort.
    });
    await new Promise<void>((r) => server.listen(0, "127.0.0.1", () => r()));
    const addr = server.address() as { port: number };
    const ac = new AbortController();
    const endStatuses: string[] = [];
    const done = streamModel(
      { cfg: cfg(`https://localhost:${addr.port}`) },
      { body: { model: "m1", messages: [] } },
      "m1",
      { runId: "run-1", turnId: "turn-1", fencingGeneration: 1n },
      ac.signal,
      { onChunk: () => true, onEnd: (s) => endStatuses.push(s), onDrain: () => () => {} },
    );
    setTimeout(() => ac.abort(), 150);
    await done;
    expect(endStatuses).toContain("aborted");
    await new Promise<void>((r) => server.close(() => r()));
  }, 10_000);

  it("applies backpressure: pauses on a full fd-5 write and resumes on drain without losing chunks", async () => {
    const sse = [
      `data: ${JSON.stringify({ choices: [{ delta: { content: "a" } }] })}\n\n`,
      `data: ${JSON.stringify({ choices: [{ delta: { content: "b" } }] })}\n\n`,
      `data: ${JSON.stringify({ choices: [{ delta: {}, finish_reason: "stop" }] })}\n\n`,
      `data: [DONE]\n\n`,
    ];
    const srv = await startServer(sse);
    try {
      const chunks: string[] = [];
      let drainCb: (() => void) | null = null;
      let writes = 0;
      let backpressured: () => void;
      const backpressuredP = new Promise<void>((r) => (backpressured = r));
      // Pretend fd 5 is full on the first write, then drain synchronously.
      const done = streamModel(
        { cfg: cfg(srv.url) },
        { body: { model: "m1", messages: [] } },
        "m1",
        { runId: "run-1", turnId: "turn-1", fencingGeneration: 1n },
        undefined,
        {
          onChunk: (d) => {
            writes += 1;
            chunks.push(d);
            if (writes === 1) {
              // First write did not drain — apply backpressure.
              return false;
            }
            return true;
          },
          onDrain: (listener) => {
            drainCb = listener;
            backpressured();
            return () => {};
          },
          onEnd: () => {},
        },
      );
      // Wait until backpressure is applied, then simulate fd-5 drain.
      await backpressuredP;
      drainCb?.();
      await done;
      expect(chunks.length).toBe(4);
      expect(chunks.some((d) => d.includes("\"a\""))).toBe(true);
      expect(chunks.some((d) => d === "[DONE]")).toBe(true);
    } finally {
      await srv.close();
    }
  }, 10_000);
});

// Suppress the unused-import lint for crypto (kept for future key checks).
void crypto;
