import { describe, it, expect } from "vitest";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { loadConfig, ConfigError } from "./config.js";

function writeCfg(yaml: string): string {
  const dir = mkdtempSync(join(tmpdir(), "harness-cfg-"));
  const path = join(dir, "config.yaml");
  writeFileSync(path, yaml);
  return path;
}

const VALID = `
controlPlane:
  url: https://control-plane:8443
  serverName: control-plane
worker:
  workerId: pod-abc
  poolId: pool-xyz
tls:
  cert: /etc/harness/tls/tls.crt
  key: /etc/harness/tls/tls.key
  ca: /etc/harness/tls/ca.crt
sandboxRoot: /data/sandboxes
egressProxyUrl: https://localhost:8444
walDir: /var/harness/wal
`;

describe("loadConfig (infra-only boot config)", () => {
  it("loads a valid infra-only config with defaults applied", () => {
    const cfg = loadConfig(writeCfg(VALID));
    expect(cfg.controlPlane.url).toBe("https://control-plane:8443");
    expect(cfg.controlPlane.serverName).toBe("control-plane");
    expect(cfg.worker.workerId).toBe("pod-abc");
    expect(cfg.worker.poolId).toBe("pool-xyz");
    expect(cfg.tls).toEqual({ cert: "/etc/harness/tls/tls.crt", key: "/etc/harness/tls/tls.key", ca: "/etc/harness/tls/ca.crt" });
    expect(cfg.sandboxRoot).toBe("/data/sandboxes");
    expect(cfg.egressProxyUrl).toBe("https://localhost:8444");
    expect(cfg.walDir).toBe("/var/harness/wal");
    // defaults
    expect(cfg.piDirs).toEqual(["/pi/product", "/pi/client"]);
    expect(cfg.probe.port).toBe(8081);
    expect(cfg.transport.http2PingIntervalMs).toBe(30_000);
    expect(cfg.reconnect.maxBackoffMs).toBe(30_000);
    expect(cfg.child.abortGraceMs).toBe(10_000);
    expect(cfg.outbox.bound).toBe(4_096);
    expect(cfg.modelRetry.maxAttempts).toBe(3);
    expect(cfg.tokenDelta.sendBufferBytes).toBe(1_048_576);
  });

  it("accepts an optional pool scope identity (defense-in-depth)", () => {
    const cfg = loadConfig(writeCfg(`${VALID}poolScopeIdentityId: scope-wf-123\n`));
    expect(cfg.poolScopeIdentityId).toBe("scope-wf-123");
  });

  it("does NOT depend on persona/model/session/toolAllowList (they are per-turn, via AssignTurn)", () => {
    // A config with only infra (no persona/model/session) loads fine — those
    // are no longer boot dependencies.
    const cfg = loadConfig(writeCfg(VALID));
    expect((cfg as unknown as Record<string, unknown>).persona).toBeUndefined();
    expect((cfg as unknown as Record<string, unknown>).model).toBeUndefined();
    expect((cfg as unknown as Record<string, unknown>).session).toBeUndefined();
  });

  it("rejects a missing required field (controlPlane.url)", () => {
    expect(() => loadConfig(writeCfg(VALID.replace("  url: https://control-plane:8443", "")))).toThrow(ConfigError);
  });

  it("rejects a missing worker identity", () => {
    expect(() => loadConfig(writeCfg(VALID.replace("  workerId: pod-abc", "")))).toThrow(ConfigError);
  });

  it("rejects a missing TLS file path", () => {
    expect(() => loadConfig(writeCfg(VALID.replace("  cert: /etc/harness/tls/tls.crt", "")))).toThrow(ConfigError);
  });

  it("rejects a missing sandbox root / egress / wal dir", () => {
    expect(() => loadConfig(writeCfg(VALID.replace("sandboxRoot: /data/sandboxes", "")))).toThrow(ConfigError);
    expect(() => loadConfig(writeCfg(VALID.replace("egressProxyUrl: https://localhost:8444", "")))).toThrow(ConfigError);
    expect(() => loadConfig(writeCfg(VALID.replace("walDir: /var/harness/wal", "")))).toThrow(ConfigError);
  });

  it("rejects non-positive or non-integer numeric tunables", () => {
    expect(() => loadConfig(writeCfg(`${VALID}probe:\n  port: 0\n`))).toThrow(ConfigError);
    expect(() => loadConfig(writeCfg(`${VALID}child:\n  abortGraceMs: -1\n`))).toThrow(ConfigError);
    expect(() => loadConfig(writeCfg(`${VALID}outbox:\n  bound: 1.5\n`))).toThrow(ConfigError);
  });

  it("rejects a malformed piDirs", () => {
    expect(() => loadConfig(writeCfg(`${VALID}piDirs: 42\n`))).toThrow(ConfigError);
  });
});
