// Harness boot config (HOR-381; HOR-395 gateway bridges). Infra-only — NO
// session/persona/model/tools at boot (those are per-turn, delivered via the
// Work AssignTurn RPC). Loaded from /etc/harness/config.yaml (ConfigMap-mounted
// by the AgentSandbox operator, HOR-245). The harness holds NO customer
// credentials; the supervisor authenticates to the tool + inference gateways
// with its SPIFFE-bound worker mTLS cert (ARCH-010). The disposable child
// receives neither endpoint nor credential — it only talks fd 4/fd 5.
//
// Cert material is re-read on each connection attempt so an idle reconnect can
// consume rotated files. Startup fails on missing identity/TLS/endpoint/sandbox
// root or unsafe numeric ranges. See harness/README.md.

import { readFileSync } from "node:fs";
import { parse } from "yaml";

export interface HarnessConfig {
  /** Control-plane gRPC server (the worker is the mTLS client). */
  controlPlane: {
    url: string; // e.g. https://control-plane:8443
    serverName: string; // expected server cert SAN
  };
  /** Worker identity (cert-SAN-bound; verified against Hello in HOR-249). */
  worker: {
    workerId: string; // Kubernetes Pod name (stable warm-worker slot, verified cert SAN)
    poolId: string; // owning pool CR UID
  };
  /** Optional pool scope identity (defense-in-depth: validate AssignTurn's scope_identity_id). */
  poolScopeIdentityId?: string;
  /** Interim residual (HOR-395, pending HOR-245): the deploy-time AgentPool
   * maximum for the fixed workspace-tool set. The supervisor intersects this
   * with AssignTurn.workspace_tools — a turn cannot widen pool permissions
   * (ARCH-016/DEC-036/DEC-038). Deny-by-default: unset/false exposes no local
   * tools regardless of the assignment. The authoritative AgentPool-owned
   * maximum is tracked as a HOR-245 acceptance criterion. */
  poolWorkspaceTools?: boolean;
  /** mTLS (certs provisioned by HOR-245; re-read each reconnect for rotation). */
  tls: { cert: string; key: string; ca: string };
  /** Sandbox mount root (the shared RWX PVC; per-sandbox-id subdirs). */
  sandboxRoot: string; // e.g. /data/sandboxes
  /** Read-only extension/package paths (pool-bound; the overlay pi/ tree). */
  piDirs: string[]; // [/pi/product, /pi/client]
  /** Tool gateway gRPC endpoint (GatewayService: discover/invoke/cancel). ARCH-010. */
  toolGateway: { url: string; serverName: string };
  /** Inference gateway HTTP/2 mTLS endpoint (/v1/chat/completions workload listener, HOR-398). ARCH-010. */
  inferenceGateway: { url: string; serverName: string };
  /** WAL spool dir (emptyDir; durable audit events; supervisor-UID-owned, child-inaccessible). */
  walDir: string; // e.g. /var/harness/wal
  /** Plain-HTTP kubelet probes (/healthz + /readyz). */
  probe: { port: number };
  /** HTTP/2 transport (the long-lived Work stream; no RPC deadline). */
  transport: {
    http2PingIntervalMs: number;
    http2PingTimeoutMs: number;
  };
  /** Reconnect bounds (bounded exponential backoff + full jitter; reset after a stable Welcome). */
  reconnect: {
    initialBackoffMs: number;
    maxBackoffMs: number;
    resetAfterMs: number;
  };
  /** Child lifecycle (IPC heartbeat + abort/shutdown escalation). */
  child: {
    livenessIntervalMs: number; // IPC heartbeat from the child (stale -> terminate)
    abortGraceMs: number; // graceful abort -> SIGTERM -> SIGKILL
  };
  /** Bounded in-memory outbox (+ WAL). Overflow fails the assignment, never drops audit silently. */
  outbox: { bound: number };
  /** pi model-retry defaults (provider-SDK maxRetries=0; one bounded pi-owned retry layer). */
  modelRetry: { maxAttempts: number };
  /** Token-delta send buffer (ephemeral; best-effort, drop-oldest on overflow). */
  tokenDelta: { sendBufferBytes: number };
}

export class ConfigError extends Error {
  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "ConfigError";
  }
}

const DEFAULTS: Omit<HarnessConfig, "controlPlane" | "worker" | "tls" | "sandboxRoot" | "toolGateway" | "inferenceGateway" | "walDir"> = {
  piDirs: ["/pi/product", "/pi/client"],
  probe: { port: 8081 },
  transport: { http2PingIntervalMs: 30_000, http2PingTimeoutMs: 10_000 },
  reconnect: { initialBackoffMs: 500, maxBackoffMs: 30_000, resetAfterMs: 60_000 },
  child: { livenessIntervalMs: 5_000, abortGraceMs: 10_000 },
  outbox: { bound: 4_096 },
  modelRetry: { maxAttempts: 3 },
  tokenDelta: { sendBufferBytes: 1_048_576 },
};

export function loadConfig(
  path: string = process.env.HARNESS_CONFIG ?? "/etc/harness/config.yaml",
): HarnessConfig {
  const raw = parse(readFileSync(path, "utf8")) as Record<string, unknown>;
  if (!raw || typeof raw !== "object") throw new ConfigError(`config at ${path} is empty or invalid`);

  const controlPlane = {
    url: str(raw, "controlPlane.url", "controlPlane.url"),
    serverName: str(raw, "controlPlane.serverName", "controlPlane.serverName"),
  };
  const worker = {
    workerId: str(raw, "worker.workerId", "worker.workerId"),
    poolId: str(raw, "worker.poolId", "worker.poolId"),
  };
  const poolScopeIdentityId = optStr(raw, "poolScopeIdentityId");
  const poolWorkspaceTools = optBool(raw, "poolWorkspaceTools");
  const tls = {
    cert: str(raw, "tls.cert", "tls.cert"),
    key: str(raw, "tls.key", "tls.key"),
    ca: str(raw, "tls.ca", "tls.ca"),
  };
  const sandboxRoot = str(raw, "sandboxRoot", "sandboxRoot");
  const toolGateway = {
    url: str(raw, "toolGateway.url", "toolGateway.url"),
    serverName: str(raw, "toolGateway.serverName", "toolGateway.serverName"),
  };
  const inferenceGateway = {
    url: str(raw, "inferenceGateway.url", "inferenceGateway.url"),
    serverName: str(raw, "inferenceGateway.serverName", "inferenceGateway.serverName"),
  };
  const walDir = str(raw, "walDir", "walDir");

  const cfg: HarnessConfig = {
    controlPlane,
    worker,
    ...(poolScopeIdentityId !== undefined ? { poolScopeIdentityId } : {}),
    ...(poolWorkspaceTools !== undefined ? { poolWorkspaceTools } : {}),
    tls,
    sandboxRoot,
    piDirs: arr(raw, "piDirs") ?? DEFAULTS.piDirs,
    toolGateway,
    inferenceGateway,
    walDir,
    probe: { ...DEFAULTS.probe, ...obj(raw, "probe") },
    transport: { ...DEFAULTS.transport, ...numObj(raw, "transport") },
    reconnect: { ...DEFAULTS.reconnect, ...numObj(raw, "reconnect") },
    child: { ...DEFAULTS.child, ...numObj(raw, "child") },
    outbox: { ...DEFAULTS.outbox, ...numObj(raw, "outbox") },
    modelRetry: { ...DEFAULTS.modelRetry, ...numObj(raw, "modelRetry") },
    tokenDelta: { ...DEFAULTS.tokenDelta, ...numObj(raw, "tokenDelta") },
  };

  // Required fields.
  requireValue(cfg.controlPlane.url, "controlPlane.url");
  requireValue(cfg.controlPlane.serverName, "controlPlane.serverName");
  requireValue(cfg.worker.workerId, "worker.workerId");
  requireValue(cfg.worker.poolId, "worker.poolId");
  requireValue(cfg.tls.cert, "tls.cert");
  requireValue(cfg.tls.key, "tls.key");
  requireValue(cfg.tls.ca, "tls.ca");
  requireValue(cfg.sandboxRoot, "sandboxRoot");
  requireValue(cfg.toolGateway.url, "toolGateway.url");
  requireValue(cfg.toolGateway.serverName, "toolGateway.serverName");
  requireValue(cfg.inferenceGateway.url, "inferenceGateway.url");
  requireValue(cfg.inferenceGateway.serverName, "inferenceGateway.serverName");
  requireValue(cfg.walDir, "walDir");

  // Numeric ranges (reject unsafe/unset-within-objects).
  requirePositive(cfg.probe.port, "probe.port");
  requirePositive(cfg.transport.http2PingIntervalMs, "transport.http2PingIntervalMs");
  requirePositive(cfg.transport.http2PingTimeoutMs, "transport.http2PingTimeoutMs");
  requirePositive(cfg.reconnect.initialBackoffMs, "reconnect.initialBackoffMs");
  requirePositive(cfg.reconnect.maxBackoffMs, "reconnect.maxBackoffMs");
  requirePositive(cfg.reconnect.resetAfterMs, "reconnect.resetAfterMs");
  requirePositive(cfg.child.livenessIntervalMs, "child.livenessIntervalMs");
  requirePositive(cfg.child.abortGraceMs, "child.abortGraceMs");
  requirePositive(cfg.outbox.bound, "outbox.bound");
  requirePositive(cfg.modelRetry.maxAttempts, "modelRetry.maxAttempts");
  requirePositive(cfg.tokenDelta.sendBufferBytes, "tokenDelta.sendBufferBytes");

  return cfg;
}

// --- helpers ---

function dig(raw: Record<string, unknown>, dotted: string): unknown {
  return dotted.split(".").reduce<unknown>((acc, k) => {
    if (acc && typeof acc === "object" && !Array.isArray(acc)) {
      return (acc as Record<string, unknown>)[k];
    }
    return undefined;
  }, raw);
}

function str(raw: Record<string, unknown>, dotted: string, name: string): string {
  const v = dig(raw, dotted);
  if (v === undefined || v === null) return "";
  if (typeof v !== "string") throw new ConfigError(`config '${name}' must be a string`);
  return v;
}
function optStr(raw: Record<string, unknown>, dotted: string): string | undefined {
  const v = dig(raw, dotted);
  if (v === undefined || v === null || v === "") return undefined;
  if (typeof v !== "string") throw new ConfigError(`config '${dotted}' must be a string`);
  return v;
}
function optBool(raw: Record<string, unknown>, dotted: string): boolean | undefined {
  const v = dig(raw, dotted);
  if (v === undefined || v === null) return undefined;
  if (typeof v !== "boolean") throw new ConfigError(`config '${dotted}' must be a boolean`);
  return v;
}
function arr(raw: Record<string, unknown>, dotted: string): string[] | undefined {
  const v = dig(raw, dotted);
  if (v === undefined || v === null) return undefined;
  if (!Array.isArray(v) || !v.every((x) => typeof x === "string"))
    throw new ConfigError(`config '${dotted}' must be a string[]`);
  return v as string[];
}
function obj(raw: Record<string, unknown>, dotted: string): Record<string, unknown> {
  const v = dig(raw, dotted);
  if (v === undefined) return {};
  if (typeof v !== "object" || v === null || Array.isArray(v))
    throw new ConfigError(`config '${dotted}' must be an object`);
  return v as Record<string, unknown>;
}
function numObj(raw: Record<string, unknown>, dotted: string): Record<string, number> {
  const o = obj(raw, dotted);
  for (const [k, v] of Object.entries(o)) {
    if (typeof v !== "number" || !Number.isFinite(v))
      throw new ConfigError(`config '${dotted}.${k}' must be a finite number`);
  }
  return o as Record<string, number>;
}
function requireValue(v: unknown, name: string): void {
  if (v === "" || v === undefined || v === null) throw new ConfigError(`config '${name}' is required`);
}
function requirePositive(v: number, name: string): void {
  if (!Number.isInteger(v) || v <= 0) throw new ConfigError(`config '${name}' must be a positive integer`);
}
