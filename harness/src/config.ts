// Harness config (HOR-351). Loaded from /etc/harness/config.yaml at boot
// (ConfigMap-mounted by the AgentSandbox operator, HOR-245).
//
// The harness holds NO real credentials. Caller identity + real gateway/tool
// credentials live in the per-sandbox egress proxy (HOR-244); this config
// carries only the placeholder model endpoint + the resolved tool allow-list.
// See harness/README.md.

import { readFileSync } from "node:fs";
import { parse } from "yaml";

export type ToolAllowList = "*" | string[];

export interface HarnessConfig {
  /** v1: inline persona text (pi systemPromptOverride). Future: Persona CRD (HOR-363). */
  persona: string;
  /** Model config — points at the egress proxy with a placeholder key (proxy injects the real one). */
  model: {
    id: string;
    endpoint: string; // egress proxy model URL, e.g. https://localhost:8444/v1
    api: string; // openai-completions (the gateway is OpenAI-compatible)
    contextWindow: number;
  };
  /** Resolved tool allow-list (HOR-243). "*" = broad-default (v1); narrows via HOR-283. */
  toolAllowList: ToolAllowList;
  /** Session selection — the control-plane owns the workflow/chat -> id mapping (HOR-249). */
  session: {
    id: string; // control-plane-generated; harness scopes by id, resume-or-create
    dir: string; // PVC mount, e.g. /data/sessions
  };
  /** Overlay pi/ tree (extensions + skills). Client overrides product (last-wins). */
  piDirs: string[]; // [/pi/product, /pi/client]
  /** Per-sandbox egress proxy sidecar — routes model + tool traffic. */
  egressProxyUrl: string; // https://localhost:<port>
  /** mTLS (certs provisioned by HOR-245). */
  tls: { cert: string; key: string; ca: string };
  /** 8443 = mTLS Connect RPC; 8081 = plain-HTTP kubelet probes (/readyz, /healthz). */
  server: { port: number; healthPort: number };
}

export class ConfigError extends Error {}

const DEFAULTS: Pick<HarnessConfig, "model" | "piDirs" | "session" | "tls" | "server"> = {
  model: { id: "", endpoint: "", api: "openai-completions", contextWindow: 131072 },
  piDirs: ["/pi/product", "/pi/client"],
  session: { id: "", dir: "/data/sessions" },
  tls: { cert: "/etc/harness/tls/tls.crt", key: "/etc/harness/tls/tls.key", ca: "/etc/harness/tls/ca.crt" },
  server: { port: 8443, healthPort: 8081 },
};

export function loadConfig(
  path: string = process.env.HARNESS_CONFIG ?? "/etc/harness/config.yaml",
): HarnessConfig {
  const raw = parse(readFileSync(path, "utf8")) as Record<string, unknown>;
  if (!raw || typeof raw !== "object") throw new ConfigError(`config at ${path} is empty or invalid`);

  const cfg = {
    persona: str(raw, "persona"),
    model: { ...DEFAULTS.model, ...obj(raw, "model") },
    toolAllowList: allowList(raw, "toolAllowList"),
    session: { ...DEFAULTS.session, ...obj(raw, "session") },
    piDirs: DEFAULTS.piDirs,
    egressProxyUrl: str(raw, "egressProxyUrl"),
    tls: { ...DEFAULTS.tls, ...obj(raw, "tls") },
    server: { ...DEFAULTS.server, ...obj(raw, "server") },
  } as HarnessConfig;

  requireValue(cfg.persona, "persona");
  requireValue(cfg.model.id, "model.id");
  requireValue(cfg.model.endpoint, "model.endpoint");
  requireValue(cfg.egressProxyUrl, "egressProxyUrl");
  requireValue(cfg.session.id, "session.id"); // control-plane-directed; never auto-detected
  return cfg;
}

function obj(raw: Record<string, unknown>, key: string): Record<string, unknown> {
  const v = raw[key];
  if (v === undefined) return {};
  if (typeof v !== "object" || v === null || Array.isArray(v))
    throw new ConfigError(`config '${key}' must be an object`);
  return v as Record<string, unknown>;
}
function str(raw: Record<string, unknown>, key: string): string {
  const v = raw[key];
  if (v !== undefined && typeof v !== "string") throw new ConfigError(`config '${key}' must be a string`);
  return (v as string) ?? "";
}
function allowList(raw: Record<string, unknown>, key: string): ToolAllowList {
  const v = raw[key];
  if (v === undefined || v === "*") return "*";
  if (Array.isArray(v) && v.every((x) => typeof x === "string")) return v as string[];
  throw new ConfigError(`config '${key}' must be "*" or a string[]`);
}
function requireValue(v: unknown, name: string): void {
  if (v === "" || v === undefined || v === null) throw new ConfigError(`config '${name}' is required`);
}
