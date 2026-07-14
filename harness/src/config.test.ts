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
persona: |
  You are an email-triage agent.
model:
  id: llama-3.1-70b
  endpoint: https://localhost:8444/v1
  contextWindow: 131072
toolAllowList: "*"
session:
  id: wf-123
egressProxyUrl: https://localhost:8444
`;

describe("loadConfig", () => {
  it("loads a valid config with defaults applied", () => {
    const cfg = loadConfig(writeCfg(VALID));
    expect(cfg.persona.trim()).toBe("You are an email-triage agent.");
    expect(cfg.model.id).toBe("llama-3.1-70b");
    expect(cfg.model.api).toBe("openai-completions"); // default
    expect(cfg.toolAllowList).toBe("*");
    expect(cfg.session.id).toBe("wf-123");
    expect(cfg.session.dir).toBe("/data/sessions"); // default
    expect(cfg.piDirs).toEqual(["/pi/product", "/pi/client"]); // default
    expect(cfg.server.port).toBe(8443); // default
  });

  it("accepts a specific tool allow-list", () => {
    const cfg = loadConfig(writeCfg(VALID.replace('toolAllowList: "*"', 'toolAllowList: ["graph_read", "graph_write"]')));
    expect(cfg.toolAllowList).toEqual(["graph_read", "graph_write"]);
  });

  it("rejects a missing required field", () => {
    expect(() => loadConfig(writeCfg(VALID.replace("egressProxyUrl: https://localhost:8444", "")))).toThrow(ConfigError);
  });

  it("rejects a missing session.id (control-plane-directed; never auto-detected)", () => {
    expect(() => loadConfig(writeCfg(VALID.replace("  id: wf-123", "")))).toThrow(ConfigError);
  });

  it("rejects a malformed toolAllowList", () => {
    expect(() => loadConfig(writeCfg(VALID.replace('toolAllowList: "*"', "toolAllowList: 42")))).toThrow(ConfigError);
  });
});
